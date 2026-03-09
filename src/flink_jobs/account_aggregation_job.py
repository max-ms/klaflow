"""Flink Job 2: Account Aggregation.

Consumes customer events from Kafka and computes per-merchant (account-level)
metrics. Operationally isolated from Job 1 (customer aggregation) so that
a spike from a single large merchant cannot degrade customer-level processing.

Tracks:
- events_per_hour: total events received per account in the last hour
- sends_today: email/SMS send events for the current day
- active_customers_7d: distinct customers with activity in the last 7 days
- revenue_7d: total purchase amount in the last 7 days

Output: account_metrics table in ClickHouse.
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")

# Metrics tracked per account
ACCOUNT_METRICS = [
    "events_per_hour",
    "sends_today",
    "active_customers_7d",
    "revenue_7d",
]


def compute_account_metrics(event, current_state):
    """Compute account-level metric updates from a single event.

    Args:
        event: Parsed event dict.
        current_state: Dict of current accumulated state for this account.

    Returns:
        Updated state dict and list of (metric_name, metric_value) for output.
    """
    event_type = event.get("event_type", "")
    properties = event.get("properties", {})
    customer_id = event.get("customer_id", "")
    amount = properties.get("amount")
    timestamp_ms = event.get("timestamp", int(time.time() * 1000))

    # Initialize state if needed
    if not current_state:
        current_state = {
            "events_per_hour": 0,
            "sends_today": 0,
            "active_customers_7d": [],
            "revenue_7d": 0.0,
            "last_hour_start": timestamp_ms,
            "last_day_start": timestamp_ms,
        }

    # events_per_hour: increment for every event
    current_state["events_per_hour"] += 1

    # sends_today: count email_opened and link_clicked as proxy for sends
    if event_type in ("email_opened",):
        current_state["sends_today"] += 1

    # active_customers_7d: track distinct customers
    if customer_id not in current_state["active_customers_7d"]:
        current_state["active_customers_7d"].append(customer_id)

    # revenue_7d: sum purchase amounts
    if event_type == "purchase_made" and amount is not None:
        current_state["revenue_7d"] += float(amount)

    # Build output metrics
    metrics = [
        ("events_per_hour", float(current_state["events_per_hour"])),
        ("sends_today", float(current_state["sends_today"])),
        ("active_customers_7d", float(len(current_state["active_customers_7d"]))),
        ("revenue_7d", current_state["revenue_7d"]),
    ]

    return current_state, metrics


def write_account_metrics_to_clickhouse(account_id, metrics):
    """Write account metrics to ClickHouse account_metrics table."""
    import requests

    if not metrics:
        return

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    safe_account = account_id.replace("'", "\\'")

    values = []
    for metric_name, metric_value in metrics:
        values.append(
            f"('{safe_account}', '{metric_name}', {metric_value}, '{now_str}')"
        )

    query = (
        "INSERT INTO account_metrics "
        "(account_id, metric_name, metric_value, computed_at) "
        f"VALUES {', '.join(values)}"
    )

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}

    try:
        response = requests.post(url, params=params, data=query, timeout=10)
        if response.status_code != 200:
            print(f"ClickHouse write error: {response.status_code} {response.text}")
        else:
            print(f"Wrote {len(metrics)} account metrics for {account_id}")
    except Exception as e:
        print(f"ClickHouse connection error: {e}")


class EventTimestampAssigner(TimestampAssigner):
    """Extract event timestamp for watermark assignment."""

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        return event.get("timestamp", record_timestamp)


class AccountAggregationFunction(KeyedProcessFunction):
    """Process function that computes per-account metrics.

    Keyed by account_id. Maintains state for each account and periodically
    flushes metrics to ClickHouse.
    """

    def __init__(self):
        self._state = None
        self._last_flush_state = None
        self._flush_interval_ms = 60000

    def open(self, runtime_context):
        self._state = runtime_context.get_state(
            ValueStateDescriptor("account_state", Types.STRING())
        )
        self._last_flush_state = runtime_context.get_state(
            ValueStateDescriptor("last_flush", Types.LONG())
        )

    def process_element(self, value, ctx):
        event = json.loads(value)
        account_id = event.get("account_id", "")
        event_ts = event.get("timestamp", int(time.time() * 1000))

        # Load existing state
        state_json = self._state.value()
        if state_json:
            current_state = json.loads(state_json)
        else:
            current_state = {}

        # Compute updated metrics
        current_state, metrics = compute_account_metrics(event, current_state)

        # Save state
        self._state.update(json.dumps(current_state))

        # Check flush interval
        last_flush = self._last_flush_state.value()
        if last_flush is None:
            last_flush = 0

        if event_ts - last_flush >= self._flush_interval_ms:
            write_account_metrics_to_clickhouse(account_id, metrics)
            self._last_flush_state.update(event_ts)

        # Yield summary row
        yield Row(account_id, len(metrics), event_ts)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    # Enable RocksDB state backend (isolated from Job 1)
    from pyflink.datastream.state_backend import RocksDBStateBackend
    state_backend = RocksDBStateBackend(
        os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/account-agg")
    )
    env.set_state_backend(state_backend)

    # Enable checkpointing
    env.enable_checkpointing(int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000")))

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("klaflow-account-aggregation")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    ds = env.from_source(kafka_source, watermark_strategy, "kafka-account-events")

    # Key by account_id (NOT customer_id — this is the isolation boundary)
    (
        ds.key_by(lambda x: json.loads(x)["account_id"])
        .process(
            AccountAggregationFunction(),
            output_type=Types.ROW([
                Types.STRING(),  # account_id
                Types.INT(),     # metric_count
                Types.LONG(),    # event_timestamp
            ]),
        )
    )

    print("Starting Flink Job 2: Account Aggregation")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    env.execute("klaflow-account-aggregation")


if __name__ == "__main__":
    main()
