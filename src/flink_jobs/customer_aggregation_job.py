"""Flink Job 1: Customer Aggregation.

Consumes customer events from Kafka, fans out each event into multiple
dimension counters (event_type x timeframe x campaign_id x product_category),
and writes aggregated counters to the ClickHouse customer_counters table.

Key architectural properties:
- Keyed by customer_id (not account_id)
- 1 event -> N counter updates (high fan-out)
- Time windows: 1h, 24h, 7d, 30d rolling counts
- State backend: RocksDB (embedded in Flink)
- Output: customer_counters table via ClickHouse HTTP interface
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

# Time windows in seconds for rolling counts
TIME_WINDOWS = {
    "1h": 3600,
    "3d": 259200,
    "7d": 604800,
    "24h": 86400,
    "30d": 2592000,
}

EVENT_TYPES = ["email_opened", "link_clicked", "purchase_made", "page_viewed", "cart_abandoned"]


def compute_fan_out_counters(event, now_ms=None):
    """Given a single event, compute counter updates for applicable time windows.

    Only emits a counter for a window if the event timestamp falls within that
    window relative to now_ms. An event from 10 days ago will increment the 30d
    counter but NOT the 7d counter.

    Returns a list of (metric_name, metric_value) tuples.
    """
    event_type = event.get("event_type", "")
    event_ts = event.get("timestamp", 0)
    properties = event.get("properties", {})
    campaign_id = properties.get("campaign_id")
    product_category = properties.get("product_category")
    amount = properties.get("amount")

    if now_ms is None:
        now_ms = int(time.time() * 1000)

    event_age_seconds = (now_ms - event_ts) / 1000.0

    counters = []

    # Only emit counters for windows the event falls within
    applicable_windows = [
        label for label, seconds in TIME_WINDOWS.items()
        if event_age_seconds <= seconds
    ]

    # Base counters: event_type x applicable timeframes
    for window_label in applicable_windows:
        counters.append((f"{event_type}_{window_label}", 1.0))

    # Campaign-specific counters
    if campaign_id:
        for window_label in applicable_windows:
            counters.append((f"{event_type}_campaign_{campaign_id}_{window_label}", 1.0))

    # Product category counters
    if product_category:
        for window_label in applicable_windows:
            counters.append((f"{event_type}_category_{product_category}_{window_label}", 1.0))

    # Purchase amount accumulators
    if event_type == "purchase_made" and amount is not None:
        for window_label in applicable_windows:
            counters.append((f"purchase_amount_{window_label}", float(amount)))

    # Total events counter
    for window_label in applicable_windows:
        counters.append((f"total_events_{window_label}", 1.0))

    return counters


def write_counters_to_clickhouse(account_id, customer_id, counters):
    """Write aggregated counters to ClickHouse customer_counters table."""
    import requests

    if not counters:
        return

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    values = []
    for metric_name, metric_value in counters:
        # Escape single quotes in metric names
        safe_metric = metric_name.replace("'", "\\'")
        safe_account = account_id.replace("'", "\\'")
        safe_customer = customer_id.replace("'", "\\'")
        values.append(
            f"('{safe_account}', '{safe_customer}', '{safe_metric}', "
            f"{metric_value}, '{now_str}')"
        )

    # Batch insert
    for i in range(0, len(values), 500):
        batch = values[i:i + 500]
        query = (
            "INSERT INTO customer_counters "
            "(account_id, customer_id, metric_name, metric_value, computed_at) "
            f"VALUES {', '.join(batch)}"
        )

        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}

        try:
            response = requests.post(url, params=params, data=query, timeout=10)
            if response.status_code != 200:
                print(f"ClickHouse write error: {response.status_code} {response.text}")
            else:
                print(f"Wrote {len(batch)} counter records for customer {customer_id}")
        except Exception as e:
            print(f"ClickHouse connection error: {e}")


class EventTimestampAssigner(TimestampAssigner):
    """Extract event timestamp for watermark assignment."""

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        return event.get("timestamp", record_timestamp)


class CustomerAggregationFunction(KeyedProcessFunction):
    """Process function that fans out each event into multiple counter updates.

    Uses RocksDB-backed state to accumulate counters per customer across
    multiple dimensions and time windows. Periodically flushes accumulated
    counters to ClickHouse.
    """

    def __init__(self):
        self._counter_state = None
        self._last_flush_state = None
        self._flush_interval_ms = 60000  # flush every 60 seconds

    def open(self, runtime_context):
        self._counter_state = runtime_context.get_state(
            ValueStateDescriptor("counters", Types.STRING())
        )
        self._last_flush_state = runtime_context.get_state(
            ValueStateDescriptor("last_flush", Types.LONG())
        )

    def process_element(self, value, ctx):
        event = json.loads(value)
        customer_id = event.get("customer_id", "")
        account_id = event.get("account_id", "")
        event_ts = event.get("timestamp", int(time.time() * 1000))

        # Compute fan-out counters for this event
        new_counters = compute_fan_out_counters(event)

        # Load existing accumulated counters from state
        state_json = self._counter_state.value()
        if state_json:
            accumulated = json.loads(state_json)
        else:
            accumulated = {"account_id": account_id, "counters": {}}

        # Merge new counters into accumulated state
        for metric_name, metric_value in new_counters:
            if metric_name in accumulated["counters"]:
                accumulated["counters"][metric_name] += metric_value
            else:
                accumulated["counters"][metric_name] = metric_value

        # Save updated state
        self._counter_state.update(json.dumps(accumulated))

        # Check if we should flush to ClickHouse
        last_flush = self._last_flush_state.value()
        if last_flush is None:
            last_flush = 0

        if event_ts - last_flush >= self._flush_interval_ms:
            # Flush accumulated counters to ClickHouse
            counter_list = [
                (name, val) for name, val in accumulated["counters"].items()
            ]
            write_counters_to_clickhouse(account_id, customer_id, counter_list)
            self._last_flush_state.update(event_ts)

        # Yield a summary row for downstream operators
        fan_out_count = len(new_counters)
        yield Row(customer_id, account_id, fan_out_count, event_ts)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    # Enable RocksDB state backend
    from pyflink.datastream.state_backend import RocksDBStateBackend
    state_backend = RocksDBStateBackend(
        os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/customer-agg")
    )
    env.set_state_backend(state_backend)

    # Enable checkpointing
    env.enable_checkpointing(int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000")))

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("klaflow-customer-aggregation")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    ds = env.from_source(kafka_source, watermark_strategy, "kafka-customer-events")

    # Key by customer_id, process with fan-out aggregation
    (
        ds.key_by(lambda x: json.loads(x)["customer_id"])
        .process(
            CustomerAggregationFunction(),
            output_type=Types.ROW([
                Types.STRING(),  # customer_id
                Types.STRING(),  # account_id
                Types.INT(),     # fan_out_count
                Types.LONG(),    # event_timestamp
            ]),
        )
    )

    print("Starting Flink Job 1: Customer Aggregation")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    env.execute("klaflow-customer-aggregation")


if __name__ == "__main__":
    main()
