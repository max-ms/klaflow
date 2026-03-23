"""Flink Job 2: Account Aggregation.

Consumes customer events from Kafka (Avro via Schema Registry) and computes
per-merchant (account-level) metrics. Operationally isolated from Job 1
(customer aggregation) so that a spike from a single large merchant cannot
degrade customer-level processing.

Tracks:
- events_per_hour: total events received per account
- sends_today: email send events (email_opened as proxy)
- active_customers_7d: distinct customers with activity
- revenue_7d: total purchase amount

Output: account_metrics table in ClickHouse.

Avro deserialization: Python-side via fastavro (same approach as Job 1).
"""

import io
import json
import os
import time
from datetime import datetime

import fastavro
from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")
FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "10000"))

# Avro schema embedded (matches customer_event.avsc)
AVRO_SCHEMA = {
    "type": "record",
    "name": "CustomerEvent",
    "namespace": "com.klaflow.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "account_id", "type": "string"},
        {"name": "event_type", "type": {
            "type": "enum", "name": "EventType",
            "symbols": ["email_opened", "link_clicked", "purchase_made",
                        "page_viewed", "cart_abandoned"]
        }},
        {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "properties", "type": {
            "type": "record", "name": "EventProperties",
            "fields": [
                {"name": "campaign_id", "type": ["null", "string"], "default": None},
                {"name": "product_id", "type": ["null", "string"], "default": None},
                {"name": "product_category", "type": ["null", "string"], "default": None},
                {"name": "amount", "type": ["null", "double"], "default": None},
                {"name": "email_subject", "type": ["null", "string"], "default": None},
            ]
        }}
    ]
}
PARSED_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA)


def decode_confluent_avro(raw_bytes):
    """Decode Confluent wire format: 1 magic byte + 4 byte schema ID + Avro payload."""
    if len(raw_bytes) < 6:
        return None
    avro_payload = raw_bytes[5:]
    return fastavro.schemaless_reader(io.BytesIO(avro_payload), PARSED_SCHEMA)


def write_account_metrics_to_clickhouse(account_id, metrics):
    """Write account metrics to ClickHouse account_metrics table."""
    import requests as req

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
        response = req.post(url, params=params, data=query, timeout=10)
        if response.status_code != 200:
            print(f"ClickHouse write error: {response.status_code} {response.text[:200]}")
        else:
            print(f"Wrote {len(metrics)} account metrics for {account_id}")
    except Exception as e:
        print(f"ClickHouse connection error: {e}")


class AvroDeserializeMap(MapFunction):
    """Deserializes raw Kafka bytes (Confluent wire format) into a structured Row.

    Output Row: (customer_id, account_id, event_type, timestamp, amount)
    """

    def map(self, value):
        raw_bytes = value[0]
        if raw_bytes is None:
            return None

        event = decode_confluent_avro(raw_bytes)
        if event is None:
            return None

        props = event.get("properties", {}) or {}
        # fastavro converts timestamp-millis to datetime; convert back to epoch ms
        ts = event["timestamp"]
        if hasattr(ts, 'timestamp'):
            ts = int(ts.timestamp() * 1000)
        else:
            ts = int(ts)
        return Row(
            event["customer_id"],
            event["account_id"],
            event["event_type"],
            ts,
            float(props["amount"]) if props.get("amount") is not None else 0.0,
        )


class AccountAggregationFunction(KeyedProcessFunction):
    """Process function that computes per-account metrics.

    Keyed by account_id. Maintains state for each account and periodically
    flushes metrics to ClickHouse.
    """

    def __init__(self, flush_interval_ms):
        self._state = None
        self._last_flush_state = None
        self._flush_interval_ms = flush_interval_ms

    def open(self, runtime_context):
        self._state = runtime_context.get_state(
            ValueStateDescriptor("account_state", Types.STRING())
        )
        self._last_flush_state = runtime_context.get_state(
            ValueStateDescriptor("last_flush", Types.LONG())
        )

    def process_element(self, value, ctx):
        # value is Row: (customer_id, account_id, event_type, timestamp, amount)
        customer_id = value[0]
        account_id = value[1]
        event_type = value[2]
        event_ts = value[3]
        amount = value[4] if value[4] != 0.0 else None

        # Load existing state from RocksDB
        state_json = self._state.value()
        if state_json:
            current_state = json.loads(state_json)
        else:
            current_state = {
                "events_per_hour": 0,
                "sends_today": 0,
                "active_customers_7d": [],
                "revenue_7d": 0.0,
            }

        # Update metrics
        current_state["events_per_hour"] += 1

        if event_type == "email_opened":
            current_state["sends_today"] += 1

        if customer_id not in current_state["active_customers_7d"]:
            current_state["active_customers_7d"].append(customer_id)

        if event_type == "purchase_made" and amount is not None:
            current_state["revenue_7d"] += float(amount)

        # Save state to RocksDB
        self._state.update(json.dumps(current_state))

        # Check flush interval
        last_flush = self._last_flush_state.value()
        if last_flush is None:
            last_flush = 0

        now_ms = int(time.time() * 1000)
        if now_ms - last_flush >= self._flush_interval_ms:
            metrics = [
                ("events_per_hour", float(current_state["events_per_hour"])),
                ("sends_today", float(current_state["sends_today"])),
                ("active_customers_7d", float(len(current_state["active_customers_7d"]))),
                ("revenue_7d", current_state["revenue_7d"]),
            ]
            write_account_metrics_to_clickhouse(account_id, metrics)
            self._last_flush_state.update(now_ms)

        # Yield summary row
        yield Row(account_id, 4, event_ts)


# Row type for deserialized events (only fields needed for account aggregation)
EVENT_ROW_TYPE = Types.ROW([
    Types.STRING(),   # customer_id
    Types.STRING(),   # account_id
    Types.STRING(),   # event_type
    Types.LONG(),     # timestamp
    Types.DOUBLE(),   # amount (nullable)
])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    # RocksDB state backend (isolated from Job 1)
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    state_backend = EmbeddedRocksDBStateBackend()
    env.set_state_backend(state_backend)

    # Checkpointing
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/account-agg")
    env.get_checkpoint_config().set_checkpoint_storage_dir(checkpoint_dir)
    env.enable_checkpointing(int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000")))

    # Table environment — used only for Kafka source with raw format
    t_env = StreamTableEnvironment.create(env)

    # Read raw bytes from Kafka — avoids Confluent Schema Registry Java deps
    t_env.execute_sql(f"""
        CREATE TABLE kafka_raw (
            `value` BYTES
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'klaflow-account-aggregation',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)

    # Convert to DataStream of raw bytes
    raw_table = t_env.from_path("kafka_raw")
    raw_ds = t_env.to_data_stream(raw_table)

    # Deserialize Avro in Python, then key by account_id
    event_ds = (
        raw_ds
        .map(AvroDeserializeMap(), output_type=EVENT_ROW_TYPE)
        .filter(lambda row: row is not None)
    )

    # Key by account_id (field index 1) — NOT customer_id, this is the isolation boundary
    (
        event_ds
        .key_by(lambda row: row[1])
        .process(
            AccountAggregationFunction(FLUSH_INTERVAL_MS),
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
    print("Avro deserialization: Python-side (fastavro)")
    env.execute("klaflow-account-aggregation")


if __name__ == "__main__":
    main()
