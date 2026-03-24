"""Flink Job 1: Customer Aggregation.

Consumes customer events from Kafka (Avro via Schema Registry), fans out each
event into multiple dimension counters (event_type x timeframe x campaign_id x
product_category), and writes aggregated counters to ClickHouse customer_counters.

Key architectural properties:
- Keyed by customer_id (not account_id)
- 1 event -> N counter updates (high fan-out)
- Time windows: 1h, 24h, 7d, 30d rolling counts
- State backend: RocksDB (embedded in Flink)
- Output: customer_counters table via ClickHouse HTTP interface

Avro deserialization approach:
- Kafka source uses 'raw' format (reads bytes directly)
- Python fastavro deserializes the Confluent wire format (5-byte prefix + Avro)
- This avoids needing the Confluent Schema Registry Java client and its
  transitive dependency chain on the Flink classpath
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
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081")
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

# Time windows in seconds for rolling counts
TIME_WINDOWS = {
    "1h": 3600,
    "3d": 259200,
    "7d": 604800,
    "24h": 86400,
    "30d": 2592000,
}


def decode_confluent_avro(raw_bytes):
    """Decode Confluent wire format: 1 magic byte + 4 byte schema ID + Avro payload."""
    if len(raw_bytes) < 6:
        return None
    # Skip 5-byte header (magic byte 0x00 + 4-byte schema ID)
    avro_payload = raw_bytes[5:]
    return fastavro.schemaless_reader(io.BytesIO(avro_payload), PARSED_SCHEMA)


def compute_fan_out_counters(event_or_type, event_ts=None, campaign_id=None, product_category=None, amount=None, now_ms=None):
    """Given event fields, compute counter updates for applicable time windows.

    Accepts either individual fields or a single event dict (for testing convenience).
    Returns a list of (metric_name, metric_value) tuples.
    """
    if isinstance(event_or_type, dict):
        event = event_or_type
        event_type = event["event_type"]
        event_ts = event["timestamp"]
        props = event.get("properties", {}) or {}
        campaign_id = props.get("campaign_id")
        product_category = props.get("product_category")
        amount = props.get("amount")
    else:
        event_type = event_or_type
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    event_age_seconds = (now_ms - event_ts) / 1000.0

    counters = []
    applicable_windows = [
        label for label, seconds in TIME_WINDOWS.items()
        if event_age_seconds <= seconds
    ]

    for window_label in applicable_windows:
        counters.append((f"{event_type}_{window_label}", 1.0))

    if campaign_id:
        for window_label in applicable_windows:
            counters.append((f"{event_type}_campaign_{campaign_id}_{window_label}", 1.0))

    if product_category:
        for window_label in applicable_windows:
            counters.append((f"{event_type}_category_{product_category}_{window_label}", 1.0))

    if event_type == "purchase_made" and amount is not None:
        for window_label in applicable_windows:
            counters.append((f"purchase_amount_{window_label}", float(amount)))

    for window_label in applicable_windows:
        counters.append((f"total_events_{window_label}", 1.0))

    return counters


def write_counters_to_clickhouse(account_id, customer_id, counters):
    """Write aggregated counters to ClickHouse customer_counters table."""
    import requests as req

    if not counters:
        return

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for metric_name, metric_value in counters:
        safe_metric = metric_name.replace("'", "\\'")
        safe_account = account_id.replace("'", "\\'")
        safe_customer = customer_id.replace("'", "\\'")
        values.append(
            f"('{safe_account}', '{safe_customer}', '{safe_metric}', "
            f"{metric_value}, '{now_str}')"
        )

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
            response = req.post(url, params=params, data=query, timeout=10)
            if response.status_code != 200:
                print(f"ClickHouse write error: {response.status_code} {response.text[:200]}")
            else:
                print(f"Wrote {len(batch)} counters for {customer_id}")
        except Exception as e:
            print(f"ClickHouse connection error: {e}")


class AvroDeserializeMap(MapFunction):
    """Deserializes raw Kafka bytes (Confluent wire format) into a structured Row.

    Output Row: (event_id, customer_id, account_id, event_type, timestamp,
                  campaign_id, product_category, amount)
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
            event["event_id"],
            event["customer_id"],
            event["account_id"],
            event["event_type"],
            ts,
            props.get("campaign_id") or "",
            props.get("product_category") or "",
            float(props["amount"]) if props.get("amount") is not None else 0.0,
        )


class CustomerAggregationFunction(KeyedProcessFunction):
    """Process function that fans out each event into multiple counter updates.

    Uses RocksDB-backed state to accumulate counters per customer across
    multiple dimensions and time windows. Periodically flushes accumulated
    counters to ClickHouse.
    """

    def __init__(self, flush_interval_ms):
        self._counter_state = None
        self._last_flush_state = None
        self._flush_interval_ms = flush_interval_ms

    def open(self, runtime_context):
        self._counter_state = runtime_context.get_state(
            ValueStateDescriptor("counters", Types.STRING())
        )
        self._last_flush_state = runtime_context.get_state(
            ValueStateDescriptor("last_flush", Types.LONG())
        )

    def process_element(self, value, ctx):
        # value is a Row: (event_id, customer_id, account_id, event_type,
        #                   timestamp, campaign_id, product_category, amount)
        customer_id = value[1]
        account_id = value[2]
        event_type = value[3]
        event_ts = value[4]
        campaign_id = value[5] or None  # empty string -> None
        product_category = value[6] or None
        amount = value[7] if value[7] != 0.0 else None

        # Compute fan-out counters
        new_counters = compute_fan_out_counters(
            event_type, event_ts, campaign_id, product_category, amount
        )

        # Load existing accumulated counters from RocksDB state
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

        # Save updated state to RocksDB
        self._counter_state.update(json.dumps(accumulated))

        # Check if we should flush to ClickHouse
        last_flush = self._last_flush_state.value()
        if last_flush is None:
            last_flush = 0

        now_ms = int(time.time() * 1000)
        if now_ms - last_flush >= self._flush_interval_ms:
            counter_list = list(accumulated["counters"].items())
            write_counters_to_clickhouse(account_id, customer_id, counter_list)
            self._last_flush_state.update(now_ms)

        # Yield a summary row for downstream operators
        fan_out_count = len(new_counters)
        yield Row(customer_id, account_id, fan_out_count, event_ts)


# Row type for deserialized events
EVENT_ROW_TYPE = Types.ROW([
    Types.STRING(),   # event_id
    Types.STRING(),   # customer_id
    Types.STRING(),   # account_id
    Types.STRING(),   # event_type
    Types.LONG(),     # timestamp
    Types.STRING(),   # campaign_id (nullable)
    Types.STRING(),   # product_category (nullable)
    Types.DOUBLE(),   # amount (nullable)
])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    # RocksDB state backend
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    state_backend = EmbeddedRocksDBStateBackend()
    env.set_state_backend(state_backend)

    # Checkpointing
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/customer-agg")
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
            'properties.group.id' = 'klaflow-customer-aggregation',
            'scan.startup.mode' = 'group-offsets',
            'format' = 'raw'
        )
    """)

    # Convert to DataStream of raw bytes
    raw_table = t_env.from_path("kafka_raw")
    raw_ds = t_env.to_data_stream(raw_table)

    # Deserialize Avro in Python, then key by customer_id
    event_ds = (
        raw_ds
        .map(AvroDeserializeMap(), output_type=EVENT_ROW_TYPE)
        .filter(lambda row: row is not None)
    )

    # Key by customer_id (field index 1), process with fan-out aggregation
    (
        event_ds
        .key_by(lambda row: row[1])
        .process(
            CustomerAggregationFunction(FLUSH_INTERVAL_MS),
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
    print("Avro deserialization: Python-side (fastavro)")
    env.execute("klaflow-customer-aggregation")


if __name__ == "__main__":
    main()
