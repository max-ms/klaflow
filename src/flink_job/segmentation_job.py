"""PyFlink segmentation job: consumes from Kafka, computes segments, writes to ClickHouse."""

import json
import os

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import WindowFunction, ProcessWindowFunction

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")

# Segment definitions
SEGMENTS = {
    "high_engagers": {
        "event_types": ["email_opened", "link_clicked"],
        "window_days": 7,
        "min_count": 3,
    },
    "recent_purchasers": {
        "event_types": ["purchase_made"],
        "window_days": 7,
        "min_count": 1,
    },
    "active_browsers": {
        "event_types": ["page_viewed"],
        "window_days": 7,
        "min_count": 5,
    },
    "cart_abandoners": {
        "event_types": ["cart_abandoned"],
        "window_days": 3,
        "min_count": 1,
    },
}


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        return event.get("timestamp", record_timestamp)


def classify_segments(customer_id, events, window):
    """Given a list of events for a customer in a window, determine segment membership."""
    import requests
    from datetime import datetime

    event_type_counts = {}
    for event in events:
        parsed = json.loads(event) if isinstance(event, str) else event
        et = parsed.get("event_type", "")
        event_type_counts[et] = event_type_counts.get(et, 0) + 1

    window_start = datetime.fromtimestamp(window.start / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    window_end = datetime.fromtimestamp(window.end / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    results = []
    for segment_name, config in SEGMENTS.items():
        count = sum(event_type_counts.get(et, 0) for et in config["event_types"])
        if count >= config["min_count"]:
            results.append({
                "customer_id": customer_id,
                "segment_name": segment_name,
                "window_start": window_start,
                "window_end": window_end,
                "event_count": count,
            })

    # Check for at_risk: customer had events before but 0 in this window
    total_events = sum(event_type_counts.values())
    if total_events == 0:
        results.append({
            "customer_id": customer_id,
            "segment_name": "at_risk",
            "window_start": window_start,
            "window_end": window_end,
            "event_count": 0,
        })

    return results


def write_to_clickhouse(segments):
    """Write segment results to ClickHouse via HTTP interface."""
    import requests

    if not segments:
        return

    values = []
    for seg in segments:
        values.append(
            f"('{seg['customer_id']}', '{seg['segment_name']}', "
            f"'{seg['window_start']}', '{seg['window_end']}', {seg['event_count']})"
        )

    query = (
        "INSERT INTO customer_segments (customer_id, segment_name, window_start, window_end, event_count) "
        f"VALUES {', '.join(values)}"
    )

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}

    response = requests.post(url, params=params, data=query)
    if response.status_code != 200:
        print(f"ClickHouse write error: {response.status_code} {response.text}")
    else:
        print(f"Wrote {len(segments)} segment records to ClickHouse")


class SegmentWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        events = list(elements)
        segments = classify_segments(key, events, context.window())
        write_to_clickhouse(segments)
        for seg in segments:
            yield Row(
                seg["customer_id"],
                seg["segment_name"],
                seg["event_count"],
            )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("klaflow-segmentation")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    ds = env.from_source(kafka_source, watermark_strategy, "kafka-source")

    # Key by customer_id, apply 7-day tumbling window
    # For demo purposes, use a shorter window (1 minute) to see results quickly
    window_size = int(os.getenv("WINDOW_SIZE_SECONDS", "60"))

    (
        ds.key_by(lambda x: json.loads(x)["customer_id"])
        .window(TumblingEventTimeWindows.of(Time.seconds(window_size)))
        .process(
            SegmentWindowFunction(),
            output_type=Types.ROW([
                Types.STRING(),
                Types.STRING(),
                Types.INT(),
            ]),
        )
    )

    env.execute("klaflow-segmentation")


if __name__ == "__main__":
    main()
