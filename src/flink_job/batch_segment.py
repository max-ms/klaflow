"""Batch segmentation: consumes all events from Kafka, computes segments, writes to ClickHouse.

Lightweight alternative to the full PyFlink job — uses the same classify_segments() logic
but reads from Kafka directly with confluent_kafka and writes to ClickHouse via HTTP.

Usage: python batch_segment.py [--window-seconds 604800]
"""

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime

import requests
from confluent_kafka import Consumer, KafkaError

# Reuse the segment definitions directly to avoid PyFlink import
SEGMENTS = {
    "high_engagers": {"event_types": ["email_opened", "link_clicked"], "min_count": 3},
    "recent_purchasers": {"event_types": ["purchase_made"], "min_count": 1},
    "active_browsers": {"event_types": ["page_viewed"], "min_count": 5},
    "cart_abandoners": {"event_types": ["cart_abandoned"], "min_count": 1},
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")


def classify_segments(customer_id, events, window_start_ms, window_end_ms):
    """Determine segment membership for a customer in a window."""
    event_type_counts = defaultdict(int)
    for event in events:
        event_type_counts[event["event_type"]] += 1

    window_start = datetime.utcfromtimestamp(window_start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.000")
    window_end = datetime.utcfromtimestamp(window_end_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.000")

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

    if not events:
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
    if not segments:
        return 0

    # Batch in chunks of 1000
    written = 0
    for i in range(0, len(segments), 1000):
        batch = segments[i:i + 1000]
        values = []
        for seg in batch:
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
            written += len(batch)

    return written


def main():
    parser = argparse.ArgumentParser(description="Batch segmentation from Kafka to ClickHouse")
    parser.add_argument("--window-seconds", type=int, default=604800, help="Window size in seconds (default: 7 days)")
    args = parser.parse_args()

    window_ms = args.window_seconds * 1000

    print(f"=== BATCH SEGMENTATION ===")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print(f"Window size: {args.window_seconds}s ({args.window_seconds // 86400}d)")

    # Consume all events from Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "klaflow-batch-segmenter",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    print("Reading events from Kafka...")
    all_events = []
    empty_polls = 0
    t0 = time.time()

    while empty_polls < 30:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
                continue
            print(f"Consumer error: {msg.error()}")
            continue

        empty_polls = 0
        try:
            # Events are Avro-encoded, but the value might be raw bytes
            # Try JSON first, fall back to treating as raw
            value = msg.value()
            if isinstance(value, bytes):
                # Skip the Avro magic byte + schema ID (5 bytes) for Confluent wire format
                if len(value) > 5 and value[0] == 0:
                    # This is Confluent Avro wire format, we need a deserializer
                    # For simplicity, try to decode the JSON portion
                    pass
                try:
                    event = json.loads(value)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Can't decode without Avro deserializer, skip
                    continue
            else:
                event = json.loads(value)
            all_events.append(event)
        except Exception as e:
            continue

        if len(all_events) % 10000 == 0:
            print(f"  Read {len(all_events):,} events...")

    consumer.close()
    read_elapsed = time.time() - t0
    print(f"Read {len(all_events):,} events in {read_elapsed:.1f}s")

    if not all_events:
        print("No events found. Events may be Avro-encoded. Trying with Avro deserializer...")
        # Re-consume with Avro deserializer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import SerializationContext, MessageField

        sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081")
        sr_client = SchemaRegistryClient({"url": sr_url})
        avro_deser = AvroDeserializer(sr_client)

        consumer2 = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "klaflow-batch-segmenter-avro",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer2.subscribe([KAFKA_TOPIC])

        empty_polls = 0
        t0 = time.time()
        while empty_polls < 30:
            msg = consumer2.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                continue

            empty_polls = 0
            try:
                event = avro_deser(msg.value(), SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
                if event:
                    all_events.append(event)
            except Exception as e:
                if len(all_events) == 0:
                    print(f"  Deserialization error: {e}")
                continue

            if len(all_events) % 10000 == 0:
                print(f"  Read {len(all_events):,} events...")

        consumer2.close()
        read_elapsed = time.time() - t0
        print(f"Read {len(all_events):,} events with Avro deserializer in {read_elapsed:.1f}s")

    if not all_events:
        print("ERROR: No events could be read. Exiting.")
        return

    # Find time range
    timestamps = [e["timestamp"] for e in all_events]
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    print(f"Time range: {datetime.utcfromtimestamp(min_ts/1000)} → {datetime.utcfromtimestamp(max_ts/1000)}")

    # Group events by customer and window
    print("Computing segments...")
    # Create windows
    windows = []
    w_start = min_ts
    while w_start < max_ts:
        w_end = w_start + window_ms
        windows.append((w_start, w_end))
        w_start = w_end

    print(f"  {len(windows)} windows of {args.window_seconds}s each")

    # Group events by customer_id
    events_by_customer = defaultdict(list)
    for event in all_events:
        events_by_customer[event["customer_id"]].append(event)

    print(f"  {len(events_by_customer)} unique customers")

    # Classify segments per window per customer
    all_segments = []
    for w_start, w_end in windows:
        for customer_id, events in events_by_customer.items():
            window_events = [e for e in events if w_start <= e["timestamp"] < w_end]
            segments = classify_segments(customer_id, window_events, w_start, w_end)
            all_segments.extend(segments)

    print(f"  {len(all_segments):,} segment records to write")

    # Write to ClickHouse
    print("Writing to ClickHouse...")
    written = write_to_clickhouse(all_segments)
    print(f"\n=== COMPLETE ===")
    print(f"Wrote {written:,} segment records to ClickHouse")


if __name__ == "__main__":
    main()
