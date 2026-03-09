"""Batch aggregation: consumes all events from Kafka, computes customer counters
and account metrics, writes to ClickHouse.

Lightweight alternative to the full PyFlink jobs -- uses confluent_kafka with
Avro deserialization (NOT PyFlink) to read events, then computes the same
fan-out counters and account metrics that the streaming jobs produce.

Processes events in a streaming fashion to avoid memory issues with large datasets.

Usage: python batch_aggregation.py
"""

import os
import time
from collections import defaultdict
from datetime import datetime

import requests
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")

# Time windows in seconds
TIME_WINDOWS = {
    "1h": 3600,
    "3d": 259200,
    "7d": 604800,
    "24h": 86400,
    "30d": 2592000,
}

# Flush accumulated counters every N events per customer
FLUSH_BATCH_SIZE = 500


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
    if properties is None:
        properties = {}
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


def compute_account_metrics(events_by_account):
    """Compute account-level metrics from grouped events."""
    results = {}
    for account_id, events in events_by_account.items():
        events_per_hour = len(events)
        sends_today = sum(1 for e in events if e.get("event_type") == "email_opened")
        active_customers = len(set(e.get("customer_id", "") for e in events))
        revenue = sum(
            float(e.get("properties", {}).get("amount", 0) or 0)
            for e in events
            if e.get("event_type") == "purchase_made"
        )
        results[account_id] = [
            ("events_per_hour", float(events_per_hour)),
            ("sends_today", float(sends_today)),
            ("active_customers_7d", float(active_customers)),
            ("revenue_7d", revenue),
        ]
    return results


def write_customer_counters_to_clickhouse(account_id, customer_id, counters):
    """Write aggregated counters to ClickHouse."""
    if not counters:
        return 0

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for metric_name, metric_value in counters:
        safe_metric = metric_name.replace("'", "\\'")
        values.append(
            f"('{account_id}', '{customer_id}', '{safe_metric}', "
            f"{metric_value}, '{now_str}')"
        )

    for i in range(0, len(values), 1000):
        batch = values[i:i + 1000]
        query = (
            "INSERT INTO customer_counters "
            "(account_id, customer_id, metric_name, metric_value, computed_at) "
            f"VALUES {', '.join(batch)}"
        )
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}
        response = requests.post(url, params=params, data=query, timeout=30)
        if response.status_code != 200:
            print(f"ClickHouse write error: {response.status_code} {response.text[:200]}")
            return 0

    return len(values)


def write_account_metrics_to_clickhouse(account_id, metrics):
    """Write account metrics to ClickHouse."""
    if not metrics:
        return 0

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    values = []
    for metric_name, metric_value in metrics:
        values.append(f"('{account_id}', '{metric_name}', {metric_value}, '{now_str}')")

    query = (
        "INSERT INTO account_metrics "
        "(account_id, metric_name, metric_value, computed_at) "
        f"VALUES {', '.join(values)}"
    )
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}
    response = requests.post(url, params=params, data=query, timeout=30)
    if response.status_code != 200:
        print(f"ClickHouse write error: {response.status_code} {response.text[:200]}")
        return 0
    return len(metrics)


def main():
    print("=== BATCH AGGREGATION (streaming mode) ===")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")
    print(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deser = AvroDeserializer(sr_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "klaflow-batch-aggregation-v5",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 120000,
        "max.poll.interval.ms": 600000,
    })
    consumer.subscribe([KAFKA_TOPIC])

    # Accumulate counters per customer in memory, flush periodically
    customer_counters = defaultdict(lambda: defaultdict(float))
    customer_event_counts = defaultdict(int)
    account_events = defaultdict(list)

    total_events = 0
    total_fan_out = 0
    total_counter_records = 0
    empty_polls = 0
    t0 = time.time()

    print("Consuming events...")

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
            event = avro_deser(
                msg.value(),
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
            )
            if not event:
                continue
        except Exception as e:
            if total_events == 0:
                print(f"  Deserialization error: {e}")
            continue

        total_events += 1
        account_id = event.get("account_id", "unknown")
        customer_id = event.get("customer_id", "unknown")
        key = (account_id, customer_id)

        # Fan out counters
        counters = compute_fan_out_counters(event)
        total_fan_out += len(counters)
        for metric_name, metric_value in counters:
            customer_counters[key][metric_name] += metric_value

        customer_event_counts[key] += 1

        # Track account events (keep lightweight — just counts, not full events)
        if len(account_events[account_id]) < 10000:
            account_events[account_id].append(event)

        # Flush customer counters when accumulated enough
        if customer_event_counts[key] >= FLUSH_BATCH_SIZE:
            counter_list = list(customer_counters[key].items())
            written = write_customer_counters_to_clickhouse(account_id, customer_id, counter_list)
            total_counter_records += written
            del customer_counters[key]
            customer_event_counts[key] = 0

        if total_events % 50000 == 0:
            elapsed = time.time() - t0
            rate = total_events / elapsed if elapsed > 0 else 0
            print(f"  {total_events:,} events ({rate:,.0f}/sec), "
                  f"{len(customer_counters)} customers buffered, "
                  f"{total_counter_records:,} counter records written")

    consumer.close()

    # Flush remaining customer counters
    print(f"\nFlushing remaining {len(customer_counters)} customer counters...")
    for (account_id, customer_id), counters in customer_counters.items():
        counter_list = list(counters.items())
        written = write_customer_counters_to_clickhouse(account_id, customer_id, counter_list)
        total_counter_records += written

    # Write account metrics
    print("\nComputing account metrics...")
    account_metrics = compute_account_metrics(account_events)
    total_account_records = 0
    for account_id, metrics in account_metrics.items():
        written = write_account_metrics_to_clickhouse(account_id, metrics)
        total_account_records += written
        print(f"  {account_id}: events={metrics[0][1]:.0f}, "
              f"active_customers={metrics[2][1]:.0f}, revenue=${metrics[3][1]:,.2f}")

    elapsed = time.time() - t0
    print(f"\n=== COMPLETE ===")
    print(f"Total events: {total_events:,}")
    print(f"Customer counters: {total_counter_records:,} records")
    print(f"Account metrics: {total_account_records} records")
    if total_events > 0:
        print(f"Fan-out ratio: {total_fan_out / total_events:.1f}x")
    print(f"Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
