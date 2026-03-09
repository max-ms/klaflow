"""Synthetic customer event producer with Avro validation via Schema Registry.

Emulates Klaviyo's event ingestion for 5 merchant accounts with 18,500 total
customers. Includes a lifecycle state machine that drives realistic behavioral
clusters (active customers generate many events, churned ones generate ~0).

Two modes:
  python producer.py --mode seed        Generate 90 days of historical events
  python producer.py --mode continuous  Emit events indefinitely at --rate/sec
"""

import argparse
import math
import os
import random
import signal
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.streaming.svc.cluster.local:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry.streaming.svc.cluster.local:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "customer-events")

# ---------------------------------------------------------------------------
# Merchant accounts & customer pools
# ---------------------------------------------------------------------------

ACCOUNTS = {
    "merchant_001": 1_000,
    "merchant_002": 5_000,
    "merchant_003": 500,
    "merchant_004": 10_000,
    "merchant_005": 2_000,
}

TOTAL_CUSTOMERS = sum(ACCOUNTS.values())  # 18,500

EVENT_TYPES = ["email_opened", "link_clicked", "purchase_made", "page_viewed", "cart_abandoned"]

# Poisson lambda per customer per day
EVENT_LAMBDAS = {
    "page_viewed":    3.0,
    "email_opened":   0.4,
    "link_clicked":   0.1,
    "cart_abandoned":  0.15,
    "purchase_made":  0.05,
}

# Reference data
CAMPAIGN_IDS = [f"camp_{i:03d}" for i in range(10)]
PRODUCT_IDS = [f"prod_{i:03d}" for i in range(50)]
PRODUCT_CATEGORIES = ["electronics", "clothing", "home", "beauty", "sports"]
EMAIL_SUBJECTS = ["Summer Sale!", "New Arrivals", "We Miss You", "Exclusive Offer", "Weekly Digest"]

# ---------------------------------------------------------------------------
# Lifecycle state machine
# ---------------------------------------------------------------------------

LIFECYCLE_STATES = ["new", "active", "at_risk", "lapsed", "churned"]

# Weekly transition probabilities
TRANSITION_PROBS = {
    "new":      {"active": 0.3},
    "active":   {"at_risk": 0.1},   # only if no purchase that week
    "at_risk":  {"lapsed": 0.2},
    "lapsed":   {"churned": 0.1},
    "churned":  {},
}

# Event rate multiplier by lifecycle state
LIFECYCLE_RATE_MULTIPLIER = {
    "new":      0.7,
    "active":   1.0,
    "at_risk":  0.5,
    "lapsed":   0.2,
    "churned":  0.0,
}


def build_customer_id(account_id: str, index: int) -> str:
    """Format: merchant_001:cust_0001"""
    return f"{account_id}:cust_{index:04d}"


def build_all_customers() -> list[dict]:
    """Create the full customer roster with initial lifecycle states."""
    customers = []
    for account_id, count in ACCOUNTS.items():
        for i in range(1, count + 1):
            customers.append({
                "customer_id": build_customer_id(account_id, i),
                "account_id": account_id,
                "lifecycle_state": "new",
                "purchased_this_week": False,
            })
    return customers


ALL_CUSTOMER_IDS = []
for _acct, _cnt in ACCOUNTS.items():
    for _i in range(1, _cnt + 1):
        ALL_CUSTOMER_IDS.append(build_customer_id(_acct, _i))


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------

def transition_lifecycle(customer: dict) -> None:
    """Apply weekly lifecycle state transition in-place."""
    state = customer["lifecycle_state"]
    transitions = TRANSITION_PROBS.get(state, {})

    # Purchase reverses at_risk -> active
    if state == "at_risk" and customer.get("purchased_this_week"):
        customer["lifecycle_state"] = "active"
        customer["purchased_this_week"] = False
        return

    # Active -> at_risk only happens if no purchase
    if state == "active" and customer.get("purchased_this_week"):
        customer["purchased_this_week"] = False
        return

    for target_state, prob in transitions.items():
        if random.random() < prob:
            customer["lifecycle_state"] = target_state
            break

    customer["purchased_this_week"] = False


# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def generate_properties(event_type: str) -> dict:
    """Generate event properties based on event type."""
    props = {
        "campaign_id": None,
        "product_id": None,
        "product_category": None,
        "amount": None,
        "email_subject": None,
    }

    if event_type in ("email_opened", "link_clicked"):
        props["campaign_id"] = random.choice(CAMPAIGN_IDS)
        props["email_subject"] = random.choice(EMAIL_SUBJECTS)
    elif event_type in ("purchase_made", "cart_abandoned"):
        props["product_id"] = random.choice(PRODUCT_IDS)
        props["product_category"] = random.choice(PRODUCT_CATEGORIES)
        props["amount"] = round(random.uniform(5.0, 500.0), 2)
        # Some purchases come from campaigns
        if random.random() < 0.3:
            props["campaign_id"] = random.choice(CAMPAIGN_IDS)
    elif event_type == "page_viewed":
        props["product_category"] = random.choice(PRODUCT_CATEGORIES)

    return props


def generate_event(
    customer_id: str = None,
    account_id: str = None,
    event_type: str = None,
    timestamp_ms: int = None,
) -> dict:
    """Generate a single synthetic customer event.

    If customer_id is not provided, picks a random one from the full pool.
    account_id is derived from customer_id if not explicitly given.
    """
    if customer_id is None:
        customer_id = random.choice(ALL_CUSTOMER_IDS)

    if account_id is None:
        # Extract account_id from customer_id format "merchant_XXX:cust_YYYY"
        account_id = customer_id.rsplit(":", 1)[0] if ":" in customer_id else "merchant_001"

    if event_type is None:
        # Weighted by Poisson lambdas to get the right relative frequencies
        weights = [EVENT_LAMBDAS[et] for et in EVENT_TYPES]
        event_type = random.choices(EVENT_TYPES, weights=weights, k=1)[0]

    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_id": account_id,
        "event_type": event_type,
        "timestamp": timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        "properties": generate_properties(event_type),
    }


def generate_daily_events_for_customer(customer: dict, day_start_ms: int, day_ms: int) -> list[dict]:
    """Generate one day of events for a single customer using Poisson draws."""
    state = customer["lifecycle_state"]
    rate_mult = LIFECYCLE_RATE_MULTIPLIER.get(state, 1.0)

    events = []
    for event_type, lam in EVENT_LAMBDAS.items():
        adjusted_lambda = lam * rate_mult
        if adjusted_lambda <= 0:
            continue
        n = _poisson_sample(adjusted_lambda)
        for _ in range(n):
            ts = day_start_ms + random.randint(0, day_ms - 1)
            event = generate_event(
                customer_id=customer["customer_id"],
                account_id=customer["account_id"],
                event_type=event_type,
                timestamp_ms=ts,
            )
            events.append(event)

            if event_type == "purchase_made":
                customer["purchased_this_week"] = True

    return events


def _poisson_sample(lam: float) -> int:
    """Sample from Poisson(lam) using inverse transform."""
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while True:
        k += 1
        p *= random.random()
        if p < L:
            break
    return k - 1


# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------

def event_to_dict(event, ctx):
    """Convert event dict to a format suitable for Avro serialization."""
    return event


def make_producer():
    """Create Kafka producer with Avro serializer."""
    schema_path = Path(__file__).parent / "schemas" / "customer_event.avsc"
    with open(schema_path) as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, schema_str, event_to_dict)
    string_serializer = StringSerializer("utf_8")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "klaflow-producer",
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
    })

    return producer, avro_serializer, string_serializer


def produce_event(producer, avro_serializer, string_serializer, event):
    """Serialize and produce a single event to Kafka."""
    producer.produce(
        topic=TOPIC,
        key=string_serializer(event["customer_id"]),
        value=avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE)),
    )


# ---------------------------------------------------------------------------
# Seed mode
# ---------------------------------------------------------------------------

def run_seed(args):
    """Generate 90 days of historical events for all customers.

    Simulates lifecycle transitions weekly. Prints progress every 10,000 events.
    """
    producer, avro_serializer, string_serializer = make_producer()
    customers = build_all_customers()

    now_ms = int(time.time() * 1000)
    day_ms = 86_400_000
    start_ms = now_ms - (90 * day_ms)

    print("=== SEED MODE ===")
    print(f"Generating 90 days of events for {TOTAL_CUSTOMERS:,} customers across {len(ACCOUNTS)} accounts")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Period: {time.strftime('%Y-%m-%d', time.gmtime(start_ms / 1000))} -> {time.strftime('%Y-%m-%d', time.gmtime(now_ms / 1000))}")

    total = 0
    last_progress = 0
    t0 = time.time()

    for day in range(90):
        day_start_ms = start_ms + (day * day_ms)

        for customer in customers:
            events = generate_daily_events_for_customer(customer, day_start_ms, day_ms)
            for event in events:
                try:
                    produce_event(producer, avro_serializer, string_serializer, event)
                    total += 1
                except BufferError:
                    producer.poll(1)
                    produce_event(producer, avro_serializer, string_serializer, event)
                    total += 1

                if total - last_progress >= 10_000:
                    elapsed = time.time() - t0
                    rate = total / elapsed if elapsed > 0 else 0
                    print(f"  Day {day + 1}/90 -- {total:,} events ({rate:,.0f} events/sec)")
                    last_progress = total

        producer.poll(0)

        # Weekly lifecycle transitions (every 7 days)
        if (day + 1) % 7 == 0:
            for customer in customers:
                transition_lifecycle(customer)

    print("Flushing remaining events...")
    producer.flush(timeout=30)

    elapsed = time.time() - t0
    print(f"\n=== SEED COMPLETE ===")
    print(f"Total events: {total:,}")
    print(f"Elapsed: {elapsed:.1f}s ({total / elapsed:,.0f} events/sec)")

    # Print lifecycle distribution at end
    state_counts = {}
    for c in customers:
        s = c["lifecycle_state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    print(f"Final lifecycle distribution: {state_counts}")


# ---------------------------------------------------------------------------
# Continuous mode
# ---------------------------------------------------------------------------

def run_continuous(args):
    """Emit events indefinitely at the specified rate with 30-second progress reports."""
    producer, avro_serializer, string_serializer = make_producer()
    rate = args.rate
    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("=== CONTINUOUS MODE ===")
    print(f"Rate: {rate} events/sec, Customers: {TOTAL_CUSTOMERS:,} across {len(ACCOUNTS)} accounts")
    print(f"Kafka: {KAFKA_BOOTSTRAP}, Schema Registry: {SCHEMA_REGISTRY_URL}")

    total = 0
    interval_count = 0
    t0 = time.time()
    last_report = t0

    while not shutdown:
        event = generate_event()
        try:
            produce_event(producer, avro_serializer, string_serializer, event)
        except BufferError:
            producer.poll(1)
            produce_event(producer, avro_serializer, string_serializer, event)

        total += 1
        interval_count += 1
        producer.poll(0)

        now = time.time()
        if now - last_report >= 30:
            actual_rate = interval_count / (now - last_report)
            print(f"  {total:,} total -- {actual_rate:.1f} events/sec (last 30s)")
            interval_count = 0
            last_report = now

        # Throttle to target rate
        expected = total / rate
        elapsed = time.time() - t0
        if elapsed < expected:
            time.sleep(expected - elapsed)

    producer.flush(timeout=10)
    elapsed = time.time() - t0
    print(f"\n=== SHUTDOWN ===")
    print(f"Total events: {total:,} in {elapsed:.1f}s ({total / elapsed:.1f} events/sec)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Klaflow event producer")
    parser.add_argument("--mode", choices=["seed", "continuous"], default="continuous",
                        help="seed: 90-day backfill; continuous: real-time stream")
    parser.add_argument("--rate", type=int, default=10,
                        help="Events per second (continuous mode only)")
    args = parser.parse_args()

    if args.mode == "seed":
        run_seed(args)
    else:
        run_continuous(args)


if __name__ == "__main__":
    main()
