"""Standalone Shopify webhook receiver for local development.

Receives Shopify webhooks via ngrok, verifies HMAC signatures, logs raw
payloads, maps them to Klaflow CustomerEvent schema, and prints a schema
compatibility report.

Run:  python -m src.shopify.webhook_receiver
Or:   cd src/shopify && python webhook_receiver.py
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Request, HTTPException
import uvicorn

# Support running as module or standalone
try:
    from src.shopify.schema_mapper import (
        map_webhook, analyze_payload, format_report, TOPIC_TO_EVENT_TYPE,
    )
    from src.shopify.kafka_producer import ShopifyKafkaProducer
except ImportError:
    from schema_mapper import (
        map_webhook, analyze_payload, format_report, TOPIC_TO_EVENT_TYPE,
    )
    from kafka_producer import ShopifyKafkaProducer


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "") or os.getenv("SHOPIFY_CLIENT_SECRET", "")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "9000"))

# Where to save raw payloads
DATA_DIR = Path(os.getenv(
    "WEBHOOK_DATA_DIR",
    str(Path(__file__).resolve().parent.parent.parent / "data" / "shopify_webhooks"),
))
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

# Ring buffer of recent payloads (last 100)
recent_payloads: List[Dict[str, Any]] = []
MAX_RECENT = 100

# Webhook counts by topic
topic_counts: Dict[str, int] = defaultdict(int)
last_received: Optional[str] = None

# Aggregated schema reports by topic
schema_reports: Dict[str, Dict[str, Any]] = {}

# Checkout tracker: token -> (payload, shop_domain, received_at)
pending_checkouts: Dict[str, tuple] = {}

# Kafka producer (initialized on startup if --kafka flag is passed)
kafka_producer: Optional[ShopifyKafkaProducer] = None
kafka_enabled = os.getenv("ENABLE_KAFKA", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Klaflow Shopify Webhook Receiver", version="1.0.0")


@app.on_event("startup")
async def startup_event():
    global kafka_producer, kafka_enabled
    if kafka_enabled:
        print("\n  [KAFKA] Initializing Kafka producer...")
        kafka_producer = ShopifyKafkaProducer()
        if not kafka_producer.connect():
            print("  [KAFKA] Failed to connect. Events will be logged but NOT produced.")
            kafka_producer = None
            kafka_enabled = False
        else:
            print("  [KAFKA] Ready. Shopify events will be produced to Kafka.\n")


def verify_hmac(body: bytes, hmac_header: str) -> bool:
    """Verify Shopify HMAC-SHA256 signature."""
    computed = base64.b64encode(
        hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            body,
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    return hmac.compare_digest(computed, hmac_header)


@app.post("/webhooks/shopify")
async def receive_webhook(request: Request):
    """Main webhook handler. Verifies HMAC, logs payload, maps and reports."""
    global last_received

    # Read raw body for HMAC verification (must happen before JSON parsing)
    body = await request.body()

    # Extract Shopify headers
    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256", "")
    topic = request.headers.get("X-Shopify-Topic", "unknown")
    shop_domain = request.headers.get("X-Shopify-Shop-Domain", "unknown")

    # Verify HMAC
    if hmac_header and not verify_hmac(body, hmac_header):
        print(f"\n[REJECTED] HMAC verification failed for {topic} from {shop_domain}")
        raise HTTPException(status_code=401, detail="HMAC verification failed")

    # Parse payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Update stats
    topic_counts[topic] += 1
    last_received = datetime.utcnow().isoformat()

    # Save raw payload to disk
    ts = int(time.time() * 1000)
    safe_topic = topic.replace("/", "_")
    filename = f"{safe_topic}_{ts}.json"
    filepath = DATA_DIR / filename
    with open(filepath, "w") as f:
        json.dump({
            "topic": topic,
            "shop_domain": shop_domain,
            "received_at": last_received,
            "hmac_verified": bool(hmac_header),
            "payload": payload,
        }, f, indent=2, default=str)

    # Add to recent buffer
    recent_payloads.append({
        "topic": topic,
        "shop_domain": shop_domain,
        "received_at": last_received,
        "file": str(filepath),
        "payload_summary": _summarize_payload(topic, payload),
    })
    if len(recent_payloads) > MAX_RECENT:
        recent_payloads.pop(0)

    # Map to Klaflow events
    mapped_events = map_webhook(topic, payload, shop_domain)

    # Generate schema compatibility report
    report = analyze_payload(topic, payload, shop_domain)
    schema_reports[topic] = report

    # Produce to Kafka if enabled
    kafka_produced = 0
    if kafka_enabled and kafka_producer and mapped_events:
        for evt in mapped_events:
            if kafka_producer.produce(evt):
                kafka_produced += 1
        kafka_producer.flush(2.0)

    # Print to console
    print(f"\n{'=' * 70}")
    print(f"  WEBHOOK RECEIVED: {topic} from {shop_domain}")
    print(f"  Time: {last_received}  |  Saved: {filename}")
    print(f"  Mapped to {len(mapped_events)} Klaflow event(s)")
    if kafka_enabled:
        print(f"  Kafka: {kafka_produced}/{len(mapped_events)} produced")
    print(f"{'=' * 70}")
    print(format_report(report))

    # Handle checkout tracking
    if topic == "checkouts/create":
        token = payload.get("token") or payload.get("id")
        if token:
            pending_checkouts[str(token)] = (payload, shop_domain, time.time())
            print(f"  [CHECKOUT TRACKER] Checkout {token} buffered. "
                  f"Waiting for matching order...")
    elif topic == "orders/create":
        checkout_token = payload.get("checkout_token")
        if checkout_token and str(checkout_token) in pending_checkouts:
            del pending_checkouts[str(checkout_token)]
            print(f"  [CHECKOUT TRACKER] Checkout {checkout_token} completed "
                  f"(order received). NOT cart_abandoned.")

    return {
        "status": "ok",
        "mapped_events": len(mapped_events),
        "kafka_produced": kafka_produced if kafka_enabled else "disabled",
    }


@app.get("/webhooks/status")
def webhook_status():
    """Health check: webhook counts, last received time."""
    result = {
        "total_received": sum(topic_counts.values()),
        "by_topic": dict(topic_counts),
        "last_received": last_received,
        "pending_checkouts": len(pending_checkouts),
        "data_dir": str(DATA_DIR),
        "kafka_enabled": kafka_enabled,
    }
    if kafka_producer:
        result["kafka"] = kafka_producer.stats
    return result


@app.get("/webhooks/payloads")
def list_payloads(limit: int = 20):
    """List recently received webhook payloads."""
    return recent_payloads[-limit:]


@app.get("/webhooks/schema-report")
def get_schema_report():
    """Aggregated schema compatibility report across all received topics."""
    if not schema_reports:
        return {"message": "No webhooks received yet. Send a test event first."}

    summary = {}
    for topic, report in schema_reports.items():
        field_statuses = {}
        for field, info in report["field_mapping"].items():
            field_statuses[field] = info["status"]

        summary[topic] = {
            "mapped_event_type": report["mapped_event_type"],
            "field_statuses": field_statuses,
            "klaflow_fields_without_source": report["klaflow_fields_without_source"],
            "notable_unmapped_shopify_fields": report["notable_unmapped_fields"],
            "total_unmapped_shopify_fields": len(report["shopify_fields_not_mapped"]),
        }

    return summary


def _summarize_payload(topic: str, payload: dict) -> dict:
    """Create a brief summary of a payload for the recent list."""
    summary = {"topic": topic}
    if topic == "orders/create":
        summary["order_id"] = payload.get("id")
        summary["total_price"] = payload.get("total_price")
        summary["line_items_count"] = len(payload.get("line_items", []))
        customer = payload.get("customer") or {}
        summary["customer_id"] = customer.get("id")
    elif topic == "checkouts/create":
        summary["checkout_token"] = payload.get("token")
        summary["total_price"] = payload.get("total_price")
        customer = payload.get("customer") or {}
        summary["customer_id"] = customer.get("id")
    return summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Klaflow Shopify Webhook Receiver")
    parser.add_argument("--kafka", action="store_true",
                        help="Enable Kafka production (requires port-forwards)")
    args = parser.parse_args()

    if args.kafka:
        kafka_enabled = True
        os.environ["ENABLE_KAFKA"] = "1"

    print(f"\nKlaflow Shopify Webhook Receiver")
    print(f"{'=' * 50}")
    print(f"  Port:       {WEBHOOK_PORT}")
    print(f"  Data dir:   {DATA_DIR}")
    print(f"  HMAC key:   {WEBHOOK_SECRET[:8]}...{WEBHOOK_SECRET[-4:]}")
    print(f"  Kafka:      {'ENABLED' if kafka_enabled else 'disabled (use --kafka)'}")
    print(f"{'=' * 50}")
    print(f"\nEndpoints:")
    print(f"  POST /webhooks/shopify     <- Shopify sends here")
    print(f"  GET  /webhooks/status      <- Check webhook counts")
    print(f"  GET  /webhooks/payloads    <- Recent payloads")
    print(f"  GET  /webhooks/schema-report <- Schema compatibility")
    print(f"\nWaiting for webhooks...\n")

    uvicorn.run(app, host="0.0.0.0", port=WEBHOOK_PORT)
