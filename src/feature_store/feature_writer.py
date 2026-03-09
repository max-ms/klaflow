"""Feature writer service: ClickHouse -> compute derived features -> Redis.

Runs on a 5-minute polling loop. Reads recently updated customer counters from
ClickHouse, computes derived features, calls the ML scoring service for CLV tier,
churn risk, and discount sensitivity, then writes the full feature vector to Redis.
"""

import logging
import os
import signal
import time

import redis
import requests

from feature_schema import CustomerFeatureVector, FEATURE_TTL_SECONDS, redis_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master.features.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ML_SCORING_URL = os.getenv("ML_SCORING_URL", "http://ml-scoring.decisions.svc.cluster.local:8000")

CH_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
CH_PARAMS = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}

POLL_INTERVAL_SECONDS = 300
LOOKBACK_MINUTES = 10


def ch_query(query):
    """Execute a ClickHouse query and return rows as list of dicts."""
    import json
    resp = requests.post(CH_URL, params={**CH_PARAMS, "default_format": "JSONEachRow"}, data=query)
    if resp.status_code != 200:
        logger.error("ClickHouse error: %s %s", resp.status_code, resp.text[:200])
        return []
    if not resp.text.strip():
        return []
    return [json.loads(line) for line in resp.text.strip().split("\n")]


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def fetch_recently_updated_profiles():
    """Fetch (account_id, customer_id) pairs updated in the last LOOKBACK_MINUTES."""
    query = f"""
        SELECT DISTINCT account_id, customer_id
        FROM customer_counters
        WHERE computed_at > now() - INTERVAL {LOOKBACK_MINUTES} MINUTE
    """
    return ch_query(query)


def fetch_all_profiles():
    """Fetch all distinct profiles."""
    query = """
        SELECT DISTINCT account_id, customer_id
        FROM customer_counters FINAL
    """
    return ch_query(query)


def fetch_counters_for_customer(account_id, customer_id):
    """Fetch all current counters for a single customer."""
    query = f"""
        SELECT metric_name, metric_value
        FROM customer_counters FINAL
        WHERE account_id = '{account_id}' AND customer_id = '{customer_id}'
    """
    rows = ch_query(query)
    return {r["metric_name"]: float(r["metric_value"]) for r in rows}


def compute_derived_features(counters):
    """Compute a CustomerFeatureVector from raw ClickHouse counters."""
    # Engagement rates
    email_opened_7d = counters.get("email_opened_7d", 0.0)
    emails_sent_7d = max(1.0, counters.get("emails_sent_7d", email_opened_7d + counters.get("link_clicked_7d", 0)))
    email_open_rate_7d = email_opened_7d / emails_sent_7d

    email_opened_30d = counters.get("email_opened_30d", 0.0)
    emails_sent_30d = max(1.0, counters.get("emails_sent_30d", email_opened_30d + counters.get("link_clicked_30d", 0)))
    email_open_rate_30d = email_opened_30d / emails_sent_30d

    # Purchase metrics
    purchase_count_30d = int(counters.get("purchase_made_30d", 0))
    days_since_last_purchase = int(counters.get("days_since_last_purchase", 999))

    total_order_value = counters.get("purchase_amount_30d", 0.0)
    avg_order_value = total_order_value / purchase_count_30d if purchase_count_30d > 0 else 0.0

    # Cart abandonment
    cart_abandon_rate_30d = counters.get("cart_abandoned_30d", 0.0)

    # Preferred send hour
    preferred_send_hour = int(counters.get("preferred_send_hour", 12))
    preferred_send_hour = max(0, min(23, preferred_send_hour))

    # Lifecycle state
    has_recent_activity = email_open_rate_7d > 0 or counters.get("page_viewed_7d", 0) > 0
    if days_since_last_purchase == 999:
        lifecycle_state = "new" if has_recent_activity else "churned"
    elif days_since_last_purchase <= 30 or purchase_count_30d > 0:
        lifecycle_state = "active"
    elif days_since_last_purchase <= 90:
        lifecycle_state = "at_risk" if has_recent_activity else "lapsed"
    elif days_since_last_purchase <= 180:
        lifecycle_state = "lapsed"
    else:
        lifecycle_state = "churned"

    return CustomerFeatureVector(
        email_open_rate_7d=round(email_open_rate_7d, 4),
        email_open_rate_30d=round(email_open_rate_30d, 4),
        purchase_count_30d=purchase_count_30d,
        days_since_last_purchase=days_since_last_purchase,
        avg_order_value=round(avg_order_value, 2),
        cart_abandon_rate_30d=round(cart_abandon_rate_30d, 4),
        preferred_send_hour=preferred_send_hour,
        lifecycle_state=lifecycle_state,
        updated_at=int(time.time() * 1000),
    )


def call_ml_scoring(features, counters):
    """Call the ML scoring service for CLV tier, churn risk, discount sensitivity."""
    payload = {
        "purchase_count_30d": features.purchase_count_30d,
        "avg_order_value": features.avg_order_value,
        "days_since_last_purchase": features.days_since_last_purchase,
        "email_open_rate_7d": features.email_open_rate_7d,
        "purchases_with_campaign_id": counters.get("purchases_with_campaign_id", 0.0),
        "total_purchases": counters.get("total_purchases", 0.0),
    }
    try:
        resp = requests.post(f"{ML_SCORING_URL}/score", json=payload, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("ML scoring unavailable (%s), using defaults", e)
        return {"clv_tier": "low", "churn_risk_score": 0.0, "discount_sensitivity": 0.0}


def write_derived_counters_to_clickhouse(account_id, customer_id, features):
    """Write ML-derived metrics back to customer_counters so segment evaluator can use them."""
    from datetime import datetime
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    clv_numeric = {"low": 1, "medium": 2, "high": 3, "vip": 4}.get(features.clv_tier, 1)
    metrics = [
        ("clv_tier", float(clv_numeric)),
        ("days_since_last_purchase", float(features.days_since_last_purchase)),
        ("discount_sensitivity", features.discount_sensitivity),
        ("churn_risk_score", features.churn_risk_score),
    ]
    values = []
    for metric_name, metric_value in metrics:
        values.append(
            f"('{account_id}', '{customer_id}', '{metric_name}', "
            f"{metric_value}, '{now_str}')"
        )
    query = (
        "INSERT INTO customer_counters "
        "(account_id, customer_id, metric_name, metric_value, computed_at) "
        f"VALUES {', '.join(values)}"
    )
    try:
        requests.post(CH_URL, params=CH_PARAMS, data=query, timeout=10)
    except Exception as e:
        logger.warning("Failed to write derived counters for %s: %s", customer_id, e)


def process_customer(redis_client, account_id, customer_id):
    """Full pipeline for a single customer."""
    counters = fetch_counters_for_customer(account_id, customer_id)
    if not counters:
        return False

    features = compute_derived_features(counters)
    ml_scores = call_ml_scoring(features, counters)
    features.clv_tier = ml_scores.get("clv_tier", "low")
    features.churn_risk_score = float(ml_scores.get("churn_risk_score", 0.0))
    features.discount_sensitivity = float(ml_scores.get("discount_sensitivity", 0.0))

    # Write to Redis (feature store for decision engine)
    key = redis_key(account_id, customer_id)
    redis_client.hset(key, mapping=features.to_redis_hash())
    redis_client.expire(key, FEATURE_TTL_SECONDS)

    # Write ML-derived metrics back to ClickHouse (for segment evaluator)
    write_derived_counters_to_clickhouse(account_id, customer_id, features)
    return True


def run_poll_cycle(redis_client, full=False):
    """Execute one poll cycle."""
    if full:
        profiles = fetch_all_profiles()
    else:
        profiles = fetch_recently_updated_profiles()

    if not profiles:
        logger.info("No profiles to update")
        return 0

    logger.info("Found %d profiles to update", len(profiles))
    updated = 0
    for profile in profiles:
        try:
            if process_customer(redis_client, profile["account_id"], profile["customer_id"]):
                updated += 1
        except Exception as e:
            logger.error("Failed to process %s/%s: %s", profile.get("account_id"), profile.get("customer_id"), e)

    logger.info("Updated %d/%d feature vectors", updated, len(profiles))
    return updated


def main():
    """Main loop."""
    shutdown = False

    def handle_signal(sig, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Feature writer starting")
    logger.info("ClickHouse: %s:%d, Redis: %s:%d", CLICKHOUSE_HOST, CLICKHOUSE_PORT, REDIS_HOST, REDIS_PORT)

    redis_client = get_redis_client()

    import sys as _sys
    if len(_sys.argv) > 1 and _sys.argv[1] == "--full":
        run_poll_cycle(redis_client, full=True)
        return

    while not shutdown:
        try:
            run_poll_cycle(redis_client)
        except Exception as e:
            logger.error("Poll cycle failed: %s", e)
        for _ in range(POLL_INTERVAL_SECONDS):
            if shutdown:
                break
            time.sleep(1)

    logger.info("Feature writer shutting down")


if __name__ == "__main__":
    main()
