"""Segment evaluation worker using the SIP two-phase pattern.

Phase 1 (scan): Find profiles that changed recently (lightweight).
Phase 2 (evaluate): Re-evaluate segment conditions for those profiles (heavy).

Runs on a polling loop, writing results to segment_membership in ClickHouse.
"""

import os
import time

import requests

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

CH_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
CH_PARAMS = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}

# Segment definitions — evaluated against customer_counters
SEGMENT_RULES = {
    "high_engagers": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE (metric_name = 'email_opened_7d' AND metric_value >= 3)
               OR (metric_name = 'link_clicked_7d' AND metric_value >= 2)
            GROUP BY account_id, customer_id
        """,
    },
    "recent_purchasers": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE metric_name = 'purchase_made_7d' AND metric_value >= 1
            GROUP BY account_id, customer_id
        """,
    },
    "at_risk": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE metric_name = 'days_since_last_purchase' AND metric_value > 30
            GROUP BY account_id, customer_id
            HAVING customer_id NOT IN (
                SELECT customer_id FROM customer_counters FINAL
                WHERE metric_name = 'purchase_made_30d' AND metric_value >= 1
            )
        """,
    },
    "active_browsers": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE metric_name = 'page_viewed_7d' AND metric_value >= 5
            GROUP BY account_id, customer_id
        """,
    },
    "cart_abandoners": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE metric_name = 'cart_abandoned_3d' AND metric_value >= 1
            GROUP BY account_id, customer_id
        """,
    },
    "vip_lapsed": {
        "query": """
            SELECT c1.account_id, c1.customer_id
            FROM customer_counters AS c1 FINAL
            INNER JOIN customer_counters AS c2 FINAL
              ON c1.account_id = c2.account_id AND c1.customer_id = c2.customer_id
            WHERE c1.metric_name = 'clv_tier' AND c1.metric_value = 4
              AND c2.metric_name = 'days_since_last_purchase' AND c2.metric_value > 60
            GROUP BY c1.account_id, c1.customer_id
        """,
    },
    "discount_sensitive": {
        "query": """
            SELECT account_id, customer_id
            FROM customer_counters FINAL
            WHERE metric_name = 'discount_sensitivity' AND metric_value >= 0.6
            GROUP BY account_id, customer_id
        """,
    },
}


def ch_query(query):
    """Execute a ClickHouse query and return rows."""
    resp = requests.post(CH_URL, params={**CH_PARAMS, "default_format": "JSONEachRow"}, data=query)
    if resp.status_code != 200:
        print(f"ClickHouse query error: {resp.status_code} {resp.text[:200]}")
        return []
    if not resp.text.strip():
        return []
    import json
    return [json.loads(line) for line in resp.text.strip().split("\n")]


def phase1_scan():
    """Find profiles that have been updated recently."""
    query = """
        SELECT DISTINCT account_id, customer_id
        FROM customer_counters
        WHERE computed_at > now() - INTERVAL 10 MINUTE
    """
    rows = ch_query(query)
    return [(r["account_id"], r["customer_id"]) for r in rows]


def phase2_evaluate(changed_profiles):
    """Evaluate all segment conditions for changed profiles."""
    if not changed_profiles:
        return []

    results = []
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    for segment_name, rule in SEGMENT_RULES.items():
        members = ch_query(rule["query"])
        member_set = {(r["account_id"], r["customer_id"]) for r in members}

        for account_id, customer_id in changed_profiles:
            in_segment = 1 if (account_id, customer_id) in member_set else 0
            results.append({
                "account_id": account_id,
                "customer_id": customer_id,
                "segment_name": segment_name,
                "in_segment": in_segment,
                "evaluated_at": now_str,
            })

    return results


def write_segment_membership(results):
    """Write segment membership records to ClickHouse."""
    if not results:
        return 0

    written = 0
    for i in range(0, len(results), 1000):
        batch = results[i:i + 1000]
        values = []
        for r in batch:
            values.append(
                f"('{r['account_id']}', '{r['customer_id']}', '{r['segment_name']}', "
                f"{r['in_segment']}, '{r['evaluated_at']}')"
            )
        query = (
            "INSERT INTO segment_membership (account_id, customer_id, segment_name, in_segment, evaluated_at) "
            f"VALUES {', '.join(values)}"
        )
        resp = requests.post(CH_URL, params=CH_PARAMS, data=query)
        if resp.status_code == 200:
            written += len(batch)
        else:
            print(f"Write error: {resp.status_code} {resp.text[:200]}")

    return written


def evaluate_all():
    """Full evaluation of all customers (used after seed/batch)."""
    all_profiles_query = """
        SELECT DISTINCT account_id, customer_id
        FROM customer_counters FINAL
    """
    rows = ch_query(all_profiles_query)
    profiles = [(r["account_id"], r["customer_id"]) for r in rows]
    print(f"Evaluating segments for {len(profiles)} profiles...")

    results = phase2_evaluate(profiles)
    written = write_segment_membership(results)
    print(f"Wrote {written} segment membership records")
    return written


def run_loop():
    """Main polling loop."""
    print(f"=== SEGMENT EVALUATION WORKER ===")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print(f"Poll interval: {POLL_INTERVAL}s")
    print(f"Segments: {', '.join(SEGMENT_RULES.keys())}")

    while True:
        try:
            changed = phase1_scan()
            if changed:
                print(f"Phase 1: {len(changed)} changed profiles")
                results = phase2_evaluate(changed)
                written = write_segment_membership(results)
                print(f"Phase 2: wrote {written} membership records")
            else:
                print("No changes detected")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        evaluate_all()
    else:
        run_loop()
