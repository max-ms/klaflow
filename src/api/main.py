"""FastAPI query layer: segments, features, scores, decisions, accounts.

All 10 endpoints from the architecture spec.
"""

import json
import os
import sys
from pathlib import Path

import redis
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="Klaflow API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse.analytics.svc.cluster.local")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "klaflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "klaflow-pass")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master.features.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CH_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
CH_PARAMS = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}


def ch_query(query):
    """Execute a ClickHouse query and return rows."""
    resp = requests.post(CH_URL, params={**CH_PARAMS, "default_format": "JSONEachRow"}, data=query)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"ClickHouse error: {resp.text[:200]}")
    if not resp.text.strip():
        return []
    return [json.loads(line) for line in resp.text.strip().split("\n")]


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ---------------------------------------------------------------------------
# Segmentation queries (over ClickHouse)
# ---------------------------------------------------------------------------

@app.get("/segments")
def list_segments():
    """All segments with member counts."""
    rows = ch_query("""
        SELECT segment_name, count() as member_count
        FROM segment_membership FINAL
        WHERE in_segment = 1
        GROUP BY segment_name
        ORDER BY member_count DESC
    """)
    return rows


@app.get("/segments/{segment_name}/customers")
def segment_customers(segment_name: str, limit: int = 100):
    """Customers in a segment."""
    rows = ch_query(f"""
        SELECT account_id, customer_id, evaluated_at
        FROM segment_membership FINAL
        WHERE segment_name = '{segment_name}' AND in_segment = 1
        ORDER BY evaluated_at DESC
        LIMIT {limit}
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Segment '{segment_name}' not found or empty")
    return rows


@app.get("/customers/{customer_id}/segments")
def customer_segments(customer_id: str):
    """Which segments a customer belongs to."""
    rows = ch_query(f"""
        SELECT segment_name, in_segment, evaluated_at
        FROM segment_membership FINAL
        WHERE customer_id = '{customer_id}'
        ORDER BY segment_name
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found")
    return [r for r in rows if int(r.get("in_segment", 0)) == 1]


# ---------------------------------------------------------------------------
# Feature store (Redis)
# ---------------------------------------------------------------------------

@app.get("/customers/{customer_id}/features")
def customer_features(customer_id: str, account_id: str = ""):
    """Full feature vector from Redis."""
    r = get_redis_client()

    # Try to find the key if account_id not provided
    if not account_id:
        # Search for matching keys
        keys = r.keys(f"customer:*:{customer_id}")
        if not keys:
            raise HTTPException(status_code=404, detail=f"No features found for '{customer_id}'")
        key = keys[0]
    else:
        key = f"customer:{account_id}:{customer_id}"

    data = r.hgetall(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"No features found for '{customer_id}'")
    return data


@app.get("/customers/{customer_id}/scores")
def customer_scores(customer_id: str, account_id: str = ""):
    """ML scores (CLV tier, churn risk, discount sensitivity)."""
    r = get_redis_client()

    if not account_id:
        keys = r.keys(f"customer:*:{customer_id}")
        if not keys:
            raise HTTPException(status_code=404, detail=f"No scores found for '{customer_id}'")
        key = keys[0]
    else:
        key = f"customer:{account_id}:{customer_id}"

    data = r.hgetall(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"No scores found for '{customer_id}'")

    return {
        "clv_tier": data.get("clv_tier", "unknown"),
        "churn_risk_score": float(data.get("churn_risk_score", 0)),
        "discount_sensitivity": float(data.get("discount_sensitivity", 0)),
    }


# ---------------------------------------------------------------------------
# Decision engine
# ---------------------------------------------------------------------------

class DecisionRequest(BaseModel):
    customer_id: str
    account_id: str
    context: Optional[str] = None


@app.post("/decide")
def decide(req: DecisionRequest):
    """Get bandit decision for a customer."""
    # Import bandit inline to avoid circular deps at module level
    try:
        sys.path.insert(0, str(Path(__file__).parent / "decision_engine"))
        from bandit import ContextualBandit
        b = ContextualBandit(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        result = b.decide(req.customer_id, req.account_id, context=req.context)

        # Log decision to ClickHouse
        feature_snapshot = json.dumps(result.get("reasoning", {}).get("feature_vector", {}))
        _log_decision(req.account_id, req.customer_id, result["arm"], feature_snapshot)

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _log_decision(account_id, customer_id, arm_chosen, feature_snapshot):
    """Write decision to ClickHouse decision_log."""
    try:
        query = (
            "INSERT INTO decision_log (account_id, customer_id, arm_chosen, feature_snapshot) "
            f"VALUES ('{account_id}', '{customer_id}', '{arm_chosen}', '{feature_snapshot}')"
        )
        requests.post(CH_URL, params=CH_PARAMS, data=query, timeout=5)
    except Exception:
        pass  # Non-critical


@app.get("/decisions/{customer_id}/history")
def decision_history(customer_id: str, limit: int = 20):
    """Past decisions + outcomes."""
    rows = ch_query(f"""
        SELECT
            d.account_id, d.customer_id, d.arm_chosen,
            d.feature_snapshot, d.created_at,
            r.reward
        FROM decision_log d
        LEFT JOIN reward_log r
            ON d.customer_id = r.customer_id
            AND d.account_id = r.account_id
            AND d.arm_chosen = r.arm_chosen
        WHERE d.customer_id = '{customer_id}'
        ORDER BY d.created_at DESC
        LIMIT {limit}
    """)
    return rows


@app.get("/bandit/arm-stats")
def arm_stats():
    """Current arm weights + win rates per arm."""
    try:
        sys.path.insert(0, str(Path(__file__).parent / "decision_engine"))
        from bandit import ContextualBandit
        b = ContextualBandit(redis_host=REDIS_HOST, redis_port=REDIS_PORT)
        return b.get_arm_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Account metrics
# ---------------------------------------------------------------------------

@app.get("/accounts/{account_id}/metrics")
def account_metrics(account_id: str):
    """Events/sends/revenue for this merchant."""
    rows = ch_query(f"""
        SELECT metric_name, metric_value, computed_at
        FROM account_metrics FINAL
        WHERE account_id = '{account_id}'
        ORDER BY metric_name
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Account '{account_id}' not found")
    return rows


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Health check endpoint."""
    try:
        ch_query("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Customer list + events (UI endpoints)
# ---------------------------------------------------------------------------

@app.get("/customers")
def list_customers(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query("", description="Search by customer_id prefix"),
):
    """Paginated customer list with feature data."""
    r = get_redis_client()

    # Get all customer keys from Redis
    # Key format: customer:{account_id}:{customer_id} where customer_id may contain colons
    all_keys = r.keys("customer:*")
    customers = []
    for k in all_keys:
        # Split into at most 3 parts: "customer", account_id, customer_id
        parts = k.split(":", 2)
        if len(parts) >= 3:
            customers.append({"account_id": parts[1], "customer_id": parts[2]})

    # Apply search filter
    if search:
        customers = [c for c in customers if search.lower() in c["customer_id"].lower()]

    customers.sort(key=lambda c: c["customer_id"])
    total = len(customers)
    start = (page - 1) * page_size
    page_customers = customers[start : start + page_size]

    # Enrich with feature data (key = customer:{account_id}:{customer_id})
    results = []
    pipeline = r.pipeline()
    for c in page_customers:
        pipeline.hgetall(f"customer:{c['account_id']}:{c['customer_id']}")
    features_list = pipeline.execute() if page_customers else []

    for c, features in zip(page_customers, features_list):
        results.append({
            "customer_id": c["customer_id"],
            "account_id": c["account_id"],
            "lifecycle_state": features.get("lifecycle_state", "unknown"),
            "clv_tier": features.get("clv_tier", "unknown"),
            "churn_risk_score": float(features.get("churn_risk_score", 0)),
            "updated_at": features.get("updated_at", ""),
        })

    return {"customers": results, "total": total, "page": page, "page_size": page_size}


@app.get("/customers/{customer_id}/events")
def customer_events(
    customer_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Event feed for a customer from ClickHouse customer_counters."""
    # We don't have raw events in ClickHouse (by design — it's the query layer).
    # Return counter history as the activity proxy.
    offset = (page - 1) * page_size
    rows = ch_query(f"""
        SELECT metric_name, metric_value, computed_at
        FROM customer_counters FINAL
        WHERE customer_id = '{customer_id}'
        ORDER BY computed_at DESC
        LIMIT {page_size} OFFSET {offset}
    """)
    total_rows = ch_query(f"""
        SELECT count() as cnt
        FROM customer_counters FINAL
        WHERE customer_id = '{customer_id}'
    """)
    total = int(total_rows[0]["cnt"]) if total_rows else 0
    return {"events": rows, "total": total, "page": page, "page_size": page_size}


@app.get("/customers/{customer_id}/all-segments")
def customer_all_segments(customer_id: str):
    """All segment evaluations for a customer (both in and not-in)."""
    rows = ch_query(f"""
        SELECT segment_name, in_segment, evaluated_at
        FROM segment_membership FINAL
        WHERE customer_id = '{customer_id}'
        ORDER BY segment_name
    """)
    return rows


@app.get("/dashboard/stats")
def dashboard_stats():
    """KPI stats for dashboard."""
    r = get_redis_client()
    total_profiles = len(r.keys("customer:*"))

    segments = ch_query("""
        SELECT count(DISTINCT segment_name) as cnt
        FROM segment_membership FINAL
        WHERE in_segment = 1
    """)
    active_segments = int(segments[0]["cnt"]) if segments else 0

    events_24h = ch_query("""
        SELECT count() as cnt
        FROM customer_counters
        WHERE computed_at > now() - INTERVAL 24 HOUR
    """)
    events_count = int(events_24h[0]["cnt"]) if events_24h else 0

    revenue = ch_query("""
        SELECT sum(metric_value) as total
        FROM customer_counters FINAL
        WHERE metric_name LIKE '%purchase%amount%'
           OR metric_name LIKE '%revenue%'
    """)
    revenue_total = float(revenue[0]["total"]) if revenue and revenue[0].get("total") else 0

    return {
        "total_profiles": total_profiles,
        "active_segments": active_segments,
        "events_24h": events_count,
        "revenue_7d": revenue_total,
    }


@app.get("/segments/{segment_name}/detail")
def segment_detail(segment_name: str):
    """Segment detail with member count and condition info."""
    rows = ch_query(f"""
        SELECT count() as member_count
        FROM segment_membership FINAL
        WHERE segment_name = '{segment_name}' AND in_segment = 1
    """)
    member_count = int(rows[0]["member_count"]) if rows else 0

    total_rows = ch_query("SELECT count(DISTINCT customer_id) as cnt FROM segment_membership FINAL")
    total = int(total_rows[0]["cnt"]) if total_rows else 1

    # Segment condition definitions
    conditions = {
        "high_engagers": "email_open_rate_7d >= 0.3 OR link_clicked_7d >= 2",
        "recent_purchasers": "purchase_count_7d >= 1",
        "at_risk": "days_since_last_purchase > 30 AND purchase_count_30d == 0",
        "active_browsers": "page_viewed_7d >= 5",
        "cart_abandoners": "cart_abandon_rate_3d >= 1",
        "vip_lapsed": "clv_tier == 'vip' AND days_since_last_purchase > 60",
        "discount_sensitive": "discount_sensitivity >= 0.6",
    }

    return {
        "segment_name": segment_name,
        "member_count": member_count,
        "total_profiles": total,
        "percentage": round(member_count / max(total, 1) * 100, 1),
        "condition": conditions.get(segment_name, "Unknown condition"),
    }


@app.get("/decisions/recent")
def recent_decisions(limit: int = Query(20, ge=1, le=100)):
    """Recent decisions across all customers."""
    rows = ch_query(f"""
        SELECT account_id, customer_id, arm_chosen, feature_snapshot, created_at
        FROM decision_log
        ORDER BY created_at DESC
        LIMIT {limit}
    """)
    return rows


# ---------------------------------------------------------------------------
# Phase 2a: Agent campaign endpoints
# ---------------------------------------------------------------------------

try:
    from agent_endpoints import router as agent_router
    app.include_router(agent_router)
except ImportError:
    pass  # Agent module not available in this deployment


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
