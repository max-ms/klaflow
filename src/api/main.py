"""FastAPI query layer: segments, features, scores, decisions, accounts.

All 10 endpoints from the architecture spec.
"""

import json
import os
import sys
from pathlib import Path

import redis
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Klaflow API", version="2.0.0")

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
