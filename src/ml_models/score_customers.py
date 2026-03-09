"""ML scoring service: toy models for CLV tier, churn risk, and discount sensitivity.

Exposes a FastAPI service with POST /score and GET /health endpoints.
These are simplified deterministic models -- the goal is architectural correctness
(scores exist as profile properties, feed the decision engine), not ML accuracy.
"""

import logging
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Klaflow ML Scoring Service", version="1.0.0")


class ScoringRequest(BaseModel):
    """Input: partial customer feature dict (counters only)."""
    purchase_count_30d: int = 0
    avg_order_value: float = 0.0
    days_since_last_purchase: int = 999
    email_open_rate_7d: float = 0.0
    purchases_with_campaign_id: float = 0.0
    total_purchases: float = 0.0
    clv_tier_override: Optional[str] = None  # for churn risk dependency


class ScoringResponse(BaseModel):
    """Output: ML-derived scores."""
    clv_tier: str
    churn_risk_score: float
    discount_sensitivity: float


def compute_clv_tier(purchase_count_30d: int, avg_order_value: float) -> str:
    """CLV tier based on purchase_count_30d x avg_order_value bracket.

    Lifetime value proxy = purchase_count_30d * avg_order_value
      < $50  -> "low"
      $50-$500  -> "medium"
      $500-$2000 -> "high"
      > $2000   -> "vip"
    """
    lifetime_value = purchase_count_30d * avg_order_value

    if lifetime_value > 2000:
        return "vip"
    elif lifetime_value > 500:
        return "high"
    elif lifetime_value >= 50:
        return "medium"
    else:
        return "low"


def compute_churn_risk(
    days_since_last_purchase: int,
    email_open_rate_7d: float,
    purchase_count_30d: int,
    clv_tier: str,
) -> float:
    """Churn risk score (0.0-1.0).

    +0.4 if days_since_last_purchase > 90
    +0.3 if days_since_last_purchase > 180
    +0.2 if email_open_rate_7d < 0.1
    +0.1 if purchase_count_30d == 0 and clv_tier in ["high", "vip"]
    """
    score = 0.0

    if days_since_last_purchase > 90:
        score += 0.4
    if days_since_last_purchase > 180:
        score += 0.3
    if email_open_rate_7d < 0.1:
        score += 0.2
    if purchase_count_30d == 0 and clv_tier in ("high", "vip"):
        score += 0.1

    return min(score, 1.0)


def compute_discount_sensitivity(
    purchases_with_campaign_id: float,
    total_purchases: float,
) -> float:
    """Discount sensitivity (0.0-1.0).

    Proxy: ratio of purchases made during known discount campaigns
    to total purchases. Simple proxy, not a real model.
    """
    if total_purchases <= 0:
        return 0.0
    return min(1.0, purchases_with_campaign_id / total_purchases)


def score_customer(req: ScoringRequest) -> ScoringResponse:
    """Score a single customer. Extracted for testability."""
    clv_tier = compute_clv_tier(req.purchase_count_30d, req.avg_order_value)

    churn_risk_score = compute_churn_risk(
        days_since_last_purchase=req.days_since_last_purchase,
        email_open_rate_7d=req.email_open_rate_7d,
        purchase_count_30d=req.purchase_count_30d,
        clv_tier=clv_tier,
    )

    discount_sensitivity = compute_discount_sensitivity(
        purchases_with_campaign_id=req.purchases_with_campaign_id,
        total_purchases=req.total_purchases,
    )

    return ScoringResponse(
        clv_tier=clv_tier,
        churn_risk_score=round(churn_risk_score, 2),
        discount_sensitivity=round(discount_sensitivity, 4),
    )


@app.post("/score", response_model=ScoringResponse)
def score_endpoint(req: ScoringRequest):
    """Score a customer's CLV tier, churn risk, and discount sensitivity."""
    result = score_customer(req)
    logger.info(
        "Scored: clv=%s churn=%.2f disc=%.4f (purchases=%d aov=%.2f days=%d)",
        result.clv_tier,
        result.churn_risk_score,
        result.discount_sensitivity,
        req.purchase_count_30d,
        req.avg_order_value,
        req.days_since_last_purchase,
    )
    return result


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ml-scoring"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
