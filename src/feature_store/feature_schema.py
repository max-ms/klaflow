"""Feature vector schema for the customer feature store.

Defines the structure of pre-computed customer feature vectors stored in Redis.
Each customer has one feature vector, keyed as customer:{account_id}:{customer_id}.
"""

from dataclasses import dataclass, field, asdict
import time


@dataclass
class CustomerFeatureVector:
    """Pre-computed customer feature vector written to Redis."""

    # Engagement metrics (derived from counters)
    email_open_rate_7d: float = 0.0       # opens / sends in last 7d
    email_open_rate_30d: float = 0.0      # opens / sends in last 30d

    # Purchase metrics
    purchase_count_30d: int = 0
    days_since_last_purchase: int = 999   # large default = never purchased
    avg_order_value: float = 0.0

    # Behavioral
    cart_abandon_rate_30d: float = 0.0
    preferred_send_hour: int = 12         # 0-23, default noon

    # Lifecycle
    lifecycle_state: str = "new"          # new|active|at_risk|lapsed|churned

    # ML-derived scores (filled by ML scoring service)
    clv_tier: str = "low"                 # low|medium|high|vip
    churn_risk_score: float = 0.0         # 0.0 - 1.0
    discount_sensitivity: float = 0.0     # 0.0 - 1.0

    # Metadata
    updated_at: int = 0                   # epoch ms

    def to_redis_hash(self) -> dict:
        """Convert to a flat dict suitable for Redis HSET (all values as strings)."""
        d = asdict(self)
        return {k: str(v) for k, v in d.items()}

    @classmethod
    def from_redis_hash(cls, data: dict) -> "CustomerFeatureVector":
        """Reconstruct from a Redis hash (all values are strings)."""
        if not data:
            return cls()
        return cls(
            email_open_rate_7d=float(data.get("email_open_rate_7d", 0.0)),
            email_open_rate_30d=float(data.get("email_open_rate_30d", 0.0)),
            purchase_count_30d=int(data.get("purchase_count_30d", 0)),
            days_since_last_purchase=int(data.get("days_since_last_purchase", 999)),
            avg_order_value=float(data.get("avg_order_value", 0.0)),
            cart_abandon_rate_30d=float(data.get("cart_abandon_rate_30d", 0.0)),
            preferred_send_hour=int(data.get("preferred_send_hour", 12)),
            lifecycle_state=data.get("lifecycle_state", "new"),
            clv_tier=data.get("clv_tier", "low"),
            churn_risk_score=float(data.get("churn_risk_score", 0.0)),
            discount_sensitivity=float(data.get("discount_sensitivity", 0.0)),
            updated_at=int(data.get("updated_at", 0)),
        )


def redis_key(account_id: str, customer_id: str) -> str:
    """Build the Redis key for a customer's feature vector."""
    return f"customer:{account_id}:{customer_id}"


FEATURE_TTL_SECONDS = 48 * 3600  # 48 hours
