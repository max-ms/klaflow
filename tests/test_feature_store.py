"""Tests for the feature store: schema, writer, and ML scoring."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "feature_store"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "ml_models"))

from feature_schema import CustomerFeatureVector, FEATURE_TTL_SECONDS, redis_key
from score_customers import (
    compute_clv_tier,
    compute_churn_risk,
    compute_discount_sensitivity,
    score_customer,
    ScoringRequest,
)


# ---------------------------------------------------------------------------
# Feature schema tests
# ---------------------------------------------------------------------------

class TestFeatureSchema:
    def test_default_values(self):
        fv = CustomerFeatureVector()
        assert fv.email_open_rate_7d == 0.0
        assert fv.purchase_count_30d == 0
        assert fv.days_since_last_purchase == 999
        assert fv.lifecycle_state == "new"
        assert fv.clv_tier == "low"

    def test_to_redis_hash(self):
        fv = CustomerFeatureVector(
            email_open_rate_7d=0.5,
            purchase_count_30d=3,
            clv_tier="high",
        )
        h = fv.to_redis_hash()
        assert isinstance(h, dict)
        assert h["email_open_rate_7d"] == "0.5"
        assert h["purchase_count_30d"] == "3"
        assert h["clv_tier"] == "high"
        # All values should be strings
        for k, v in h.items():
            assert isinstance(v, str), f"Key {k} has non-string value"

    def test_from_redis_hash(self):
        data = {
            "email_open_rate_7d": "0.4",
            "purchase_count_30d": "5",
            "clv_tier": "vip",
            "churn_risk_score": "0.3",
        }
        fv = CustomerFeatureVector.from_redis_hash(data)
        assert fv.email_open_rate_7d == 0.4
        assert fv.purchase_count_30d == 5
        assert fv.clv_tier == "vip"
        assert fv.churn_risk_score == 0.3

    def test_from_empty_hash(self):
        fv = CustomerFeatureVector.from_redis_hash({})
        assert fv.email_open_rate_7d == 0.0
        assert fv.lifecycle_state == "new"

    def test_redis_key(self):
        key = redis_key("merchant_001", "cust_0001")
        assert key == "customer:merchant_001:cust_0001"

    def test_roundtrip(self):
        original = CustomerFeatureVector(
            email_open_rate_7d=0.35,
            email_open_rate_30d=0.28,
            purchase_count_30d=7,
            days_since_last_purchase=14,
            avg_order_value=85.50,
            cart_abandon_rate_30d=0.12,
            lifecycle_state="active",
            clv_tier="high",
            churn_risk_score=0.15,
            discount_sensitivity=0.4,
            preferred_send_hour=9,
        )
        h = original.to_redis_hash()
        restored = CustomerFeatureVector.from_redis_hash(h)
        assert restored.email_open_rate_7d == original.email_open_rate_7d
        assert restored.purchase_count_30d == original.purchase_count_30d
        assert restored.clv_tier == original.clv_tier

    def test_ttl_is_48_hours(self):
        assert FEATURE_TTL_SECONDS == 48 * 60 * 60

    def test_different_accounts_different_keys(self):
        k1 = redis_key("merchant_001", "cust_001")
        k2 = redis_key("merchant_002", "cust_001")
        assert k1 != k2


# ---------------------------------------------------------------------------
# ML scoring tests
# ---------------------------------------------------------------------------

class TestCLVTier:
    def test_low_tier(self):
        assert compute_clv_tier(1, 10.0) == "low"  # $10

    def test_medium_tier(self):
        assert compute_clv_tier(5, 20.0) == "medium"  # $100

    def test_high_tier(self):
        assert compute_clv_tier(10, 100.0) == "high"  # $1000

    def test_vip_tier(self):
        assert compute_clv_tier(10, 250.0) == "vip"  # $2500

    def test_zero_purchases(self):
        assert compute_clv_tier(0, 100.0) == "low"

    def test_boundary_50(self):
        # >= $50 is medium in this implementation
        assert compute_clv_tier(1, 50.0) == "medium"
        assert compute_clv_tier(1, 49.99) == "low"


class TestChurnRisk:
    def test_no_risk(self):
        score = compute_churn_risk(10, 0.5, 3, "low")
        assert score == 0.0

    def test_90_day_risk(self):
        score = compute_churn_risk(91, 0.5, 3, "low")
        assert score == 0.4

    def test_180_day_risk(self):
        score = compute_churn_risk(181, 0.5, 3, "low")
        assert score == 0.7  # 0.4 + 0.3

    def test_low_open_rate(self):
        score = compute_churn_risk(10, 0.05, 3, "low")
        assert score == 0.2

    def test_high_value_no_purchase(self):
        score = compute_churn_risk(10, 0.5, 0, "vip")
        assert score == 0.1

    def test_max_risk(self):
        score = compute_churn_risk(200, 0.05, 0, "high")
        assert score == pytest.approx(1.0)  # 0.4 + 0.3 + 0.2 + 0.1

    def test_capped_at_1(self):
        score = compute_churn_risk(200, 0.05, 0, "vip")
        assert score <= 1.0


class TestDiscountSensitivity:
    def test_no_purchases(self):
        assert compute_discount_sensitivity(0, 0) == 0.0

    def test_all_campaign_purchases(self):
        assert compute_discount_sensitivity(5, 5) == 1.0

    def test_half_campaign(self):
        assert compute_discount_sensitivity(3, 6) == 0.5

    def test_no_campaign_purchases(self):
        assert compute_discount_sensitivity(0, 10) == 0.0


class TestScoreCustomerEndToEnd:
    def test_full_scoring(self):
        req = ScoringRequest(
            purchase_count_30d=10,
            avg_order_value=100.0,
            days_since_last_purchase=15,
            email_open_rate_7d=0.4,
            purchases_with_campaign_id=3.0,
            total_purchases=10.0,
        )
        result = score_customer(req)
        assert result.clv_tier == "high"  # 10 * 100 = $1000
        assert result.churn_risk_score == 0.0  # active, good open rate
        assert result.discount_sensitivity == pytest.approx(0.3, abs=0.01)

    def test_default_request(self):
        req = ScoringRequest()
        result = score_customer(req)
        assert result.clv_tier == "low"
        # days_since_last_purchase=999 > 180, email_open_rate_7d=0 < 0.1
        # -> 0.4 + 0.3 + 0.2 = 0.9
        assert result.churn_risk_score == 0.9
        assert result.discount_sensitivity == 0.0
