"""Tests for the FastAPI query layer (all 10 endpoints)."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock redis and numpy before importing
mock_redis_module = MagicMock()
sys.modules["redis"] = mock_redis_module

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "api"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "decision_engine"))

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Segment endpoints
# ---------------------------------------------------------------------------

class TestListSegments:
    @patch("main.ch_query")
    def test_returns_segments(self, mock_ch):
        mock_ch.return_value = [
            {"segment_name": "high_engagers", "member_count": "42"},
            {"segment_name": "recent_purchasers", "member_count": "15"},
        ]
        response = client.get("/segments")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    @patch("main.ch_query")
    def test_returns_empty_list(self, mock_ch):
        mock_ch.return_value = []
        response = client.get("/segments")
        assert response.status_code == 200
        assert response.json() == []


class TestSegmentCustomers:
    @patch("main.ch_query")
    def test_returns_customers(self, mock_ch):
        mock_ch.return_value = [
            {"account_id": "merchant_001", "customer_id": "cust_0001", "evaluated_at": "2024-01-01"},
        ]
        response = client.get("/segments/high_engagers/customers")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["customer_id"] == "cust_0001"

    @patch("main.ch_query")
    def test_404_on_empty_segment(self, mock_ch):
        mock_ch.return_value = []
        response = client.get("/segments/nonexistent/customers")
        assert response.status_code == 404


class TestCustomerSegments:
    @patch("main.ch_query")
    def test_returns_customer_segments(self, mock_ch):
        mock_ch.return_value = [
            {"segment_name": "high_engagers", "in_segment": 1, "evaluated_at": "2024-01-01"},
            {"segment_name": "at_risk", "in_segment": 0, "evaluated_at": "2024-01-01"},
        ]
        response = client.get("/customers/cust_0001/segments")
        assert response.status_code == 200
        data = response.json()
        # Should filter to only in_segment=1
        assert len(data) == 1
        assert data[0]["segment_name"] == "high_engagers"

    @patch("main.ch_query")
    def test_404_on_unknown_customer(self, mock_ch):
        mock_ch.return_value = []
        response = client.get("/customers/unknown/segments")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Feature store endpoints
# ---------------------------------------------------------------------------

class TestCustomerFeatures:
    @patch("main.get_redis_client")
    def test_returns_features_with_account_id(self, mock_redis):
        mock_r = MagicMock()
        mock_r.hgetall.return_value = {
            "email_open_rate_7d": "0.4",
            "clv_tier": "high",
        }
        mock_redis.return_value = mock_r

        response = client.get("/customers/cust_0001/features?account_id=merchant_001")
        assert response.status_code == 200
        data = response.json()
        assert data["clv_tier"] == "high"

    @patch("main.get_redis_client")
    def test_404_when_no_features(self, mock_redis):
        mock_r = MagicMock()
        mock_r.keys.return_value = []
        mock_redis.return_value = mock_r

        response = client.get("/customers/unknown/features")
        assert response.status_code == 404


class TestCustomerScores:
    @patch("main.get_redis_client")
    def test_returns_scores(self, mock_redis):
        mock_r = MagicMock()
        mock_r.hgetall.return_value = {
            "clv_tier": "vip",
            "churn_risk_score": "0.3",
            "discount_sensitivity": "0.6",
        }
        mock_redis.return_value = mock_r

        response = client.get("/customers/cust_0001/scores?account_id=merchant_001")
        assert response.status_code == 200
        data = response.json()
        assert data["clv_tier"] == "vip"
        assert data["churn_risk_score"] == 0.3


# ---------------------------------------------------------------------------
# Decision endpoints
# ---------------------------------------------------------------------------

class TestDecisionHistory:
    @patch("main.ch_query")
    def test_returns_history(self, mock_ch):
        mock_ch.return_value = [
            {
                "account_id": "merchant_001",
                "customer_id": "cust_001",
                "arm_chosen": "email_10pct_discount",
                "feature_snapshot": "{}",
                "created_at": "2024-01-01",
                "reward": "1.0",
            }
        ]
        response = client.get("/decisions/cust_001/history")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1


# ---------------------------------------------------------------------------
# Account metrics
# ---------------------------------------------------------------------------

class TestAccountMetrics:
    @patch("main.ch_query")
    def test_returns_metrics(self, mock_ch):
        mock_ch.return_value = [
            {"metric_name": "events_per_hour", "metric_value": "1000", "computed_at": "2024-01-01"},
            {"metric_name": "revenue_7d", "metric_value": "5000.50", "computed_at": "2024-01-01"},
        ]
        response = client.get("/accounts/merchant_001/metrics")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    @patch("main.ch_query")
    def test_404_on_unknown_account(self, mock_ch):
        mock_ch.return_value = []
        response = client.get("/accounts/unknown/metrics")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    @patch("main.ch_query")
    def test_healthy(self, mock_ch):
        mock_ch.return_value = [{"1": "1"}]
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    @patch("main.ch_query")
    def test_unhealthy(self, mock_ch):
        mock_ch.side_effect = Exception("connection refused")
        response = client.get("/health")
        assert response.status_code == 503
