"""Tests for the aggregation logic (customer counters + account metrics).

Tests the fan-out pattern: 1 event should produce multiple metric updates
across (event_type x timeframe x campaign_id x product_category) dimensions.
Tests that customer and account aggregation are separate concerns.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Mock pyflink before importing any module that references it
sys.modules["pyflink"] = MagicMock()
sys.modules["pyflink.common"] = MagicMock()
sys.modules["pyflink.common.serialization"] = MagicMock()
sys.modules["pyflink.common.time"] = MagicMock()
sys.modules["pyflink.common.watermark_strategy"] = MagicMock()
sys.modules["pyflink.datastream"] = MagicMock()
sys.modules["pyflink.datastream.connectors"] = MagicMock()
sys.modules["pyflink.datastream.connectors.kafka"] = MagicMock()
sys.modules["pyflink.datastream.functions"] = MagicMock()
sys.modules["pyflink.datastream.state"] = MagicMock()
sys.modules["pyflink.datastream.state_backend"] = MagicMock()

# Mock confluent_kafka for batch_aggregation imports
sys.modules["confluent_kafka"] = MagicMock()
sys.modules["confluent_kafka.schema_registry"] = MagicMock()
sys.modules["confluent_kafka.schema_registry.avro"] = MagicMock()
sys.modules["confluent_kafka.serialization"] = MagicMock()

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "flink_jobs"))

from customer_aggregation_job import compute_fan_out_counters, TIME_WINDOWS
from batch_aggregation import (
    compute_fan_out_counters as batch_compute_fan_out_counters,
    compute_account_metrics,
)


import time

# Use a fixed "now" so tests are deterministic; events created with this
# timestamp are 0 seconds old and fall within all time windows.
NOW_MS = 1700000000000


def make_event(
    customer_id="cust_001",
    account_id="merchant_001",
    event_type="email_opened",
    timestamp_ms=NOW_MS,
    campaign_id=None,
    product_id=None,
    product_category=None,
    amount=None,
    email_subject=None,
):
    """Create a test event matching the Avro schema."""
    return {
        "event_id": "test-uuid",
        "customer_id": customer_id,
        "account_id": account_id,
        "event_type": event_type,
        "timestamp": timestamp_ms,
        "properties": {
            "campaign_id": campaign_id,
            "product_id": product_id,
            "product_category": product_category,
            "amount": amount,
            "email_subject": email_subject,
        },
    }


# ---------------------------------------------------------------------------
# Customer aggregation fan-out tests
# ---------------------------------------------------------------------------


class TestFanOut:
    """Test that a single event fans out to multiple counter updates."""

    def test_basic_event_produces_multiple_counters(self):
        event = make_event(event_type="email_opened")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        # 5 base counters (1h, 24h, 3d, 7d, 30d) + 5 total_events = 10
        assert len(counters) >= 10

    def test_fan_out_increases_with_campaign_id(self):
        event_no_campaign = make_event(event_type="email_opened")
        event_with_campaign = make_event(event_type="email_opened", campaign_id="camp_001")

        counters_no = compute_fan_out_counters(event_no_campaign, now_ms=NOW_MS)
        counters_with = compute_fan_out_counters(event_with_campaign, now_ms=NOW_MS)

        # Campaign counters add one per time window
        assert len(counters_with) == len(counters_no) + len(TIME_WINDOWS)

    def test_fan_out_increases_with_product_category(self):
        event_no_cat = make_event(event_type="page_viewed")
        event_with_cat = make_event(event_type="page_viewed", product_category="electronics")

        counters_no = compute_fan_out_counters(event_no_cat, now_ms=NOW_MS)
        counters_with = compute_fan_out_counters(event_with_cat, now_ms=NOW_MS)

        assert len(counters_with) == len(counters_no) + len(TIME_WINDOWS)

    def test_fan_out_with_both_campaign_and_category(self):
        event = make_event(
            event_type="link_clicked",
            campaign_id="camp_002",
            product_category="shoes",
        )
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        # 5 base + 5 campaign + 5 category + 5 total_events = 20
        assert len(counters) == 20

    def test_purchase_event_adds_amount_counters(self):
        event = make_event(event_type="purchase_made", amount=99.99)
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        # Should include purchase_amount accumulators
        assert "purchase_amount_1h" in metric_names
        assert "purchase_amount_7d" in metric_names
        assert "purchase_amount_30d" in metric_names

    def test_non_purchase_no_amount_counters(self):
        event = make_event(event_type="email_opened")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]
        assert not any(name.startswith("purchase_amount_") for name in metric_names)


class TestMetricNaming:
    """Test that metric names follow the expected convention."""

    def test_base_metric_names(self):
        event = make_event(event_type="email_opened")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        assert "email_opened_1h" in metric_names
        assert "email_opened_24h" in metric_names
        assert "email_opened_7d" in metric_names
        assert "email_opened_30d" in metric_names

    def test_campaign_metric_names(self):
        event = make_event(event_type="email_opened", campaign_id="camp_001")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        assert "email_opened_campaign_camp_001_1h" in metric_names
        assert "email_opened_campaign_camp_001_7d" in metric_names
        assert "email_opened_campaign_camp_001_30d" in metric_names

    def test_category_metric_names(self):
        event = make_event(event_type="purchase_made", product_category="electronics", amount=50.0)
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        assert "purchase_made_category_electronics_7d" in metric_names
        assert "purchase_made_category_electronics_30d" in metric_names

    def test_total_events_counters(self):
        event = make_event(event_type="page_viewed")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        assert "total_events_1h" in metric_names
        assert "total_events_7d" in metric_names
        assert "total_events_30d" in metric_names

    def test_all_event_types_produce_counters(self):
        for event_type in ["email_opened", "link_clicked", "purchase_made",
                           "page_viewed", "cart_abandoned"]:
            event = make_event(event_type=event_type)
            counters = compute_fan_out_counters(event, now_ms=NOW_MS)
            metric_names = [c[0] for c in counters]
            assert f"{event_type}_7d" in metric_names


class TestCounterValues:
    """Test that counter values are correct."""

    def test_base_counter_value_is_1(self):
        event = make_event(event_type="email_opened")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        for name, value in counters:
            if name.startswith("email_opened_") and "campaign" not in name:
                assert value == 1.0

    def test_purchase_amount_is_event_amount(self):
        event = make_event(event_type="purchase_made", amount=123.45)
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        amount_counters = [(n, v) for n, v in counters if n.startswith("purchase_amount_")]
        for name, value in amount_counters:
            assert value == 123.45


# ---------------------------------------------------------------------------
# Batch fan-out consistency tests
# ---------------------------------------------------------------------------


class TestBatchFanOutConsistency:
    """Test that batch_aggregation.compute_fan_out_counters matches the Flink version."""

    def test_batch_and_flink_produce_same_counters(self):
        event = make_event(event_type="email_opened", campaign_id="camp_003")
        flink_counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        batch_counters = batch_compute_fan_out_counters(event, now_ms=NOW_MS)

        flink_names = sorted([c[0] for c in flink_counters])
        batch_names = sorted([c[0] for c in batch_counters])
        assert flink_names == batch_names

    def test_batch_purchase_matches_flink(self):
        event = make_event(event_type="purchase_made", amount=200.0, product_category="hats")
        flink_counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        batch_counters = batch_compute_fan_out_counters(event, now_ms=NOW_MS)

        flink_dict = dict(flink_counters)
        batch_dict = dict(batch_counters)
        assert flink_dict == batch_dict


# ---------------------------------------------------------------------------
# Account aggregation tests
# ---------------------------------------------------------------------------


class TestAccountAggregation:
    """Test account-level metrics computation."""

    def test_events_per_hour_counts_all_events(self):
        events = {
            "merchant_001": [
                make_event(event_type="email_opened"),
                make_event(event_type="page_viewed"),
                make_event(event_type="purchase_made", amount=10.0),
            ]
        }
        metrics = compute_account_metrics(events)
        metric_dict = dict(metrics["merchant_001"])
        assert metric_dict["events_per_hour"] == 3.0

    def test_sends_today_counts_email_opened(self):
        events = {
            "merchant_001": [
                make_event(event_type="email_opened"),
                make_event(event_type="email_opened"),
                make_event(event_type="page_viewed"),
            ]
        }
        metrics = compute_account_metrics(events)
        metric_dict = dict(metrics["merchant_001"])
        assert metric_dict["sends_today"] == 2.0

    def test_active_customers_counts_distinct(self):
        events = {
            "merchant_001": [
                make_event(customer_id="cust_001", event_type="page_viewed"),
                make_event(customer_id="cust_001", event_type="email_opened"),
                make_event(customer_id="cust_002", event_type="page_viewed"),
                make_event(customer_id="cust_003", event_type="purchase_made", amount=5.0),
            ]
        }
        metrics = compute_account_metrics(events)
        metric_dict = dict(metrics["merchant_001"])
        assert metric_dict["active_customers_7d"] == 3.0

    def test_revenue_sums_purchase_amounts(self):
        events = {
            "merchant_001": [
                make_event(event_type="purchase_made", amount=50.0),
                make_event(event_type="purchase_made", amount=25.50),
                make_event(event_type="email_opened"),
            ]
        }
        metrics = compute_account_metrics(events)
        metric_dict = dict(metrics["merchant_001"])
        assert metric_dict["revenue_7d"] == 75.50

    def test_revenue_zero_without_purchases(self):
        events = {
            "merchant_001": [
                make_event(event_type="email_opened"),
                make_event(event_type="page_viewed"),
            ]
        }
        metrics = compute_account_metrics(events)
        metric_dict = dict(metrics["merchant_001"])
        assert metric_dict["revenue_7d"] == 0.0

    def test_multiple_accounts_isolated(self):
        events = {
            "merchant_001": [
                make_event(account_id="merchant_001", event_type="purchase_made", amount=100.0),
            ],
            "merchant_002": [
                make_event(account_id="merchant_002", event_type="email_opened"),
                make_event(account_id="merchant_002", event_type="email_opened"),
            ],
        }
        metrics = compute_account_metrics(events)
        assert "merchant_001" in metrics
        assert "merchant_002" in metrics

        m1 = dict(metrics["merchant_001"])
        m2 = dict(metrics["merchant_002"])

        assert m1["revenue_7d"] == 100.0
        assert m2["revenue_7d"] == 0.0
        assert m1["events_per_hour"] == 1.0
        assert m2["events_per_hour"] == 2.0


# ---------------------------------------------------------------------------
# Isolation tests
# ---------------------------------------------------------------------------


class TestIsolation:
    """Test that customer and account aggregation are separate concerns."""

    def test_customer_counters_keyed_by_customer_id(self):
        """Customer counters should be per-customer, not per-account."""
        event = make_event(customer_id="cust_001", account_id="merchant_001")
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        assert len(counters) > 0
        metric_names = [c[0] for c in counters]
        assert "events_per_hour" not in metric_names
        assert "sends_today" not in metric_names
        assert "active_customers_7d" not in metric_names
        assert "revenue_7d" not in metric_names

    def test_old_event_excluded_from_short_windows(self):
        """An event 10 days old should appear in 30d but NOT in 7d/3d/24h/1h."""
        ten_days_ago = NOW_MS - (10 * 86400 * 1000)
        event = make_event(event_type="email_opened", timestamp_ms=ten_days_ago)
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        metric_names = [c[0] for c in counters]

        assert "email_opened_30d" in metric_names
        assert "email_opened_7d" not in metric_names
        assert "email_opened_3d" not in metric_names
        assert "email_opened_24h" not in metric_names
        assert "email_opened_1h" not in metric_names

    def test_very_old_event_produces_no_counters(self):
        """An event older than 30 days should produce zero counters."""
        sixty_days_ago = NOW_MS - (60 * 86400 * 1000)
        event = make_event(event_type="email_opened", timestamp_ms=sixty_days_ago)
        counters = compute_fan_out_counters(event, now_ms=NOW_MS)
        assert len(counters) == 0

    def test_account_metrics_are_account_level(self):
        """Account metrics should not include per-customer fan-out counters."""
        events = {
            "merchant_001": [make_event(event_type="email_opened")]
        }
        metrics = compute_account_metrics(events)
        metric_names = [m[0] for m in metrics["merchant_001"]]
        # Account metrics should only have the four defined metrics
        assert set(metric_names) == {
            "events_per_hour", "sends_today", "active_customers_7d", "revenue_7d"
        }
        # Should NOT have fan-out counters like email_opened_7d
        assert "email_opened_7d" not in metric_names
