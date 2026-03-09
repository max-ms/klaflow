"""Tests for the event producer."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Mock confluent_kafka before importing producer
sys.modules["confluent_kafka"] = MagicMock()
sys.modules["confluent_kafka.schema_registry"] = MagicMock()
sys.modules["confluent_kafka.schema_registry.avro"] = MagicMock()
sys.modules["confluent_kafka.serialization"] = MagicMock()

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "producer"))

from producer import (
    ACCOUNTS,
    ALL_CUSTOMER_IDS,
    CAMPAIGN_IDS,
    EMAIL_SUBJECTS,
    EVENT_LAMBDAS,
    EVENT_TYPES,
    LIFECYCLE_RATE_MULTIPLIER,
    LIFECYCLE_STATES,
    PRODUCT_CATEGORIES,
    PRODUCT_IDS,
    TOTAL_CUSTOMERS,
    build_all_customers,
    build_customer_id,
    generate_daily_events_for_customer,
    generate_event,
    generate_properties,
    transition_lifecycle,
)


# -----------------------------------------------------------------------
# Account structure
# -----------------------------------------------------------------------

class TestAccounts:
    def test_five_accounts(self):
        assert len(ACCOUNTS) == 5

    def test_account_ids(self):
        for i in range(1, 6):
            assert f"merchant_{i:03d}" in ACCOUNTS

    def test_account_sizes(self):
        assert ACCOUNTS["merchant_001"] == 1_000
        assert ACCOUNTS["merchant_002"] == 5_000
        assert ACCOUNTS["merchant_003"] == 500
        assert ACCOUNTS["merchant_004"] == 10_000
        assert ACCOUNTS["merchant_005"] == 2_000

    def test_total_customers(self):
        assert TOTAL_CUSTOMERS == 18_500

    def test_all_customer_ids_count(self):
        assert len(ALL_CUSTOMER_IDS) == 18_500


# -----------------------------------------------------------------------
# Customer ID format
# -----------------------------------------------------------------------

class TestCustomerIdFormat:
    def test_format_includes_account(self):
        cid = build_customer_id("merchant_001", 42)
        assert cid == "merchant_001:cust_0042"

    def test_all_ids_have_colon_separator(self):
        for cid in ALL_CUSTOMER_IDS[:100]:
            assert ":" in cid
            parts = cid.split(":")
            assert len(parts) == 2
            assert parts[0].startswith("merchant_")
            assert parts[1].startswith("cust_")

    def test_ids_per_account(self):
        for account_id, count in ACCOUNTS.items():
            matching = [c for c in ALL_CUSTOMER_IDS if c.startswith(f"{account_id}:")]
            assert len(matching) == count, f"{account_id} should have {count} customers"


# -----------------------------------------------------------------------
# Event generation
# -----------------------------------------------------------------------

class TestGenerateEvent:
    def test_returns_dict(self):
        event = generate_event()
        assert isinstance(event, dict)

    def test_has_required_fields(self):
        event = generate_event()
        assert "event_id" in event
        assert "customer_id" in event
        assert "account_id" in event
        assert "event_type" in event
        assert "timestamp" in event
        assert "properties" in event

    def test_account_id_present(self):
        """account_id is required on every event per CLAUDE.md."""
        for _ in range(100):
            event = generate_event()
            assert event["account_id"] is not None
            assert event["account_id"].startswith("merchant_")

    def test_account_id_matches_customer_id(self):
        """account_id should be consistent with customer_id prefix."""
        for _ in range(100):
            event = generate_event()
            expected_account = event["customer_id"].rsplit(":", 1)[0]
            assert event["account_id"] == expected_account

    def test_event_id_is_uuid(self):
        import uuid
        event = generate_event()
        uuid.UUID(event["event_id"])  # Raises if invalid

    def test_customer_id_from_pool(self):
        event = generate_event()
        assert event["customer_id"] in ALL_CUSTOMER_IDS

    def test_event_type_valid(self):
        event = generate_event()
        assert event["event_type"] in EVENT_TYPES

    def test_timestamp_is_positive_int(self):
        event = generate_event()
        assert isinstance(event["timestamp"], int)
        assert event["timestamp"] > 0

    def test_specific_customer_id(self):
        event = generate_event(customer_id="merchant_001:cust_0042", account_id="merchant_001")
        assert event["customer_id"] == "merchant_001:cust_0042"
        assert event["account_id"] == "merchant_001"

    def test_specific_timestamp(self):
        event = generate_event(timestamp_ms=1700000000000)
        assert event["timestamp"] == 1700000000000

    def test_specific_event_type(self):
        event = generate_event(event_type="purchase_made")
        assert event["event_type"] == "purchase_made"


# -----------------------------------------------------------------------
# Event distribution (Poisson-weighted)
# -----------------------------------------------------------------------

class TestEventDistribution:
    def test_poisson_weighted_distribution(self):
        """Event distribution follows Poisson lambdas: page_viewed most common,
        purchase_made least common."""
        counts = {et: 0 for et in EVENT_TYPES}
        for _ in range(10_000):
            event = generate_event()
            counts[event["event_type"]] += 1

        # page_viewed (lambda=3.0) should dominate
        assert counts["page_viewed"] > counts["email_opened"], \
            f"page_viewed ({counts['page_viewed']}) should exceed email_opened ({counts['email_opened']})"
        # email_opened (0.4) > cart_abandoned (0.15)
        assert counts["email_opened"] > counts["cart_abandoned"]
        # purchase_made (0.05) should be rarest
        assert counts["purchase_made"] < counts["email_opened"]
        assert counts["purchase_made"] < counts["page_viewed"]

    def test_lambdas_defined_for_all_types(self):
        for et in EVENT_TYPES:
            assert et in EVENT_LAMBDAS
            assert EVENT_LAMBDAS[et] > 0


# -----------------------------------------------------------------------
# Properties
# -----------------------------------------------------------------------

class TestGenerateProperties:
    def test_email_has_campaign_and_subject(self):
        props = generate_properties("email_opened")
        assert props["campaign_id"] is not None
        assert props["campaign_id"] in CAMPAIGN_IDS
        assert props["email_subject"] is not None
        assert props["email_subject"] in EMAIL_SUBJECTS

    def test_link_clicked_has_campaign_and_subject(self):
        props = generate_properties("link_clicked")
        assert props["campaign_id"] is not None
        assert props["email_subject"] is not None

    def test_purchase_has_amount_and_product_category(self):
        props = generate_properties("purchase_made")
        assert props["amount"] is not None
        assert props["amount"] > 0
        assert props["product_id"] is not None
        assert props["product_id"] in PRODUCT_IDS
        assert props["product_category"] is not None
        assert props["product_category"] in PRODUCT_CATEGORIES

    def test_cart_abandoned_has_product_category(self):
        props = generate_properties("cart_abandoned")
        assert props["product_category"] is not None
        assert props["product_category"] in PRODUCT_CATEGORIES
        assert props["amount"] is not None

    def test_page_view_has_product_category(self):
        props = generate_properties("page_viewed")
        assert props["product_category"] is not None
        assert props["product_category"] in PRODUCT_CATEGORIES
        assert props["amount"] is None
        assert props["campaign_id"] is None

    def test_all_property_keys_present(self):
        """Every event should have all 5 property keys (some null)."""
        for et in EVENT_TYPES:
            props = generate_properties(et)
            assert "campaign_id" in props
            assert "product_id" in props
            assert "product_category" in props
            assert "amount" in props
            assert "email_subject" in props


# -----------------------------------------------------------------------
# Lifecycle state machine
# -----------------------------------------------------------------------

class TestLifecycleStateMachine:
    def test_initial_state_is_new(self):
        customers = build_all_customers()
        for c in customers[:100]:
            assert c["lifecycle_state"] == "new"

    def test_new_to_active_transition(self):
        """With enough trials, new -> active should happen ~30% of the time."""
        transitioned = 0
        trials = 1000
        for _ in range(trials):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "new", "purchased_this_week": False}
            transition_lifecycle(c)
            if c["lifecycle_state"] == "active":
                transitioned += 1
        # Should be around 300 +/- some variance
        assert 150 < transitioned < 450, f"new->active: {transitioned}/{trials}"

    def test_active_to_at_risk_no_purchase(self):
        """active -> at_risk at ~10% rate when no purchase."""
        transitioned = 0
        trials = 1000
        for _ in range(trials):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "active", "purchased_this_week": False}
            transition_lifecycle(c)
            if c["lifecycle_state"] == "at_risk":
                transitioned += 1
        assert 30 < transitioned < 200, f"active->at_risk: {transitioned}/{trials}"

    def test_purchase_prevents_active_to_at_risk(self):
        """If purchased this week, active should NOT transition to at_risk."""
        for _ in range(200):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "active", "purchased_this_week": True}
            transition_lifecycle(c)
            assert c["lifecycle_state"] == "active"

    def test_purchase_reverses_at_risk_to_active(self):
        """at_risk + purchase -> active."""
        c = {"customer_id": "test", "account_id": "test",
             "lifecycle_state": "at_risk", "purchased_this_week": True}
        transition_lifecycle(c)
        assert c["lifecycle_state"] == "active"

    def test_at_risk_to_lapsed(self):
        transitioned = 0
        trials = 1000
        for _ in range(trials):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "at_risk", "purchased_this_week": False}
            transition_lifecycle(c)
            if c["lifecycle_state"] == "lapsed":
                transitioned += 1
        assert 100 < transitioned < 350, f"at_risk->lapsed: {transitioned}/{trials}"

    def test_lapsed_to_churned(self):
        transitioned = 0
        trials = 1000
        for _ in range(trials):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "lapsed", "purchased_this_week": False}
            transition_lifecycle(c)
            if c["lifecycle_state"] == "churned":
                transitioned += 1
        assert 30 < transitioned < 200, f"lapsed->churned: {transitioned}/{trials}"

    def test_churned_stays_churned(self):
        for _ in range(100):
            c = {"customer_id": "test", "account_id": "test",
                 "lifecycle_state": "churned", "purchased_this_week": False}
            transition_lifecycle(c)
            assert c["lifecycle_state"] == "churned"

    def test_purchased_flag_resets_after_transition(self):
        c = {"customer_id": "test", "account_id": "test",
             "lifecycle_state": "active", "purchased_this_week": True}
        transition_lifecycle(c)
        assert c["purchased_this_week"] is False


# -----------------------------------------------------------------------
# Lifecycle affects event rates
# -----------------------------------------------------------------------

class TestLifecycleEventRates:
    def test_churned_generates_no_events(self):
        customer = {
            "customer_id": "merchant_001:cust_0001",
            "account_id": "merchant_001",
            "lifecycle_state": "churned",
            "purchased_this_week": False,
        }
        events = generate_daily_events_for_customer(customer, 1700000000000, 86_400_000)
        assert len(events) == 0

    def test_active_generates_events(self):
        """Active customers should generate events most of the time."""
        customer = {
            "customer_id": "merchant_001:cust_0001",
            "account_id": "merchant_001",
            "lifecycle_state": "active",
            "purchased_this_week": False,
        }
        total_events = 0
        for _ in range(10):
            events = generate_daily_events_for_customer(customer, 1700000000000, 86_400_000)
            total_events += len(events)
        # 10 days * ~3.7 events/day expected = ~37 events
        assert total_events > 10, f"Active customer should generate events, got {total_events}"

    def test_rate_multipliers_defined(self):
        for state in LIFECYCLE_STATES:
            assert state in LIFECYCLE_RATE_MULTIPLIER

    def test_churned_multiplier_is_zero(self):
        assert LIFECYCLE_RATE_MULTIPLIER["churned"] == 0.0

    def test_active_multiplier_is_highest(self):
        assert LIFECYCLE_RATE_MULTIPLIER["active"] >= LIFECYCLE_RATE_MULTIPLIER["at_risk"]
        assert LIFECYCLE_RATE_MULTIPLIER["active"] >= LIFECYCLE_RATE_MULTIPLIER["lapsed"]


# -----------------------------------------------------------------------
# Daily event generation
# -----------------------------------------------------------------------

class TestDailyEventGeneration:
    def test_events_have_correct_account(self):
        customer = {
            "customer_id": "merchant_003:cust_0042",
            "account_id": "merchant_003",
            "lifecycle_state": "active",
            "purchased_this_week": False,
        }
        events = generate_daily_events_for_customer(customer, 1700000000000, 86_400_000)
        for event in events:
            assert event["account_id"] == "merchant_003"
            assert event["customer_id"] == "merchant_003:cust_0042"

    def test_timestamps_within_day(self):
        day_start = 1700000000000
        day_ms = 86_400_000
        customer = {
            "customer_id": "merchant_001:cust_0001",
            "account_id": "merchant_001",
            "lifecycle_state": "active",
            "purchased_this_week": False,
        }
        events = generate_daily_events_for_customer(customer, day_start, day_ms)
        for event in events:
            assert day_start <= event["timestamp"] < day_start + day_ms

    def test_purchase_sets_purchased_flag(self):
        """If a purchase_made event is generated, purchased_this_week should be True."""
        customer = {
            "customer_id": "merchant_001:cust_0001",
            "account_id": "merchant_001",
            "lifecycle_state": "active",
            "purchased_this_week": False,
        }
        # Run many times until we get a purchase
        for _ in range(200):
            customer["purchased_this_week"] = False
            events = generate_daily_events_for_customer(customer, 1700000000000, 86_400_000)
            has_purchase = any(e["event_type"] == "purchase_made" for e in events)
            if has_purchase:
                assert customer["purchased_this_week"] is True
                return
        pytest.skip("No purchase generated in 200 daily runs")


# -----------------------------------------------------------------------
# Build all customers
# -----------------------------------------------------------------------

class TestBuildAllCustomers:
    def test_total_count(self):
        customers = build_all_customers()
        assert len(customers) == 18_500

    def test_per_account_count(self):
        customers = build_all_customers()
        for account_id, expected_count in ACCOUNTS.items():
            actual = sum(1 for c in customers if c["account_id"] == account_id)
            assert actual == expected_count, f"{account_id}: expected {expected_count}, got {actual}"

    def test_customer_structure(self):
        customers = build_all_customers()
        c = customers[0]
        assert "customer_id" in c
        assert "account_id" in c
        assert "lifecycle_state" in c
        assert "purchased_this_week" in c


# -----------------------------------------------------------------------
# Avro schema
# -----------------------------------------------------------------------

class TestAvroSchema:
    def test_schema_file_exists(self):
        schema_path = Path(__file__).parent.parent / "src" / "producer" / "schemas" / "customer_event.avsc"
        assert schema_path.exists()

    def test_schema_is_valid_json(self):
        schema_path = Path(__file__).parent.parent / "src" / "producer" / "schemas" / "customer_event.avsc"
        with open(schema_path) as f:
            schema = json.load(f)
        assert schema["type"] == "record"
        assert schema["name"] == "CustomerEvent"

    def test_schema_has_account_id(self):
        schema_path = Path(__file__).parent.parent / "src" / "producer" / "schemas" / "customer_event.avsc"
        with open(schema_path) as f:
            schema = json.load(f)
        field_names = [f["name"] for f in schema["fields"]]
        assert "account_id" in field_names

    def test_schema_has_all_required_fields(self):
        schema_path = Path(__file__).parent.parent / "src" / "producer" / "schemas" / "customer_event.avsc"
        with open(schema_path) as f:
            schema = json.load(f)
        field_names = [f["name"] for f in schema["fields"]]
        assert "event_id" in field_names
        assert "customer_id" in field_names
        assert "account_id" in field_names
        assert "event_type" in field_names
        assert "timestamp" in field_names
        assert "properties" in field_names

    def test_schema_properties_include_new_fields(self):
        schema_path = Path(__file__).parent.parent / "src" / "producer" / "schemas" / "customer_event.avsc"
        with open(schema_path) as f:
            schema = json.load(f)
        props_field = next(f for f in schema["fields"] if f["name"] == "properties")
        prop_names = [f["name"] for f in props_field["type"]["fields"]]
        assert "product_category" in prop_names
        assert "email_subject" in prop_names
