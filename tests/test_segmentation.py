"""Tests for the segment evaluation worker (SIP two-phase pattern)."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "segment_worker"))

from segment_evaluator import (
    SEGMENT_RULES,
    phase1_scan,
    phase2_evaluate,
    write_segment_membership,
)


class TestSegmentRules:
    def test_seven_segments_defined(self):
        assert len(SEGMENT_RULES) == 7

    def test_expected_segments_present(self):
        expected = [
            "high_engagers", "recent_purchasers", "at_risk",
            "active_browsers", "cart_abandoners", "vip_lapsed",
            "discount_sensitive",
        ]
        for seg in expected:
            assert seg in SEGMENT_RULES, f"Missing segment: {seg}"

    def test_all_rules_have_query(self):
        for name, rule in SEGMENT_RULES.items():
            assert "query" in rule, f"Segment {name} missing 'query'"
            assert len(rule["query"].strip()) > 0

    def test_queries_reference_customer_counters(self):
        for name, rule in SEGMENT_RULES.items():
            assert "customer_counters" in rule["query"], (
                f"Segment {name} query should reference customer_counters"
            )

    def test_queries_use_final(self):
        for name, rule in SEGMENT_RULES.items():
            assert "FINAL" in rule["query"], (
                f"Segment {name} query should use FINAL for ReplacingMergeTree"
            )


class TestPhase1Scan:
    @patch("segment_evaluator.ch_query")
    def test_returns_tuples(self, mock_ch):
        mock_ch.return_value = [
            {"account_id": "merchant_001", "customer_id": "cust_001"},
            {"account_id": "merchant_002", "customer_id": "cust_002"},
        ]
        result = phase1_scan()
        assert len(result) == 2
        assert result[0] == ("merchant_001", "cust_001")

    @patch("segment_evaluator.ch_query")
    def test_empty_result(self, mock_ch):
        mock_ch.return_value = []
        result = phase1_scan()
        assert result == []


class TestPhase2Evaluate:
    @patch("segment_evaluator.ch_query")
    def test_evaluates_all_segments(self, mock_ch):
        mock_ch.return_value = [
            {"account_id": "merchant_001", "customer_id": "cust_001"},
        ]
        profiles = [("merchant_001", "cust_001")]
        results = phase2_evaluate(profiles)

        segment_names = {r["segment_name"] for r in results}
        assert len(segment_names) == 7

    @patch("segment_evaluator.ch_query")
    def test_in_segment_when_matching(self, mock_ch):
        mock_ch.return_value = [
            {"account_id": "merchant_001", "customer_id": "cust_001"},
        ]
        profiles = [("merchant_001", "cust_001")]
        results = phase2_evaluate(profiles)

        for r in results:
            assert r["in_segment"] in (0, 1)

    @patch("segment_evaluator.ch_query")
    def test_empty_profiles_returns_empty(self, mock_ch):
        results = phase2_evaluate([])
        assert results == []
        mock_ch.assert_not_called()

    @patch("segment_evaluator.ch_query")
    def test_result_structure(self, mock_ch):
        mock_ch.return_value = []
        profiles = [("merchant_001", "cust_001")]
        results = phase2_evaluate(profiles)

        for r in results:
            assert "account_id" in r
            assert "customer_id" in r
            assert "segment_name" in r
            assert "in_segment" in r
            assert "evaluated_at" in r


class TestWriteSegmentMembership:
    @patch("segment_evaluator.requests.post")
    def test_writes_batches(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        results = [
            {
                "account_id": "merchant_001",
                "customer_id": f"cust_{i:04d}",
                "segment_name": "high_engagers",
                "in_segment": 1,
                "evaluated_at": "2024-01-01 00:00:00",
            }
            for i in range(10)
        ]
        written = write_segment_membership(results)
        assert written == 10
        assert mock_post.called

    @patch("segment_evaluator.requests.post")
    def test_empty_results(self, mock_post):
        written = write_segment_membership([])
        assert written == 0
        mock_post.assert_not_called()

    @patch("segment_evaluator.requests.post")
    def test_handles_write_error(self, mock_post):
        mock_post.return_value = MagicMock(status_code=500, text="Internal error")
        results = [
            {
                "account_id": "merchant_001",
                "customer_id": "cust_001",
                "segment_name": "high_engagers",
                "in_segment": 1,
                "evaluated_at": "2024-01-01 00:00:00",
            }
        ]
        written = write_segment_membership(results)
        assert written == 0
