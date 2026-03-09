"""Tests for the decision engine — arms, bandit, and reward logger."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock redis before importing decision engine modules.
mock_redis_module = MagicMock()
sys.modules["redis"] = mock_redis_module

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "decision_engine"))

from arms import (
    ARMS,
    REWARD_EMAIL_OPENED,
    REWARD_NO_ACTION,
    REWARD_PURCHASE,
    REWARD_UNSUBSCRIBED,
    REWARD_WINDOW_HOURS,
)
from bandit import FEATURE_KEYS, ContextualBandit
from reward_logger import compute_reward, process_rewards


# ---------------------------------------------------------------------------
# Arms tests
# ---------------------------------------------------------------------------

class TestArms:
    def test_five_arms_defined(self):
        assert len(ARMS) == 5

    def test_all_arms_are_strings(self):
        for arm in ARMS:
            assert isinstance(arm, str)

    def test_expected_arms_present(self):
        assert "email_no_offer" in ARMS
        assert "email_10pct_discount" in ARMS
        assert "email_free_shipping" in ARMS
        assert "sms_nudge" in ARMS
        assert "no_send" in ARMS

    def test_no_duplicate_arms(self):
        assert len(ARMS) == len(set(ARMS))


class TestRewardSignals:
    def test_purchase_reward(self):
        assert REWARD_PURCHASE == 1.0

    def test_email_opened_reward(self):
        assert REWARD_EMAIL_OPENED == 0.1

    def test_unsubscribed_reward(self):
        assert REWARD_UNSUBSCRIBED == -0.5

    def test_no_action_reward(self):
        assert REWARD_NO_ACTION == 0.0

    def test_reward_window(self):
        assert REWARD_WINDOW_HOURS == 72

    def test_purchase_is_strongest_positive(self):
        assert REWARD_PURCHASE > REWARD_EMAIL_OPENED > REWARD_NO_ACTION

    def test_unsubscribed_is_negative(self):
        assert REWARD_UNSUBSCRIBED < REWARD_NO_ACTION


# ---------------------------------------------------------------------------
# Bandit tests
# ---------------------------------------------------------------------------

class TestThompsonSampling:
    @pytest.fixture
    def bandit(self):
        """Create a bandit with mocked Redis."""
        mock_redis_instance = MagicMock()
        mock_redis_instance.hgetall.return_value = {
            "email_open_rate_7d": "0.4",
            "email_open_rate_30d": "0.35",
            "purchase_count_30d": "3",
            "days_since_last_purchase": "5",
            "avg_order_value": "75.0",
            "cart_abandon_rate_30d": "0.1",
            "churn_risk_score": "0.2",
            "discount_sensitivity": "0.3",
        }
        mock_redis_module.Redis.return_value = mock_redis_instance
        return ContextualBandit(redis_host="localhost", redis_port=6379)

    def test_sampling_returns_float(self, bandit):
        samples = bandit._thompson_sample()
        for arm in ARMS:
            assert isinstance(samples[arm], float)

    def test_sampling_values_between_0_and_1(self, bandit):
        for _ in range(100):
            samples = bandit._thompson_sample()
            for arm in ARMS:
                assert 0.0 <= samples[arm] <= 1.0

    def test_biased_sampling_with_asymmetric_params(self, bandit):
        """An arm with high alpha should sample higher on average."""
        bandit.alpha["email_10pct_discount"] = 100.0
        bandit.beta_param["email_10pct_discount"] = 1.0
        bandit.alpha["no_send"] = 1.0
        bandit.beta_param["no_send"] = 100.0

        discount_sum = 0.0
        nosend_sum = 0.0
        trials = 1000
        for _ in range(trials):
            samples = bandit._thompson_sample()
            discount_sum += samples["email_10pct_discount"]
            nosend_sum += samples["no_send"]

        assert discount_sum / trials > nosend_sum / trials

    def test_update_beta_positive_reward(self, bandit):
        old_alpha = bandit.alpha["email_no_offer"]
        bandit.update_beta("email_no_offer", 1.0)
        assert bandit.alpha["email_no_offer"] == old_alpha + 1.0

    def test_update_beta_negative_reward(self, bandit):
        old_beta = bandit.beta_param["sms_nudge"]
        bandit.update_beta("sms_nudge", -0.5)
        assert bandit.beta_param["sms_nudge"] == old_beta + 0.5

    def test_update_beta_zero_reward(self, bandit):
        old_beta = bandit.beta_param["no_send"]
        bandit.update_beta("no_send", 0.0)
        # Zero reward increments beta by 1.0 (failure).
        assert bandit.beta_param["no_send"] == old_beta + 1.0

    def test_update_beta_unknown_arm(self, bandit):
        # Should not raise.
        bandit.update_beta("nonexistent_arm", 1.0)


# ---------------------------------------------------------------------------
# Decision flow tests
# ---------------------------------------------------------------------------

class TestDecisionFlow:
    @pytest.fixture
    def bandit(self):
        mock_redis_instance = MagicMock()
        mock_redis_instance.hgetall.return_value = {
            "email_open_rate_7d": "0.5",
            "email_open_rate_30d": "0.4",
            "purchase_count_30d": "2",
            "days_since_last_purchase": "10",
            "avg_order_value": "50.0",
            "cart_abandon_rate_30d": "0.2",
            "churn_risk_score": "0.3",
            "discount_sensitivity": "0.6",
        }
        mock_redis_module.Redis.return_value = mock_redis_instance
        return ContextualBandit(redis_host="localhost", redis_port=6379)

    def test_decide_returns_dict(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        assert isinstance(result, dict)

    def test_decide_has_arm(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        assert "arm" in result
        assert result["arm"] in ARMS

    def test_decide_has_score(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        assert "score" in result
        assert isinstance(result["score"], float)

    def test_decide_has_reasoning(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        assert "reasoning" in result
        reasoning = result["reasoning"]
        assert "linear_scores" in reasoning
        assert "thompson_samples" in reasoning
        assert "combined_scores" in reasoning
        assert "feature_vector" in reasoning

    def test_decide_with_context(self, bandit):
        result = bandit.decide("cust_001", "merchant_001", context="cart_abandoned")
        assert result["reasoning"]["context"] == "cart_abandoned"

    def test_decide_reasoning_has_all_arms(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        for arm in ARMS:
            assert arm in result["reasoning"]["linear_scores"]
            assert arm in result["reasoning"]["thompson_samples"]
            assert arm in result["reasoning"]["combined_scores"]

    def test_decide_feature_vector_has_all_keys(self, bandit):
        result = bandit.decide("cust_001", "merchant_001")
        fv = result["reasoning"]["feature_vector"]
        for key in FEATURE_KEYS:
            assert key in fv

    def test_decide_with_missing_features(self):
        """Customer with no features in Redis should still get a decision."""
        mock_redis_instance = MagicMock()
        mock_redis_instance.hgetall.return_value = {}
        mock_redis_module.Redis.return_value = mock_redis_instance

        b = ContextualBandit(redis_host="localhost", redis_port=6379)
        result = b.decide("cust_999", "merchant_001")
        assert result["arm"] in ARMS


# ---------------------------------------------------------------------------
# Weight update tests
# ---------------------------------------------------------------------------

class TestWeightUpdate:
    @pytest.fixture
    def bandit(self):
        mock_redis_instance = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis_instance
        return ContextualBandit(redis_host="localhost", redis_port=6379)

    def test_update_with_no_data(self, bandit):
        result = bandit.update_weights([])
        assert result["updated"] is False

    def test_update_with_valid_data(self, bandit):
        import numpy as np

        now_ms = time.time() * 1000
        data = []
        rng = np.random.default_rng(seed=99)
        for _ in range(50):
            data.append({
                "arm": "email_10pct_discount",
                "features": rng.random(len(FEATURE_KEYS)).tolist(),
                "reward": float(rng.choice([0.0, 0.1, 1.0])),
                "timestamp": now_ms - rng.integers(0, 48 * 3600 * 1000),
            })

        result = bandit.update_weights(data)
        assert result["updated"] is True
        assert "weight_deltas" in result
        assert "email_10pct_discount" in result["weight_deltas"]

    def test_update_ignores_unknown_arms(self, bandit):
        import numpy as np

        data = [
            {
                "arm": "unknown_arm",
                "features": [0.0] * len(FEATURE_KEYS),
                "reward": 1.0,
                "timestamp": time.time() * 1000,
            }
        ]
        result = bandit.update_weights(data)
        # Only one entry for an unknown arm => no_data essentially.
        assert result["updated"] is True


# ---------------------------------------------------------------------------
# Arm stats tests
# ---------------------------------------------------------------------------

class TestArmStats:
    @pytest.fixture
    def bandit(self):
        mock_redis_instance = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis_instance
        return ContextualBandit(redis_host="localhost", redis_port=6379)

    def test_get_arm_stats_has_all_arms(self, bandit):
        stats = bandit.get_arm_stats()
        for arm in ARMS:
            assert arm in stats

    def test_arm_stats_structure(self, bandit):
        stats = bandit.get_arm_stats()
        for arm in ARMS:
            assert "weights" in stats[arm]
            assert "alpha" in stats[arm]
            assert "beta" in stats[arm]
            assert "expected_thompson" in stats[arm]
            assert isinstance(stats[arm]["weights"], list)
            assert len(stats[arm]["weights"]) == len(FEATURE_KEYS)

    def test_initial_expected_thompson_is_half(self, bandit):
        stats = bandit.get_arm_stats()
        for arm in ARMS:
            # Beta(1,1) => expected = 1/(1+1) = 0.5
            assert stats[arm]["expected_thompson"] == 0.5


# ---------------------------------------------------------------------------
# Reward logger tests
# ---------------------------------------------------------------------------

class TestRewardClosure:
    @patch("reward_logger.check_unsubscribed", return_value=False)
    @patch("reward_logger.check_email_opened", return_value=False)
    @patch("reward_logger.check_purchase", return_value=True)
    def test_compute_reward_purchase(self, mock_purch, mock_email, mock_unsub):
        reward = compute_reward("cust_001", "merchant_001", "2024-01-01 00:00:00")
        assert reward == REWARD_PURCHASE

    @patch("reward_logger.check_unsubscribed", return_value=False)
    @patch("reward_logger.check_email_opened", return_value=True)
    @patch("reward_logger.check_purchase", return_value=False)
    def test_compute_reward_email_opened(self, mock_purch, mock_email, mock_unsub):
        reward = compute_reward("cust_001", "merchant_001", "2024-01-01 00:00:00")
        assert reward == REWARD_EMAIL_OPENED

    @patch("reward_logger.check_unsubscribed", return_value=True)
    @patch("reward_logger.check_email_opened", return_value=False)
    @patch("reward_logger.check_purchase", return_value=True)
    def test_compute_reward_unsubscribe_takes_priority(
        self, mock_purch, mock_email, mock_unsub
    ):
        reward = compute_reward("cust_001", "merchant_001", "2024-01-01 00:00:00")
        assert reward == REWARD_UNSUBSCRIBED

    @patch("reward_logger.check_unsubscribed", return_value=False)
    @patch("reward_logger.check_email_opened", return_value=False)
    @patch("reward_logger.check_purchase", return_value=False)
    def test_compute_reward_no_action(self, mock_purch, mock_email, mock_unsub):
        reward = compute_reward("cust_001", "merchant_001", "2024-01-01 00:00:00")
        assert reward == REWARD_NO_ACTION

    @patch("reward_logger.write_reward")
    @patch("reward_logger.compute_reward", return_value=1.0)
    @patch("reward_logger.fetch_pending_decisions")
    def test_process_rewards_count(self, mock_fetch, mock_compute, mock_write):
        mock_fetch.return_value = [
            {
                "customer_id": "cust_001",
                "account_id": "merchant_001",
                "arm_chosen": "email_10pct_discount",
                "decided_at": "2024-01-01 12:00:00",
                "feature_snapshot": "{}",
            },
            {
                "customer_id": "cust_002",
                "account_id": "merchant_001",
                "arm_chosen": "no_send",
                "decided_at": "2024-01-01 12:05:00",
                "feature_snapshot": "{}",
            },
        ]
        count = process_rewards()
        assert count == 2
        assert mock_write.call_count == 2

    @patch("reward_logger.fetch_pending_decisions")
    def test_process_rewards_empty(self, mock_fetch):
        mock_fetch.return_value = []
        count = process_rewards()
        assert count == 0
