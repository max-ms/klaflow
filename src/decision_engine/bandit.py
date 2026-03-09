"""Thompson Sampling contextual bandit for per-customer action selection."""

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import redis

from arms import ARMS


# Feature keys used from the Redis feature vector (order matters for dot product).
FEATURE_KEYS = [
    "email_open_rate_7d",
    "email_open_rate_30d",
    "purchase_count_30d",
    "days_since_last_purchase",
    "avg_order_value",
    "cart_abandon_rate_30d",
    "churn_risk_score",
    "discount_sensitivity",
]


class ContextualBandit:
    """Thompson Sampling contextual bandit with linear scoring."""

    def __init__(
        self,
        redis_host: str = "redis-master.features.svc.cluster.local",
        redis_port: int = 6379,
        clickhouse_host: str = "clickhouse.analytics.svc.cluster.local",
        clickhouse_port: int = 8123,
    ):
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port

        self.num_features = len(FEATURE_KEYS)
        self.num_arms = len(ARMS)

        # Linear weight vectors per arm — small random initial values.
        rng = np.random.default_rng(seed=42)
        self.weights: Dict[str, np.ndarray] = {
            arm: rng.normal(0, 0.1, size=self.num_features) for arm in ARMS
        }

        # Beta distribution parameters per arm for Thompson Sampling.
        self.alpha: Dict[str, float] = {arm: 1.0 for arm in ARMS}
        self.beta_param: Dict[str, float] = {arm: 1.0 for arm in ARMS}

    def _fetch_features(self, account_id: str, customer_id: str) -> np.ndarray:
        """Fetch the customer feature vector from Redis."""
        key = f"customer:{account_id}:{customer_id}"
        raw = self.redis_client.hgetall(key)

        features = np.zeros(self.num_features)
        for i, fkey in enumerate(FEATURE_KEYS):
            val = raw.get(fkey)
            if val is not None:
                try:
                    features[i] = float(val)
                except (ValueError, TypeError):
                    features[i] = 0.0
        return features

    def _score_arms(self, features: np.ndarray) -> Dict[str, float]:
        """Compute linear score for each arm: weights[arm] dot features."""
        scores = {}
        for arm in ARMS:
            scores[arm] = float(np.dot(self.weights[arm], features))
        return scores

    def _thompson_sample(self) -> Dict[str, float]:
        """Sample from Beta(alpha, beta) for each arm."""
        samples = {}
        for arm in ARMS:
            samples[arm] = float(
                np.random.beta(self.alpha[arm], self.beta_param[arm])
            )
        return samples

    def decide(
        self,
        customer_id: str,
        account_id: str,
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Make a bandit decision for a customer.

        Returns dict with: arm, score, reasoning.
        """
        features = self._fetch_features(account_id, customer_id)
        linear_scores = self._score_arms(features)
        thompson_samples = self._thompson_sample()

        # Combined score: linear model score + Thompson exploration sample.
        combined_scores = {}
        for arm in ARMS:
            combined_scores[arm] = linear_scores[arm] + thompson_samples[arm]

        chosen_arm = max(combined_scores, key=combined_scores.get)
        chosen_score = combined_scores[chosen_arm]

        reasoning = {
            "linear_scores": linear_scores,
            "thompson_samples": thompson_samples,
            "combined_scores": combined_scores,
            "context": context,
            "feature_vector": {
                FEATURE_KEYS[i]: float(features[i])
                for i in range(self.num_features)
            },
        }

        return {
            "arm": chosen_arm,
            "score": round(chosen_score, 4),
            "reasoning": reasoning,
        }

    def update_beta(self, arm: str, reward: float) -> None:
        """Update Beta parameters for an arm based on observed reward.

        Positive reward increments alpha (successes),
        non-positive increments beta (failures).
        """
        if arm not in self.alpha:
            return
        if reward > 0:
            self.alpha[arm] += reward
        else:
            self.beta_param[arm] += abs(reward) if reward < 0 else 1.0

    def update_weights(self, reward_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Refit linear weights from reward log data.

        Each entry in reward_data should have:
            arm, features (list of floats), reward, timestamp

        Recent observations (< 24h old) are weighted 2x.
        """
        if not reward_data:
            return {"status": "no_data", "updated": False}

        old_weights = {arm: w.copy() for arm, w in self.weights.items()}
        now = time.time() * 1000  # epoch ms
        one_day_ms = 24 * 3600 * 1000

        # Group by arm.
        arm_data: Dict[str, Tuple[List[np.ndarray], List[float]]] = {
            arm: ([], []) for arm in ARMS
        }

        for entry in reward_data:
            arm = entry.get("arm")
            if arm not in ARMS:
                continue
            feats = entry.get("features")
            reward = entry.get("reward", 0.0)
            ts = entry.get("timestamp", 0)

            if feats is None or len(feats) != self.num_features:
                continue

            # Recency weight: 2x for observations < 24h old.
            weight = 2.0 if (now - ts) < one_day_ms else 1.0

            feat_arr = np.array(feats, dtype=float)
            arm_data[arm][0].append(feat_arr * weight)
            arm_data[arm][1].append(reward * weight)

        # Simple least-squares update per arm.
        for arm in ARMS:
            X_list, y_list = arm_data[arm]
            if len(X_list) < 2:
                continue

            X = np.stack(X_list)
            y = np.array(y_list)

            # Ridge regression: w = (X^T X + lambda I)^{-1} X^T y
            ridge_lambda = 1.0
            XtX = X.T @ X + ridge_lambda * np.eye(self.num_features)
            Xty = X.T @ y
            try:
                self.weights[arm] = np.linalg.solve(XtX, Xty)
            except np.linalg.LinAlgError:
                pass  # Keep existing weights if solve fails.

        # Compute weight deltas for logging.
        deltas = {}
        for arm in ARMS:
            deltas[arm] = float(np.linalg.norm(self.weights[arm] - old_weights[arm]))

        return {"status": "updated", "updated": True, "weight_deltas": deltas}

    def get_arm_stats(self) -> Dict[str, Any]:
        """Return current arm weights and Beta parameters."""
        stats = {}
        for arm in ARMS:
            stats[arm] = {
                "weights": self.weights[arm].tolist(),
                "alpha": self.alpha[arm],
                "beta": self.beta_param[arm],
                "expected_thompson": self.alpha[arm]
                / (self.alpha[arm] + self.beta_param[arm]),
            }
        return stats
