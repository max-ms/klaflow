"""Reward closure: joins decisions with outcomes in ClickHouse.

Runs as a background process on a 5-minute polling loop.
For each decision in the last 72 hours, checks whether the customer:
  - made a purchase  (strong positive reward)
  - opened an email  (weak positive reward)
  - unsubscribed     (negative reward)
"""

import logging
import time
from typing import Any, Dict, List

import requests

from arms import (
    REWARD_EMAIL_OPENED,
    REWARD_NO_ACTION,
    REWARD_PURCHASE,
    REWARD_UNSUBSCRIBED,
    REWARD_WINDOW_HOURS,
)

logger = logging.getLogger(__name__)

# ClickHouse connection defaults (in-cluster).
DEFAULT_CLICKHOUSE_HOST = "clickhouse.analytics.svc.cluster.local"
DEFAULT_CLICKHOUSE_PORT = 8123
DEFAULT_CLICKHOUSE_USER = "klaflow"
DEFAULT_CLICKHOUSE_PASSWORD = "klaflow-pass"


def _ch_query(
    query: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
    user: str = DEFAULT_CLICKHOUSE_USER,
    password: str = DEFAULT_CLICKHOUSE_PASSWORD,
) -> List[Dict[str, Any]]:
    """Execute a ClickHouse query and return rows as list of dicts."""
    url = f"http://{host}:{port}/"
    params = {
        "user": user,
        "password": password,
        "query": query + " FORMAT JSON",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", [])


def _ch_insert(
    query: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
    user: str = DEFAULT_CLICKHOUSE_USER,
    password: str = DEFAULT_CLICKHOUSE_PASSWORD,
) -> None:
    """Execute a ClickHouse INSERT statement."""
    url = f"http://{host}:{port}/"
    params = {"user": user, "password": password}
    resp = requests.post(url, params=params, data=query, timeout=30)
    resp.raise_for_status()


def fetch_pending_decisions(
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> List[Dict[str, Any]]:
    """Fetch decisions from the last 72h that have no reward logged yet."""
    query = f"""
        SELECT
            d.customer_id,
            d.account_id,
            d.arm_chosen,
            d.decided_at,
            d.feature_snapshot
        FROM decision_log d
        LEFT JOIN reward_log r
            ON d.customer_id = r.customer_id
            AND d.decided_at = r.decided_at
        WHERE d.decided_at > now() - INTERVAL {REWARD_WINDOW_HOURS} HOUR
          AND r.customer_id IS NULL
        LIMIT 1000
    """
    return _ch_query(query, host=host, port=port)


def check_purchase(
    customer_id: str,
    account_id: str,
    after_timestamp: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> bool:
    """Check if customer made a purchase after the decision timestamp."""
    query = f"""
        SELECT count() as cnt
        FROM customer_counters
        WHERE customer_id = '{customer_id}'
          AND account_id = '{account_id}'
          AND metric_name = 'purchase_made_count'
          AND computed_at > '{after_timestamp}'
    """
    rows = _ch_query(query, host=host, port=port)
    if rows and int(rows[0].get("cnt", 0)) > 0:
        return True
    return False


def check_email_opened(
    customer_id: str,
    account_id: str,
    after_timestamp: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> bool:
    """Check if customer opened an email after the decision timestamp."""
    query = f"""
        SELECT count() as cnt
        FROM customer_counters
        WHERE customer_id = '{customer_id}'
          AND account_id = '{account_id}'
          AND metric_name = 'email_opened_count'
          AND computed_at > '{after_timestamp}'
    """
    rows = _ch_query(query, host=host, port=port)
    if rows and int(rows[0].get("cnt", 0)) > 0:
        return True
    return False


def check_unsubscribed(
    customer_id: str,
    account_id: str,
    after_timestamp: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> bool:
    """Check if customer unsubscribed after the decision timestamp."""
    query = f"""
        SELECT count() as cnt
        FROM customer_counters
        WHERE customer_id = '{customer_id}'
          AND account_id = '{account_id}'
          AND metric_name = 'unsubscribed_count'
          AND computed_at > '{after_timestamp}'
    """
    rows = _ch_query(query, host=host, port=port)
    if rows and int(rows[0].get("cnt", 0)) > 0:
        return True
    return False


def compute_reward(
    customer_id: str,
    account_id: str,
    after_timestamp: str,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> float:
    """Determine the reward for a decision by checking outcomes.

    Priority order: unsubscribe (negative) > purchase (strong positive) >
    email opened (weak positive) > no action.
    """
    if check_unsubscribed(customer_id, account_id, after_timestamp, host, port):
        return REWARD_UNSUBSCRIBED
    if check_purchase(customer_id, account_id, after_timestamp, host, port):
        return REWARD_PURCHASE
    if check_email_opened(customer_id, account_id, after_timestamp, host, port):
        return REWARD_EMAIL_OPENED
    return REWARD_NO_ACTION


def write_reward(
    customer_id: str,
    account_id: str,
    arm_chosen: str,
    decided_at: str,
    reward: float,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> None:
    """Write a reward entry to the reward_log table."""
    query = f"""
        INSERT INTO reward_log
            (customer_id, account_id, arm_chosen, decided_at, reward, logged_at)
        VALUES
            ('{customer_id}', '{account_id}', '{arm_chosen}',
             '{decided_at}', {reward}, now())
    """
    _ch_insert(query, host=host, port=port)


def process_rewards(
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> int:
    """Main reward closure loop iteration.

    Fetches pending decisions, computes rewards, writes to reward_log.
    Returns number of rewards processed.
    """
    decisions = fetch_pending_decisions(host=host, port=port)
    count = 0

    for decision in decisions:
        customer_id = decision["customer_id"]
        account_id = decision["account_id"]
        arm_chosen = decision["arm_chosen"]
        decided_at = decision["decided_at"]

        reward = compute_reward(customer_id, account_id, decided_at, host, port)
        write_reward(
            customer_id, account_id, arm_chosen, decided_at, reward, host, port
        )
        count += 1

    return count


def run_loop(
    interval_seconds: int = 300,
    host: str = DEFAULT_CLICKHOUSE_HOST,
    port: int = DEFAULT_CLICKHOUSE_PORT,
) -> None:
    """Run the reward closure on a polling loop (default 5 minutes)."""
    logger.info("Starting reward logger loop (interval=%ds)", interval_seconds)
    while True:
        try:
            processed = process_rewards(host=host, port=port)
            logger.info("Processed %d reward entries", processed)
        except Exception:
            logger.exception("Error in reward processing loop")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_loop()
