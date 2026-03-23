"""Outcome monitoring and report generation for agent campaigns.

Polls the decision/reward log to compute campaign outcomes after the
observation window (default 72h). Generates a human-readable report.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    from agent.tools import get_arm_performance, get_campaign_outcomes
except ImportError:
    from tools import get_arm_performance, get_campaign_outcomes

logger = logging.getLogger(__name__)


@dataclass
class CampaignOutcome:
    """Outcome metrics for a completed campaign."""
    campaign_id: str
    total_targeted: int
    total_scheduled: int
    total_excluded: int
    arm_breakdown: Dict[str, int]
    hours_elapsed: float
    conversion_estimate: Optional[float] = None


def compute_outcomes(
    campaign_id: str,
    scheduled_actions: List[dict],
    excluded_count: int = 0,
) -> CampaignOutcome:
    """Compute campaign outcome metrics from scheduled actions."""
    arm_counts: Dict[str, int] = {}
    for action in scheduled_actions:
        arm = action.get("arm", "unknown")
        arm_counts[arm] = arm_counts.get(arm, 0) + 1

    return CampaignOutcome(
        campaign_id=campaign_id,
        total_targeted=len(scheduled_actions) + excluded_count,
        total_scheduled=len(scheduled_actions),
        total_excluded=excluded_count,
        arm_breakdown=arm_counts,
        hours_elapsed=0,
    )


def format_report(outcome: CampaignOutcome) -> str:
    """Generate a human-readable campaign report."""
    lines = [
        f"Campaign {outcome.campaign_id} — Outcome Report",
        "=" * 50,
        f"  Customers targeted:  {outcome.total_targeted}",
        f"  Actions scheduled:   {outcome.total_scheduled}",
        f"  Excluded:            {outcome.total_excluded}",
        "",
        "  Arm breakdown:",
    ]

    total = max(outcome.total_scheduled, 1)
    for arm, count in sorted(outcome.arm_breakdown.items(), key=lambda x: -x[1]):
        pct = count / total * 100
        lines.append(f"    {arm:<25s} {count:>5d}  ({pct:5.1f}%)")

    if outcome.conversion_estimate is not None:
        lines.append(f"\n  Estimated conversion: {outcome.conversion_estimate:.1%}")

    lines.append("=" * 50)
    return "\n".join(lines)


def poll_outcomes(campaign_id: str, check_interval_sec: int = 300) -> dict:
    """Check campaign outcomes (called periodically after campaign execution).

    In production this would poll ClickHouse reward_log. For now it
    returns the latest arm performance stats as a proxy.
    """
    try:
        outcomes = get_campaign_outcomes(campaign_id)
        arm_stats = get_arm_performance()
        return {
            "campaign_id": campaign_id,
            "outcomes": outcomes,
            "arm_stats": arm_stats,
        }
    except Exception as e:
        logger.warning("Failed to poll outcomes for %s: %s", campaign_id, e)
        return {"campaign_id": campaign_id, "error": str(e)}
