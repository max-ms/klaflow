"""Structured intent extraction from natural language campaign goals.

Parses a merchant's natural language goal into a structured CampaignIntent
that the agent loop can use for validation and logging. The actual tool
selection is done by the LLM — this module provides structured metadata.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class CampaignIntent:
    """Structured representation of a campaign goal."""
    raw_goal: str
    target_segments: List[str] = field(default_factory=list)
    exclusion_days: Optional[int] = None
    exclude_vip: bool = False
    exclude_recent_purchasers: bool = False
    objective: str = "engagement"  # engagement | win_back | upsell | retention

    def summary(self) -> str:
        parts = [f"Objective: {self.objective}"]
        if self.target_segments:
            parts.append(f"Target segments: {', '.join(self.target_segments)}")
        if self.exclusion_days:
            parts.append(f"Exclude contacted in last {self.exclusion_days} days")
        if self.exclude_vip:
            parts.append("Exclude VIP from discounts")
        if self.exclude_recent_purchasers:
            parts.append("Exclude recent purchasers")
        return " | ".join(parts)


# Known segment names for matching.
KNOWN_SEGMENTS = [
    "high_engagers", "recent_purchasers", "at_risk",
    "active_browsers", "cart_abandoners", "vip_lapsed", "discount_sensitive",
]

# Patterns for extracting exclusion windows.
_DAYS_PATTERN = re.compile(r"(?:last|past|within)\s+(\d+)\s+days?", re.IGNORECASE)
_SKIP_CONTACTED_PATTERN = re.compile(
    r"(?:skip|exclude|don'?t\s+(?:send|contact|message))\s+.*?(?:contacted|messaged|sent)",
    re.IGNORECASE,
)


def parse_goal(goal: str) -> CampaignIntent:
    """Extract structured intent from a natural language goal.

    This is a lightweight rule-based parser. The LLM handles the actual
    campaign planning — this just extracts metadata for logging and validation.
    """
    intent = CampaignIntent(raw_goal=goal)
    goal_lower = goal.lower()

    # Detect objective.
    if any(w in goal_lower for w in ["win-back", "win back", "re-engage", "reengage", "lapsed"]):
        intent.objective = "win_back"
    elif any(w in goal_lower for w in ["upsell", "cross-sell", "upgrade"]):
        intent.objective = "upsell"
    elif any(w in goal_lower for w in ["retain", "churn", "at risk", "at-risk"]):
        intent.objective = "retention"
    elif any(w in goal_lower for w in ["cart abandon", "abandoned cart"]):
        intent.objective = "cart_recovery"

    # Detect target segments.
    for seg in KNOWN_SEGMENTS:
        # Match both underscore and space/hyphen forms.
        variants = [seg, seg.replace("_", " "), seg.replace("_", "-")]
        if any(v in goal_lower for v in variants):
            intent.target_segments.append(seg)

    # Infer segments from objective if none explicitly mentioned.
    if not intent.target_segments:
        if intent.objective == "win_back":
            intent.target_segments = ["at_risk", "vip_lapsed"]
        elif intent.objective == "retention":
            intent.target_segments = ["at_risk"]
        elif intent.objective == "cart_recovery":
            intent.target_segments = ["cart_abandoners"]

    # Detect exclusion window.
    if _SKIP_CONTACTED_PATTERN.search(goal):
        days_match = _DAYS_PATTERN.search(goal)
        if days_match:
            intent.exclusion_days = int(days_match.group(1))
        else:
            intent.exclusion_days = 7  # default

    # Check for days pattern even without explicit skip language.
    if intent.exclusion_days is None:
        days_match = _DAYS_PATTERN.search(goal)
        if days_match and any(w in goal_lower for w in ["skip", "exclude", "don't"]):
            intent.exclusion_days = int(days_match.group(1))

    # VIP exclusion from discounts.
    if "vip" in goal_lower and any(w in goal_lower for w in ["don't discount", "no discount", "skip discount"]):
        intent.exclude_vip = True

    # Recent purchaser exclusion.
    if any(w in goal_lower for w in ["skip recent purchaser", "exclude recent purchaser"]):
        intent.exclude_recent_purchasers = True

    return intent
