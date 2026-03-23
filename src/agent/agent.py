"""LLM orchestration loop for the AI marketing agent.

Receives a merchant goal in natural language, calls tools in LLM-determined
sequence, schedules actions via the existing bandit, and returns a campaign
report. No LangChain — explicit tool loop over the Anthropic API.
"""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import anthropic

try:
    from agent.tool_schemas import TOOL_SCHEMAS
    from agent.tools import call_tool
except ImportError:
    from tool_schemas import TOOL_SCHEMAS
    from tools import call_tool

logger = logging.getLogger(__name__)

MODEL = os.getenv("AGENT_MODEL", "claude-sonnet-4-20250514")
MAX_TURNS = int(os.getenv("AGENT_MAX_TURNS", "30"))

SYSTEM_PROMPT = """\
You are a marketing automation agent for the Klaflow platform. You help \
merchants execute targeted marketing campaigns by calling tools to query \
customer data, apply exclusions, and schedule per-customer actions.

You have access to tools that query real customer segments, feature vectors, \
and a contextual bandit that selects the best action per customer.

When given a campaign goal:
1. Identify the target segment(s) by calling get_segments first.
2. Get the customer list from the relevant segment(s).
3. Apply any exclusion rules the merchant specified (recent contact, \
   suppression list, specific feature filters).
4. For each qualifying customer, call request_decision to get the bandit's \
   recommended action.
5. Schedule each action via schedule_action.
6. Summarize what you did: how many targeted, how many excluded, arm \
   breakdown, and any notable observations.

Important rules:
- Always check the suppression list before scheduling actions.
- Always respect contact frequency limits specified in the goal.
- Never create new arms — use only the existing five: email_no_offer, \
  email_10pct_discount, email_free_shipping, sms_nudge, no_send.
- If the bandit returns no_send, do NOT schedule that customer.
- Work in batches — you don't need to call tools one customer at a time \
  if you can process groups efficiently.
- Be concise in your reasoning. Focus on executing the campaign.
"""


@dataclass
class ToolCall:
    """Record of a single tool invocation."""
    tool_name: str
    arguments: dict
    result: Any
    duration_ms: float


@dataclass
class Campaign:
    """State for a running campaign."""
    campaign_id: str
    account_id: str
    goal: str
    status: str = "planning"
    tool_calls: List[ToolCall] = field(default_factory=list)
    scheduled_actions: List[dict] = field(default_factory=list)
    excluded_count: int = 0
    error: Optional[str] = None
    report: Optional[str] = None
    created_at: float = field(default_factory=time.time)


# In-memory campaign store — replaced by ClickHouse in production.
_campaigns: Dict[str, Campaign] = {}


def get_campaign(campaign_id: str) -> Optional[Campaign]:
    return _campaigns.get(campaign_id)


def list_campaigns(account_id: Optional[str] = None) -> List[Campaign]:
    if account_id:
        return [c for c in _campaigns.values() if c.account_id == account_id]
    return list(_campaigns.values())


def run_campaign(account_id: str, goal: str) -> Campaign:
    """Execute a campaign: parse goal, call tools, schedule actions.

    This is the main entry point. It runs the full LLM tool loop
    synchronously and returns the completed campaign.
    """
    campaign = Campaign(
        campaign_id=f"c_{uuid.uuid4().hex[:12]}",
        account_id=account_id,
        goal=goal,
    )
    _campaigns[campaign.campaign_id] = campaign

    try:
        _run_tool_loop(campaign)
    except Exception as e:
        campaign.status = "failed"
        campaign.error = str(e)
        logger.exception("Campaign %s failed", campaign.campaign_id)

    return campaign


def _run_tool_loop(campaign: Campaign) -> None:
    """The explicit LLM tool loop. No framework magic."""
    client = anthropic.Anthropic()

    messages = [
        {
            "role": "user",
            "content": (
                f"Execute this campaign for account '{campaign.account_id}':\n\n"
                f"{campaign.goal}\n\n"
                f"Campaign ID: {campaign.campaign_id}\n"
                "Call the necessary tools to identify customers, apply exclusions, "
                "get bandit decisions, and schedule actions. Then summarize the results."
            ),
        }
    ]

    campaign.status = "executing"
    logger.info(
        "Campaign %s: starting tool loop for account %s",
        campaign.campaign_id, campaign.account_id,
    )

    for turn in range(MAX_TURNS):
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )

        # Check if the model wants to use tools or is done.
        if response.stop_reason == "end_turn":
            # Model is done — extract final text as the report.
            text_parts = [
                block.text for block in response.content
                if block.type == "text"
            ]
            campaign.report = "\n".join(text_parts)
            campaign.status = "completed"
            logger.info(
                "Campaign %s: completed after %d turns, %d actions scheduled",
                campaign.campaign_id, turn + 1, len(campaign.scheduled_actions),
            )
            return

        if response.stop_reason != "tool_use":
            # Unexpected stop — grab any text and finish.
            text_parts = [
                block.text for block in response.content
                if block.type == "text"
            ]
            campaign.report = "\n".join(text_parts) if text_parts else "Agent stopped unexpectedly."
            campaign.status = "completed"
            return

        # Process tool calls from this turn.
        # First, add the assistant message (with all content blocks) to history.
        messages.append({"role": "assistant", "content": response.content})

        # Execute each tool call and collect results.
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_name = block.name
            arguments = block.input
            tool_use_id = block.id

            logger.info(
                "Campaign %s [turn %d]: calling %s(%s)",
                campaign.campaign_id, turn + 1, tool_name,
                _summarize_args(arguments),
            )

            t0 = time.time()
            try:
                result = call_tool(tool_name, arguments)
            except Exception as e:
                result = {"error": str(e)}
                logger.warning(
                    "Campaign %s: tool %s failed: %s",
                    campaign.campaign_id, tool_name, e,
                )
            duration_ms = (time.time() - t0) * 1000

            # Record the tool call.
            tc = ToolCall(
                tool_name=tool_name,
                arguments=arguments,
                result=result,
                duration_ms=round(duration_ms, 1),
            )
            campaign.tool_calls.append(tc)

            # Track scheduled actions.
            if tool_name == "schedule_action" and isinstance(result, dict) and result.get("scheduled"):
                campaign.scheduled_actions.append(result)

            # Truncate large results to stay within context limits.
            result_str = json.dumps(result, default=str)
            if len(result_str) > 10000:
                result_str = result_str[:10000] + '..."}'

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": result_str,
            })

        # Add all tool results as a single user message.
        messages.append({"role": "user", "content": tool_results})

    # Exhausted turns.
    campaign.status = "completed"
    campaign.report = f"Campaign reached maximum turns ({MAX_TURNS}). Partial execution."
    logger.warning("Campaign %s: exhausted %d turns", campaign.campaign_id, MAX_TURNS)


def _summarize_args(args: dict) -> str:
    """Short summary of tool arguments for logging."""
    parts = []
    for k, v in args.items():
        s = str(v)
        if len(s) > 50:
            s = s[:50] + "..."
        parts.append(f"{k}={s}")
    return ", ".join(parts)


def campaign_summary(campaign: Campaign) -> dict:
    """Serialize a campaign to a JSON-friendly dict."""
    arm_counts: Dict[str, int] = {}
    for action in campaign.scheduled_actions:
        arm = action.get("arm", "unknown")
        arm_counts[arm] = arm_counts.get(arm, 0) + 1

    return {
        "campaign_id": campaign.campaign_id,
        "account_id": campaign.account_id,
        "goal": campaign.goal,
        "status": campaign.status,
        "targeted_count": len(campaign.scheduled_actions) + campaign.excluded_count,
        "scheduled_count": len(campaign.scheduled_actions),
        "excluded_count": campaign.excluded_count,
        "arm_breakdown": arm_counts,
        "tool_call_count": len(campaign.tool_calls),
        "tool_call_log": [
            {
                "tool": tc.tool_name,
                "args": tc.arguments,
                "duration_ms": tc.duration_ms,
            }
            for tc in campaign.tool_calls
        ],
        "report": campaign.report,
        "error": campaign.error,
        "created_at": campaign.created_at,
    }
