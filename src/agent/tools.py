"""Tool function wrappers over the existing FastAPI endpoints.

Each function calls the Klaflow API and returns structured data for the LLM.
No direct database or Redis access — all data goes through the API layer.
"""

import os
from typing import Any, Dict, List, Optional

import httpx

API_BASE = os.getenv("KLAFLOW_API_URL", "http://localhost:8000")

_client: Optional[httpx.Client] = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(base_url=API_BASE, timeout=30.0)
    return _client


def _api_get(path: str, params: Optional[dict] = None) -> Any:
    """GET request to the Klaflow API. Returns parsed JSON or raises."""
    resp = _get_client().get(path, params=params)
    resp.raise_for_status()
    return resp.json()


def _api_post(path: str, json_body: dict) -> Any:
    """POST request to the Klaflow API. Returns parsed JSON or raises."""
    resp = _get_client().post(path, json=json_body)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Segment tools
# ---------------------------------------------------------------------------

def get_segments() -> list[dict]:
    """List all segments with member counts."""
    return _api_get("/segments")


def get_segment_members(segment_name: str, limit: int = 100) -> list[dict]:
    """Get customers in a segment."""
    return _api_get(f"/segments/{segment_name}/customers", params={"limit": limit})


def get_customer_segments(customer_id: str) -> list[dict]:
    """Which segments a customer belongs to."""
    return _api_get(f"/customers/{customer_id}/segments")


# ---------------------------------------------------------------------------
# Customer tools
# ---------------------------------------------------------------------------

def get_customer_features(customer_id: str, account_id: str = "") -> dict:
    """Full feature vector from Redis for a customer."""
    params = {}
    if account_id:
        params["account_id"] = account_id
    return _api_get(f"/customers/{customer_id}/features", params=params)


def get_customer_decision_history(customer_id: str, limit: int = 20) -> list[dict]:
    """Past decisions and outcomes for a customer."""
    return _api_get(f"/decisions/{customer_id}/history", params={"limit": limit})


# ---------------------------------------------------------------------------
# Exclusion tools
# ---------------------------------------------------------------------------

def check_recent_contact(customer_id: str, days: int = 7) -> dict:
    """Check if a customer was contacted recently.

    Looks at the decision history and checks whether any decision was made
    within the specified number of days. Returns a dict with:
      - contacted: bool
      - last_contact_at: str or None
      - arm_used: str or None
    """
    history = _api_get(f"/decisions/{customer_id}/history", params={"limit": 50})
    if not history:
        return {"contacted": False, "last_contact_at": None, "arm_used": None}

    # decision_log rows have created_at timestamps
    # The API returns them ordered by created_at DESC, so the first is most recent
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    for decision in history:
        created_at_str = decision.get("created_at", "")
        if not created_at_str:
            continue
        try:
            # ClickHouse returns datetime as "YYYY-MM-DD HH:MM:SS"
            created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue

        if created_at >= cutoff:
            return {
                "contacted": True,
                "last_contact_at": created_at_str,
                "arm_used": decision.get("arm_chosen"),
            }

    return {"contacted": False, "last_contact_at": None, "arm_used": None}


def get_suppression_list(account_id: str) -> dict:
    """Get globally suppressed customers for an account.

    Suppressed = customers with lifecycle_state 'churned' who have unsubscribed.
    Since we don't have an explicit suppression table, we derive this from
    features: churned customers with churn_risk_score == 1.0.
    Returns a dict with customer_ids and count.
    """
    # Get segment members for churned-like segments and cross-reference features
    # Use the customers list endpoint to find churned customers for this account
    suppressed = []
    page = 1
    while True:
        data = _api_get("/customers", params={"page": page, "page_size": 200})
        for c in data.get("customers", []):
            if c.get("account_id") == account_id and c.get("lifecycle_state") == "churned":
                suppressed.append(c["customer_id"])
        if page * 200 >= data.get("total", 0):
            break
        page += 1

    return {"account_id": account_id, "suppressed_customer_ids": suppressed, "count": len(suppressed)}


# ---------------------------------------------------------------------------
# Decision tools
# ---------------------------------------------------------------------------

def request_decision(customer_id: str, account_id: str, context: str = "") -> dict:
    """Call the existing bandit to get an arm decision for a customer."""
    body = {"customer_id": customer_id, "account_id": account_id}
    if context:
        body["context"] = context
    return _api_post("/decide", json_body=body)


def schedule_action(customer_id: str, account_id: str, arm: str, campaign_id: str) -> dict:
    """Record a scheduled action for a customer.

    In a full deployment this would write to the agent-actions Kafka topic.
    For now, it returns the action as confirmation — the Kafka integration
    will be added when the agent service is deployed to k8s.
    """
    return {
        "scheduled": True,
        "customer_id": customer_id,
        "account_id": account_id,
        "arm": arm,
        "campaign_id": campaign_id,
    }


# ---------------------------------------------------------------------------
# Measurement tools
# ---------------------------------------------------------------------------

def get_campaign_outcomes(campaign_id: str, hours_elapsed: int = 72) -> dict:
    """Get outcomes for a campaign.

    Since campaign tracking is being added in Phase 2a, this returns
    aggregate decision stats as a proxy. Will be wired to the campaign
    table once agent_endpoints.py is implemented.
    """
    # For now return recent decision stats as a stand-in
    recent = _api_get("/decisions/recent", params={"limit": 100})
    return {
        "campaign_id": campaign_id,
        "hours_elapsed": hours_elapsed,
        "decisions_sampled": len(recent),
        "note": "Full campaign tracking available after agent endpoints are deployed",
    }


def get_arm_performance(arm_name: str = "", days: int = 7) -> dict:
    """Historical arm stats from the bandit.

    If arm_name is provided, filters to that arm. Otherwise returns all arms.
    """
    stats = _api_get("/bandit/arm-stats")
    if arm_name:
        # Filter to the requested arm
        arm_data = stats.get("arms", {}).get(arm_name)
        if arm_data:
            return {"arm": arm_name, **arm_data}
        return {"arm": arm_name, "error": f"Arm '{arm_name}' not found"}
    return stats


# ---------------------------------------------------------------------------
# Tool registry — maps tool names to callables for the agent loop
# ---------------------------------------------------------------------------

TOOL_REGISTRY: Dict[str, Any] = {
    "get_segments": get_segments,
    "get_segment_members": get_segment_members,
    "get_customer_segments": get_customer_segments,
    "get_customer_features": get_customer_features,
    "get_customer_decision_history": get_customer_decision_history,
    "check_recent_contact": check_recent_contact,
    "get_suppression_list": get_suppression_list,
    "request_decision": request_decision,
    "schedule_action": schedule_action,
    "get_campaign_outcomes": get_campaign_outcomes,
    "get_arm_performance": get_arm_performance,
}


def call_tool(tool_name: str, arguments: dict) -> Any:
    """Dispatch a tool call by name. Used by the agent loop."""
    func = TOOL_REGISTRY.get(tool_name)
    if func is None:
        raise ValueError(f"Unknown tool: {tool_name}")
    return func(**arguments)
