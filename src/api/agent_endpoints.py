"""Agent campaign endpoints — added to the existing FastAPI app.

POST /agent/campaign        — create and execute a campaign
GET  /agent/campaign/{id}   — get campaign status and report
GET  /agent/campaigns       — list all campaigns
POST /agent/campaign/{id}/pause
POST /agent/campaign/{id}/cancel
"""

import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Add agent package parent to path so we can import agent.agent, agent.goal_parser
_agent_parent = str(Path(__file__).parent.parent)
if _agent_parent not in sys.path:
    sys.path.insert(0, _agent_parent)

from agent.agent import (
    Campaign,
    campaign_summary,
    get_campaign,
    list_campaigns,
    run_campaign,
)
from agent.goal_parser import parse_goal

router = APIRouter(prefix="/agent", tags=["agent"])


class CampaignRequest(BaseModel):
    account_id: str
    goal: str


@router.post("/campaign")
def create_campaign(req: CampaignRequest):
    """Create and execute a campaign from a natural language goal."""
    intent = parse_goal(req.goal)
    campaign = run_campaign(req.account_id, req.goal)
    return {
        **campaign_summary(campaign),
        "parsed_intent": {
            "objective": intent.objective,
            "target_segments": intent.target_segments,
            "exclusion_days": intent.exclusion_days,
            "summary": intent.summary(),
        },
    }


@router.get("/campaign/{campaign_id}")
def get_campaign_detail(campaign_id: str):
    """Get campaign status, tool call log, and report."""
    campaign = get_campaign(campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    return campaign_summary(campaign)


@router.get("/campaigns")
def list_all_campaigns(account_id: str = ""):
    """List all campaigns, optionally filtered by account."""
    campaigns = list_campaigns(account_id if account_id else None)
    return [campaign_summary(c) for c in campaigns]


@router.post("/campaign/{campaign_id}/pause")
def pause_campaign(campaign_id: str):
    """Pause a running campaign."""
    campaign = get_campaign(campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    if campaign.status not in ("planning", "executing"):
        raise HTTPException(status_code=400, detail=f"Cannot pause campaign in '{campaign.status}' state")
    campaign.status = "paused"
    return {"campaign_id": campaign_id, "status": "paused"}


@router.post("/campaign/{campaign_id}/cancel")
def cancel_campaign(campaign_id: str):
    """Cancel a campaign."""
    campaign = get_campaign(campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    campaign.status = "cancelled"
    return {"campaign_id": campaign_id, "status": "cancelled"}
