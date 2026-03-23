"""JSON schemas for the Anthropic tool_use API.

Each schema defines a tool the LLM can call. These are passed directly
to the Anthropic API's `tools` parameter in the Messages request.
"""

TOOL_SCHEMAS = [
    # -----------------------------------------------------------------
    # Segment tools
    # -----------------------------------------------------------------
    {
        "name": "get_segments",
        "description": (
            "List all customer segments with their current member counts. "
            "Use this to understand what segments exist and how large they are "
            "before targeting customers."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_segment_members",
        "description": (
            "Get the list of customers who belong to a specific segment. "
            "Returns customer_id, account_id, and when they were last evaluated. "
            "Use this to build a target list from a segment."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "segment_name": {
                    "type": "string",
                    "description": (
                        "Name of the segment to query. One of: high_engagers, "
                        "recent_purchasers, at_risk, active_browsers, "
                        "cart_abandoners, vip_lapsed, discount_sensitive."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of customers to return. Default 100.",
                    "default": 100,
                },
            },
            "required": ["segment_name"],
        },
    },
    {
        "name": "get_customer_segments",
        "description": (
            "Get all segments a specific customer currently belongs to. "
            "Use this to understand a customer's profile before deciding "
            "what action to take."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to look up.",
                },
            },
            "required": ["customer_id"],
        },
    },
    # -----------------------------------------------------------------
    # Customer tools
    # -----------------------------------------------------------------
    {
        "name": "get_customer_features",
        "description": (
            "Get the full feature vector for a customer from the feature store. "
            "Includes: email_open_rate_7d, email_open_rate_30d, purchase_count_30d, "
            "days_since_last_purchase, avg_order_value, cart_abandon_rate_30d, "
            "lifecycle_state, clv_tier, churn_risk_score, discount_sensitivity, "
            "preferred_send_hour, and updated_at."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to look up.",
                },
                "account_id": {
                    "type": "string",
                    "description": "The merchant account ID. Optional — if omitted, searches across all accounts.",
                    "default": "",
                },
            },
            "required": ["customer_id"],
        },
    },
    {
        "name": "get_customer_decision_history",
        "description": (
            "Get past bandit decisions and their outcomes for a customer. "
            "Each record shows which arm was chosen, the feature snapshot at "
            "decision time, and the reward (if observed). Use this to avoid "
            "repeating ineffective actions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to look up.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of decisions to return. Default 20.",
                    "default": 20,
                },
            },
            "required": ["customer_id"],
        },
    },
    # -----------------------------------------------------------------
    # Exclusion tools
    # -----------------------------------------------------------------
    {
        "name": "check_recent_contact",
        "description": (
            "Check whether a customer was contacted (received a decision) "
            "within a specified number of days. Use this to enforce frequency "
            "caps and avoid over-messaging. Returns contacted (bool), "
            "last_contact_at, and arm_used."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to check.",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back. Default 7.",
                    "default": 7,
                },
            },
            "required": ["customer_id"],
        },
    },
    {
        "name": "get_suppression_list",
        "description": (
            "Get the list of globally suppressed customers for a merchant account. "
            "Suppressed customers (churned/unsubscribed) must never be contacted. "
            "Always check this before scheduling actions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "string",
                    "description": "The merchant account ID.",
                },
            },
            "required": ["account_id"],
        },
    },
    # -----------------------------------------------------------------
    # Decision tools
    # -----------------------------------------------------------------
    {
        "name": "request_decision",
        "description": (
            "Request a bandit decision for a specific customer. The contextual "
            "bandit selects the best action (arm) based on the customer's current "
            "feature vector using Thompson Sampling. Returns the chosen arm, "
            "score, and reasoning. Arms: email_no_offer, email_10pct_discount, "
            "email_free_shipping, sms_nudge, no_send."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to decide for.",
                },
                "account_id": {
                    "type": "string",
                    "description": "The merchant account ID.",
                },
                "context": {
                    "type": "string",
                    "description": (
                        "Optional context for the decision, e.g. 'cart_abandoned', "
                        "'win_back', 'upsell'. Helps the bandit select appropriately."
                    ),
                    "default": "",
                },
            },
            "required": ["customer_id", "account_id"],
        },
    },
    {
        "name": "schedule_action",
        "description": (
            "Schedule a marketing action for a customer. Writes the decision "
            "to the execution queue. Call request_decision first to get the arm, "
            "then schedule_action to commit it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {
                    "type": "string",
                    "description": "The customer ID to target.",
                },
                "account_id": {
                    "type": "string",
                    "description": "The merchant account ID.",
                },
                "arm": {
                    "type": "string",
                    "description": (
                        "The action to take. One of: email_no_offer, "
                        "email_10pct_discount, email_free_shipping, sms_nudge, no_send."
                    ),
                    "enum": [
                        "email_no_offer",
                        "email_10pct_discount",
                        "email_free_shipping",
                        "sms_nudge",
                        "no_send",
                    ],
                },
                "campaign_id": {
                    "type": "string",
                    "description": "The campaign ID this action belongs to.",
                },
            },
            "required": ["customer_id", "account_id", "arm", "campaign_id"],
        },
    },
    # -----------------------------------------------------------------
    # Measurement tools
    # -----------------------------------------------------------------
    {
        "name": "get_campaign_outcomes",
        "description": (
            "Get outcome metrics for a campaign after a specified observation "
            "window. Returns conversion rates and decision counts. Use this "
            "to evaluate whether a campaign is performing well."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "campaign_id": {
                    "type": "string",
                    "description": "The campaign ID to check.",
                },
                "hours_elapsed": {
                    "type": "integer",
                    "description": "Hours since campaign started. Default 72 (3 days).",
                    "default": 72,
                },
            },
            "required": ["campaign_id"],
        },
    },
    {
        "name": "get_arm_performance",
        "description": (
            "Get historical performance stats for bandit arms. Shows win rates, "
            "alpha/beta parameters, and selection counts. Use this to understand "
            "which actions have historically worked best."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "arm_name": {
                    "type": "string",
                    "description": (
                        "Specific arm to query. If omitted, returns stats for all arms. "
                        "One of: email_no_offer, email_10pct_discount, "
                        "email_free_shipping, sms_nudge, no_send."
                    ),
                    "default": "",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days of history to consider. Default 7.",
                    "default": 7,
                },
            },
            "required": [],
        },
    },
]
