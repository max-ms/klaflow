"""Action space and reward signal definitions for the contextual bandit."""

# Pre-defined action space — never dynamically generated at runtime.
ARMS = [
    "email_no_offer",
    "email_10pct_discount",
    "email_free_shipping",
    "sms_nudge",
    "no_send",
]

# Reward signal constants.
REWARD_PURCHASE = 1.0        # Purchase within 72h of send
REWARD_EMAIL_OPENED = 0.1    # Weak positive signal
REWARD_UNSUBSCRIBED = -0.5   # Negative — protect list health
REWARD_NO_ACTION = 0.0       # No observed outcome

# Reward attribution window (hours).
REWARD_WINDOW_HOURS = 72
