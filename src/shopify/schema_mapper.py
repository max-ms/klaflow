"""Maps Shopify webhook payloads to Klaflow CustomerEvent schema.

Produces both mapped events and a schema compatibility report showing
what maps cleanly, what's lost, and what Shopify fields have no equivalent.
"""

import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any


# Current Klaflow Avro event_type enum values
VALID_EVENT_TYPES = {
    "email_opened", "link_clicked", "purchase_made",
    "page_viewed", "cart_abandoned",
}

# Shopify topic -> Klaflow event_type
TOPIC_TO_EVENT_TYPE = {
    "orders/create": "purchase_made",
    "checkouts/create": "cart_abandoned",
}


def iso_to_epoch_ms(iso_str: str) -> int:
    """Convert ISO-8601 timestamp to epoch milliseconds."""
    # Handle Shopify's format: 2024-01-15T10:30:00-05:00
    # Python 3.7+ handles timezone offsets in fromisoformat
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        dt = datetime.utcnow()
    return int(dt.timestamp() * 1000)


def map_order_to_events(payload: dict, shop_domain: str) -> List[Dict[str, Any]]:
    """Map an orders/create payload to one or more Klaflow CustomerEvent dicts.

    Emits one event per line item so each product gets its own counter update
    in the Flink fan-out. If there are no line items, emits a single event
    with the order total.
    """
    customer_id = _extract_customer_id(payload)
    created_at = payload.get("created_at", "")
    timestamp = iso_to_epoch_ms(created_at)
    line_items = payload.get("line_items", [])

    events = []

    if not line_items:
        # Single event with order total
        events.append({
            "event_id": str(payload.get("id", uuid.uuid4())),
            "customer_id": customer_id,
            "account_id": shop_domain,
            "event_type": "purchase_made",
            "timestamp": timestamp,
            "properties": {
                "campaign_id": _extract_campaign_id(payload),
                "product_id": None,
                "product_category": None,
                "amount": _safe_float(payload.get("total_price")),
                "email_subject": None,
            },
        })
    else:
        for i, item in enumerate(line_items):
            quantity = int(item.get("quantity", 1))
            unit_price = _safe_float(item.get("price"))
            events.append({
                "event_id": f"{payload.get('id', uuid.uuid4())}_{i}",
                "customer_id": customer_id,
                "account_id": shop_domain,
                "event_type": "purchase_made",
                "timestamp": timestamp,
                "properties": {
                    "campaign_id": _extract_campaign_id(payload),
                    "product_id": str(item.get("product_id", "")),
                    "product_category": _extract_product_category(item),
                    "amount": unit_price * quantity if unit_price else None,
                    "email_subject": None,
                },
            })

    return events


def map_checkout_to_events(payload: dict, shop_domain: str) -> List[Dict[str, Any]]:
    """Map a checkouts/create payload to Klaflow cart_abandoned events.

    Note: not every checkout is actually abandoned. The webhook fires when a
    checkout is CREATED, not when it's abandoned. The receiver tracks checkout
    tokens and only finalizes these as cart_abandoned if no matching
    orders/create arrives within a timeout window.
    """
    customer_id = _extract_customer_id(payload)
    created_at = payload.get("created_at", "")
    timestamp = iso_to_epoch_ms(created_at)
    line_items = payload.get("line_items", [])

    events = []
    first_item = line_items[0] if line_items else {}

    events.append({
        "event_id": str(payload.get("token", payload.get("id", uuid.uuid4()))),
        "customer_id": customer_id,
        "account_id": shop_domain,
        "event_type": "cart_abandoned",
        "timestamp": timestamp,
        "properties": {
            "campaign_id": None,
            "product_id": str(first_item.get("product_id", "")) if first_item else None,
            "product_category": first_item.get("product_type") if first_item else None,
            "amount": _safe_float(payload.get("total_price")),
            "email_subject": None,
        },
    })

    return events


def map_webhook(topic: str, payload: dict, shop_domain: str) -> List[Dict[str, Any]]:
    """Route a webhook topic to the appropriate mapper."""
    if topic == "orders/create":
        return map_order_to_events(payload, shop_domain)
    elif topic == "checkouts/create":
        return map_checkout_to_events(payload, shop_domain)
    else:
        return []


# ---------------------------------------------------------------------------
# Schema compatibility report
# ---------------------------------------------------------------------------

def analyze_payload(topic: str, payload: dict, shop_domain: str) -> Dict[str, Any]:
    """Analyze a Shopify payload against the Klaflow CustomerEvent schema.

    Returns a structured report showing what maps, what doesn't, and
    what Shopify fields are dropped.
    """
    event_type = TOPIC_TO_EVENT_TYPE.get(topic, "unknown")
    mapped_events = map_webhook(topic, payload, shop_domain)

    # Collect all top-level Shopify fields
    shopify_fields = set(payload.keys())
    # Collect nested fields of interest
    if "customer" in payload and isinstance(payload["customer"], dict):
        for k in payload["customer"]:
            shopify_fields.add(f"customer.{k}")
    if "line_items" in payload and payload["line_items"]:
        for k in payload["line_items"][0]:
            shopify_fields.add(f"line_items[].{k}")
    if "shipping_address" in payload and isinstance(payload.get("shipping_address"), dict):
        for k in payload["shipping_address"]:
            shopify_fields.add(f"shipping_address.{k}")
    if "billing_address" in payload and isinstance(payload.get("billing_address"), dict):
        for k in payload["billing_address"]:
            shopify_fields.add(f"billing_address.{k}")

    # Fields we actually use
    used_shopify_fields = set()
    field_mapping = {}

    # event_id
    if topic == "orders/create":
        field_mapping["event_id"] = {
            "source": "payload.id", "status": "mapped",
            "shopify_value": payload.get("id"),
        }
        used_shopify_fields.add("id")
    elif topic == "checkouts/create":
        field_mapping["event_id"] = {
            "source": "payload.token or payload.id", "status": "mapped",
            "shopify_value": payload.get("token", payload.get("id")),
        }
        used_shopify_fields.update({"token", "id"})

    # customer_id — cascading fallback
    customer = payload.get("customer") or {}
    resolved_cid = _extract_customer_id(payload)
    if customer.get("id"):
        cid_source = "payload.customer.id"
        cid_status = "mapped"
        cid_note = "numeric -> string"
    elif payload.get("contact_email"):
        cid_source = "payload.contact_email"
        cid_status = "mapped"
        cid_note = "no customer object; using contact_email as customer_id"
    elif payload.get("email"):
        cid_source = "payload.email"
        cid_status = "mapped"
        cid_note = "no customer object; using email as customer_id"
    else:
        cid_source = "payload.id (fallback)"
        cid_status = "partial"
        cid_note = "no customer/email; using order_<id> as synthetic customer_id"
    field_mapping["customer_id"] = {
        "source": cid_source, "status": cid_status,
        "shopify_value": resolved_cid, "note": cid_note,
    }
    used_shopify_fields.update({"customer.id", "contact_email", "email"})

    # account_id
    field_mapping["account_id"] = {
        "source": "X-Shopify-Shop-Domain header",
        "status": "mapped",
        "shopify_value": shop_domain,
    }

    # event_type
    field_mapping["event_type"] = {
        "source": f"hardcoded '{event_type}'",
        "status": "mapped" if event_type in VALID_EVENT_TYPES else "unmapped",
        "shopify_value": topic,
    }

    # timestamp
    field_mapping["timestamp"] = {
        "source": "payload.created_at",
        "status": "mapped" if payload.get("created_at") else "missing",
        "shopify_value": payload.get("created_at"),
        "note": "ISO-8601 -> epoch_ms",
    }
    used_shopify_fields.add("created_at")

    # properties.amount
    field_mapping["properties.amount"] = {
        "source": "payload.total_price",
        "status": "mapped" if payload.get("total_price") else "missing",
        "shopify_value": payload.get("total_price"),
    }
    used_shopify_fields.add("total_price")

    # properties.product_id
    line_items = payload.get("line_items", [])
    first_item = line_items[0] if line_items else {}
    field_mapping["properties.product_id"] = {
        "source": "payload.line_items[0].product_id",
        "status": "mapped" if first_item.get("product_id") else "missing",
        "shopify_value": first_item.get("product_id"),
        "note": f"{len(line_items)} line items total" if len(line_items) > 1 else None,
    }
    used_shopify_fields.add("line_items[].product_id")

    # properties.product_category
    field_mapping["properties.product_category"] = {
        "source": "payload.line_items[0].product_type",
        "status": "mapped" if first_item.get("product_type") else "missing",
        "shopify_value": first_item.get("product_type"),
        "note": "Shopify product_type -> Klaflow product_category",
    }
    used_shopify_fields.add("line_items[].product_type")

    # properties.campaign_id
    referring_site = payload.get("referring_site", "")
    field_mapping["properties.campaign_id"] = {
        "source": "payload.referring_site (UTM extraction)",
        "status": "partial" if referring_site else "unmapped",
        "shopify_value": referring_site or None,
        "note": "No direct Shopify equivalent; best-effort from referring_site UTM params",
    }
    if referring_site:
        used_shopify_fields.add("referring_site")

    # properties.email_subject
    field_mapping["properties.email_subject"] = {
        "source": "n/a",
        "status": "unmapped",
        "shopify_value": None,
        "note": "Not available from Shopify webhooks",
    }

    # Shopify fields not mapped
    unmapped_shopify = sorted(shopify_fields - used_shopify_fields)

    # Categorize unmapped fields
    notable_unmapped = []
    for f in unmapped_shopify:
        if f in ("discount_codes", "shipping_address", "billing_address",
                 "fulfillment_status", "financial_status", "tags", "note",
                 "tax_lines", "shipping_lines", "currency",
                 "line_items[].quantity", "line_items[].sku",
                 "line_items[].title", "line_items[].variant_id",
                 "line_items[].variant_title", "line_items[].vendor",
                 "customer.email", "customer.first_name", "customer.last_name",
                 "customer.tags", "customer.orders_count",
                 "customer.total_spent"):
            notable_unmapped.append(f)

    return {
        "topic": topic,
        "mapped_event_type": event_type,
        "mapped_event_count": len(mapped_events),
        "field_mapping": field_mapping,
        "shopify_fields_not_mapped": unmapped_shopify,
        "notable_unmapped_fields": notable_unmapped,
        "klaflow_fields_without_source": [
            k for k, v in field_mapping.items() if v["status"] == "unmapped"
        ],
        "mapped_events_preview": mapped_events[:2],  # show first 2 for preview
    }


def format_report(report: Dict[str, Any]) -> str:
    """Format a schema compatibility report for console output."""
    lines = []
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  SCHEMA COMPATIBILITY REPORT — {report['topic']}")
    lines.append("=" * 70)
    lines.append(f"  Mapped event type: {report['mapped_event_type']}")
    lines.append(f"  Events generated:  {report['mapped_event_count']}")
    lines.append("")

    lines.append("  FIELD MAPPING")
    lines.append("  " + "-" * 66)
    for field, info in report["field_mapping"].items():
        status_icon = {
            "mapped": "+", "partial": "~", "unmapped": "-", "missing": "!"
        }.get(info["status"], "?")
        source = info["source"]
        value = info.get("shopify_value")
        value_str = f" = {json.dumps(value)}" if value is not None else ""
        note = f"  ({info['note']})" if info.get("note") else ""
        lines.append(f"  [{status_icon}] {field:35s} <- {source}{value_str}{note}")

    if report["klaflow_fields_without_source"]:
        lines.append("")
        lines.append("  KLAFLOW FIELDS WITH NO SHOPIFY SOURCE")
        lines.append("  " + "-" * 66)
        for f in report["klaflow_fields_without_source"]:
            lines.append(f"    - {f}")

    if report["notable_unmapped_fields"]:
        lines.append("")
        lines.append("  NOTABLE SHOPIFY FIELDS NOT MAPPED (available for future use)")
        lines.append("  " + "-" * 66)
        for f in sorted(report["notable_unmapped_fields"]):
            lines.append(f"    - {f}")

    lines.append("")
    lines.append(f"  ALL UNMAPPED SHOPIFY FIELDS ({len(report['shopify_fields_not_mapped'])} total)")
    lines.append("  " + "-" * 66)
    for f in report["shopify_fields_not_mapped"][:20]:
        lines.append(f"    - {f}")
    if len(report["shopify_fields_not_mapped"]) > 20:
        lines.append(f"    ... and {len(report['shopify_fields_not_mapped']) - 20} more")

    if report.get("mapped_events_preview"):
        lines.append("")
        lines.append("  MAPPED EVENT PREVIEW")
        lines.append("  " + "-" * 66)
        for evt in report["mapped_events_preview"]:
            lines.append(f"  {json.dumps(evt, indent=4, default=str)}")

    lines.append("")
    lines.append("=" * 70)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_customer_id(payload: dict) -> str:
    """Extract customer_id from Shopify order payload.

    Tries in order: customer.id, contact_email, email, user_id, order id.
    Draft orders and POS orders may not have a customer object.
    """
    customer = payload.get("customer") or {}
    if customer.get("id"):
        return str(customer["id"])
    # Fallback: use contact_email or email as customer identifier
    if payload.get("contact_email"):
        return payload["contact_email"]
    if payload.get("email"):
        return payload["email"]
    # Last resort: use order id prefixed to indicate it's synthetic
    return f"order_{payload.get('id', 'unknown')}"


def _extract_product_category(item: dict) -> Optional[str]:
    """Extract product category from a Shopify line item.

    Shopify's product_type field is often empty. Do NOT fall back to vendor
    (that's the manufacturer, not the category). Return None if unavailable.
    """
    product_type = item.get("product_type")
    if product_type:
        return product_type
    return None


def _safe_float(value) -> Optional[float]:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _extract_campaign_id(payload: dict) -> Optional[str]:
    """Best-effort extraction of campaign_id from Shopify order payload.

    Checks referring_site for UTM parameters, landing_site, and source_name.
    """
    # Check referring_site for utm_campaign
    referring = payload.get("referring_site", "") or ""
    if "utm_campaign=" in referring:
        for param in referring.split("?")[-1].split("&"):
            if param.startswith("utm_campaign="):
                return param.split("=", 1)[1]

    # Check landing_site for utm_campaign
    landing = payload.get("landing_site", "") or ""
    if "utm_campaign=" in landing:
        for param in landing.split("?")[-1].split("&"):
            if param.startswith("utm_campaign="):
                return param.split("=", 1)[1]

    return None
