#!/usr/bin/env python3
"""Register/list/delete Shopify webhooks via the Admin API.

Uses the same OAuth flow as shopify_bulk_create.py to obtain an access token.

Usage:
  python src/shopify/register_webhooks.py --ngrok-url https://abc123.ngrok-free.app
  python src/shopify/register_webhooks.py --list
  python src/shopify/register_webhooks.py --delete-all
"""

import argparse
import http.server
import os
import secrets
import sys
import threading
import time
import urllib.parse

import requests

# ---------------------------------------------------------------------------
# Shopify app credentials (same as shopify_bulk_create.py)
# ---------------------------------------------------------------------------

SHOP_DOMAIN = "max-ms.myshopify.com"
CLIENT_ID = "3e1462e37e22ddb617aee14f739de809"
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "")
SCOPES = "read_customers,write_customers,read_inventory,read_orders,write_orders,read_products,write_products"
REDIRECT_PORT = 3000
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/callback"
API_VERSION = "2025-01"

BASE_URL = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}"

# Webhook topics to register
# Note: checkouts/create requires read_checkouts scope (Shopify Plus only)
WEBHOOK_TOPICS = [
    "orders/create",
]

# ---------------------------------------------------------------------------
# OAuth (reused from shopify_bulk_create.py)
# ---------------------------------------------------------------------------

_auth_code = None
_auth_evt = threading.Event()


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if "code" in params:
            _auth_code = params["code"][0]
            body = b"<h1>Authorized! You can close this tab.</h1>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(400)
            self.end_headers()
        _auth_evt.set()

    def log_message(self, *_):
        pass


def obtain_access_token():
    """Run OAuth flow to get an access token."""
    state = secrets.token_hex(16)
    auth_url = (
        f"https://{SHOP_DOMAIN}/admin/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        f"&state={state}"
    )

    print(f"\nOpening browser for Shopify authorization...")
    print(f"If it doesn't open, paste this URL:\n  {auth_url}\n")

    server = http.server.HTTPServer(("localhost", REDIRECT_PORT), _CallbackHandler)
    t = threading.Thread(target=server.serve_forever)
    t.daemon = True
    t.start()

    import webbrowser
    webbrowser.open(auth_url)

    _auth_evt.wait(timeout=120)
    server.shutdown()

    if not _auth_code:
        print("ERROR: Timed out waiting for authorization.")
        sys.exit(1)

    resp = requests.post(
        f"https://{SHOP_DOMAIN}/admin/oauth/access_token",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "code": _auth_code},
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"ERROR: Token exchange failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    token = resp.json().get("access_token", "")
    if not token:
        print(f"ERROR: No access_token in response: {resp.text}")
        sys.exit(1)

    print(f"Access token obtained: {token[:8]}...{token[-4:]}")
    return token


def _headers(token):
    return {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Webhook CRUD
# ---------------------------------------------------------------------------

def list_webhooks(token):
    """List all registered webhooks."""
    resp = requests.get(
        f"{BASE_URL}/webhooks.json",
        headers=_headers(token),
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"ERROR: {resp.status_code} {resp.text[:300]}")
        return []

    webhooks = resp.json().get("webhooks", [])
    if not webhooks:
        print("\nNo webhooks registered.")
        return webhooks

    print(f"\n{'=' * 70}")
    print(f"  REGISTERED WEBHOOKS ({len(webhooks)})")
    print(f"{'=' * 70}")
    for wh in webhooks:
        print(f"  ID: {wh['id']}")
        print(f"    Topic:   {wh['topic']}")
        print(f"    Address: {wh['address']}")
        print(f"    Format:  {wh.get('format', 'json')}")
        print(f"    Created: {wh.get('created_at', '?')}")
        print()

    return webhooks


def register_webhooks(token, ngrok_url):
    """Register webhooks for the configured topics."""
    # Normalize URL
    ngrok_url = ngrok_url.rstrip("/")
    webhook_address = f"{ngrok_url}/webhooks/shopify"

    print(f"\nRegistering webhooks -> {webhook_address}")
    print(f"{'=' * 70}")

    for topic in WEBHOOK_TOPICS:
        payload = {
            "webhook": {
                "topic": topic,
                "address": webhook_address,
                "format": "json",
            }
        }
        resp = requests.post(
            f"{BASE_URL}/webhooks.json",
            headers=_headers(token),
            json=payload,
            timeout=15,
        )

        if resp.status_code in (200, 201):
            wh = resp.json().get("webhook", {})
            print(f"  [OK] {topic} -> {webhook_address}  (id={wh.get('id')})")
        elif resp.status_code == 422:
            # May already exist
            errors = resp.json().get("errors", {})
            print(f"  [SKIP] {topic}: {errors}")
        else:
            print(f"  [FAIL] {topic}: {resp.status_code} {resp.text[:200]}")

        time.sleep(0.5)  # rate limit

    print(f"\nDone. Use --list to verify.")


def delete_all_webhooks(token):
    """Delete all registered webhooks."""
    webhooks = list_webhooks(token)
    if not webhooks:
        return

    print(f"\nDeleting {len(webhooks)} webhook(s)...")
    for wh in webhooks:
        resp = requests.delete(
            f"{BASE_URL}/webhooks/{wh['id']}.json",
            headers=_headers(token),
            timeout=15,
        )
        if resp.status_code == 200:
            print(f"  [DELETED] {wh['topic']} (id={wh['id']})")
        else:
            print(f"  [FAIL] {wh['id']}: {resp.status_code}")
        time.sleep(0.5)


def delete_webhook(token, webhook_id):
    """Delete a specific webhook by ID."""
    resp = requests.delete(
        f"{BASE_URL}/webhooks/{webhook_id}.json",
        headers=_headers(token),
        timeout=15,
    )
    if resp.status_code == 200:
        print(f"  [DELETED] id={webhook_id}")
    else:
        print(f"  [FAIL] {webhook_id}: {resp.status_code} {resp.text[:200]}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Manage Shopify webhooks for Klaflow")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ngrok-url", help="Register webhooks pointing to this ngrok URL")
    group.add_argument("--list", action="store_true", help="List current webhooks")
    group.add_argument("--delete-all", action="store_true", help="Delete all webhooks")
    group.add_argument("--delete", type=int, help="Delete a specific webhook by ID")

    args = parser.parse_args()

    print(f"Shop: {SHOP_DOMAIN}")
    token = obtain_access_token()

    if args.list:
        list_webhooks(token)
    elif args.delete_all:
        delete_all_webhooks(token)
    elif args.delete:
        delete_webhook(token, args.delete)
    elif args.ngrok_url:
        register_webhooks(token, args.ngrok_url)


if __name__ == "__main__":
    main()
