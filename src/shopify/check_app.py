#!/usr/bin/env python3
"""Check Shopify app configuration before setting up webhooks.

Verifies: API version, granted scopes, existing webhooks.
"""

import http.server
import os
import secrets
import sys
import threading
import urllib.parse

import requests

SHOP_DOMAIN = "max-ms.myshopify.com"
CLIENT_ID = "3e1462e37e22ddb617aee14f739de809"
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "")
SCOPES = "read_customers,write_customers,read_inventory,read_orders,write_orders,read_products,write_products,read_checkouts"
REDIRECT_PORT = 3000
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/callback"

_auth_code = None
_auth_evt = threading.Event()


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if "code" in params:
            _auth_code = params["code"][0]
            body = b"<h1>Done! Return to terminal.</h1>"
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


def obtain_token():
    state = secrets.token_hex(16)
    auth_url = (
        f"https://{SHOP_DOMAIN}/admin/oauth/authorize"
        f"?client_id={CLIENT_ID}&scope={SCOPES}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        f"&state={state}"
    )
    server = http.server.HTTPServer(("localhost", REDIRECT_PORT), _CallbackHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    import webbrowser
    print(f"Opening browser for auth...\nURL: {auth_url}\n")
    webbrowser.open(auth_url)
    _auth_evt.wait(timeout=120)
    server.shutdown()

    if not _auth_code:
        print("ERROR: Auth timed out"); sys.exit(1)

    resp = requests.post(
        f"https://{SHOP_DOMAIN}/admin/oauth/access_token",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "code": _auth_code},
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"ERROR: {resp.status_code} {resp.text}"); sys.exit(1)
    return resp.json()["access_token"]


def main():
    print("=" * 60)
    print("  SHOPIFY APP PRE-FLIGHT CHECK")
    print("=" * 60)

    token = obtain_token()
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    # 1. Check granted scopes
    print("\n1. GRANTED SCOPES")
    print("-" * 60)
    resp = requests.get(
        f"https://{SHOP_DOMAIN}/admin/oauth/access_scopes.json",
        headers=headers, timeout=15,
    )
    if resp.status_code == 200:
        scopes = [s["handle"] for s in resp.json().get("access_scopes", [])]
        for s in sorted(scopes):
            print(f"   [+] {s}")

        # Check what we need
        needed = {"read_orders", "read_checkouts"}
        missing = needed - set(scopes)
        if missing:
            print(f"\n   WARNING: Missing scopes for webhooks: {missing}")
            print(f"   Go to: Settings -> Apps -> Develop apps -> your app -> Configuration")
            print(f"   Add the missing scopes and reinstall the app.")
        else:
            print(f"\n   All required scopes present for order + checkout webhooks.")
    else:
        print(f"   ERROR: {resp.status_code} {resp.text[:200]}")

    # 2. Check API versions
    print("\n2. SUPPORTED API VERSIONS")
    print("-" * 60)
    # Try a few versions to see which work
    for version in ["2025-04", "2025-01", "2024-10", "2024-07", "2024-04", "2024-01"]:
        resp = requests.get(
            f"https://{SHOP_DOMAIN}/admin/api/{version}/shop.json",
            headers=headers, timeout=10,
        )
        status = "OK" if resp.status_code == 200 else f"FAIL ({resp.status_code})"
        marker = "+" if resp.status_code == 200 else "-"
        print(f"   [{marker}] {version}: {status}")

    # 3. Check existing webhooks
    print("\n3. EXISTING WEBHOOKS")
    print("-" * 60)
    resp = requests.get(
        f"https://{SHOP_DOMAIN}/admin/api/2024-01/webhooks.json",
        headers=headers, timeout=15,
    )
    if resp.status_code == 200:
        webhooks = resp.json().get("webhooks", [])
        if webhooks:
            for wh in webhooks:
                print(f"   [{wh['id']}] {wh['topic']} -> {wh['address']}")
        else:
            print("   No webhooks registered yet. (This is expected.)")
    else:
        print(f"   ERROR: {resp.status_code} {resp.text[:200]}")

    # 4. Check shop info
    print("\n4. SHOP INFO")
    print("-" * 60)
    resp = requests.get(
        f"https://{SHOP_DOMAIN}/admin/api/2024-01/shop.json",
        headers=headers, timeout=15,
    )
    if resp.status_code == 200:
        shop = resp.json()["shop"]
        print(f"   Name:     {shop.get('name')}")
        print(f"   Domain:   {shop.get('domain')}")
        print(f"   Plan:     {shop.get('plan_display_name')}")
        print(f"   Currency: {shop.get('currency')}")
        print(f"   Timezone: {shop.get('iana_timezone')}")
    else:
        print(f"   ERROR: {resp.status_code}")

    print("\n" + "=" * 60)
    print("  PRE-FLIGHT CHECK COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
