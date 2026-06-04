#!/usr/bin/env python3
"""Cookie Refresher for Chaturbate DVR.

Reads current cookies from Supabase, uses Scrapling's StealthySession to
bypass Cloudflare at chaturbate.com, gets a fresh cf_clearance, merges it
with the existing sessionid/csrftoken, and writes the result back to Supabase.

Usage: python scripts/cookie_refresher.py
Requires .env with SUPABASE_URL, SUPABASE_API_KEY.
ALL_PROXY env var is used automatically (set by workflow step).
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    from scrapling.fetchers import StealthySession
except ImportError:
    print("::warning::Scrapling not installed — skipping cookie refresh")
    sys.exit(0)


def load_dotenv(path=".env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("\"'")
        if key and not os.environ.get(key):
            os.environ[key] = val


def supabase_request(method, url, api_key, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("apikey", api_key)
    req.add_header("Authorization", f"Bearer {api_key}")
    if body:
        req.add_header("Content-Type", "application/json")
    if method == "PATCH":
        req.add_header("Prefer", "return=representation")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        print(f"  [WARN] Supabase {method} failed: {e}")
        return None


def parse_cookies(cookie_str):
    result = {}
    if not cookie_str:
        return result
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            result[k.strip()] = v.strip()
    return result


def join_cookies(cookie_dict):
    return "; ".join(f"{k}={v}" for k, v in cookie_dict.items())


def main():
    print("=" * 50)
    print("  Cookie Refresher")
    print("=" * 50)

    load_dotenv(".env")

    supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    supabase_key = os.environ.get("SUPABASE_API_KEY", "")
    proxy = os.environ.get("ALL_PROXY", "")

    if not supabase_url or not supabase_key:
        print("  [SKIP] SUPABASE_URL or SUPABASE_API_KEY not set")
        return

    rest = f"{supabase_url}/rest/v1"
    get_url = f"{rest}/app_settings?key=eq.dvr_settings&select=value"

    # --- Load current cookies from Supabase ---
    print("\n[1/4] Loading current cookies from Supabase...")
    settings = supabase_request("GET", get_url, supabase_key)

    cookie_str = ""
    user_agent = os.environ.get("USER_AGENT", "")

    if settings and len(settings) > 0:
        val = settings[0].get("value", {})
        cookie_str = val.get("cookies", "")
        if not user_agent:
            user_agent = val.get("user_agent", "")

    if not cookie_str:
        print("  [SKIP] No cookies found in Supabase — nothing to refresh")
        return

    if not user_agent:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        )

    old = parse_cookies(cookie_str)
    print(f"  Loaded {len(old)} cookies")
    print(f"  sessionid: {'✓' if 'sessionid' in old else '✗'}")
    print(f"  csrftoken: {'✓' if 'csrftoken' in old else '✗'}")
    print(f"  cf_clearance: {'✓' if 'cf_clearance' in old else '✗'}")
    print(f"  Proxy: {'✓' if proxy else '✗ (direct)'}")

    # --- Launch Scrapling and visit chaturbate.com ---
    print("\n[2/4] Launching browser with Cloudflare bypass...")

    new_browser_cookies = {}

    def capture_cookies(page):
        cb = page.context.cookies()
        for c in cb:
            domain = c.get("domain", "")
            if "chaturbate.com" in domain:
                new_browser_cookies[c["name"]] = c["value"]

    session_kwargs = {
        "headless": True,
        "solve_cloudflare": True,
        "timeout": 60000,
        "block_webrtc": True,
        "hide_canvas": True,
    }
    if proxy:
        session_kwargs["proxy"] = proxy
    if user_agent:
        session_kwargs["useragent"] = user_agent

    try:
        with StealthySession(**session_kwargs) as session:
            session.fetch(
                "https://chaturbate.com",
                cookies=cookie_str,
                page_action=capture_cookies,
                wait=3000,
            )
    except Exception as e:
        print(f"  [WARN] Scrapling request failed: {e}")
        return

    if not new_browser_cookies:
        print("  [WARN] No cookies returned from browser — keeping existing")
        return

    print(f"  Got {len(new_browser_cookies)} cookies from browser session")
    print(f"  Fresh cf_clearance: {'✓' if 'cf_clearance' in new_browser_cookies else '✗'}")

    # --- Merge cookies ---
    print("\n[3/4] Merging cookies...")

    merged = dict(old)
    for key in ("cf_clearance", "__cf_bm", "__cfruid"):
        if key in new_browser_cookies:
            merged[key] = new_browser_cookies[key]
    if "sessionid" in new_browser_cookies:
        merged["sessionid"] = new_browser_cookies["sessionid"]
    if "csrftoken" in new_browser_cookies:
        merged["csrftoken"] = new_browser_cookies["csrftoken"]

    new_cookie_str = join_cookies(merged)

    old_cf = old.get("cf_clearance", "")
    new_cf = merged.get("cf_clearance", "")
    refreshed = bool(new_cf and new_cf != old_cf)

    print(f"  Refreshed: {refreshed}")
    if refreshed and len(new_cf) > 10:
        print(f"  cf_clearance: ...{new_cf[-20:]}")

    if not refreshed:
        print("  [SKIP] cf_clearance unchanged — nothing to update")
        return

    # --- Write back to Supabase ---
    print("\n[4/4] Saving to Supabase...")

    settings_value = {
        "cookies": new_cookie_str,
        "user_agent": user_agent,
    }
    for key in ("sessionid", "csrftoken", "cf_clearance"):
        if key in merged:
            settings_value[key] = merged[key]

    patch_url = f"{rest}/app_settings?key=eq.dvr_settings"
    result = supabase_request("PATCH", patch_url, supabase_key, {"value": settings_value})

    if result is not None:
        print("  [OK] Cookies updated in Supabase")
    else:
        print("  Row may not exist yet, trying INSERT...")
        result = supabase_request(
            "POST",
            f"{rest}/app_settings",
            supabase_key,
            {"key": "dvr_settings", "value": settings_value},
        )
        if result is not None:
            print("  [OK] Cookies inserted into Supabase")
        else:
            print("  [ERROR] Failed to save cookies to Supabase")
            sys.exit(1)

    print("\n" + "=" * 50)
    print("  Cookie refresh complete")
    print("=" * 50)


if __name__ == "__main__":
    main()
