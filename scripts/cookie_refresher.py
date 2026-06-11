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
import threading
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
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        print(f"  [WARN] Supabase {method} HTTP {e.code}: {error_body[:300]}")
        return None
    except (urllib.error.URLError, TimeoutError) as e:
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


def cookies_to_playwright_format(cookie_str):
    """Convert 'k=v; k2=v2' string to Playwright cookie dict list."""
    cookies = []
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            cookies.append({
                "name": k.strip(),
                "value": v.strip(),
                "domain": ".chaturbate.com",
                "path": "/",
            })
    return cookies


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

    # --- If no cookies in Supabase, seed from .env ---
    if not cookie_str:
        env_cookies = os.environ.get("COOKIES", "")
        if env_cookies:
            print("  No cookies in Supabase — seeding from .env...")
            cookie_str = env_cookies
            if not user_agent:
                user_agent = os.environ.get(
                    "USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36",
                )
            # Save .env cookies to Supabase immediately so DVR can use them even if refresh fails
            seed_value = {
                "cookies": cookie_str,
                "user_agent": user_agent,
            }
            for key in ("sessionid", "csrftoken", "cf_clearance"):
                val = extract_single_cookie(cookie_str, key)
                if val:
                    seed_value[key] = val
            save_to_supabase(rest, supabase_key, seed_value, is_seed=True)
        else:
            print("  [SKIP] No cookies found in Supabase or .env")
            return

    if not user_agent:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        )
    print(f"  Resolved User-Agent: {user_agent}")
    print(f"UA_EXTRACTED={user_agent}")

    old = parse_cookies(cookie_str)
    print(f"  Loaded {len(old)} cookies")
    print(f"  sessionid: {'[OK]' if 'sessionid' in old else '[NO]'}")
    print(f"  csrftoken: {'[OK]' if 'csrftoken' in old else '[NO]'}")
    print(f"  cf_clearance: {'[OK]' if 'cf_clearance' in old else '[NO]'}")
    print(f"  Proxy: {'[OK] ' + proxy if proxy else '[NO] (direct)'}")

    # --- Launch Scrapling and visit chaturbate.com ---
    print("\n[2/4] Launching browser with Cloudflare bypass...")

    new_browser_cookies = {}

    def capture_cookies(page):
        cb = page.context.cookies()
        for c in cb:
            domain = c.get("domain", "")
            if "chaturbate.com" in domain:
                new_browser_cookies[c["name"]] = c["value"]

    # NOTE: We do NOT pass the proxy to Scrapling.
    # The SOCKS5 proxy is too slow for the full browser + Cloudflare challenge.
    # cf_clearance is a Cloudflare cookie — it works from any IP.
    # The proxy is only needed for the DVR to bypass geo-restrictions.
    saved_proxy = os.environ.pop("ALL_PROXY", None)
    saved_all = os.environ.pop("all_proxy", None)
    saved_socks = os.environ.pop("SOCKS_PROXY", None)

    session_kwargs = {
        "headless": True,
        "solve_cloudflare": True,
        "timeout": 90000,
        "block_webrtc": True,
        "hide_canvas": True,
        "network_idle": False,
    }
    if user_agent:
        session_kwargs["useragent"] = user_agent
    if cookie_str:
        session_kwargs["cookies"] = cookies_to_playwright_format(cookie_str)

    HARD_TIMEOUT = 120  # seconds — kill entire Scrapling if it hangs

    def _run_scrapling(result_container):
        try:
            with StealthySession(**session_kwargs) as session:
                session.fetch(
                    "https://chaturbate.com",
                    page_action=capture_cookies,
                    wait=3000,
                )
            result_container["done"] = True
        except Exception as e:
            result_container["error"] = str(e)

    max_retries = 2
    for attempt in range(1, max_retries + 1):
        result = {}
        t = threading.Thread(target=_run_scrapling, args=(result,), daemon=True)
        t.start()
        t.join(timeout=HARD_TIMEOUT)
        if t.is_alive():
            print(f"  [WARN] Attempt {attempt}/{max_retries} timed out after {HARD_TIMEOUT}s — browser may be hung")
        elif result.get("error"):
            print(f"  [WARN] Attempt {attempt}/{max_retries} failed: {result['error']}")
        else:
            break  # success
        if attempt < max_retries:
            print(f"  Retrying in 5s...")
            time.sleep(5)
        else:
            print(f"  [ERROR] All attempts failed")
            # Restore proxy env vars
            if saved_proxy: os.environ["ALL_PROXY"] = saved_proxy
            if saved_all: os.environ["all_proxy"] = saved_all
            if saved_socks: os.environ["SOCKS_PROXY"] = saved_socks
            return

    # Restore proxy env vars
    if saved_proxy: os.environ["ALL_PROXY"] = saved_proxy
    if saved_all: os.environ["all_proxy"] = saved_all
    if saved_socks: os.environ["SOCKS_PROXY"] = saved_socks

    if not new_browser_cookies:
        print("  [WARN] No cookies returned from browser — keeping existing")
        return

    print(f"  Got {len(new_browser_cookies)} cookies from browser session")
    print(f"  Fresh cf_clearance: {'[OK]' if 'cf_clearance' in new_browser_cookies else '[NO]'}")

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
    if new_cf and len(new_cf) > 10:
        print(f"  cf_clearance: ...{new_cf[-20:]}")

    # --- Write back to Supabase (always overwrite with latest) ---
    print("\n[4/4] Saving to Supabase...")

    settings_value = {
        "cookies": new_cookie_str,
        "user_agent": user_agent,
    }
    for key in ("sessionid", "csrftoken", "cf_clearance"):
        if key in merged:
            settings_value[key] = merged[key]

    save_to_supabase(rest, supabase_key, settings_value)


def extract_single_cookie(cookie_str, name):
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if "=" in pair:
            k, _, v = pair.partition("=")
            if k.strip() == name:
                return v.strip()
    return None


def save_to_supabase(rest, api_key, value, is_seed=False):
    patch_url = f"{rest}/app_settings?key=eq.dvr_settings"
    result = supabase_request("PATCH", patch_url, api_key, {"value": value})

    # PATCH returns [] when no row matched — that's NOT a successful update
    if result is not None and result != []:
        label = "seeded" if is_seed else "saved"
        print(f"  [OK] Cookies {label} to Supabase")
    else:
        label = "seed" if is_seed else "save"
        print(f"  Row may not exist, trying INSERT for {label}...")
        result = supabase_request(
            "POST",
            f"{rest}/app_settings",
            api_key,
            {"key": "dvr_settings", "value": value},
        )
        if result is not None:
            print(f"  [OK] Cookies {label}d into Supabase")
        else:
            print(f"  [ERROR] Failed to {label} cookies to Supabase")
            if not is_seed:
                sys.exit(1)

    if is_seed and result is not None:
        print("  Now proceeding to refresh cf_clearance via Scrapling...")


if __name__ == "__main__":
    main()
