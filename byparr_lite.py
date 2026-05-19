#!/usr/bin/env python3
"""
byparr_lite.py — Minimal FlareSolverr-compatible HTTP server using Playwright
with the system Chromium (installed via nix).

Replicates the docker-compose cookie-refresher architecture natively on Replit.
Runs on port 8191 matching the app's default Byparr URL.

Supported commands (FlareSolverr protocol):
  sessions.create  → no-op
  sessions.destroy → no-op
  request.get      → opens URL in headless Chromium, waits for CF, returns cookies
"""

import json
import os
import shutil
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from urllib.parse import urlparse

CHROMIUM_LOCK = Lock()

# Prefer system-installed Chromium (Replit/nix), fall back to Playwright's
# bundled browser (Docker/GitHub Actions).
SYSTEM_CHROMIUM = shutil.which("chromium") or shutil.which("chromium-browser")


def _get_proxy_config() -> dict | None:
    """Return Playwright proxy config from env vars, or None if no proxy is set."""
    proxy_url = os.environ.get("PROXY_URL") or os.environ.get("PROXY_SERVER") or ""
    if not proxy_url:
        return None
    proxy_user = os.environ.get("PROXY_USERNAME", "")
    proxy_pass = os.environ.get("PROXY_PASSWORD", "")
    cfg: dict = {"server": proxy_url}
    if proxy_user:
        cfg["username"] = proxy_user
    if proxy_pass:
        cfg["password"] = proxy_pass
    print(f"[byparr-lite] Using proxy: {proxy_url}", flush=True)
    return cfg


def _launch_browser(p, max_timeout_ms: int):
    """Launch Chromium, using system binary if available, else Playwright's bundled one."""
    launch_kwargs = {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    }
    if SYSTEM_CHROMIUM:
        launch_kwargs["executable_path"] = SYSTEM_CHROMIUM
    return p.chromium.launch(**launch_kwargs)


def solve_cloudflare(url: str, max_timeout_ms: int = 120000) -> tuple:
    from playwright.sync_api import sync_playwright

    deadline = time.time() + (max_timeout_ms / 1000)
    proxy_cfg = _get_proxy_config()
    with CHROMIUM_LOCK:
        with sync_playwright() as p:
            browser = _launch_browser(p, max_timeout_ms)
            ctx_kwargs = {
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            }
            if proxy_cfg:
                ctx_kwargs["proxy"] = proxy_cfg
            context = browser.new_context(**ctx_kwargs)
            page = context.new_page()

            # Use wait_until="commit" so we get the HTTP response immediately
            # even when Cloudflare blocks or shows a challenge page.
            # domcontentloaded may never fire on blocked pages.
            try:
                page.goto(url, timeout=max_timeout_ms, wait_until="commit")
            except Exception as e:
                print(f"[byparr-lite] goto failed: {e}", flush=True)
                try:
                    browser.close()
                except Exception:
                    pass
                return [], ""

            page_title = ""
            try:
                page_title = page.title()
            except Exception:
                pass
            print(f"[byparr-lite] Page loaded: url={url} title={page_title!r}", flush=True)

            # Poll for cf_clearance cookie (Cloudflare challenge completion)
            while time.time() < deadline:
                cookies = context.cookies()
                if any(c["name"] == "cf_clearance" for c in cookies):
                    print("[byparr-lite] cf_clearance obtained", flush=True)
                    break
                time.sleep(2)

            all_cookies = context.cookies()
            found = [c["name"] for c in all_cookies if c["name"] in ("cf_clearance", "csrftoken")]
            if not found:
                page_content = ""
                try:
                    page_content = page.content()[:500]
                except Exception:
                    pass
                print(f"[byparr-lite] No cf_clearance after {max_timeout_ms}ms. Page begins: {page_content!r}", flush=True)

            try:
                user_agent = page.evaluate("navigator.userAgent")
            except Exception:
                user_agent = ""
            try:
                browser.close()
            except Exception:
                pass

    return all_cookies, user_agent


def make_ok_response(cookies: list, user_agent: str, url: str) -> dict:
    return {
        "status": "ok",
        "message": "",
        "solution": {
            "url": url,
            "status": 200,
            "response": "",
            "cookies": [
                {
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c.get("domain", ""),
                    "path": c.get("path", "/"),
                    "expires": c.get("expires", -1),
                    "size": len(c["name"]) + len(c["value"]),
                    "httpOnly": c.get("httpOnly", False),
                    "secure": c.get("secure", False),
                    "sameSite": c.get("sameSite", "Lax"),
                }
                for c in cookies
            ],
            "userAgent": user_agent,
        },
    }


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, data: dict):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/health", "/backend-health"):
            self._send_json(200, {"status": "ok", "version": "byparr-lite"})
        else:
            self._send_json(404, {"status": "error", "message": "not found"})

    def do_POST(self):
        if self.path != "/v1":
            self._send_json(404, {"status": "error", "message": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            req = json.loads(self.rfile.read(length))
        except Exception as e:
            self._send_json(400, {"status": "error", "message": f"bad json: {e}"})
            return

        cmd = req.get("cmd", "")

        if cmd in ("sessions.create", "sessions.destroy", "sessions.list"):
            self._send_json(200, {"status": "ok"})
            return

        if cmd == "request.get":
            url = req.get("url", "")
            max_timeout = req.get("maxTimeout", 60000)
            parsed = urlparse(url)
            if not parsed.scheme or parsed.scheme not in ("http", "https") or not parsed.netloc:
                self._send_json(400, {"status": "error", "message": "url must be a valid http(s) URL"})
                return
            print(f"[byparr-lite] Solving CF challenge for: {url}", flush=True)
            try:
                cookies, user_agent = solve_cloudflare(url, max_timeout)
                cf_names = [
                    c["name"]
                    for c in cookies
                    if c["name"] in ("cf_clearance", "csrftoken")
                ]
                print(f"[byparr-lite] Done — cookies: {cf_names}", flush=True)
                self._send_json(200, make_ok_response(cookies, user_agent, url))
            except Exception as e:
                print(f"[byparr-lite] ERROR: {e}", flush=True)
                self._send_json(500, {"status": "error", "message": str(e)})
            return

        self._send_json(400, {"status": "error", "message": f"unknown cmd: {cmd}"})

    def log_message(self, fmt, *args):
        print(f"[byparr-lite] {fmt % args}", flush=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8191))
    if SYSTEM_CHROMIUM:
        print(f"[byparr-lite] Using system Chromium: {SYSTEM_CHROMIUM}", flush=True)
    else:
        print("[byparr-lite] No system Chromium found — using Playwright's bundled browser", flush=True)
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[byparr-lite] Listening on :{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
