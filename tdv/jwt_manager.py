"""
TradingView JWT lifecycle manager with encrypted Hugging Face Bucket persistence.

Key guarantees:
1. Reuses the per-node cached JWT (`control/jwt/node_{i}.json.enc` in the HF bucket)
   across runs as long as the JWT is unexpired (`exp > now + safety_margin`) and
   has not been rejected by TradingView.
2. Only exchanges `sessionid` + `sessionid_sign` cookies (or falls back to headless
   Playwright login) when the saved JWT is expired, missing, or invalidated at runtime.
3. Never returns or falls back to `unauthorized_user_token`. If no valid JWT with
   `user_id > 0` can be obtained, raises `TVAuthError` so the pipeline fails fast
   with `AUTH_FAILED` (exit code 3).
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests

from tdv.bucket import BucketClient
from tdv.config import TOTAL_NOTEBOOKS, get_jwt_bucket_path
from tdv.crypto import decrypt_json, derive_key, encrypt_json

JWT_EXPIRY_SAFETY_MARGIN_SECONDS = 600  # Refresh if < 10 minutes remaining


class TVAuthError(RuntimeError):
    """Raised when a valid authenticated TradingView JWT cannot be obtained."""


def decode_jwt_payload(jwt_token: str) -> dict[str, Any]:
    """Decode the payload section of a JWT without requiring PyJWT."""
    if not jwt_token or jwt_token == "unauthorized_user_token":
        return {}
    parts = jwt_token.strip().split(".")
    if len(parts) != 3:
        return {}
    payload_b64 = parts[1]
    pad = "=" * ((4 - len(payload_b64) % 4) % 4)
    try:
        raw = base64.urlsafe_b64decode((payload_b64 + pad).encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def is_jwt_alive(
    jwt_token: str | None,
    safety_margin_seconds: int = JWT_EXPIRY_SAFETY_MARGIN_SECONDS,
    now_ts: float | None = None,
) -> tuple[bool, dict[str, Any]]:
    """
    Validate a TradingView JWT string.

    Returns `(is_valid, payload)` where `is_valid` is True iff:
    - The token decodes to a valid JSON payload
    - `user_id` is an integer > 0 (authenticated user, never anonymous)
    - `exp` is in the future by at least `safety_margin_seconds`
    """
    if not jwt_token or jwt_token == "unauthorized_user_token":
        return False, {}
    payload = decode_jwt_payload(jwt_token)
    if not payload:
        return False, {}
    user_id = payload.get("user_id")
    exp = payload.get("exp")
    if not isinstance(user_id, int) or user_id <= 0:
        return False, payload
    now = now_ts if now_ts is not None else time.time()
    if not isinstance(exp, (int, float)) or exp <= (now + safety_margin_seconds):
        return False, payload
    return True, payload


def extract_jwt_from_cookies(
    sessionid: str,
    sessionid_sign: str = "",
    timeout: int = 20,
) -> str | None:
    """
    Exchange TradingView `sessionid` (+ `sessionid_sign`) cookies for a fresh
    `auth_token` JWT by requesting `https://www.tradingview.com/`.
    """
    if not sessionid:
        return None
    cookie_parts = [f"sessionid={sessionid}"]
    if sessionid_sign:
        cookie_parts.append(f"sessionid_sign={sessionid_sign}")
    headers = {
        "Cookie": "; ".join(cookie_parts),
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    try:
        resp = requests.get("https://www.tradingview.com/", headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return None
        match = re.search(r'"auth_token"\s*:\s*"([^"]+)"', resp.text)
        if not match:
            return None
        token = match.group(1).strip()
        alive, _ = is_jwt_alive(token, safety_margin_seconds=60)
        return token if alive else None
    except Exception as exc:
        print(f"[JWTManager] Cookie exchange HTTP error: {exc}", flush=True)
        return None


def playwright_login_and_extract(
    email: str,
    password: str,
) -> dict[str, str] | None:
    """
    Fallback headless Chromium login via Playwright when cookies are expired/revoked.
    Returns `{"sessionid": ..., "sessionid_sign": ..., "jwt_token": ...}` or `None`.
    """
    if not email or not password:
        return None

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[JWTManager] Installing playwright + chromium for fallback login...", flush=True)
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "playwright"],
                check=True,
                timeout=120,
            )
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
                timeout=180,
            )
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            print(f"[JWTManager] Playwright installation failed: {exc}", flush=True)
            return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            page.goto(
                "https://www.tradingview.com/accounts/signin/",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            page.wait_for_timeout(3000)

            email_btn = page.locator("button[name='Email'], span:has-text('Email')").first
            if email_btn.is_visible():
                email_btn.click()
                page.wait_for_timeout(1000)

            user_input = page.locator("input[name='id_username'], input[id='id_username']").first
            user_input.fill(email)
            page.wait_for_timeout(500)

            pass_input = page.locator("input[name='id_password'], input[id='id_password']").first
            pass_input.fill(password)
            page.wait_for_timeout(500)

            submit_btn = page.locator(
                "button[type='submit']:has-text('Sign in'), button:has-text('Sign in')"
            ).last
            submit_btn.click()
            page.wait_for_timeout(6000)

            body_text = page.locator("body").inner_text().lower()
            if "recaptcha" in body_text or "security check" in body_text or "captcha" in body_text:
                print(f"[JWTManager] CAPTCHA challenge blocked headless login for {email}.", flush=True)
                browser.close()
                return None

            cookies = {c["name"]: c["value"] for c in context.cookies()}
            if "sessionid" not in cookies:
                browser.close()
                return None

            page.goto("https://www.tradingview.com/", wait_until="domcontentloaded", timeout=60000)
            html = page.content()
            browser.close()

            match = re.search(r'"auth_token"\s*:\s*"([^"]+)"', html)
            jwt_tok = match.group(1).strip() if match else ""
            if not jwt_tok:
                jwt_tok = extract_jwt_from_cookies(
                    cookies.get("sessionid", ""),
                    cookies.get("sessionid_sign", ""),
                ) or ""
            alive, _ = is_jwt_alive(jwt_tok, safety_margin_seconds=60)
            if not alive:
                return None
            return {
                "sessionid": cookies.get("sessionid", ""),
                "sessionid_sign": cookies.get("sessionid_sign", ""),
                "jwt_token": jwt_tok,
            }
    except Exception as exc:
        print(f"[JWTManager] Playwright login error for {email}: {exc}", flush=True)
        return None


def _load_env_sessions_pool() -> list[dict[str, str]]:
    """
    Parse optional `TV_SESSIONS_JSON` and `TV_EMAILS_JSON` from environment.
    Supports either a JSON list of account dicts or a dict keyed by node index.
    """
    raw_sessions = os.environ.get("TV_SESSIONS_JSON", "").strip()
    raw_emails = os.environ.get("TV_EMAILS_JSON", "").strip()

    pool: list[dict[str, str]] = [{} for _ in range(TOTAL_NOTEBOOKS)]

    if raw_sessions:
        try:
            parsed = json.loads(raw_sessions)
            if isinstance(parsed, list):
                for idx, entry in enumerate(parsed[:TOTAL_NOTEBOOKS]):
                    if isinstance(entry, dict):
                        pool[idx].update({k: str(v) for k, v in entry.items() if v is not None})
            elif isinstance(parsed, dict):
                for k, entry in parsed.items():
                    idx = int(k)
                    if 0 <= idx < TOTAL_NOTEBOOKS and isinstance(entry, dict):
                        pool[idx].update({ek: str(ev) for ek, ev in entry.items() if ev is not None})
        except Exception as exc:
            print(f"[JWTManager] Warning: failed to parse TV_SESSIONS_JSON: {exc}", flush=True)

    if raw_emails:
        try:
            parsed_e = json.loads(raw_emails)
            if isinstance(parsed_e, list):
                for idx, em in enumerate(parsed_e[:TOTAL_NOTEBOOKS]):
                    if em and not pool[idx].get("email"):
                        pool[idx]["email"] = str(em)
        except Exception as exc:
            print(f"[JWTManager] Warning: failed to parse TV_EMAILS_JSON: {exc}", flush=True)

    return pool


class JWTManager:
    """
    Per-node JWT manager backed by encrypted records in the HF Storage Bucket
    (`control/jwt/node_{i}.json.enc`).
    """

    def __init__(
        self,
        notebook_id: int,
        bucket: BucketClient | None = None,
        data_password: str | None = None,
    ) -> None:
        self.notebook_id = int(notebook_id) % TOTAL_NOTEBOOKS
        self.bucket = bucket or BucketClient()
        password = data_password or os.environ.get("DATA_PASSWORD", "").strip()
        if not password:
            raise TVAuthError("DATA_PASSWORD is required to decrypt/encrypt JWT records.")
        self.fernet_key = derive_key(password)
        self._lock = threading.Lock()
        self._cached_record: dict[str, Any] | None = None
        self._env_pool = _load_env_sessions_pool()

    def _read_bucket_record(self, node_idx: int) -> dict[str, Any] | None:
        remote_path = get_jwt_bucket_path(node_idx)
        try:
            raw_enc = self.bucket.download_bytes(remote_path, must_exist=False)
            if raw_enc is None:
                return None
            return decrypt_json(raw_enc, self.fernet_key)
        except Exception as exc:
            print(f"[JWTManager] Could not read/decrypt {remote_path}: {exc}", flush=True)
            return None

    def _write_bucket_record(self, node_idx: int, record: dict[str, Any]) -> None:
        remote_path = get_jwt_bucket_path(node_idx)
        try:
            enc_bytes = encrypt_json(record, self.fernet_key)
            self.bucket.upload_bytes(remote_path, enc_bytes)
        except Exception as exc:
            print(f"[JWTManager] Warning: failed to persist JWT record to {remote_path}: {exc}", flush=True)

    def _build_record(
        self,
        node_idx: int,
        jwt_token: str,
        payload: dict[str, Any],
        email: str = "",
        sessionid: str = "",
        sessionid_sign: str = "",
    ) -> dict[str, Any]:
        return {
            "notebook_id": node_idx,
            "email": email,
            "sessionid": sessionid,
            "sessionid_sign": sessionid_sign,
            "jwt_token": jwt_token,
            "user_id": int(payload.get("user_id", 0)),
            "exp": int(payload.get("exp", 0)),
            "iat": int(payload.get("iat", 0)),
            "plan": str(payload.get("plan", "")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_jwt(self, force_refresh: bool = False) -> str:
        """
        Return a valid, authenticated TradingView JWT (`user_id > 0`).

        Order of resolution:
        1. In-memory cached JWT (if `not force_refresh` and still unexpired).
        2. Encrypted bucket record `control/jwt/node_{i}.json.enc` (if `not force_refresh`
           and the saved `jwt_token` is still unexpired).
        3. Cookie exchange (`sessionid` + `sessionid_sign`) for node `i` (from bucket record
           or `TV_SESSIONS_JSON`), persisting the refreshed JWT back to the bucket.
        4. Headless Playwright login for node `i` using `TV_LOGIN_PASSWORD`.
        5. Fallback across remaining nodes `(i + offset) % 20` (bucket cached JWT ->
           cookie exchange -> Playwright login).
        6. Raises `TVAuthError` if all attempts fail (never returns `unauthorized_user_token`).
        """
        with self._lock:
            # 1. Check in-memory cache first
            if not force_refresh and self._cached_record:
                tok = self._cached_record.get("jwt_token", "")
                alive, _ = is_jwt_alive(tok)
                if alive:
                    return tok

            # 2. Try primary node then fallback pool nodes
            login_password = os.environ.get("TV_LOGIN_PASSWORD", "").strip()
            for offset in range(TOTAL_NOTEBOOKS):
                candidate_idx = (self.notebook_id + offset) % TOTAL_NOTEBOOKS
                bucket_rec = self._read_bucket_record(candidate_idx) or {}
                env_rec = self._env_pool[candidate_idx] if candidate_idx < len(self._env_pool) else {}

                email = bucket_rec.get("email") or env_rec.get("email") or ""
                sessionid = bucket_rec.get("sessionid") or env_rec.get("sessionid") or ""
                sessionid_sign = (
                    bucket_rec.get("sessionid_sign") or env_rec.get("sessionid_sign") or ""
                )
                saved_jwt = bucket_rec.get("jwt_token") or env_rec.get("jwt_token") or ""

                # 2a. Reuse saved JWT if still valid and not force-refreshing primary node
                if not (force_refresh and offset == 0) and saved_jwt:
                    alive, payload = is_jwt_alive(saved_jwt)
                    if alive:
                        rec = self._build_record(
                            candidate_idx, saved_jwt, payload, email, sessionid, sessionid_sign
                        )
                        self._cached_record = rec
                        exp_rem_min = max(0, int((payload["exp"] - time.time()) / 60))
                        print(
                            f"[JWTManager] Reusing valid cached JWT from node_{candidate_idx} "
                            f"(user_id={payload['user_id']}, expires in {exp_rem_min}m).",
                            flush=True,
                        )
                        return saved_jwt

                # 2b. Saved JWT is expired or rejected -> exchange session cookies for a fresh JWT
                if sessionid:
                    fresh_jwt = extract_jwt_from_cookies(sessionid, sessionid_sign)
                    alive, payload = is_jwt_alive(fresh_jwt, safety_margin_seconds=60)
                    if alive and fresh_jwt:
                        rec = self._build_record(
                            candidate_idx, fresh_jwt, payload, email, sessionid, sessionid_sign
                        )
                        self._write_bucket_record(candidate_idx, rec)
                        if candidate_idx != self.notebook_id:
                            self._write_bucket_record(self.notebook_id, rec)
                        self._cached_record = rec
                        print(
                            f"[JWTManager] Refreshed JWT via sessionid cookie for node_{candidate_idx} "
                            f"(user_id={payload['user_id']}) and saved to bucket.",
                            flush=True,
                        )
                        return fresh_jwt

                # 2c. Cookie exchange failed -> try Playwright login if email + password available
                if email and login_password:
                    pw_res = playwright_login_and_extract(email, login_password)
                    if pw_res:
                        fresh_jwt = pw_res["jwt_token"]
                        alive, payload = is_jwt_alive(fresh_jwt, safety_margin_seconds=60)
                        if alive:
                            rec = self._build_record(
                                candidate_idx,
                                fresh_jwt,
                                payload,
                                email=email,
                                sessionid=pw_res.get("sessionid", ""),
                                sessionid_sign=pw_res.get("sessionid_sign", ""),
                            )
                            self._write_bucket_record(candidate_idx, rec)
                            if candidate_idx != self.notebook_id:
                                self._write_bucket_record(self.notebook_id, rec)
                            self._cached_record = rec
                            print(
                                f"[JWTManager] Refreshed JWT via Playwright login for {email} "
                                f"(user_id={payload['user_id']}) and saved to bucket.",
                                flush=True,
                            )
                            return fresh_jwt

            raise TVAuthError(
                f"CRITICAL AUTH FAILURE: Node {self.notebook_id} exhausted all {TOTAL_NOTEBOOKS} "
                "account slots without obtaining a valid authenticated JWT (user_id > 0). "
                "Refusing to scrape with unauthorized_user_token."
            )

