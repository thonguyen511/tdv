"""
TradingView WebSocket framing, dynamic study version discovery, Ultimate tier
verification, and 1D corporate-action event merging.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import random
import re
import string
import time
from typing import Any

import requests
import websocket


def generate_session(prefix: str) -> str:
    """Generate a random TradingView session identifier (e.g. `cs_xxxxxxxxxxxx`)."""
    chars = string.ascii_lowercase + string.digits
    return f"{prefix}_" + "".join(random.choice(chars) for _ in range(12))


def create_ws_message(func: str, param_list: list[Any]) -> str:
    """Format a TradingView Socket.IO WebSocket frame `~m~{len}~m~{json}`."""
    payload = json.dumps({"m": func, "p": param_list}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def parse_ws_messages(buffer: str) -> tuple[list[str], str]:
    """
    Parse all complete `~m~{length}~m~{payload}` frames from `buffer`.
    Returns `(messages, remaining_unparsed_buffer)`.
    """
    messages: list[str] = []
    while buffer.startswith("~m~"):
        end_len_idx = buffer.find("~m~", 3)
        if end_len_idx == -1:
            break
        try:
            length = int(buffer[3:end_len_idx])
        except ValueError:
            buffer = buffer[end_len_idx + 3 :]
            continue
        start_msg = end_len_idx + 3
        if len(buffer) < start_msg + length:
            break
        messages.append(buffer[start_msg : start_msg + length])
        buffer = buffer[start_msg + length :]
    return messages, buffer


def verify_cookie_tier(
    sessionid: str,
    sessionid_sign: str = "",
) -> tuple[bool, str | None, str]:
    """
    Verify a TradingView session cookie and determine whether it belongs to a
    paid Ultimate/Pro tier (`prodata.tradingview.com`) or Free tier (`data.tradingview.com`).

    Returns `(is_valid_authenticated, jwt_token, tier_label)`.
    """
    if not sessionid:
        return False, None, "Free"
    cookie_str = f"sessionid={sessionid}"
    if sessionid_sign:
        cookie_str += f"; sessionid_sign={sessionid_sign}"
    headers = {
        "Cookie": cookie_str,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        resp = requests.get("https://www.tradingview.com/", headers=headers, timeout=15)
        if resp.status_code != 200:
            return False, None, "Free"
        jwt_match = re.search(r'"auth_token"\s*:\s*"([^"]+)"', resp.text)
        jwt_token = jwt_match.group(1) if jwt_match else None
        if not jwt_token or jwt_token == "unauthorized_user_token":
            return False, None, "Free"
        is_pro = bool(
            re.search(r'"is_pro"\s*:\s*true', resp.text)
            or "prodata.tradingview.com" in resp.text
        )
        return True, jwt_token, ("Ultimate" if is_pro else "Free")
    except Exception:
        return False, None, "Free"


DEFAULT_STUDY_DESCRIPTORS: list[tuple[str, str, str]] = [
    ("st_splits", "Splits@tv-basicstudies-281", "raw_split"),
    ("st_divs", "Dividends@tv-basicstudies-281", "raw_dividend"),
    ("st_earns", "Earnings@tv-basicstudies-281", "raw_earnings"),
    ("st_rolls", "BarSetContinuousRollDates@tv-corestudies-50", "raw_roll"),
]


def resolve_live_study_descriptors(jwt_token: str) -> list[tuple[str, str, str]]:
    """
    Query TradingView's live `request_studies_metadata` endpoint once at startup
    to resolve the latest study version numbers for Splits, Dividends, Earnings,
    and Continuous Roll Dates. Falls back to `DEFAULT_STUDY_DESCRIPTORS` on timeout.
    """
    descriptors = list(DEFAULT_STUDY_DESCRIPTORS)
    ws = None
    try:
        ws = websocket.create_connection(
            "wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart",
            header=[
                "Origin: https://www.tradingview.com",
                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            ],
            timeout=15,
        )
        ws.send(create_ws_message("set_auth_token", [jwt_token]))
        ws.send(create_ws_message("request_studies_metadata", ["st_meta_req"]))

        targets: dict[str, tuple[str, str]] = {
            "Splits@tv-basicstudies-": ("st_splits", "raw_split"),
            "Dividends@tv-basicstudies-": ("st_divs", "raw_dividend"),
            "Earnings@tv-basicstudies-": ("st_earns", "raw_earnings"),
            "BarSetContinuousRollDates@tv-corestudies-": ("st_rolls", "raw_roll"),
        }
        best_versions: dict[str, tuple[int, str]] = {}
        buf = ""
        start = time.time()
        done = False

        while time.time() - start < 10.0 and not done:
            raw = ws.recv()
            if not raw:
                break
            buf += raw
            msgs, buf = parse_ws_messages(buf)
            for msg in msgs:
                if msg.startswith("~h~"):
                    ws.send(f"~m~{len(msg)}~m~{msg}")
                    continue
                if not msg.startswith("{"):
                    continue
                data = json.loads(msg)
                if data.get("m") == "studies_metadata":
                    params = data.get("p", [])
                    meta = params[1] if len(params) > 1 else params[0]
                    for item in meta.get("metainfo", []):
                        sid = item.get("id", "").rstrip("!")
                        if "PriceSupport" in sid:
                            continue
                        for prefix in targets:
                            if sid.startswith(prefix):
                                ver_str = sid.replace(prefix, "")
                                try:
                                    ver = int(ver_str)
                                except ValueError:
                                    ver = 0
                                if prefix not in best_versions or ver >= best_versions[prefix][0]:
                                    best_versions[prefix] = (ver, sid)
                    done = True
                    break

        if best_versions:
            resolved: list[tuple[str, str, str]] = []
            for idx, (prefix, (st_id, col_name)) in enumerate(targets.items()):
                if prefix in best_versions:
                    resolved.append((st_id, best_versions[prefix][1], col_name))
                else:
                    resolved.append(descriptors[idx])
            print(
                f"[Protocol] Dynamically resolved study versions: {[d[1] for d in resolved]}",
                flush=True,
            )
            return resolved
    except Exception as exc:
        print(
            f"[Protocol] Dynamic study metadata lookup fallback to defaults ({exc})",
            flush=True,
        )
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
    return descriptors


def merge_events_to_1d(
    bars: dict[float, list[Any]],
    study_events: dict[str, list[dict[str, Any]]],
    quote_data: dict[str, Any],
) -> None:
    """
    Attach raw corporate-action and roll study payloads (`raw_split`,
    `raw_dividend`, `raw_earnings`, `raw_roll`, `last_splits`) onto the matching
    `1D` bar in `bars`.

    In TradingView's study response (`data[st_id]["st"]`), each item has
    `"v": [bar_ts, ...]`, where `v[0]` is the exact `1D` bar unix timestamp.
    """
    if not bars:
        return
    sorted_ts = sorted(bars.keys())
    col_idx_map = {
        "raw_split": 6,
        "raw_dividend": 7,
        "raw_earnings": 8,
        "raw_roll": 9,
    }
    date_to_ts: dict[str, float] = {}
    for ts in sorted_ts:
        while len(bars[ts]) < 11:
            bars[ts].append(None)
        d_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
        date_to_ts[d_str] = ts

    # 1. Map splits from quote_data["last_splits"] by UTC date if present
    last_splits = quote_data.get("last_splits", [])
    if isinstance(last_splits, list) and last_splits:
        for sp_item in last_splits:
            if isinstance(sp_item, (list, tuple)) and len(sp_item) >= 2:
                try:
                    d_str = datetime.fromtimestamp(int(sp_item[0]), tz=timezone.utc).strftime("%Y-%m-%d")
                    if d_str in date_to_ts:
                        bars[date_to_ts[d_str]][6] = json.dumps(sp_item)
                except Exception:
                    pass
        if sorted_ts:
            bars[sorted_ts[-1]][10] = json.dumps(last_splits)

    # 2. Map study events (`st` items where v[0] is bar timestamp)
    for col_name, events in study_events.items():
        if not events:
            continue
        c_idx = col_idx_map[col_name]
        for ev in events:
            v = ev.get("v", [])
            if not v or v[0] is None:
                continue
            ev_ts = float(v[0])
            matched_ts = None
            if ev_ts in bars:
                matched_ts = ev_ts
            else:
                ev_d = datetime.fromtimestamp(int(ev_ts), tz=timezone.utc).strftime("%Y-%m-%d")
                if ev_d in date_to_ts:
                    matched_ts = date_to_ts[ev_d]
            if matched_ts is not None:
                bars[matched_ts][c_idx] = json.dumps(v)
