"""
TradingView WebSocket protocol framing, account tier probing, dynamic study
metadata resolution, and vectorized corporate-action / roll attachment
(`attach_events_to_df`) across all intervals (`1D`, `1W`, `1M`, and intraday).
"""

from __future__ import annotations

import json
import random
import re
import ssl
import string
import time
from typing import Any

import numpy as np
import pandas as pd
import requests
import websocket


def generate_session_id(prefix: str = "cs") -> str:
    """Generate a random 12-char TradingView session identifier."""
    rand_part = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{prefix}_{rand_part}"


def create_ws_message(func: str, param_list: list[Any]) -> str:
    """Format a TradingView Socket.IO ~m~ framed JSON message."""
    payload = json.dumps({"m": func, "p": param_list}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def parse_ws_messages(buffer: str) -> tuple[list[str], str]:
    """
    Stream-parse complete `~m~<len>~m~<payload>` frames from `buffer`.
    Returns `(complete_messages, remaining_unparsed_buffer)`.
    """
    messages: list[str] = []
    idx = 0
    n = len(buffer)
    while idx < n:
        if not buffer.startswith("~m~", idx):
            next_marker = buffer.find("~m~", idx)
            if next_marker == -1:
                return messages, ""
            idx = next_marker
        end_len_idx = buffer.find("~m~", idx + 3)
        if end_len_idx == -1:
            break
        len_str = buffer[idx + 3 : end_len_idx]
        try:
            msg_len = int(len_str)
        except ValueError:
            idx = end_len_idx + 3
            continue
        start_msg = end_len_idx + 3
        end_msg = start_msg + msg_len
        if end_msg > n:
            break
        messages.append(buffer[start_msg:end_msg])
        idx = end_msg
    return messages, buffer[idx:]


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


DEFAULT_STUDY_SCRIPTS: dict[str, str] = {
    "dividends": "Dividends@tv-basicstudies-281",
    "earnings": "Earnings@tv-basicstudies-281",
    "rolls": "BarSetContinuousRollDates@tv-corestudies-50",
}


def resolve_live_study_descriptors(jwt_token: str) -> dict[str, str]:
    """
    Query TradingView's live `request_studies_metadata` endpoint once at startup
    to resolve the latest study version numbers for Dividends, Earnings, and
    Continuous Roll Dates. Falls back to `DEFAULT_STUDY_SCRIPTS` on timeout.
    """
    scripts = dict(DEFAULT_STUDY_SCRIPTS)
    ws = None
    try:
        ws = websocket.create_connection(
            "wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart",
            header=[
                "Origin: https://www.tradingview.com",
                "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            ],
            sslopt={"cert_reqs": ssl.CERT_NONE},
            timeout=15,
        )
        ws.send(create_ws_message("set_auth_token", [jwt_token]))
        ws.send(create_ws_message("request_studies_metadata", ["st_meta_req"]))

        targets: dict[str, str] = {
            "Dividends@tv-basicstudies-": "dividends",
            "Earnings@tv-basicstudies-": "earnings",
            "BarSetContinuousRollDates@tv-corestudies-": "rolls",
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
                        for prefix, key_name in targets.items():
                            if sid.startswith(prefix):
                                ver_str = sid.replace(prefix, "")
                                try:
                                    ver = int(ver_str)
                                except ValueError:
                                    ver = 0
                                if key_name not in best_versions or ver >= best_versions[key_name][0]:
                                    best_versions[key_name] = (ver, sid)
                    done = True
                    break

        if best_versions:
            for key_name, (_, sid) in best_versions.items():
                scripts[key_name] = sid
            print(
                f"[Protocol] Dynamically resolved study versions: {scripts}",
                flush=True,
            )
            return scripts
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
    return scripts


def attach_events_to_df(
    df: pd.DataFrame,
    raw_events: dict[str, Any] | None,
    interval: str = "1D",
) -> pd.DataFrame:
    """
    Attach corporate events (`splits`, `dividends`, `earnings`, continuous `rolls`)
    to candlestick bars for ANY interval (`1D`, `1W`, `1M`, or intraday).
    Matches the exact logic from `run_first/event_engine.py` and `run_weekly/merge_engine.py`.
    """
    if df.empty:
        return df

    if "raw_split" not in df.columns:
        df["raw_split"] = np.nan
    if "raw_dividend" not in df.columns:
        df["raw_dividend"] = None
    if "raw_earnings" not in df.columns:
        df["raw_earnings"] = None
    if "raw_roll" not in df.columns:
        df["raw_roll"] = None

    if not raw_events:
        return df

    times = df["time"].values
    n_times = len(times)
    if n_times == 0:
        return df

    max_delta = 8 * 86400 if interval == "1W" else (35 * 86400 if interval == "1M" else 86400)

    def find_target_idx(event_ts_sec: float) -> int | None:
        if interval in ("1D", "1", "5", "15", "60", "240"):
            # Day-aligned match: Find first candle on or after start of UTC day
            start_of_day = (int(event_ts_sec) // 86400) * 86400
            end_of_day = start_of_day + 86400
            idx = int(np.searchsorted(times, start_of_day))
            if idx < n_times and times[idx] < end_of_day:
                return idx
            return None
        else:
            # Macro (1W, 1M): Find the candle that opened on or before event timestamp
            cand_idx = int(np.searchsorted(times, event_ts_sec, side="right")) - 1
            if 0 <= cand_idx < n_times and (event_ts_sec - times[cand_idx]) <= max_delta:
                return cand_idx
            return None

    # 1. Splits from Quote `last_splits`: item = [ts_sec, split_factor]
    for sp in raw_events.get("splits", []):
        if isinstance(sp, (list, tuple)) and len(sp) >= 2:
            try:
                sp_ts, sp_factor = float(sp[0]), float(sp[1])
                idx = find_target_idx(sp_ts)
                if idx is not None:
                    df.iat[idx, df.columns.get_loc("raw_split")] = sp_factor
            except Exception:
                pass

    # 2. Dividends: v = [bar_ts, gross_payout, ex_date_ms, adjusted_payout, payment_date_ms]
    for d in raw_events.get("dividends", []):
        v = d.get("v", []) if isinstance(d, dict) else []
        if len(v) >= 3 and v[2]:
            try:
                ex_ts = float(v[2]) / 1000.0
                idx = find_target_idx(ex_ts)
                if idx is None and v[0] and float(v[0]) > 100000:
                    idx = find_target_idx(float(v[0]))
                if idx is not None:
                    df.iat[idx, df.columns.get_loc("raw_dividend")] = tuple(v)
            except Exception:
                pass

    # 3. Earnings: v = [bar_ts, std_eps, est_eps, period_s, ann_ms, rep_eps, rev_est_M, rev_act_M, flags]
    for e in raw_events.get("earnings", []):
        v = e.get("v", []) if isinstance(e, dict) else []
        if len(v) >= 5 and v[4]:
            try:
                ann_ts = float(v[4]) / 1000.0
                idx = find_target_idx(ann_ts)
                if idx is None and v[0] and float(v[0]) > 100000:
                    idx = find_target_idx(float(v[0]))
                if idx is not None:
                    df.iat[idx, df.columns.get_loc("raw_earnings")] = tuple(v)
            except Exception:
                pass

    # 4. Continuous Rolls: v = [bar_ts, from_code, to_code, switch_date_yyyymmdd]
    for r in raw_events.get("rolls", []):
        v = r.get("v", []) if isinstance(r, dict) else []
        if len(v) >= 4 and v[0] is not None:
            try:
                roll_ts = float(v[0])
                idx = find_target_idx(roll_ts)
                if idx is not None:
                    df.iat[idx, df.columns.get_loc("raw_roll")] = tuple(v[:4])
            except Exception:
                pass

    return df
