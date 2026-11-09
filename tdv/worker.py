"""
Persistent WebSocket Worker Pool for Two-Stage Chunk Execution.

Implements:
- Persistent WebSocket connections per worker thread (reused across tasks by
  creating/deleting `chart_create_session` and `quote_create_session` on the
  open socket, matching `run_first/worker_pool.py` and `run_weekly/event_fetcher.py`).
- Stage 1 (`1D`):
  - Non-futures: Quote Session (`quote_set_fields` `["base_name", "last_splits"]`)
    + Chart Studies (`Dividends`, `Earnings` with `{}`).
  - Futures (`is_futures=True`): Chart Studies (`st_roll_a` with `{"currenttime": "now"}`
    and `st_roll_b` with `{}` on `BarSetContinuousRollDates`).
  - Waits for `main_series_completed` AND `required_studies.issubset(completed_studies)`.
  - Captures `has_intraday` from `symbol_resolved`.
- Stage 2 (`1W`, `1M`, and `has_intraday`-filtered intraday `1, 5, 15, 60, 240`):
  - Backwards paging via `request_more_data` in Phase 1 (`fetch_full_history=True`).
  - Single-batch fetch in Phase 2 (`fetch_full_history=False`).
  - Attaches corporate actions (`raw_split`, `raw_dividend`, `raw_earnings`, `raw_roll`)
    to all intervals via `attach_events_to_df`.
"""

from __future__ import annotations

import json
import queue
import random
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import websocket

from tdv.config import BARS_INITIAL_LOAD, BARS_MORE_LOAD, MAX_HISTORY_BATCHES, MAX_TASK_RETRIES
from tdv.jwt_manager import JWTManager, TVAuthError
from tdv.protocol import (
    DEFAULT_STUDY_SCRIPTS,
    attach_events_to_df,
    create_ws_message,
    generate_session_id,
    parse_ws_messages,
)


@dataclass
class TaskItem:
    symbol: str
    interval: str
    is_futures: bool = False
    retries: int = 0


@dataclass
class ChunkExecutionContext:
    """Shared state for a single 2,000-symbol chunk across the 8-worker pool."""

    fetch_full_history: bool
    study_scripts: dict[str, str]
    jwt_manager: JWTManager
    has_ultimate: bool = False
    ultimate_jwt: str | None = None

    # Queues
    primary_queue: queue.Queue[TaskItem | None] = field(default_factory=queue.Queue)
    intraday_queue: queue.Queue[TaskItem | None] = field(default_factory=queue.Queue)

    # Collected results (protected by results_lock)
    results_lock: threading.Lock = field(default_factory=threading.Lock)
    chunk_dfs: dict[str, list[pd.DataFrame]] = field(default_factory=dict)
    symbol_events: dict[str, dict[str, Any]] = field(default_factory=dict)
    symbol_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    success_logs: list[tuple[str, str, str]] = field(default_factory=list)
    failure_logs: list[tuple[str, str, str]] = field(default_factory=list)
    network_retry_tasks: list[TaskItem] = field(default_factory=list)

    # Abort / Auth flags
    abort_event: threading.Event = field(default_factory=threading.Event)
    auth_failed_event: threading.Event = field(default_factory=threading.Event)
    consecutive_429_errors: int = 0


def _connect_ws(endpoint: str, jwt_token: str, timeout: int = 15) -> websocket.WebSocket:
    """Open a TradingView WebSocket connection and authenticate with `jwt_token`."""
    ws = websocket.create_connection(
        f"wss://{endpoint}/socket.io/websocket?from=chart%2F&type=chart",
        header=[
            "Origin: https://www.tradingview.com",
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        ],
        sslopt={"cert_reqs": ssl.CERT_NONE},
        timeout=timeout,
    )
    ws.recv()  # Read initial handshake packet
    ws.send(create_ws_message("set_auth_token", [jwt_token]))
    return ws


def _execute_task_on_ws(
    ws: websocket.WebSocket,
    ws_buffer: str,
    task: TaskItem,
    ctx: ChunkExecutionContext,
) -> tuple[
    str,
    str,
    str,
    dict[float, list[Any]],
    dict[str, list[Any]],
    dict[str, bool],
]:
    """
    Execute a single `(symbol, interval)` scrape on an already-connected WebSocket `ws`.

    Returns:
        `(status, error_reason, updated_ws_buffer, bars_dict, raw_events, resolved_meta)`
        where `status` is one of:
        - `"OK"`: Completed normally
        - `"SYMBOL_ERROR"`: Definitive TradingView symbol/series error (do not retry)
        - `"AUTH_ERROR"`: JWT expired or rejected (`critical_error` / auth error)
        - `"NETWORK_ERROR"`: Socket closed or timed out (reconnect and retry)
    """
    symbol = task.symbol
    interval = task.interval
    is_1d = interval == "1D"
    is_futures = task.is_futures or symbol.endswith("1!") or symbol.endswith("2!")

    chart_session = generate_session_id("cs")
    quote_session = generate_session_id("qs") if (is_1d and not is_futures) else None
    series_id = "sds_1"
    sym_id = "sds_sym_1"

    # 1. Quote session for fast stock splits on 1D (`quote_set_fields` uses flat args!)
    if is_1d and quote_session:
        ws.send(create_ws_message("quote_create_session", [quote_session]))
        ws.send(
            create_ws_message(
                "quote_set_fields",
                [quote_session, "base_name", "last_splits"],
            )
        )
        ws.send(create_ws_message("quote_add_symbols", [quote_session, symbol]))

    # 2. Chart session for unadjusted OHLCV bars & event studies
    ws.send(create_ws_message("chart_create_session", [chart_session, ""]))
    resolve_config = json.dumps({"symbol": symbol, "adjustment": "none"})
    ws.send(
        create_ws_message(
            "resolve_symbol",
            [chart_session, sym_id, f"={resolve_config}"],
        )
    )
    ws.send(
        create_ws_message(
            "create_series",
            [chart_session, series_id, "s1", sym_id, interval, BARS_INITIAL_LOAD, ""],
        )
    )

    # 3. Add Event Studies on 1D probe (matching `run_first/worker_pool.py`)
    required_studies: set[str] = set()
    completed_studies: set[str] = set()

    div_script = ctx.study_scripts.get("dividends", DEFAULT_STUDY_SCRIPTS["dividends"])
    earn_script = ctx.study_scripts.get("earnings", DEFAULT_STUDY_SCRIPTS["earnings"])
    roll_script = ctx.study_scripts.get("rolls", DEFAULT_STUDY_SCRIPTS["rolls"])

    if is_1d:
        if not is_futures:
            ws.send(
                create_ws_message(
                    "create_study",
                    [chart_session, "st_div", "st1", series_id, div_script, {}],
                )
            )
            ws.send(
                create_ws_message(
                    "create_study",
                    [chart_session, "st_earn", "st1", series_id, earn_script, {}],
                )
            )
            required_studies.update(["st_div", "st_earn"])
        else:
            ws.send(
                create_ws_message(
                    "create_study",
                    [chart_session, "st_roll_a", "st1", series_id, roll_script, {"currenttime": "now"}],
                )
            )
            ws.send(
                create_ws_message(
                    "create_study",
                    [chart_session, "st_roll_b", "st2", series_id, roll_script, {}],
                )
            )
            required_studies.update(["st_roll_a", "st_roll_b"])

    bars: dict[float, list[Any]] = {}
    raw_events: dict[str, list[Any]] = {
        "splits": [],
        "dividends": [],
        "earnings": [],
        "rolls": [],
    }
    resolved_meta: dict[str, bool] = {}

    main_series_completed = False
    prev_bar_count = 0
    history_batches = 0

    status = "OK"
    error_reason = ""
    last_wait = time.time()
    series_completed_at: float | None = None

    while True:
        if ctx.abort_event.is_set() or ctx.auth_failed_event.is_set():
            status = "NETWORK_ERROR"
            error_reason = "Aborted"
            break

        now_ts = time.time()
        if is_1d and main_series_completed and series_completed_at is not None and (now_ts - series_completed_at > 6.0):
            # Main 1D series completed >6s ago; don't let a silent study hang mutual funds/indices
            status = "OK"
            break

        if now_ts - last_wait > 25.0:
            if bars and main_series_completed:
                # Series already completed; don't discard valid bars if a study timed out
                status = "OK"
            else:
                status = "NETWORK_ERROR"
                error_reason = "Recv timeout (>25s)"
            break

        try:
            ws.settimeout(3.0)
            raw_msg = ws.recv()
        except websocket.WebSocketTimeoutException:
            if main_series_completed and (
                not is_1d
                or required_studies.issubset(completed_studies)
                or (series_completed_at is not None and time.time() - series_completed_at > 6.0)
            ):
                break
            continue
        except Exception as exc:
            if bars and main_series_completed:
                status = "OK"
            else:
                status = "NETWORK_ERROR"
                error_reason = f"Socket recv error: {exc}"
            break

        if not raw_msg:
            if bars and main_series_completed:
                status = "OK"
            else:
                status = "NETWORK_ERROR"
                error_reason = "Socket closed by remote"
            break

        ws_buffer += raw_msg
        messages, ws_buffer = parse_ws_messages(ws_buffer)

        done_loop = False
        for msg in messages:
            if msg.startswith("~h~"):
                ws.send(f"~m~{len(msg)}~m~{msg}")
                continue
            if not msg.startswith("{"):
                continue
            try:
                data = json.loads(msg)
            except Exception:
                continue

            last_wait = time.time()
            m_type = data.get("m")
            params = data.get("p", [])

            # Capture Quote Session splits (`params[0] == quote_session`)
            if (
                m_type == "qsd"
                and is_1d
                and quote_session
                and len(params) >= 2
                and params[0] == quote_session
                and isinstance(params[1], dict)
            ):
                v_dict = params[1].get("v", {})
                if "last_splits" in v_dict and isinstance(v_dict["last_splits"], list):
                    raw_events["splits"] = v_dict["last_splits"]

            # Capture Symbol Info (`params[0] == chart_session`)
            elif (
                m_type == "symbol_resolved"
                and len(params) >= 2
                and params[0] == chart_session
            ):
                meta = (
                    params[2]
                    if len(params) >= 3 and isinstance(params[2], dict)
                    else (params[1] if isinstance(params[1], dict) else {})
                )
                if isinstance(meta, dict):
                    resolved_meta["has_intraday"] = bool(meta.get("has_intraday", True))
                    resolved_meta["has_adjustment"] = bool(meta.get("has_adjustment", False))

            # Capture Bars and Studies (`params[0] == chart_session`)
            elif (
                m_type in ("timescale_update", "du")
                and len(params) >= 2
                and params[0] == chart_session
                and isinstance(params[1], dict)
            ):
                payload_dict = params[1]
                if series_id in payload_dict:
                    s_list = payload_dict[series_id].get("s", [])
                    for item in s_list:
                        v = item.get("v")
                        if v and len(v) >= 5:
                            bars[v[0]] = [
                                v[0],
                                v[1],
                                v[2],
                                v[3],
                                v[4],
                                v[5] if len(v) > 5 else 0.0,
                            ]
                if is_1d:
                    if "st_div" in payload_dict:
                        st = payload_dict["st_div"].get("st", [])
                        if st:
                            raw_events["dividends"].extend(st)
                    if "st_earn" in payload_dict:
                        st = payload_dict["st_earn"].get("st", [])
                        if st:
                            raw_events["earnings"].extend(st)
                    for roll_key in ("st_roll_a", "st_roll_b"):
                        if roll_key in payload_dict:
                            st = payload_dict[roll_key].get("st", [])
                            if st:
                                raw_events["rolls"].extend(st)

            elif (
                m_type == "series_completed"
                and len(params) >= 1
                and params[0] == chart_session
            ):
                current_count = len(bars)
                if ctx.fetch_full_history and not is_1d:
                    if (
                        current_count > prev_bar_count
                        and current_count > 0
                        and history_batches < MAX_HISTORY_BATCHES
                    ):
                        prev_bar_count = current_count
                        history_batches += 1
                        ws.send(
                            create_ws_message(
                                "request_more_data",
                                [chart_session, series_id, BARS_MORE_LOAD],
                            )
                        )
                    else:
                        main_series_completed = True
                        if series_completed_at is None:
                            series_completed_at = time.time()
                else:
                    # 1D initial load (2,000,000 bars covers all 1D history in 1 batch) or Weekly Phase 2
                    main_series_completed = True
                    if series_completed_at is None:
                        series_completed_at = time.time()

                if main_series_completed and (not is_1d or required_studies.issubset(completed_studies)):
                    done_loop = True
                    break

            elif (
                m_type in ("study_completed", "study_error")
                and is_1d
                and len(params) >= 2
                and params[0] == chart_session
            ):
                completed_studies.add(str(params[1]))
                if main_series_completed and required_studies.issubset(completed_studies):
                    done_loop = True
                    break

            elif (
                m_type in ("symbol_error", "series_error")
                and len(params) >= 1
                and params[0] == chart_session
            ):
                status = "SYMBOL_ERROR"
                error_reason = f"{m_type}: {params}"
                done_loop = True
                break

            elif m_type in ("critical_error", "error"):
                err_str = str(params).lower()
                if "auth" in err_str or "token" in err_str or "permission" in err_str:
                    status = "AUTH_ERROR"
                else:
                    status = "NETWORK_ERROR"
                error_reason = str(params)
                done_loop = True
                break

        if done_loop:
            break

    # Clean up chart/quote sessions on the persistent socket if still healthy
    if status in ("OK", "SYMBOL_ERROR"):
        try:
            ws.send(create_ws_message("chart_delete_session", [chart_session]))
            if is_1d and quote_session:
                ws.send(create_ws_message("quote_delete_session", [quote_session]))
        except Exception:
            pass

    return status, error_reason, ws_buffer, bars, raw_events, resolved_meta


def _bars_to_dataframe_with_events(
    symbol: str,
    interval: str,
    bars: dict[float, list[Any]],
    raw_events: dict[str, Any] | None,
) -> pd.DataFrame:
    """
    Convert `bars` dictionary into a sorted DataFrame and attach corporate action
    and roll events (`raw_split`, `raw_dividend`, `raw_earnings`, `raw_roll`) via
    `attach_events_to_df`.
    """
    if not bars:
        return pd.DataFrame()
    sorted_rows = [bars[ts] for ts in sorted(bars.keys())]
    arr = np.array(sorted_rows, dtype=np.float64)
    df = pd.DataFrame(
        {
            "symbol": symbol,
            "time": arr[:, 0].astype(np.int64),
            "open": arr[:, 1],
            "high": arr[:, 2],
            "low": arr[:, 3],
            "close": arr[:, 4],
            "volume": arr[:, 5] if arr.shape[1] >= 6 else np.zeros(arr.shape[0], dtype=np.float64),
        }
    )
    return attach_events_to_df(df=df, raw_events=raw_events, interval=interval)


def _worker_thread_loop(
    worker_id: int,
    role: str,
    ctx: ChunkExecutionContext,
) -> None:
    """
    Worker loop servicing tasks from `ctx.primary_queue` (and `ctx.intraday_queue`
    when running in dual Ultimate + Free mode).
    """
    is_ultimate_worker = role == "ultimate" and ctx.has_ultimate and bool(ctx.ultimate_jwt)
    endpoint = "prodata.tradingview.com" if is_ultimate_worker else "data.tradingview.com"

    ws: websocket.WebSocket | None = None
    ws_buffer = ""

    def _close_ws():
        nonlocal ws, ws_buffer
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
            ws = None
        ws_buffer = ""

    while not ctx.abort_event.is_set() and not ctx.auth_failed_event.is_set():
        task: TaskItem | None = None
        picked_queue: queue.Queue[TaskItem | None] | None = None

        if is_ultimate_worker:
            try:
                task = ctx.intraday_queue.get_nowait()
                picked_queue = ctx.intraday_queue
            except queue.Empty:
                try:
                    task = ctx.primary_queue.get_nowait()
                    picked_queue = ctx.primary_queue
                except queue.Empty:
                    break
        else:
            try:
                task = ctx.primary_queue.get_nowait()
                picked_queue = ctx.primary_queue
            except queue.Empty:
                try:
                    task = ctx.intraday_queue.get_nowait()
                    picked_queue = ctx.intraday_queue
                except queue.Empty:
                    break

        if task is None:
            if picked_queue is not None:
                picked_queue.task_done()
            break

        try:
            time.sleep(random.uniform(0.02, 0.05))
            reused_socket = ws is not None

            if ws is None:
                try:
                    jwt_tok = (
                        ctx.ultimate_jwt
                        if is_ultimate_worker and ctx.ultimate_jwt
                        else ctx.jwt_manager.get_jwt(force_refresh=False)
                    )
                except TVAuthError as auth_exc:
                    print(f"[Worker-{worker_id}] {auth_exc}", flush=True)
                    ctx.auth_failed_event.set()
                    ctx.abort_event.set()
                    break

                try:
                    ws = _connect_ws(endpoint, jwt_tok)
                    ws_buffer = ""
                    with ctx.results_lock:
                        ctx.consecutive_429_errors = 0
                except Exception as conn_exc:
                    err_text = str(conn_exc)
                    if "429" in err_text or "Too Many Requests" in err_text:
                        with ctx.results_lock:
                            ctx.consecutive_429_errors += 1
                            if ctx.consecutive_429_errors >= 12:
                                print(
                                    f"[Worker-{worker_id}] Rate limit threshold exceeded (429).",
                                    flush=True,
                                )
                                ctx.abort_event.set()
                    time.sleep(2.0)
                    _close_ws()
                    if task.retries < MAX_TASK_RETRIES:
                        task.retries += 1
                        with ctx.results_lock:
                            ctx.network_retry_tasks.append(task)
                    else:
                        with ctx.results_lock:
                            if task.interval == "1D":
                                ctx.symbol_metadata[task.symbol] = {
                                    "has_intraday": False,
                                    "has_adjustment": False,
                                    "symbol_error": True,
                                }
                            ctx.failure_logs.append(
                                (task.symbol, task.interval, f"Connect failed: {conn_exc}")
                            )
                    continue

            try:
                (
                    status,
                    error_reason,
                    ws_buffer,
                    bars,
                    raw_events,
                    resolved_meta,
                ) = _execute_task_on_ws(ws, ws_buffer, task, ctx)
            except Exception as send_exc:
                # `ws.send()` on a socket the server already closed raises
                # BrokenPipeError / WebSocketConnectionClosedException. Treat it as a
                # transient NETWORK_ERROR so the thread survives and the task is retried
                # instead of silently killing the worker and dropping the task.
                status = "NETWORK_ERROR"
                error_reason = f"Socket send error: {type(send_exc).__name__}: {send_exc}"
                bars, raw_events, resolved_meta = {}, {}, {}

            # If a reused persistent socket was closed by the server between tasks
            # (BrokenPipeError on send or immediate EOF on recv), immediately reconnect
            # a fresh socket and retry the task in-place without burning `task.retries`.
            if (
                status == "NETWORK_ERROR"
                and reused_socket
                and not bars
                and not ctx.abort_event.is_set()
                and not ctx.auth_failed_event.is_set()
            ):
                _close_ws()
                time.sleep(random.uniform(0.1, 0.3))
                try:
                    jwt_tok = (
                        ctx.ultimate_jwt
                        if is_ultimate_worker and ctx.ultimate_jwt
                        else ctx.jwt_manager.get_jwt(force_refresh=False)
                    )
                    ws = _connect_ws(endpoint, jwt_tok)
                    ws_buffer = ""
                    (
                        status,
                        error_reason,
                        ws_buffer,
                        bars,
                        raw_events,
                        resolved_meta,
                    ) = _execute_task_on_ws(ws, ws_buffer, task, ctx)
                except TVAuthError as auth_exc:
                    print(f"[Worker-{worker_id}] {auth_exc}", flush=True)
                    ctx.auth_failed_event.set()
                    ctx.abort_event.set()
                    break
                except Exception as retry_exc:
                    _close_ws()
                    status = "NETWORK_ERROR"
                    error_reason = f"Reconnect retry error: {type(retry_exc).__name__}: {retry_exc}"
                    bars, raw_events, resolved_meta = {}, {}, {}

            if status == "AUTH_ERROR":
                _close_ws()
                if not is_ultimate_worker:
                    try:
                        ctx.jwt_manager.get_jwt(force_refresh=True)
                    except TVAuthError as auth_exc:
                        print(f"[Worker-{worker_id}] {auth_exc}", flush=True)
                        ctx.auth_failed_event.set()
                        ctx.abort_event.set()
                        break
                if task.retries < MAX_TASK_RETRIES:
                    task.retries += 1
                    with ctx.results_lock:
                        ctx.network_retry_tasks.append(task)
                else:
                    with ctx.results_lock:
                        if task.interval == "1D":
                            ctx.symbol_metadata[task.symbol] = {
                                "has_intraday": False,
                                "has_adjustment": False,
                                "symbol_error": True,
                            }
                        ctx.failure_logs.append(
                            (task.symbol, task.interval, f"Auth error: {error_reason}")
                        )
                continue

            if status == "NETWORK_ERROR":
                _close_ws()
                time.sleep(random.uniform(0.3, 1.0))
                if task.retries < MAX_TASK_RETRIES:
                    task.retries += 1
                    with ctx.results_lock:
                        ctx.network_retry_tasks.append(task)
                else:
                    with ctx.results_lock:
                        if task.interval == "1D":
                            ctx.symbol_metadata[task.symbol] = {
                                "has_intraday": False,
                                "has_adjustment": False,
                                "symbol_error": True,
                            }
                        ctx.failure_logs.append(
                            (task.symbol, task.interval, f"Network error: {error_reason}")
                        )
                continue

            if status == "SYMBOL_ERROR":
                with ctx.results_lock:
                    if task.interval == "1D":
                        ctx.symbol_metadata[task.symbol] = {
                            "has_intraday": False,
                            "has_adjustment": False,
                            "symbol_error": True,
                        }
                    ctx.failure_logs.append(
                        (task.symbol, task.interval, f"Symbol error: {error_reason}")
                    )
                continue

            # status == "OK"
            if task.interval == "1D":
                with ctx.results_lock:
                    if bars:
                        ctx.symbol_events[task.symbol] = raw_events
                        ctx.symbol_metadata[task.symbol] = {
                            "has_intraday": bool(resolved_meta.get("has_intraday", True)),
                            "has_adjustment": bool(resolved_meta.get("has_adjustment", False)),
                            "symbol_error": False,
                        }
                    else:
                        # 1D returned 0 bars (series_completed_but_empty) -> skip Stage 2 for this empty symbol
                        ctx.symbol_metadata[task.symbol] = {
                            "has_intraday": False,
                            "has_adjustment": False,
                            "symbol_error": True,
                        }
                events_for_sym = raw_events
            else:
                with ctx.results_lock:
                    events_for_sym = ctx.symbol_events.get(task.symbol)

            if bars:
                df_sym = _bars_to_dataframe_with_events(
                    symbol=task.symbol,
                    interval=task.interval,
                    bars=bars,
                    raw_events=events_for_sym,
                )
                with ctx.results_lock:
                    ctx.chunk_dfs.setdefault(task.interval, []).append(df_sym)
                    ctx.success_logs.append((task.symbol, task.interval, "OK"))
            else:
                with ctx.results_lock:
                    ctx.failure_logs.append((task.symbol, task.interval, "series_completed_but_empty"))

        finally:
            if picked_queue is not None:
                picked_queue.task_done()

    _close_ws()


def run_task_batch_with_retries(
    tasks: list[TaskItem],
    ctx: ChunkExecutionContext,
    total_workers: int = 8,
    ultimate_workers: int = 5,
) -> None:
    """
    Execute a batch of `TaskItem`s across the 8-worker pool, automatically
    retrying transient `NETWORK_ERROR` tasks up to `MAX_TASK_RETRIES` rounds.
    """
    pending = list(tasks)
    round_num = 0

    while pending and not ctx.abort_event.is_set() and not ctx.auth_failed_event.is_set():
        round_num += 1
        ctx.network_retry_tasks.clear()

        for item in pending:
            if ctx.has_ultimate and item.interval not in ("1D", "1W", "1M"):
                ctx.intraday_queue.put(item)
            else:
                ctx.primary_queue.put(item)

        threads: list[threading.Thread] = []
        if ctx.has_ultimate:
            free_count = max(1, total_workers - ultimate_workers)
            for i in range(ultimate_workers):
                t = threading.Thread(
                    target=_worker_thread_loop,
                    args=(i, "ultimate", ctx),
                    daemon=True,
                )
                t.start()
                threads.append(t)
            for i in range(free_count):
                t = threading.Thread(
                    target=_worker_thread_loop,
                    args=(ultimate_workers + i, "free", ctx),
                    daemon=True,
                )
                t.start()
                threads.append(t)
        else:
            for i in range(total_workers):
                t = threading.Thread(
                    target=_worker_thread_loop,
                    args=(i, "free", ctx),
                    daemon=True,
                )
                t.start()
                threads.append(t)

        for t in threads:
            t.join()

        while not ctx.primary_queue.empty():
            try:
                ctx.primary_queue.get_nowait()
                ctx.primary_queue.task_done()
            except queue.Empty:
                break
        while not ctx.intraday_queue.empty():
            try:
                ctx.intraday_queue.get_nowait()
                ctx.intraday_queue.task_done()
            except queue.Empty:
                break

        if ctx.network_retry_tasks and not ctx.abort_event.is_set() and not ctx.auth_failed_event.is_set():
            pending = list(ctx.network_retry_tasks)
            ctx.network_retry_tasks.clear()
            backoff_sec = min(30.0, 4.0 * round_num)
            print(
                f"[WorkerPool] Retrying {len(pending)} transient network tasks "
                f"(round {round_num + 1}, backoff {backoff_sec:.0f}s)...",
                flush=True,
            )
            time.sleep(backoff_sec)
            if round_num >= 2:
                try:
                    ctx.jwt_manager.get_jwt(force_refresh=True)
                except Exception:
                    pass
        else:
            break
