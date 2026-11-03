"""
Persistent WebSocket worker threads for the two-stage TradingView pipeline.

Key features:
- Requests raw unadjusted bars via `{"symbol": symbol, "adjustment": "none"}` in
  `resolve_symbol`.
- Records `has_intraday` and `has_adjustment` from `symbol_resolved` during
  Stage 1 (`1D`) so Stage 2 only queues eligible symbols for intraday intervals.
- Supports both Phase 1 (`fetch_full_history=True`, looping `request_more_data`
  backwards up to `MAX_HISTORY_BATCHES`) and Phase 2 (`fetch_full_history=False`,
  single-batch recent history fetch).
- Fixes the worker pool routing bug: when running on the Free account pool
  (no Ultimate account), all 8 workers service Stage 1 (`1D`) and Stage 2
  (`1W`, `1M`, and eligible intraday intervals). When an Ultimate account is
  configured, 5 Ultimate workers + 3 Free workers run in dual-pool mode, with
  Ultimate workers also assisting on `1D`/`1W`/`1M` whenever `intraday_queue`
  is empty.
- Never scrapes with `unauthorized_user_token`: if JWT refresh raises `TVAuthError`,
  signals `auth_failed_event` immediately.
"""

from __future__ import annotations

import json
import queue
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import websocket

from tdv.config import (
    BARS_INITIAL_LOAD,
    BARS_MORE_LOAD,
    MAX_HISTORY_BATCHES,
    MAX_TASK_RETRIES,
)
from tdv.jwt_manager import JWTManager, TVAuthError
from tdv.protocol import create_ws_message, generate_session, merge_events_to_1d, parse_ws_messages


@dataclass
class TaskItem:
    symbol: str
    interval: str
    retries: int = 0


@dataclass
class ChunkExecutionContext:
    """Shared per-chunk state across the 8 worker threads."""

    fetch_full_history: bool
    study_descriptors: list[tuple[str, str, str]]
    jwt_manager: JWTManager
    has_ultimate: bool = False
    ultimate_jwt: str | None = None

    # Queues
    primary_queue: queue.Queue[TaskItem | None] = field(default_factory=queue.Queue)
    intraday_queue: queue.Queue[TaskItem | None] = field(default_factory=queue.Queue)

    # Locks & Synchronization
    results_lock: threading.Lock = field(default_factory=threading.Lock)
    abort_event: threading.Event = field(default_factory=threading.Event)
    auth_failed_event: threading.Event = field(default_factory=threading.Event)

    # Outputs populated by workers
    # interval -> list[row]
    chunk_bars: dict[str, list[list[Any]]] = field(default_factory=dict)
    # symbol -> {"has_intraday": bool, "has_adjustment": bool}
    symbol_metadata: dict[str, dict[str, bool]] = field(default_factory=dict)
    # [(symbol, interval, status_tag)]
    success_logs: list[tuple[str, str, str]] = field(default_factory=list)
    # [(symbol, interval, reason)]
    failure_logs: list[tuple[str, str, str]] = field(default_factory=list)
    # Tasks that suffered transient network errors and need retry
    network_retry_tasks: list[TaskItem] = field(default_factory=list)
    consecutive_429_errors: int = 0


def _connect_ws(endpoint: str, jwt_token: str) -> websocket.WebSocket:
    ws_url = f"wss://{endpoint}/socket.io/websocket?from=chart%2F&type=chart"
    headers = [
        "Origin: https://www.tradingview.com",
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    ]
    ws = websocket.create_connection(
        ws_url,
        header=headers,
        timeout=25,
        sslopt={"cert_reqs": ssl.CERT_NONE},
    )
    ws.send(create_ws_message("set_auth_token", [jwt_token]))
    return ws


def _execute_task_on_ws(
    ws: websocket.WebSocket,
    ws_buffer: str,
    task: TaskItem,
    ctx: ChunkExecutionContext,
) -> tuple[str, str, str, dict[float, list[Any]], dict[str, list[dict[str, Any]]], dict[str, Any], dict[str, bool]]:
    """
    Execute a single `(symbol, interval)` scrape over an open WebSocket connection.

    Returns `(status, error_reason, remaining_ws_buffer, bars, study_events, quote_data, resolved_meta)`.
    `status` is one of:
    - `"OK"`
    - `"SYMBOL_ERROR"`
    - `"AUTH_ERROR"`
    - `"NETWORK_ERROR"`
    """
    symbol = task.symbol
    interval = task.interval
    is_1d = interval == "1D"

    chart_session = generate_session("cs")
    quote_session = generate_session("qs") if is_1d else None
    sym_id = "sds_sym_1"
    series_id = "sds_1"

    ws.send(create_ws_message("chart_create_session", [chart_session, ""]))
    ws.send(create_ws_message("switch_timezone", [chart_session, "Etc/UTC"]))

    if is_1d and quote_session:
        ws.send(create_ws_message("quote_create_session", [quote_session]))
        ws.send(
            create_ws_message(
                "quote_set_fields",
                [quote_session, "base_name", "short_name", "last_splits"],
            )
        )
        ws.send(create_ws_message("quote_add_symbols", [quote_session, symbol]))
        ws.send(create_ws_message("quote_fast_symbols", [quote_session, symbol]))

    # Request rawest nominal unadjusted bars via adjustment="none"
    resolve_Config = json.dumps({"symbol": symbol, "adjustment": "none"})
    ws.send(
        create_ws_message(
            "resolve_symbol",
            [chart_session, sym_id, f"={resolve_Config}"],
        )
    )
    ws.send(
        create_ws_message(
            "create_series",
            [chart_session, series_id, "s1", sym_id, interval, BARS_INITIAL_LOAD, ""],
        )
    )

    study_col_map: dict[str, str] = {}
    if is_1d:
        for idx_st, (st_id, st_script, col_name) in enumerate(ctx.study_descriptors):
            study_col_map[st_id] = col_name
            ws.send(
                create_ws_message(
                    "create_study",
                    [
                        chart_session,
                        st_id,
                        f"st{idx_st}",
                        series_id,
                        st_script,
                        {"currenttime": "now"},
                    ],
                )
            )

    bars: dict[float, list[Any]] = {}
    study_events: dict[str, list[dict[str, Any]]] = {
        col: [] for _, _, col in ctx.study_descriptors
    }
    quote_data: dict[str, Any] = {}
    resolved_meta: dict[str, bool] = {}

    completed_studies = 0
    total_studies = len(ctx.study_descriptors) if is_1d else 0
    main_series_completed = False
    prev_bar_count = 0
    history_batches = 0

    status = "OK"
    error_reason = ""
    last_Wait = time.time()

    while True:
        if ctx.abort_event.is_set() or ctx.auth_failed_event.is_set():
            status = "NETWORK_ERROR"
            error_reason = "Aborted"
            break

        if time.time() - last_Wait > 25.0:
            status = "NETWORK_ERROR"
            error_reason = "Recv timeout (>25s)"
            break

        try:
            raw_msg = ws.recv()
        except Exception as exc:
            status = "NETWORK_ERROR"
            error_reason = f"Socket recv error: {exc}"
            break

        if not raw_msg:
            status = "NETWORK_ERROR"
            error_reason = "Socket closed by remote"
            break

        last_Wait = time.time()
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

            m_type = data.get("m")
            params = data.get("p", [])

            if m_type == "qsd" and is_1d and len(params) >= 2 and isinstance(params[1], dict):
                v_dict = params[1].get("v", {})
                if "last_splits" in v_dict:
                    quote_data["last_splits"] = v_dict["last_splits"]

            elif m_type == "symbol_resolved" and len(params) >= 2:
                meta = params[2] if len(params) >= 3 and isinstance(params[2], dict) else (
                    params[1] if isinstance(params[1], dict) else {}
                )
                if isinstance(meta, dict):
                    resolved_meta["has_intraday"] = bool(meta.get("has_intraday", False))
                    resolved_meta["has_adjustment"] = bool(meta.get("has_adjustment", False))

            elif m_type in ("timescale_update", "du") and len(params) >= 2 and isinstance(params[1], dict):
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
                    for st_id, col_name in study_col_map.items():
                        if st_id in payload_dict:
                            st_items = (
                                payload_dict[st_id].get("st", [])
                                or payload_dict[st_id].get("s", [])
                            )
                            if st_items:
                                study_events[col_name].extend(st_items)

            elif m_type == "series_completed":
                current_count = len(bars)
                if ctx.fetch_full_history:
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
                else:
                    # Phase 2 (Weekly): single batch of recent bars
                    main_series_completed = True

                if main_series_completed and (not is_1d or completed_studies >= total_studies):
                    done_loop = True
                    break

            elif m_type == "study_completed" and is_1d:
                completed_studies += 1
                if main_series_completed and completed_studies >= total_studies:
                    done_loop = True
                    break

            elif m_type == "study_error" and is_1d:
                completed_studies += 1
                if main_series_completed and completed_studies >= total_studies:
                    done_loop = True
                    break

            elif m_type in ("symbol_error", "series_error"):
                status = "SYMBOL_ERROR"
                error_reason = str(params)
                done_loop = True
                break

            elif m_type == "critical_error":
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

    return status, error_reason, ws_buffer, bars, study_events, quote_data, resolved_meta


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
            # Prioritize intraday_queue, but assist on primary_queue if intraday_queue is empty
            try:
                task = ctx.intraday_queue.get_nowait()
                picked_queue = ctx.intraday_queue
            except queue.Empty:
                try:
                    task = ctx.primary_queue.get(timeout=0.5)
                    picked_queue = ctx.primary_queue
                except queue.Empty:
                    continue
        else:
            try:
                task = ctx.primary_queue.get(timeout=0.5)
                picked_queue = ctx.primary_queue
            except queue.Empty:
                continue

        if task is None:
            if picked_queue is not None:
                picked_queue.task_done()
            break

        try:
            # Ensure active WebSocket connection with a valid authenticated JWT
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
                            ctx.failure_logs.append(
                                (task.symbol, task.interval, f"Connect failed: {conn_exc}")
                            )
                    continue

            (
                status,
                error_reason,
                ws_buffer,
                bars,
                study_events,
                quote_data,
                resolved_meta,
            ) = _execute_task_on_ws(ws, ws_buffer, task, ctx)

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
                        ctx.failure_logs.append(
                            (task.symbol, task.interval, f"Auth error: {error_reason}")
                        )
                continue

            if status == "NETWORK_ERROR":
                _close_ws()
                if task.retries < MAX_TASK_RETRIES:
                    task.retries += 1
                    with ctx.results_lock:
                        ctx.network_retry_tasks.append(task)
                else:
                    with ctx.results_lock:
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
                merge_events_to_1d(bars, study_events, quote_data)

            with ctx.results_lock:
                if task.interval == "1D":
                    ctx.symbol_metadata[task.symbol] = {
                        "has_intraday": bool(resolved_meta.get("has_intraday", False)),
                        "has_adjustment": bool(resolved_meta.get("has_adjustment", False)),
                        "symbol_error": False,
                    }
                if bars:
                    target_list = ctx.chunk_bars.setdefault(task.interval, [])
                    for ts in sorted(bars.keys()):
                        b = bars[ts]
                        if task.interval == "1D":
                            target_list.append(
                                [
                                    task.symbol,
                                    b[0],
                                    b[1],
                                    b[2],
                                    b[3],
                                    b[4],
                                    b[5],
                                    b[6],
                                    b[7],
                                    b[8],
                                    b[9],
                                    b[10],
                                ]
                            )
                        else:
                            target_list.append(
                                [task.symbol, b[0], b[1], b[2], b[3], b[4], b[5]]
                            )
                    ctx.success_logs.append((task.symbol, task.interval, "OK"))
                else:
                    ctx.failure_logs.append((task.symbol, task.interval, "No bars returned"))

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
            # Enqueue poison pills so workers exit cleanly once queues drain
            for _ in range(ultimate_workers):
                ctx.intraday_queue.put(None)
            for _ in range(total_workers):
                ctx.primary_queue.put(None)
        else:
            # All 8 workers run on the Free account pool servicing primary_queue
            for i in range(total_workers):
                t = threading.Thread(
                    target=_worker_thread_loop,
                    args=(i, "free", ctx),
                    daemon=True,
                )
                t.start()
                threads.append(t)
            for _ in range(total_workers):
                ctx.primary_queue.put(None)

        for t in threads:
            t.join()

        # Drain any leftover None sentinels
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

        if ctx.network_retry_tasks and not ctx.abort_event.is_set():
            pending = list(ctx.network_retry_tasks)
            ctx.network_retry_tasks.clear()
            print(
                f"[WorkerPool] Retrying {len(pending)} transient network tasks (round {round_num + 1})...",
                flush=True,
            )
            time.sleep(2.0)
        else:
            break

