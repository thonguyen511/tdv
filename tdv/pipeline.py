"""
Core Two-Stage, Chunk-Based Execution Pipeline shared by Phase 1 (Initial Full Scan)
and Phase 2 (Weekly Maintenance).

Stage 1 (per 2,000-symbol chunk):
  - Scrape `1D` bars + dynamic corporate action & roll studies (`Splits`, `Dividends`,
    `Earnings`, `RollDates`, `last_splits`) using full `EXCHANGE:SYMBOL` tickers
    (and expanded futures `contracts`).
  - Record `has_intraday` and `has_adjustment` from `symbol_resolved`.
  - Overwrite-merge and encrypt `1D` chunk.

Stage 2 (per 2,000-symbol chunk):
  - Queue `1W` and `1M` for all valid symbols.
  - Queue intraday intervals (`1`, `5`, `15`, `60`, `240`) ONLY for symbols where
    Stage 1 reported `has_intraday == True`. Log daily-only symbols in `checkpoint.log`
    as `{timestamp}, {sym}, INTRADAY, skipped, has_intraday is False`.
  - Overwrite-merge and encrypt each interval's `.parquet.enc` chunk.
  - Log every saved symbol with its bar count and full bucket chunk path in
    `checkpoint.log`:
    `{timestamp}, {sym}, {interval}, checkpoint, Bars: {c}, data/{exchange}/{interval}/{exchange}_{interval}_chunk_{chunk_idx}.parquet.enc`
  - Atomically batch-upload all interval `.parquet.enc` files + updated
    `checkpoint.log` and `failures.log` to the Hugging Face Storage Bucket.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from tdv.bucket import BucketClient
from tdv.config import (
    ALL_INTERVALS,
    CHUNK_SIZE,
    EXIT_AUTH_FAILED,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_RATE_LIMITED,
    INTRADAY_INTERVALS,
    STAGE1_INTERVAL,
    STAGE2_MACRO_INTERVALS,
    TOTAL_WORKERS,
    ULTIMATE_POOL_WORKERS,
    compute_weekly_run_tag,
    get_chunk_bucket_path,
    get_log_bucket_dir,
    get_repo_root,
    get_staging_root,
)
from tdv.crypto import derive_key, encrypt_dataframe_parquet
from tdv.jwt_manager import JWTManager, TVAuthError
from tdv.map_planner import ChunkAssignment, expand_node_assignments
from tdv.protocol import resolve_live_study_descriptors, verify_cookie_tier
from tdv.worker import ChunkExecutionContext, TaskItem, run_task_batch_with_retries

COLUMNS_1D = [
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "raw_split",
    "raw_dividend",
    "raw_earnings",
    "raw_roll",
    "last_splits",
]
COLUMNS_STANDARD = ["symbol", "time", "open", "high", "low", "close", "volume"]


def _parse_checkpoint_text(raw_text: str) -> tuple[set[tuple[str, str]], dict[str, bool]]:
    """
    Parse `checkpoint.log` (and `failures.log`) lines into:
    - `completed_pairs`: set of `(symbol, interval)` pairs already completed or skipped
    - `known_intraday`: dict mapping `symbol -> has_intraday` inferred from prior logs

    Supports the full log format:
    - `2026-11-03 12:00:00, NASDAQ:NVDA, 1D, checkpoint, Bars: 6988, data/NASDAQ/1D/NASDAQ_1D_chunk_6.parquet.enc`
    - `2026-11-03 12:00:00, OTC:ABCD, INTRADAY, skipped, has_intraday is False`
    """
    completed_pairs: set[tuple[str, str]] = set()
    known_intraday: dict[str, bool] = {}
    for line in raw_text.splitlines():
        parts = [p.strip() for p in line.strip().split(",")]
        if len(parts) >= 3:
            sym, iv = parts[1], parts[2]
            status = parts[3] if len(parts) >= 4 else "checkpoint"
            if iv == "INTRADAY" or status in ("skipped", "SKIPPED_NO_INTRADAY"):
                known_intraday[sym] = False
                for intra_iv in INTRADAY_INTERVALS:
                    completed_pairs.add((sym, intra_iv))
            else:
                completed_pairs.add((sym, iv))
                if iv in INTRADAY_INTERVALS and status in ("checkpoint", "OK"):
                    known_intraday[sym] = True
    return completed_pairs, known_intraday


def _merge_and_encrypt_interval_chunk(
    bucket: BucketClient,
    exchange: str,
    interval: str,
    chunk_idx: int,
    new_rows: list[list[Any]],
    fernet_key: bytes,
) -> tuple[bytes, dict[str, int]]:
    """
    Download existing `.parquet.enc` chunk from the bucket (if present), merge
    `new_rows` deduplicated by `["symbol", "time"]` (`keep="last"`), sort by
    `["symbol", "time"]`, and return `(encrypted_parquet_bytes, new_symbol_bar_counts)`.
    """
    cols = COLUMNS_1D if interval == STAGE1_INTERVAL else COLUMNS_STANDARD
    new_df = pd.DataFrame(new_rows, columns=cols)
    new_symbol_counts = {str(k): int(v) for k, v in new_df["symbol"].value_counts().to_dict().items()}

    existing_df, exists_on_bucket = bucket.download_encrypted_chunk(
        exchange=exchange,
        interval=interval,
        chunk_idx=chunk_idx,
        fernet_key=fernet_key,
    )
    if exists_on_bucket and existing_df is not None and not existing_df.empty:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=["symbol", "time"], keep="last", inplace=True)
    else:
        combined_df = new_df.drop_duplicates(subset=["symbol", "time"], keep="last")

    combined_df.sort_values(by=["symbol", "time"], inplace=True)
    combined_df.reset_index(drop=True, inplace=True)
    return encrypt_dataframe_parquet(combined_df, fernet_key), new_symbol_counts


def run_node_pipeline(
    notebook_id: int,
    fetch_full_history: bool,
    run_tag: str | None = None,
    map_path: Path | None = None,
    data_dir: Path | None = None,
    max_chunks: int | None = None,
) -> int:
    """
    Execute the two-stage pipeline for `notebook_id`.

    Args:
        notebook_id: Node index (`0..19`).
        fetch_full_history: `True` for Phase 1 (`INITIAL` full backwards paging),
            `False` for Phase 2 (`WEEK_<WWYY>` single-batch overwrite-merge).
        run_tag: Optional override for log namespace (`"INITIAL"` or `"WEEK_<WWYY>"`).
        map_path: Optional override path to `map.json`.
        data_dir: Optional override path to `data/` CSV directory.
        max_chunks: Optional cap on number of chunks to process (useful for testing).

    Returns:
        Process exit code (`EXIT_OK=0`, `EXIT_ERROR=1`, `EXIT_RATE_LIMITED=2`, `EXIT_AUTH_FAILED=3`).
    """
    repo_root = get_repo_root()
    resolved_map_path = map_path or (repo_root / "map.json")
    resolved_data_dir = data_dir or (repo_root / "data")
    effective_run_tag = run_tag or ("INITIAL" if fetch_full_history else compute_weekly_run_tag())
    phase_label = "PHASE 1 (INITIAL FULL SCAN)" if fetch_full_history else f"PHASE 2 ({effective_run_tag})"

    data_password = os.environ.get("DATA_PASSWORD", "").strip()
    if not data_password:
        print("[Pipeline] CRITICAL: DATA_PASSWORD environment variable is not set.", flush=True)
        return EXIT_ERROR

    fernet_key = derive_key(data_password)
    bucket = BucketClient()

    # 1. Authenticate node JWT (fail fast with EXIT_AUTH_FAILED if no valid JWT)
    try:
        jwt_manager = JWTManager(
            notebook_id=notebook_id,
            bucket=bucket,
            data_password=data_password,
        )
        initial_jwt = jwt_manager.get_jwt(force_refresh=False)
    except TVAuthError as auth_exc:
        print(f"[Pipeline] {auth_exc}", flush=True)
        return EXIT_AUTH_FAILED

    # 2. Optional Ultimate tier verification (dual-pool support if TV_ULTIMATE_* set)
    ult_session = os.environ.get("TV_ULTIMATE_SESSIONID", "").strip()
    ult_sign = os.environ.get("TV_ULTIMATE_SESSIONID_SIGN", "").strip()
    has_ultimate = False
    ultimate_jwt: str | None = None
    if ult_session:
        valid_u, u_jwt, tier = verify_cookie_tier(ult_session, ult_sign)
        if valid_u and tier == "Ultimate" and u_jwt:
            has_ultimate = True
            ultimate_jwt = u_jwt
            print(
                f"[Pipeline] Verified Ultimate account -> Dual-Pool (5 Ultimate + 3 Free workers).",
                flush=True,
            )
        else:
            print(
                f"[Pipeline] TV_ULTIMATE_SESSIONID not Ultimate ({tier}); using 8 Free workers.",
                flush=True,
            )
    else:
        print(
            f"[Pipeline] Running 8 Free-pool workers on authenticated JWT for Node {notebook_id}.",
            flush=True,
        )

    # 3. Dynamically resolve latest TradingView study versions
    study_descriptors = resolve_live_study_descriptors(initial_jwt)

    # 4. Load map.json assignments for this node
    if not resolved_map_path.exists():
        print(f"[Pipeline] CRITICAL: map.json not found at {resolved_map_path}", flush=True)
        return EXIT_ERROR

    map_data = json.loads(resolved_map_path.read_text(encoding="utf-8"))
    assignments: list[ChunkAssignment] = expand_node_assignments(
        map_data=map_data,
        notebook_id=notebook_id,
        data_dir=resolved_data_dir,
        chunk_size=CHUNK_SIZE,
    )
    if max_chunks is not None:
        assignments = assignments[:max_chunks]

    total_assigned_symbols = sum(len(a.symbols) for a in assignments)
    print(
        f"=================================================================\n"
        f" [TDV] {phase_label} | NODE {notebook_id}\n"
        f" Bucket: {bucket.bucket_id} | Chunks: {len(assignments)} ({total_assigned_symbols:,} symbols)\n"
        f"=================================================================",
        flush=True,
    )

    # 5. Prepare staging directory & sync existing checkpoint/failure logs from bucket
    staging_root = get_staging_root() / f"node_{notebook_id}_{effective_run_tag}"
    staging_root.mkdir(parents=True, exist_ok=True)
    local_checkpoint = staging_root / "checkpoint.log"
    local_failures = staging_root / "failures.log"

    remote_log_dir = get_log_bucket_dir(notebook_id, effective_run_tag)
    remote_checkpoint_path = f"{remote_log_dir}/checkpoint.log"
    remote_failures_path = f"{remote_log_dir}/failures.log"
    remote_completed_path = f"{remote_log_dir}/COMPLETED"

    completed_pairs: set[tuple[str, str]] = set()
    known_intraday: dict[str, bool] = {}

    existing_cp_bytes = bucket.download_bytes(remote_checkpoint_path, must_exist=False)
    if existing_cp_bytes is not None:
        local_checkpoint.write_bytes(existing_cp_bytes)
        completed_pairs, known_intraday = _parse_checkpoint_text(
            existing_cp_bytes.decode("utf-8", errors="replace")
        )
        print(
            f"[Pipeline] Resumed {len(completed_pairs):,} completed (symbol, interval) records "
            f"from {remote_checkpoint_path}.",
            flush=True,
        )
    else:
        local_checkpoint.write_text("", encoding="utf-8")

    existing_fail_bytes = bucket.download_bytes(remote_failures_path, must_exist=False)
    if existing_fail_bytes is not None:
        local_failures.write_bytes(existing_fail_bytes)
        fail_pairs, _ = _parse_checkpoint_text(
            existing_fail_bytes.decode("utf-8", errors="replace")
        )
        completed_pairs.update(fail_pairs)
    else:
        local_failures.write_text("", encoding="utf-8")

    # 6. Process each 2,000-symbol chunk in Two Stages
    for pos, chunk in enumerate(assignments, start=1):
        exchange = chunk.exchange
        chunk_idx = chunk.chunk_idx
        chunk_syms = chunk.symbols

        # Quick check if every (symbol, interval) in this chunk is already recorded
        all_done = all(
            (sym, iv) in completed_pairs
            for sym in chunk_syms
            for iv in ALL_INTERVALS
        )
        if all_done:
            print(
                f"--> [{exchange}] Chunk {chunk_idx}/{chunk.total_file_chunks} "
                f"({pos}/{len(assignments)}): 100% completed in checkpoint. Skipping.",
                flush=True,
            )
            continue

        print(
            f"\n--> [{exchange}] Chunk {chunk_idx}/{chunk.total_file_chunks} "
            f"(Node chunk {pos}/{len(assignments)}, {len(chunk_syms)} symbols)",
            flush=True,
        )

        ctx = ChunkExecutionContext(
            fetch_full_history=fetch_full_history,
            study_descriptors=study_descriptors,
            jwt_manager=jwt_manager,
            has_ultimate=has_ultimate,
            ultimate_jwt=ultimate_jwt,
        )
        # Pre-populate known intraday capability from resumed checkpoint if available
        for sym in chunk_syms:
            if sym in known_intraday:
                ctx.symbol_metadata[sym] = {
                    "has_intraday": known_intraday[sym],
                    "has_adjustment": False,
                    "symbol_error": False,
                }

        # -------------------------------------------------------------------
        # STAGE 1: 1D Discovery + Corporate Action Studies
        # -------------------------------------------------------------------
        tasks_1d = [
            TaskItem(symbol=sym, interval=STAGE1_INTERVAL)
            for sym in chunk_syms
            if (sym, STAGE1_INTERVAL) not in completed_pairs
        ]

        if tasks_1d:
            print(
                f"    [Stage 1] Scanning 1D + Studies for {len(tasks_1d)} symbols across {TOTAL_WORKERS} workers...",
                flush=True,
            )
            run_task_batch_with_retries(
                tasks=tasks_1d,
                ctx=ctx,
                total_workers=TOTAL_WORKERS,
                ultimate_workers=ULTIMATE_POOL_WORKERS,
            )

        if ctx.auth_failed_event.is_set():
            print("[Pipeline] Auth failure signaled during Stage 1. Aborting.", flush=True)
            return EXIT_AUTH_FAILED

        if ctx.abort_event.is_set():
            print("[Pipeline] Rate-limit abort signaled during Stage 1.", flush=True)
            return EXIT_RATE_LIMITED

        # -------------------------------------------------------------------
        # STAGE 2: Macro (1W, 1M) + has_intraday-Filtered Intraday (1..240)
        # -------------------------------------------------------------------
        tasks_stage2: list[TaskItem] = []
        intraday_skipped_syms: list[str] = []

        for sym in chunk_syms:
            meta = ctx.symbol_metadata.get(sym, {})
            if meta.get("symbol_error", False):
                continue

            # Queue 1W and 1M
            for macro_iv in STAGE2_MACRO_INTERVALS:
                if (sym, macro_iv) not in completed_pairs:
                    tasks_stage2.append(TaskItem(symbol=sym, interval=macro_iv))

            # Queue Intraday ONLY when has_intraday is True
            has_intraday = bool(meta.get("has_intraday", known_intraday.get(sym, False)))
            if has_intraday:
                for intra_iv in INTRADAY_INTERVALS:
                    if (sym, intra_iv) not in completed_pairs:
                        tasks_stage2.append(TaskItem(symbol=sym, interval=intra_iv))
            else:
                if any((sym, intra_iv) not in completed_pairs for intra_iv in INTRADAY_INTERVALS):
                    intraday_skipped_syms.append(sym)
                    for intra_iv in INTRADAY_INTERVALS:
                        completed_pairs.add((sym, intra_iv))

        if tasks_stage2:
            intra_sym_count = sum(
                1 for sym in chunk_syms if ctx.symbol_metadata.get(sym, {}).get("has_intraday", False)
            )
            print(
                f"    [Stage 2] {intra_sym_count}/{len(chunk_syms)} symbols have intraday. "
                f"Running {len(tasks_stage2)} tasks (1W, 1M + eligible intraday)...",
                flush=True,
            )
            run_task_batch_with_retries(
                tasks=tasks_stage2,
                ctx=ctx,
                total_workers=TOTAL_WORKERS,
                ultimate_workers=ULTIMATE_POOL_WORKERS,
            )

        if ctx.auth_failed_event.is_set():
            print("[Pipeline] Auth failure signaled during Stage 2. Aborting.", flush=True)
            return EXIT_AUTH_FAILED

        if ctx.abort_event.is_set():
            print("[Pipeline] Rate-limit abort signaled during Stage 2.", flush=True)
            return EXIT_RATE_LIMITED

        # -------------------------------------------------------------------
        # Merge, Encrypt & Atomically Upload Chunk + Logs to Bucket
        # -------------------------------------------------------------------
        batch_uploads: list[tuple[bytes | str | Path, str]] = []
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        checkpoint_lines: list[str] = []

        for interval in ALL_INTERVALS:
            rows = ctx.chunk_bars.get(interval, [])
            if not rows:
                continue
            enc_bytes, sym_counts = _merge_and_encrypt_interval_chunk(
                bucket=bucket,
                exchange=exchange,
                interval=interval,
                chunk_idx=chunk_idx,
                new_rows=rows,
                fernet_key=fernet_key,
            )
            remote_chunk_path = get_chunk_bucket_path(exchange, interval, chunk_idx)
            batch_uploads.append((enc_bytes, remote_chunk_path))

            # Log full symbol, bar count, and full bucket path in checkpoint.log (matching old code)
            for sym_name, b_count in sym_counts.items():
                checkpoint_lines.append(
                    f"{now_ts}, {sym_name}, {interval}, checkpoint, Bars: {b_count}, {remote_chunk_path}"
                )
                completed_pairs.add((sym_name, interval))

        # Log skipped intraday symbols in checkpoint.log (matching old code)
        for sym_name in intraday_skipped_syms:
            checkpoint_lines.append(
                f"{now_ts}, {sym_name}, INTRADAY, skipped, has_intraday is False"
            )

        if checkpoint_lines:
            with local_checkpoint.open("a", encoding="utf-8") as f_cp:
                for line in checkpoint_lines:
                    f_cp.write(line + "\n")

        if ctx.failure_logs:
            with local_failures.open("a", encoding="utf-8") as f_fl:
                for sym, iv, reason in ctx.failure_logs:
                    clean_reason = reason.replace("\n", " ").replace("\r", " ").strip()
                    f_fl.write(f"{now_ts}, {sym}, {iv}, tradingview_error, {clean_reason}\n")
                    completed_pairs.add((sym, iv))

        batch_uploads.append((local_checkpoint.read_bytes(), remote_checkpoint_path))
        batch_uploads.append((local_failures.read_bytes(), remote_failures_path))

        print(
            f"    [Bucket Sync] Uploading {len(batch_uploads) - 2} encrypted Parquet chunks "
            f"+ logs to {bucket.bucket_id}...",
            flush=True,
        )
        bucket.upload_files_batch(batch_uploads)

    # 7. Mark Node Run as COMPLETED in Bucket
    completed_ts = datetime.now(timezone.utc).isoformat().encode("utf-8")
    bucket.upload_bytes(remote_completed_path, completed_ts)
    print(
        f"[Pipeline] Node {notebook_id} completed all {len(assignments)} chunks for {effective_run_tag}! "
        f"Marker written to {remote_completed_path}.",
        flush=True,
    )

    try:
        shutil.rmtree(staging_root, ignore_errors=True)
    except Exception:
        pass

    return EXIT_OK
