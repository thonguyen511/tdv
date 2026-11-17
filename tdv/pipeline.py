"""
Core Two-Stage, Chunk-Based Execution Pipeline shared by Initial Full Scan (`INITIAL`)
and Weekly Maintenance (`WEEK_<WWYY>`).

Architecture:
- Initial vs Weekly Jobs:
  - `Initial` (`fetch_full_history=True`, `run_tag="INITIAL"`):
    Uses the premium cookie/JWT (`TV_ULTIMATE_SESSIONID` / `TV_ULTIMATE_SESSIONID_SIGN`)
    when configured, falling back cleanly to the node's Free cookie during testing.
    When a premium cookie is active, BOTH Stage 1 (`1D` + studies) and Stage 2
    intraday timeframes (via WebSocket Bar Replay) use the premium endpoint.
  - `Weekly` (`fetch_full_history=False`, `run_tag="WEEK_<WWYY>"`):
    Always uses Free cookies/JWTs (`has_ultimate=False`). Sends `1,000,000` bars per
    request; TradingView caps to the Free JWT limit and returns recent bars that
    overlap and overwrite-merge with the prior week's data.

- Two-Stage Per-Chunk Execution (both Initial and Weekly):
  - Stage 1 (`1D` + Universal Studies + `has_intraday` Filtering):
    Runs ALL studies (`last_splits`, `Splits`, `Dividends`, `Earnings`,
    `BarSetContinuousRollDates`) on `1D` for EVERY ticker extracted from the CSV.
  - Stage 2 (`1W`, `1M`, and `has_intraday`-filtered `1, 5, 15, 60, 240`):
    Marks daily-only symbols with `E3` in `log/node_{x}_{yyyy}.csv`, attaches corporate
    actions via `attach_events_to_df`, overwrite-merges and encrypts `.parquet.enc`
    chunks, and atomically uploads chunks + updated `log/node_{x}_{yyyy}.csv` to the
    Hugging Face Storage Bucket.
"""

from __future__ import annotations

import gc
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
    MAX_ALLOWED_MISSING_RATE_PCT,
    STAGE1_INTERVAL,
    STAGE2_MACRO_INTERVALS,
    STATUS_ERR_EMPTY,
    STATUS_ERR_SYMBOL,
    STATUS_ERR_TRANSIENT,
    STATUS_OK,
    STATUS_SKIPPED_NO_INTRADAY,
    TOTAL_WORKERS,
    ULTIMATE_POOL_WORKERS,
    compute_weekly_run_tag,
    get_chunk_bucket_path,
    get_repo_root,
    get_staging_root,
)
from tdv.crypto import derive_key, encrypt_dataframe_parquet
from tdv.jwt_manager import JWTManager, TVAuthError
from tdv.log_manager import NodeYearlyLog, load_or_create_node_yearly_log
from tdv.map_planner import ChunkAssignment
from tdv.protocol import resolve_live_study_descriptors, verify_cookie_tier
from tdv.worker import ChunkExecutionContext, TaskItem, run_task_batch_with_retries

COLUMNS_EVENT = [
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
]
COLUMNS_LEGACY_12 = COLUMNS_EVENT + ["last_splits"]
COLUMNS_STANDARD = ["symbol", "time", "open", "high", "low", "close", "volume"]


def count_node_task_stats(
    bucket: BucketClient,
    notebook_id: int,
    run_tag: str,
    map_path: Path | None = None,
    data_dir: Path | None = None,
) -> tuple[int, int, float]:
    """
    Download `log/node_{notebook_id}_{year}.csv` from the HF Bucket, reconcile
    against `map.json` + `data/*.csv`, and return
    `(missing_tasks_count, total_tasks_count, missing_rate_pct)` for `run_tag`.
    """
    repo_root = get_repo_root()
    resolved_map_path = map_path or (repo_root / "map.json")
    resolved_data_dir = data_dir or (repo_root / "data")
    map_data = json.loads(resolved_map_path.read_text(encoding="utf-8"))

    node_log, assignments = load_or_create_node_yearly_log(
        bucket=bucket,
        notebook_id=notebook_id,
        run_tag=run_tag,
        map_data=map_data,
        data_dir=resolved_data_dir,
        chunk_size=CHUNK_SIZE,
    )
    return node_log.count_run_stats(run_tag=run_tag, assignments=assignments)


def count_node_missing_tasks(
    bucket: BucketClient,
    notebook_id: int,
    run_tag: str,
    map_path: Path | None = None,
    data_dir: Path | None = None,
) -> int:
    """
    Return the exact number of `(symbol, interval)` tasks across all assigned
    chunks that are missing (`""`) or marked transient error (`"E4"`).
    """
    missing_cnt, _, _ = count_node_task_stats(bucket, notebook_id, run_tag, map_path, data_dir)
    return missing_cnt


def _merge_and_encrypt_interval_chunk(
    bucket: BucketClient,
    exchange: str,
    interval: str,
    chunk_idx: int,
    new_rows: list[Any],
    fernet_key: bytes,
) -> tuple[bytes, dict[str, int]]:
    """
    Download existing `.parquet.enc` chunk from the bucket (if present), merge
    `new_rows` (either a list of `pd.DataFrame`s or a list of row lists)
    deduplicated by `["symbol", "time"]` (`keep="last"`), sort by
    `["symbol", "time"]`, and return `(encrypted_parquet_bytes, new_symbol_bar_counts)`.
    """
    if new_rows and isinstance(new_rows[0], pd.DataFrame):
        new_df = pd.concat(new_rows, ignore_index=True)
    else:
        row_len = len(new_rows[0]) if new_rows else 7
        if row_len == 12:
            cols = COLUMNS_LEGACY_12
        elif row_len == 11:
            cols = COLUMNS_EVENT
        else:
            cols = COLUMNS_STANDARD
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
        fetch_full_history: `True` for Initial job (`INITIAL` with optional
            premium cookie/JWT + Bar Replay), `False` for Weekly job (`WEEK_<WWYY>`
            using Free cookies/JWTs only).
        run_tag: Optional override for run column (`"INITIAL"` or `"WEEK_<WWYY>"`).
        map_path: Optional override path to `map.json`.
        data_dir: Optional override path to `data/` CSV directory.
        max_chunks: Optional cap on number of chunks to process (useful for testing).

    Returns:
        Process exit code (`EXIT_OK=0`, `EXIT_ERROR=1`, `EXIT_RATE_LIMITED=2`, `EXIT_AUTH_FAILED=3`).
    """
    repo_root = get_repo_root()
    resolved_map_path = map_path or (repo_root / "map.json")
    resolved_data_dir = data_dir or (repo_root / "data")
    is_initial_job = bool(fetch_full_history)
    effective_run_tag = run_tag or ("INITIAL" if is_initial_job else compute_weekly_run_tag())
    job_label = "INITIAL FULL SCAN" if is_initial_job else f"WEEKLY MAINTENANCE ({effective_run_tag})"

    data_password = os.environ.get("DATA_PASSWORD", "").strip()
    if not data_password:
        print("[Pipeline] CRITICAL: DATA_PASSWORD environment variable is not set.", flush=True)
        return EXIT_ERROR

    fernet_key = derive_key(data_password)
    bucket = BucketClient()

    # 1. Authenticate node Free JWT (fail fast with EXIT_AUTH_FAILED if no valid JWT)
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

    # 2. Premium cookie/JWT check (ONLY used during Initial job; Weekly job uses Free cookies)
    has_ultimate = False
    ultimate_jwt: str | None = None
    if is_initial_job:
        ult_session = os.environ.get("TV_ULTIMATE_SESSIONID", "").strip()
        ult_sign = os.environ.get("TV_ULTIMATE_SESSIONID_SIGN", "").strip()
        if ult_session:
            valid_u, u_jwt, tier = verify_cookie_tier(ult_session, ult_sign)
            if valid_u and tier == "Ultimate" and u_jwt:
                has_ultimate = True
                ultimate_jwt = u_jwt
                print(
                    "[Pipeline] Initial job: Verified Premium/Ultimate cookie -> "
                    "using Premium workers for Phase 1 (1D + studies) and Phase 2 intraday (Bar Replay).",
                    flush=True,
                )
            else:
                print(
                    f"[Pipeline] Initial job: TV_ULTIMATE_SESSIONID tier={tier}; "
                    f"falling back to {TOTAL_WORKERS} Free workers.",
                    flush=True,
                )
        else:
            print(
                f"[Pipeline] Initial job (free/testing mode): TV_ULTIMATE_SESSIONID not set; "
                f"using {TOTAL_WORKERS} Free workers for Node {notebook_id}.",
                flush=True,
            )
    else:
        print(
            f"[Pipeline] Weekly job ({effective_run_tag}): using {TOTAL_WORKERS} Free-pool workers "
            f"for Node {notebook_id} (premium cookie not used after initial run).",
            flush=True,
        )

    # 3. Dynamically resolve latest TradingView study versions
    study_scripts = resolve_live_study_descriptors(ultimate_jwt or initial_jwt)

    # 4. Load map.json and yearly node log CSV (`log/node_{notebook_id}_{year}.csv`)
    if not resolved_map_path.exists():
        print(f"[Pipeline] CRITICAL: map.json not found at {resolved_map_path}", flush=True)
        return EXIT_ERROR

    map_data = json.loads(resolved_map_path.read_text(encoding="utf-8"))
    node_log, all_assignments = load_or_create_node_yearly_log(
        bucket=bucket,
        notebook_id=notebook_id,
        run_tag=effective_run_tag,
        map_data=map_data,
        data_dir=resolved_data_dir,
        chunk_size=CHUNK_SIZE,
    )
    assignments = all_assignments[:max_chunks] if max_chunks is not None else all_assignments

    total_assigned_symbols = sum(len(a.symbols) for a in assignments)
    print(
        f"=================================================================\n"
        f" [TDV] {job_label} | NODE {notebook_id}\n"
        f" Bucket: {bucket.bucket_id} | Log: {node_log.remote_bucket_path}\n"
        f" Chunks: {len(assignments)} ({total_assigned_symbols:,} symbols)\n"
        f"=================================================================",
        flush=True,
    )

    # 5. Prepare staging directory & resume state from `node_log`
    staging_root = get_staging_root() / f"node_{notebook_id}_{effective_run_tag}"
    staging_root.mkdir(parents=True, exist_ok=True)
    local_log_csv = staging_root / f"node_{notebook_id}_{node_log.year}.csv"
    remote_log_path = node_log.remote_bucket_path

    completed_pairs, known_intraday, failed_1d_symbols = node_log.prepare_run_state(
        run_tag=effective_run_tag,
        assignments=assignments,
    )
    local_log_csv.write_bytes(node_log.to_csv_bytes())
    log_dirty = True  # Ensure initial/updated yearly CSV schema is synced even if all chunks skip

    if completed_pairs:
        print(
            f"[Pipeline] Resumed {len(completed_pairs):,} verified (symbol, timeframe) records "
            f"for {effective_run_tag} from {remote_log_path}.",
            flush=True,
        )

    # 6. Process each chunk in Two Stages
    for pos, chunk in enumerate(assignments, start=1):
        exchange = chunk.exchange
        chunk_idx = chunk.chunk_idx
        chunk_syms = chunk.symbols

        all_done = all(
            (sym, iv) in completed_pairs
            for sym in chunk_syms
            for iv in ALL_INTERVALS
        )
        if all_done:
            print(
                f"--> [{exchange}] Chunk {chunk_idx}/{chunk.total_file_chunks} "
                f"({pos}/{len(assignments)}): 100% completed in {remote_log_path}. Skipping.",
                flush=True,
            )
            continue

        print(
            f"\n--> [{exchange}] Chunk {chunk_idx}/{chunk.total_file_chunks} "
            f"(Node chunk {pos}/{len(assignments)}, {len(chunk_syms)} symbols)",
            flush=True,
        )

        ctx = ChunkExecutionContext(
            fetch_full_history=is_initial_job,
            study_scripts=study_scripts,
            jwt_manager=jwt_manager,
            has_ultimate=has_ultimate,
            ultimate_jwt=ultimate_jwt,
        )
        for sym in chunk_syms:
            if sym in failed_1d_symbols:
                ctx.symbol_metadata[sym] = {
                    "has_intraday": False,
                    "has_adjustment": False,
                    "symbol_error": True,
                }
            elif sym in known_intraday:
                ctx.symbol_metadata[sym] = {
                    "has_intraday": known_intraday[sym],
                    "has_adjustment": False,
                    "symbol_error": False,
                }

        # -------------------------------------------------------------------
        # PHASE / STAGE 1: 1D Scan + Universal Studies + has_intraday Filter
        # -------------------------------------------------------------------
        tasks_1d = [
            TaskItem(symbol=sym, interval=STAGE1_INTERVAL)
            for sym in chunk_syms
            if sym not in failed_1d_symbols
            and any((sym, iv) not in completed_pairs for iv in ALL_INTERVALS)
        ]

        if tasks_1d:
            print(
                f"    [Stage 1] Scanning 1D + all studies for {len(tasks_1d)} symbols...",
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

        transient_1d_syms = {
            sym
            for sym, iv, err_code, _ in ctx.failure_logs
            if iv == STAGE1_INTERVAL and err_code == STATUS_ERR_TRANSIENT
        }

        # -------------------------------------------------------------------
        # PHASE / STAGE 2: Remaining Timeframes (1W, 1M + has_intraday 1..240)
        # -------------------------------------------------------------------
        tasks_stage2: list[TaskItem] = []
        intraday_skipped_syms: list[str] = []

        for sym in chunk_syms:
            if sym in transient_1d_syms:
                continue
            if sym not in ctx.symbol_metadata and (sym, STAGE1_INTERVAL) not in completed_pairs:
                continue
            meta = ctx.symbol_metadata.get(sym, {})
            if sym in failed_1d_symbols or meta.get("symbol_error", False):
                continue

            for macro_iv in STAGE2_MACRO_INTERVALS:
                if (sym, macro_iv) not in completed_pairs:
                    tasks_stage2.append(TaskItem(symbol=sym, interval=macro_iv))

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
        # Merge, Encrypt & Atomically Upload Chunk + Yearly Node CSV Log
        # -------------------------------------------------------------------
        batch_uploads: list[tuple[bytes | str | Path, str]] = []

        for interval in ALL_INTERVALS:
            dfs = ctx.chunk_dfs.pop(interval, [])
            if not dfs:
                continue
            enc_bytes, sym_counts = _merge_and_encrypt_interval_chunk(
                bucket=bucket,
                exchange=exchange,
                interval=interval,
                chunk_idx=chunk_idx,
                new_rows=dfs,
                fernet_key=fernet_key,
            )
            del dfs
            gc.collect()

            remote_chunk_path = get_chunk_bucket_path(exchange, interval, chunk_idx)
            batch_uploads.append((enc_bytes, remote_chunk_path))

            for sym_name in sym_counts:
                node_log.set_status(sym_name, interval, effective_run_tag, STATUS_OK)
                completed_pairs.add((sym_name, interval))

        # Mark intraday skipped (E3)
        for sym_name in intraday_skipped_syms:
            for intra_iv in INTRADAY_INTERVALS:
                node_log.set_status(sym_name, intra_iv, effective_run_tag, STATUS_SKIPPED_NO_INTRADAY)
                completed_pairs.add((sym_name, intra_iv))

        # Record E1 (symbol error), E2 (empty), or E4 (transient error)
        for sym, iv, err_code, reason in ctx.failure_logs:
            if err_code in (STATUS_ERR_SYMBOL, STATUS_ERR_EMPTY):
                node_log.set_status(sym, iv, effective_run_tag, err_code)
                completed_pairs.add((sym, iv))
                if iv == STAGE1_INTERVAL:
                    failed_1d_symbols.add(sym)
                    for dead_iv in ALL_INTERVALS:
                        node_log.set_status(sym, dead_iv, effective_run_tag, err_code)
                        completed_pairs.add((sym, dead_iv))
            else:
                print(
                    f"    [Integrity] Transient failure ({err_code}) for ({sym}, {iv}): "
                    f"{reason} -> will retry.",
                    flush=True,
                )
                node_log.set_status(sym, iv, effective_run_tag, STATUS_ERR_TRANSIENT)

        csv_bytes = node_log.to_csv_bytes()
        local_log_csv.write_bytes(csv_bytes)
        batch_uploads.append((csv_bytes, remote_log_path))

        print(
            f"    [Bucket Sync] Uploading {len(batch_uploads) - 1} encrypted Parquet chunk(s) "
            f"+ {remote_log_path} to {bucket.bucket_id}...",
            flush=True,
        )
        bucket.upload_files_batch(batch_uploads)
        log_dirty = False

    if log_dirty:
        csv_bytes = node_log.to_csv_bytes()
        local_log_csv.write_bytes(csv_bytes)
        bucket.upload_bytes(remote_log_path, csv_bytes)

    # 7. Data Flow Integrity Verification on `node_log`
    missing_tasks_count, total_node_tasks, missing_rate_pct = node_log.count_run_stats(
        run_tag=effective_run_tag,
        assignments=assignments,
    )

    if missing_tasks_count > 0:
        if missing_rate_pct <= MAX_ALLOWED_MISSING_RATE_PCT:
            print(
                f"[Pipeline] Data Integrity Gate: {missing_tasks_count}/{total_node_tasks} task(s) "
                f"({missing_rate_pct:.3f}%) are incomplete for Node {notebook_id} ({effective_run_tag}). "
                f"Within allowed threshold ({MAX_ALLOWED_MISSING_RATE_PCT}%) -> exiting DONE.",
                flush=True,
            )
        else:
            print(
                f"[Pipeline] Data Integrity Gate: {missing_tasks_count}/{total_node_tasks} task(s) "
                f"({missing_rate_pct:.3f}% > {MAX_ALLOWED_MISSING_RATE_PCT}%) are still incomplete for Node {notebook_id} ({effective_run_tag}). "
                f"Returning EXIT_ERROR so FleetManager re-pushes continuation.",
                flush=True,
            )
            return EXIT_ERROR

    print(
        f"[Pipeline] Node {notebook_id} verified {total_node_tasks - missing_tasks_count}/{total_node_tasks} "
        f"tasks complete ({100.0 - missing_rate_pct:.3f}%) for {effective_run_tag} in {remote_log_path}!",
        flush=True,
    )

    try:
        shutil.rmtree(staging_root, ignore_errors=True)
    except Exception:
        pass

    return EXIT_OK
