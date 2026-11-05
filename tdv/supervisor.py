"""
11h 45m (42,300s) Supervisor & Watchdog Timer for Kaggle Worker Nodes.

Responsibilities:
1. Optional `START_DELAY_SECONDS` cooldown sleep before starting work (used when
   recovering from `RATE_LIMITED` or `ERROR` without relying on a GitHub cron).
2. Resolve the active phase (`initial` vs `weekly`) from `--phase`, `PIPELINE_PHASE`,
   or the HF Storage Bucket `control/state.json`.
3. Launch `tdv.phase1_initial` or `tdv.phase2_weekly` as a child process and monitor
   it against the 11h 45m (`42,300s`) Kaggle watchdog timer.
4. Purge `/kaggle/working` and temporary staging folders before container exit so
   Kaggle teardown finishes in seconds.
5. Dispatch the appropriate callback (`DONE`, `TIMEOUT`, `RATE_LIMITED`,
   `AUTH_FAILED`, or `ERROR`) to GitHub Actions (`thonguyen511/tdv`).
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.bucket import BucketClient
from tdv.callback import send_github_callback
from tdv.config import (
    BUCKET_STATE_PATH,
    DEFAULT_TIMEOUT_SECONDS,
    EXIT_AUTH_FAILED,
    EXIT_OK,
    EXIT_RATE_LIMITED,
    get_repo_root,
    get_staging_root,
)


def _resolve_active_phase(explicit_phase: str | None) -> tuple[str, str]:
    """
    Determine `(phase_mode, run_tag)` where `phase_mode` is `'initial'` or `'weekly'`.
    """
    if explicit_phase and explicit_phase.lower() in ("initial", "weekly"):
        mode = explicit_phase.lower()
        return mode, ("INITIAL" if mode == "initial" else os.environ.get("WEEK_TAG", ""))

    env_phase = os.environ.get("PIPELINE_PHASE", "").strip().lower()
    if env_phase in ("initial", "weekly"):
        return env_phase, ("INITIAL" if env_phase == "initial" else os.environ.get("WEEK_TAG", ""))

    try:
        bucket = BucketClient()
        state = bucket.read_json(BUCKET_STATE_PATH, default={"phase": "initial"})
        raw_phase = str(state.get("phase", "initial")).strip()
        if raw_phase.upper().startswith("WEEK_"):
            return "weekly", raw_phase.upper()
        if raw_phase.lower() == "weekly":
            return "weekly", str(state.get("run_tag", ""))
    except Exception as exc:
        print(f"[Supervisor] Could not read {BUCKET_STATE_PATH} ({exc}); defaulting to initial.", flush=True)

    return "initial", "INITIAL"


def _cleanup_kaggle_working() -> None:
    """
    Remove temporary staging directories and any clutter inside `/kaggle/working`
    so Kaggle does not spend minutes archiving thousands of files on exit.
    """
    try:
        staging = get_staging_root()
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    except Exception:
        pass

    kw = Path("/kaggle/working")
    if kw.exists():
        for child in kw.iterdir():
            if child.name.startswith("__notebook") or child.name.endswith(".ipynb"):
                continue
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV 11h 45m Node Supervisor")
    parser.add_argument(
        "--notebook-id",
        type=int,
        default=int(os.environ.get("NOTEBOOK_ID", "0")),
    )
    parser.add_argument(
        "--phase",
        type=str,
        default=None,
        help="Explicit phase override: 'initial' or 'weekly'",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))),
    )
    parser.add_argument(
        "--start-delay",
        type=int,
        default=int(os.environ.get("START_DELAY_SECONDS", "0")),
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
    )
    args = parser.parse_args()

    if args.start_delay > 0:
        print(
            f"[Supervisor] Cooldown delay requested: sleeping {args.start_delay}s before starting Node {args.notebook_id}...",
            flush=True,
        )
        time.sleep(args.start_delay)

    phase_mode, run_tag = _resolve_active_phase(args.phase)
    target_module = "tdv.phase1_initial" if phase_mode == "initial" else "tdv.phase2_weekly"

    cmd = [
        sys.executable,
        "-m",
        target_module,
        "--notebook-id",
        str(args.notebook_id),
    ]
    if phase_mode == "weekly" and run_tag.startswith("WEEK_"):
        cmd.extend(["--run-tag", run_tag])
    if args.max_chunks is not None:
        cmd.extend(["--max-chunks", str(args.max_chunks)])

    # Stagger default 11h 45m watchdog by 60s per node_id so all 20 nodes never fire
    # simultaneous repository_dispatch callbacks in the same second.
    effective_timeout = args.timeout
    if args.timeout == DEFAULT_TIMEOUT_SECONDS and "TIMEOUT_SECONDS" not in os.environ:
        effective_timeout = max(3600, args.timeout - (int(args.notebook_id) * 60))

    print(
        f"=================================================================\n"
        f" [SUPERVISOR] Node {args.notebook_id} | Phase: {phase_mode.upper()} ({run_tag or 'auto'})\n"
        f" Watchdog Limit: {effective_timeout}s ({effective_timeout / 3600:.2f} hours)\n"
        f" Command: {' '.join(cmd)}\n"
        f"=================================================================",
        flush=True,
    )

    repo_root = get_repo_root()
    start_time = time.time()
    process = subprocess.Popen(cmd, cwd=str(repo_root))
    callback_status = "ERROR"

    try:
        while True:
            retcode = process.poll()
            if retcode is not None:
                if retcode == EXIT_OK:
                    print(f"\n[Supervisor] Child exited 0 (DONE).", flush=True)
                    callback_status = "DONE"
                elif retcode == EXIT_RATE_LIMITED:
                    print(f"\n[Supervisor] Child exited {EXIT_RATE_LIMITED} (RATE_LIMITED).", flush=True)
                    callback_status = "RATE_LIMITED"
                elif retcode == EXIT_AUTH_FAILED:
                    print(f"\n[Supervisor] Child exited {EXIT_AUTH_FAILED} (AUTH_FAILED).", flush=True)
                    callback_status = "AUTH_FAILED"
                else:
                    print(f"\n[Supervisor] Child exited with code {retcode} (ERROR).", flush=True)
                    callback_status = "ERROR"
                break

            elapsed = time.time() - start_time
            if elapsed >= effective_timeout:
                print(
                    f"\n[Supervisor] Watchdog ({effective_timeout}s) reached! "
                    f"Terminating child process cleanly for session handoff...",
                    flush=True,
                )
                try:
                    process.send_signal(signal.SIGTERM)
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                callback_status = "TIMEOUT"
                break

            time.sleep(5)
    except KeyboardInterrupt:
        try:
            process.terminate()
        except Exception:
            pass
        callback_status = "TIMEOUT"

    _cleanup_kaggle_working()
    send_github_callback(
        node_id=args.notebook_id,
        status=callback_status,
        phase=run_tag or phase_mode,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

