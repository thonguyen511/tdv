"""
Standby Master Clock Notebook (`tdv.standby_clock`).

Runs inside a single lightweight Kaggle session between weekly cycles so that
GitHub Actions requires NO `schedule:` cron:
- Computes the exact seconds until next Monday `00:00:00 UTC`.
- If the wait exceeds `42,300s` (11h 45m), sleeps `42,300s` and dispatches
  `REARM_STANDBY` to GitHub Actions, which immediately re-pushes `tdv-standby`.
- When Monday `00:00:00 UTC` arrives within the current session window, sleeps
  until Monday `00:00:05 UTC` and dispatches `TRIGGER_WEEKLY` to launch all 20
  weekly worker nodes.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.callback import send_github_callback
from tdv.config import BUCKET_STATE_PATH, DEFAULT_TIMEOUT_SECONDS, compute_weekly_run_tag


def _is_current_monday_untriggered(current: datetime) -> bool:
    """
    Return True if `current` is Monday in UTC and this week's `WEEK_<WWYY>` cycle
    has not yet been triggered according to `control/state.json` (or if it is within
    the first 2 hours of Monday 00:00 UTC after a boundary handoff).
    """
    if current.weekday() != 0:
        return False
    expected_tag = compute_weekly_run_tag(current)
    try:
        from tdv.bucket import BucketClient

        if os.environ.get("HF_TOKEN", "").strip():
            bucket = BucketClient()
            state = bucket.read_json(BUCKET_STATE_PATH, default=None)
            if isinstance(state, dict):
                stored_tag = str(state.get("run_tag", ""))
                return stored_tag != expected_tag
    except Exception:
        pass
    # Fallback if HF_TOKEN not present: if booted on Monday 00:00..01:00 UTC, trigger immediately
    return current.hour == 0


def seconds_until_next_monday_utc(
    now: datetime | None = None,
    check_untriggered_monday: bool = False,
) -> tuple[float, datetime]:
    """Return `(seconds_remaining, target_monday_00_00_utc)`."""
    current = now or datetime.now(timezone.utc)
    if check_untriggered_monday and _is_current_monday_untriggered(current):
        monday_zero = current.replace(hour=0, minute=0, second=0, microsecond=0)
        return 0.0, monday_zero

    days_ahead = (7 - current.weekday()) % 7
    if days_ahead == 0:
        # Already Monday (and current week already triggered/completed) -> target next week's Monday 00:00:00 UTC
        days_ahead = 7
    next_monday = (current + timedelta(days=days_ahead)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return (next_monday - current).total_seconds(), next_monday


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV Standby Master Clock")
    parser.add_argument(
        "--max-session-seconds",
        type=int,
        default=int(os.environ.get("STANDBY_MAX_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))),
    )
    args = parser.parse_args()

    callback_sent = False
    interrupted = False

    def _on_signal(signum: int, _frame: object) -> None:
        nonlocal interrupted
        print(f"[StandbyClock] Received signal {signum}; waking up to dispatch callback...", flush=True)
        interrupted = True

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    now = datetime.now(timezone.utc)
    wait_seconds, next_monday = seconds_until_next_monday_utc(now, check_untriggered_monday=True)

    print(
        f"=================================================================\n"
        f" [STANDBY MASTER CLOCK]\n"
        f" Current UTC : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Next Monday : {next_monday.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Remaining   : {wait_seconds:.0f}s ({wait_seconds / 3600:.2f} hours)\n"
        f"=================================================================",
        flush=True,
    )

    try:
        if wait_seconds > args.max_session_seconds:
            # Avoid handing off right on the Monday 00:00 UTC boundary:
            # if the next session would have <30 minutes left before Monday 00:00 UTC,
            # hand off 30 minutes early so the final standby session boots well before Monday 00:00 UTC.
            sleep_budget = args.max_session_seconds
            if wait_seconds < args.max_session_seconds + 1800:
                sleep_budget = max(60, int(wait_seconds) - 1800)

            print(
                f"[StandbyClock] Wait exceeds {args.max_session_seconds}s session cap. "
                f"Sleeping {sleep_budget}s then firing REARM_STANDBY...",
                flush=True,
            )
            slept = 0
            while slept < sleep_budget and not interrupted:
                step = min(30, sleep_budget - slept)
                time.sleep(step)
                slept += step
            callback_sent = send_github_callback(node_id=99, status="REARM_STANDBY", phase="standby")
        else:
            target_sleep = max(1, int(wait_seconds) + 5)
            print(
                f"[StandbyClock] Monday 00:00 UTC is within reach! "
                f"Sleeping {target_sleep}s then firing TRIGGER_WEEKLY...",
                flush=True,
            )
            slept = 0
            while slept < target_sleep and not interrupted:
                step = min(30, target_sleep - slept)
                time.sleep(step)
                slept += step

            # Verify whether we reached Monday 00:00 UTC (or within 60s of it)
            end_now = datetime.now(timezone.utc)
            rem_after, _ = seconds_until_next_monday_utc(end_now, check_untriggered_monday=True)
            if interrupted and rem_after > 120:
                callback_sent = send_github_callback(node_id=99, status="REARM_STANDBY", phase="standby")
            else:
                callback_sent = send_github_callback(node_id=99, status="TRIGGER_WEEKLY", phase="weekly")
    finally:
        if not callback_sent:
            end_now = datetime.now(timezone.utc)
            rem_after, _ = seconds_until_next_monday_utc(end_now, check_untriggered_monday=True)
            fallback_status = "TRIGGER_WEEKLY" if rem_after <= 120 else "REARM_STANDBY"
            fallback_phase = "weekly" if fallback_status == "TRIGGER_WEEKLY" else "standby"
            print(
                f"[StandbyClock] Fallback guarantee: dispatching {fallback_status}...",
                flush=True,
            )
            send_github_callback(node_id=99, status=fallback_status, phase=fallback_phase)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

