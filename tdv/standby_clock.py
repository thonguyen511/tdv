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
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.callback import send_github_callback
from tdv.config import DEFAULT_TIMEOUT_SECONDS


def seconds_until_next_monday_utc(now: datetime | None = None) -> tuple[float, datetime]:
    """Return `(seconds_remaining, target_monday_00_00_utc)`."""
    current = now or datetime.now(timezone.utc)
    days_ahead = (7 - current.weekday()) % 7
    if days_ahead == 0:
        # Already Monday -> target next week's Monday 00:00:00 UTC
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

    now = datetime.now(timezone.utc)
    wait_seconds, next_monday = seconds_until_next_monday_utc(now)

    print(
        f"=================================================================\n"
        f" [STANDBY MASTER CLOCK]\n"
        f" Current UTC : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Next Monday : {next_monday.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Remaining   : {wait_seconds:.0f}s ({wait_seconds / 3600:.2f} hours)\n"
        f"=================================================================",
        flush=True,
    )

    if wait_seconds > args.max_session_seconds:
        print(
            f"[StandbyClock] Wait exceeds {args.max_session_seconds}s session cap. "
            f"Sleeping {args.max_session_seconds}s then firing REARM_STANDBY...",
            flush=True,
        )
        slept = 0
        while slept < args.max_session_seconds:
            step = min(30, args.max_session_seconds - slept)
            time.sleep(step)
            slept += step
        send_github_callback(node_id=99, status="REARM_STANDBY", phase="standby")
    else:
        target_sleep = max(1, int(wait_seconds) + 5)
        print(
            f"[StandbyClock] Monday 00:00 UTC is within reach! "
            f"Sleeping {target_sleep}s then firing TRIGGER_WEEKLY...",
            flush=True,
        )
        slept = 0
        while slept < target_sleep:
            step = min(30, target_sleep - slept)
            time.sleep(step)
            slept += step
        send_github_callback(node_id=99, status="TRIGGER_WEEKLY", phase="weekly")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

