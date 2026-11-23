"""
Standby Master Clock Notebook (`tdv.standby_clock`).

Runs inside a single lightweight Kaggle session rotating round-robin across
Kaggle accounts (`Account 0 -> Account 1 -> Account 2 -> Account 3 -> Account 0`)
between weekly cycles so that GitHub Actions requires NO `schedule:` cron:
- Computes the exact seconds until the next scheduled Monday `00:00:00 UTC`
  (or triggers immediately if a scheduled weekly run is already overdue).
- If the wait exceeds `41,400s` (11h 30m), sleeps `41,400s` and dispatches
  `REARM_STANDBY` to GitHub Actions, which boots and verifies `tdv-standby` on
  the next Kaggle account in rotation (failing over automatically if Kaggle's
  container provisioning fails) before retiring this session.
- Confirms handoff via `control/state.json` in the HF Bucket and retries the
  GitHub dispatch if the handoff is not acknowledged.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.callback import send_github_callback
from tdv.config import (
    BUCKET_STATE_PATH,
    STANDBY_MAX_SESSION_SECONDS,
    compute_next_monday_utc,
    compute_weekly_run_tag,
)


def _read_bucket_state() -> dict[str, Any] | None:
    """Read `control/state.json` from the HF Bucket when `HF_TOKEN` is configured."""
    try:
        from tdv.bucket import BucketClient

        if os.environ.get("HF_TOKEN", "").strip():
            bucket = BucketClient()
            state = bucket.read_json(BUCKET_STATE_PATH, default=None)
            if isinstance(state, dict):
                return state
    except Exception:
        pass
    return None


def _parse_iso_utc(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_weekly_due(current: datetime, state: dict[str, Any] | None = None) -> bool:
    """
    Return True if a weekly cycle is currently due:
    1. `control/state.json` records `standby.next_weekly_utc <= current` and
       this week's `WEEK_<WWYY>` tag has not yet been started, OR
    2. `current` is Monday in UTC and this week's `WEEK_<WWYY>` tag has not yet
       been started according to `control/state.json` (or first hour of Monday
       if HF_TOKEN is unavailable).
    """
    expected_tag = compute_weekly_run_tag(current)
    resolved_state = state if state is not None else _read_bucket_state()

    if isinstance(resolved_state, dict):
        stored_tag = str(resolved_state.get("run_tag", ""))
        if stored_tag == expected_tag:
            return False

        next_weekly_dt = _parse_iso_utc(
            resolved_state.get("standby", {}).get("next_weekly_utc")
        )
        if next_weekly_dt is not None and current >= next_weekly_dt:
            return True

        if current.weekday() == 0:
            return True
        return False

    # Fallback if HF_TOKEN not present: if booted on Monday 00:00..00:59 UTC, trigger immediately
    return current.weekday() == 0 and current.hour == 0


def seconds_until_next_monday_utc(
    now: datetime | None = None,
    check_untriggered_monday: bool = False,
    state: dict[str, Any] | None = None,
) -> tuple[float, datetime]:
    """Return `(seconds_remaining, target_monday_00_00_utc)`."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)

    resolved_state = state if state is not None else (_read_bucket_state() if check_untriggered_monday else None)

    if check_untriggered_monday and _is_weekly_due(current, state=resolved_state):
        monday_zero = current.replace(hour=0, minute=0, second=0, microsecond=0)
        return 0.0, monday_zero

    if isinstance(resolved_state, dict):
        next_weekly_dt = _parse_iso_utc(
            resolved_state.get("standby", {}).get("next_weekly_utc")
        )
        if next_weekly_dt is not None and next_weekly_dt > current:
            return (next_weekly_dt - current).total_seconds(), next_weekly_dt

    next_monday = compute_next_monday_utc(current)
    return (next_monday - current).total_seconds(), next_monday


def _is_handoff_confirmed(
    status: str,
    session_id: str,
    state: dict[str, Any] | None,
) -> bool:
    """Return True when `control/state.json` confirms the dispatched handoff completed."""
    if not isinstance(state, dict):
        return False

    phase = str(state.get("phase", "")).strip()
    if status == "TRIGGER_WEEKLY":
        return phase != "standby"

    # REARM_STANDBY
    if phase != "standby":
        return True
    if not session_id:
        return False
    active_sid = str(state.get("standby", {}).get("session_id", "")).strip()
    return bool(active_sid) and active_sid != session_id


def _dispatch_with_handoff_confirmation(
    status: str,
    phase: str,
    session_id: str,
    account_idx: int | None,
    is_interrupted,
    confirm_attempts: int = 3,
    confirm_wait_seconds: int = 240,
    poll_interval_seconds: int = 20,
) -> bool:
    """
    Dispatch `REARM_STANDBY` or `TRIGGER_WEEKLY` and wait for `control/state.json`
    to confirm the successor standby (or weekly fleet) has booted. If unconfirmed
    after `confirm_wait_seconds`, re-dispatch up to `confirm_attempts` times.
    """
    extra: dict[str, Any] = {}
    if session_id:
        extra["session_id"] = session_id
    if account_idx is not None:
        extra["account_idx"] = account_idx

    can_confirm = bool(os.environ.get("HF_TOKEN", "").strip()) and (
        status == "TRIGGER_WEEKLY" or bool(session_id)
    )

    any_sent = False
    for attempt in range(1, confirm_attempts + 1):
        sent = send_github_callback(
            node_id=99,
            status=status,
            phase=phase,
            extra_payload=extra or None,
        )
        any_sent = any_sent or sent

        if not can_confirm or is_interrupted():
            return any_sent

        print(
            f"[StandbyClock] Waiting up to {confirm_wait_seconds}s for handoff confirmation "
            f"(attempt {attempt}/{confirm_attempts}, status={status})...",
            flush=True,
        )
        deadline = time.time() + confirm_wait_seconds
        while time.time() < deadline and not is_interrupted():
            step = min(poll_interval_seconds, max(1.0, deadline - time.time()))
            time.sleep(step)
            if is_interrupted():
                print(
                    "[StandbyClock] Interrupted during handoff confirmation "
                    "(fleet cleanup in progress); exiting cleanly.",
                    flush=True,
                )
                return True
            live_state = _read_bucket_state()
            if _is_handoff_confirmed(status, session_id, live_state):
                sb_info = (live_state or {}).get("standby", {})
                print(
                    f"[StandbyClock] Handoff confirmed! phase={(live_state or {}).get('phase')} "
                    f"active_acc={sb_info.get('account_idx')} active_sid={sb_info.get('session_id')}. "
                    f"Exiting cleanly.",
                    flush=True,
                )
                return True

        if attempt < confirm_attempts and not is_interrupted():
            print(
                f"[StandbyClock] Handoff not yet reflected in control/state.json after "
                f"{confirm_wait_seconds}s; re-dispatching {status}...",
                flush=True,
            )

    return any_sent


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV Standby Master Clock")
    parser.add_argument(
        "--max-session-seconds",
        type=int,
        default=int(os.environ.get("STANDBY_MAX_SECONDS", str(STANDBY_MAX_SESSION_SECONDS))),
    )
    args = parser.parse_args()

    session_id = os.environ.get("STANDBY_SESSION_ID", "").strip()
    raw_acc = os.environ.get("STANDBY_ACCOUNT_IDX", "").strip()
    account_idx = int(raw_acc) if raw_acc.isdigit() else None

    callback_sent = False
    interrupted = False

    def _on_signal(signum: int, _frame: object) -> None:
        nonlocal interrupted
        print(f"[StandbyClock] Received signal {signum}; waking up...", flush=True)
        interrupted = True

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    now = datetime.now(timezone.utc)
    wait_seconds, next_monday = seconds_until_next_monday_utc(now, check_untriggered_monday=True)

    print(
        f"=================================================================\n"
        f" [STANDBY MASTER CLOCK]\n"
        f" Account Idx : {account_idx if account_idx is not None else 'n/a'} | Session: {session_id or 'n/a'}\n"
        f" Current UTC : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Next Monday : {next_monday.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f" Remaining   : {wait_seconds:.0f}s ({wait_seconds / 3600:.2f} hours)\n"
        f"=================================================================",
        flush=True,
    )

    extra_payload: dict[str, Any] = {}
    if session_id:
        extra_payload["session_id"] = session_id
    if account_idx is not None:
        extra_payload["account_idx"] = account_idx

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
            callback_sent = _dispatch_with_handoff_confirmation(
                status="REARM_STANDBY",
                phase="standby",
                session_id=session_id,
                account_idx=account_idx,
                is_interrupted=lambda: interrupted,
            )
        else:
            target_sleep = max(1, int(wait_seconds) + 5) if wait_seconds > 0 else 1
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
                callback_sent = send_github_callback(
                    node_id=99,
                    status="REARM_STANDBY",
                    phase="standby",
                    extra_payload=extra_payload or None,
                )
            else:
                callback_sent = _dispatch_with_handoff_confirmation(
                    status="TRIGGER_WEEKLY",
                    phase="weekly",
                    session_id=session_id,
                    account_idx=account_idx,
                    is_interrupted=lambda: interrupted,
                )
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
            send_github_callback(
                node_id=99,
                status=fallback_status,
                phase=fallback_phase,
                extra_payload=extra_payload or None,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
