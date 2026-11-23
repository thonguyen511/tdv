"""
Central configuration and constants for the `tdv` pipeline.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Repository & Storage Identifiers
# ---------------------------------------------------------------------------
DEFAULT_HF_BUCKET_ID = "thonguyen511/temp"
DEFAULT_GITHUB_REPO = "thonguyen511/tdv"
DEFAULT_FREE_WS_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart"
DEFAULT_PREMIUM_WS_URL = "wss://prodata.tradingview.com/socket.io/websocket?from=chart%2F&type=chart"


def get_ws_url(is_premium: bool = False) -> str:
    """
    Return the TradingView WebSocket URL for Free or Premium/Ultimate workers.
    Allows overriding the Premium WS endpoint via `TV_PREMIUM_WS_URL` or `TV_ULTIMATE_WS_URL`.
    """
    if is_premium:
        override = (
            os.environ.get("TV_PREMIUM_WS_URL", "").strip()
            or os.environ.get("TV_ULTIMATE_WS_URL", "").strip()
        )
        if override:
            return override
        return DEFAULT_PREMIUM_WS_URL
    return os.environ.get("TV_FREE_WS_URL", "").strip() or DEFAULT_FREE_WS_URL

# ---------------------------------------------------------------------------
# Fleet & Chunking Parameters
# ---------------------------------------------------------------------------
TOTAL_NOTEBOOKS = 20
NODES_PER_KAGGLE_ACCOUNT = 5
TOTAL_KAGGLE_ACCOUNTS = 4
CHUNK_SIZE = 2000

# Worker pool sizing per notebook:
# - Default Free-pool mode: all 8 workers connect to data.tradingview.com using
#   the node's authenticated Free account JWT.
# - Initial / Premium mode (when TV_ULTIMATE_* credentials are provided and verified):
#   5 Ultimate workers on prodata.tradingview.com + 3 Free workers on
#   data.tradingview.com = 8 total workers (or Ultimate used in Phase 1 1D + Phase 2).
TOTAL_WORKERS = 8
FREE_POOL_WORKERS = 8
ULTIMATE_POOL_WORKERS = 5
FREE_DUAL_WORKERS = 3

# ---------------------------------------------------------------------------
# Timeframe Definitions (8 timeframes per symbol)
# ---------------------------------------------------------------------------
STAGE1_INTERVAL = "1D"
STAGE2_MACRO_INTERVALS = ["1W", "1M"]
INTRADAY_INTERVALS = ["1", "5", "15", "60", "240"]
DAILY_INTERVALS = ["1D", "1W", "1M"]
ALL_INTERVALS = DAILY_INTERVALS + INTRADAY_INTERVALS

# ---------------------------------------------------------------------------
# Scraping & Retry Parameters
# ---------------------------------------------------------------------------
# Always request 1,000,000 bars; TradingView WS API automatically caps to the JWT limit.
BARS_INITIAL_LOAD = 1_000_000
BARS_MORE_LOAD = 1_000_000
MAX_HISTORY_BATCHES = 1000
MAX_REPLAY_BATCHES = 200
MAX_TASK_RETRIES = 6

# ---------------------------------------------------------------------------
# Yearly Node Log CSV Status Codes (`log/node_{x}_{yyyy}.csv`)
# ---------------------------------------------------------------------------
STATUS_OK = "1"                    # Scanned & saved (>0 bars)
STATUS_NULL = ""                   # Not scanned yet (null / empty cell in CSV)
STATUS_ERR_SYMBOL = "E1"           # Definitive TradingView symbol_error / series_error
STATUS_ERR_EMPTY = "E2"            # Definitive series_completed with 0 bars returned
STATUS_SKIPPED_NO_INTRADAY = "E3"  # Intraday skipped because 1D reported has_intraday=False
STATUS_ERR_TRANSIENT = "E4"        # Transient network/socket/timeout error (retried on resume)
STATUS_ERR_STUDY = "E5"            # Study error

DEFINITIVE_DONE_STATUSES = frozenset({
    STATUS_OK,
    STATUS_ERR_SYMBOL,
    STATUS_ERR_EMPTY,
    STATUS_SKIPPED_NO_INTRADAY,
})

# ---------------------------------------------------------------------------
# Supervisor & Standby Timing
# ---------------------------------------------------------------------------
# Kaggle hard cap is 12h (43,200s); 11h 45m (42,300s) leaves 15 minutes for
# final chunk flush, /kaggle/working cleanup, and GitHub callback dispatch.
DEFAULT_TIMEOUT_SECONDS = 42300

# Standby clock runs 11h 30m (41,400s) per Kaggle session so there is a full
# 30-minute margin for confirmed handoff & retry before Kaggle's 12h hard cap.
STANDBY_MAX_SESSION_SECONDS = 41400

# Post-push Kaggle kernel boot verification parameters (catches immediate
# Kaggle infrastructure crashes such as stale-token SignBlob errors).
BOOT_VERIFY_TIMEOUT_SECONDS = 150
BOOT_VERIFY_STABLE_SECONDS = 30
BOOT_VERIFY_POLL_INTERVAL_SECONDS = 10

# Cooldown delay injected via START_DELAY_SECONDS when recovering from
# RATE_LIMITED or transient ERROR without relying on a GitHub cron schedule.
RATE_LIMIT_COOLDOWN_SECONDS = 900   # 15 minutes
ERROR_COOLDOWN_SECONDS = 300        # 5 minutes
MAX_CONSECUTIVE_NODE_FAILURES = 6
# Maximum allowed missing/error task rate (percentage) for a node to be considered complete.
MAX_ALLOWED_MISSING_RATE_PCT = 2.0

# ---------------------------------------------------------------------------
# Supervisor / Worker Process Exit Codes
# ---------------------------------------------------------------------------
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_RATE_LIMITED = 2
EXIT_AUTH_FAILED = 3

# ---------------------------------------------------------------------------
# Bucket Layout Paths
# ---------------------------------------------------------------------------
BUCKET_STATE_PATH = "control/state.json"
BUCKET_JWT_DIR = "control/jwt"

_CHUNK_LABEL_RE = re.compile(r"^\s*([^\[\]]+?)\s*\[\s*(\d+)\s*\]\s*$")


def format_chunk_label(exchange: str, chunk_idx: int) -> str:
    """Format a chunk column value as `EXCHANGE [N]` (e.g. `ABAXX [1]`, `NASDAQ [6]`)."""
    return f"{exchange.strip()} [{int(chunk_idx)}]"


def parse_chunk_label(chunk_label: str, fallback_exchange: str = "") -> tuple[str, int]:
    """
    Parse a `chunk` column label such as `ABAXX [1]` (or legacy `ABAXX_1` / `1`)
    into `(exchange, chunk_idx)`.
    """
    raw = str(chunk_label or "").strip()
    m = _CHUNK_LABEL_RE.match(raw)
    if m:
        return m.group(1).strip(), int(m.group(2))
    if "_" in raw:
        ex_part, idx_part = raw.rsplit("_", 1)
        if idx_part.isdigit():
            return ex_part.strip(), int(idx_part)
    if raw.isdigit():
        return fallback_exchange.strip(), int(raw)
    return fallback_exchange.strip() or raw, 1


def resolve_year_from_run_tag(run_tag: str | None = None, dt: datetime | None = None) -> int:
    """Resolve the 4-digit UTC year for `log/node_{x}_{yyyy}.csv`."""
    if run_tag:
        tag = run_tag.strip().upper()
        if tag.startswith("WEEK_") and len(tag) >= 9 and tag[-2:].isdigit():
            return 2000 + int(tag[-2:])
    now = dt or datetime.now(timezone.utc)
    return int(now.year)


def get_node_yearly_log_bucket_path(notebook_id: int, year: int | None = None) -> str:
    """Return the HF Bucket path `log/node_{x}_{yyyy}.csv` for a node's yearly log CSV."""
    yr = int(year) if year is not None else datetime.now(timezone.utc).year
    return f"log/node_{int(notebook_id)}_{yr}.csv"


def get_jwt_bucket_path(notebook_id: int) -> str:
    """Return the encrypted JWT cache path in the HF bucket for a node."""
    return f"{BUCKET_JWT_DIR}/node_{int(notebook_id)}.json.enc"


def get_chunk_bucket_path(exchange: str, interval: str, chunk_idx: int) -> str:
    """Return the remote HF bucket path for an encrypted Parquet chunk."""
    return f"data/{exchange}/{interval}/{exchange}_{interval}_chunk_{chunk_idx}.parquet.enc"


def get_log_bucket_dir(notebook_id: int, run_tag: str) -> str:
    """Return the legacy remote HF bucket directory for a node's run logs."""
    return f"log/NODE_{int(notebook_id)}/{run_tag}"


def compute_weekly_run_tag(dt: datetime | None = None) -> str:
    """Compute the canonical weekly run tag `WEEK_<WWYY>` in UTC."""
    now = dt or datetime.now(timezone.utc)
    week_num = now.strftime("%W")
    year_short = now.strftime("%y")
    return f"WEEK_{week_num}{year_short}"


def compute_next_monday_utc(dt: datetime | None = None) -> datetime:
    """Return the next Monday `00:00:00 UTC` strictly after `dt`."""
    current = dt or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    days_ahead = (7 - current.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (current + timedelta(days=days_ahead)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def get_repo_root() -> Path:
    """Return the root directory of the `tdv` repository."""
    return Path(__file__).resolve().parent.parent


def get_staging_root() -> Path:
    """
    Return a staging directory outside `/kaggle/working` when running on Kaggle
    so Kaggle does not archive temporary files on container teardown.
    """
    override = os.environ.get("TDV_STAGING_DIR", "").strip()
    if override:
        return Path(override)
    if Path("/kaggle/working").exists():
        return Path("/tmp/tdv_staging")
    return get_repo_root() / "staging"

