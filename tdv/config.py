"""
Central configuration and constants for the `tdv` pipeline.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Repository & Storage Identifiers
# ---------------------------------------------------------------------------
DEFAULT_HF_BUCKET_ID = "thonguyen511/temp"
DEFAULT_GITHUB_REPO = "thonguyen511/tdv"

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
# - Dual-pool mode (when TV_ULTIMATE_* credentials are provided and verified):
#   5 Ultimate workers on prodata.tradingview.com + 3 Free workers on
#   data.tradingview.com = 8 total workers.
TOTAL_WORKERS = 8
FREE_POOL_WORKERS = 8
ULTIMATE_POOL_WORKERS = 5
FREE_DUAL_WORKERS = 3

# ---------------------------------------------------------------------------
# Timeframe Definitions
# ---------------------------------------------------------------------------
STAGE1_INTERVAL = "1D"
STAGE2_MACRO_INTERVALS = ["1W", "1M"]
INTRADAY_INTERVALS = ["1", "5", "15", "60", "240"]
DAILY_INTERVALS = ["1D", "1W", "1M"]
ALL_INTERVALS = DAILY_INTERVALS + INTRADAY_INTERVALS

# ---------------------------------------------------------------------------
# Scraping & Retry Parameters
# ---------------------------------------------------------------------------
BARS_INITIAL_LOAD = 50000
BARS_MORE_LOAD = 50000
MAX_HISTORY_BATCHES = 1000  # Phase 1: up to 50M bars backwards paging
MAX_TASK_RETRIES = 6

# ---------------------------------------------------------------------------
# Supervisor & Standby Timing
# ---------------------------------------------------------------------------
# Kaggle hard cap is 12h (43,200s); 11h 45m (42,300s) leaves 15 minutes for
# final chunk flush, /kaggle/working cleanup, and GitHub callback dispatch.
DEFAULT_TIMEOUT_SECONDS = 42300

# Cooldown delay injected via START_DELAY_SECONDS when recovering from
# RATE_LIMITED or transient ERROR without relying on a GitHub cron schedule.
RATE_LIMIT_COOLDOWN_SECONDS = 900   # 15 minutes
ERROR_COOLDOWN_SECONDS = 300        # 5 minutes
MAX_CONSECUTIVE_NODE_FAILURES = 6

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


def get_jwt_bucket_path(notebook_id: int) -> str:
    """Return the encrypted JWT cache path in the HF bucket for a node."""
    return f"{BUCKET_JWT_DIR}/node_{int(notebook_id)}.json.enc"


def get_chunk_bucket_path(exchange: str, interval: str, chunk_idx: int) -> str:
    """Return the remote HF bucket path for an encrypted Parquet chunk."""
    return f"data/{exchange}/{interval}/{exchange}_{interval}_chunk_{chunk_idx}.parquet.enc"


def get_log_bucket_dir(notebook_id: int, run_tag: str) -> str:
    """Return the remote HF bucket directory for a node's run logs."""
    return f"log/NODE_{int(notebook_id)}/{run_tag}"


def compute_weekly_run_tag(dt: datetime | None = None) -> str:
    """Compute the canonical weekly run tag `WEEK_<WWYY>` in UTC."""
    now = dt or datetime.now(timezone.utc)
    week_num = now.strftime("%W")
    year_short = now.strftime("%y")
    return f"WEEK_{week_num}{year_short}"


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

