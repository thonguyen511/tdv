"""
Weekly Job (`WEEK_<WWYY>` Incremental Maintenance).

Always uses Free cookies/JWTs (no premium cookie needed after the initial run).
Requests `1,000,000` bars per series (TradingView caps to the Free JWT limit,
overlapping the previous week), overwrite-merges deduplicated by `["symbol", "time"]`,
re-encrypts in place, and updates the `WEEK_<WWYY>` column in `log/node_{i}_{yyyy}.csv`.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.config import compute_weekly_run_tag
from tdv.pipeline import run_node_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV Phase 2: Weekly Incremental Maintenance")
    parser.add_argument(
        "--notebook-id",
        type=int,
        default=int(os.environ.get("NOTEBOOK_ID", "0")),
        help="Node index (0..19)",
    )
    parser.add_argument(
        "--run-tag",
        type=str,
        default=os.environ.get("WEEK_TAG", "").strip() or None,
        help="Optional explicit weekly tag (e.g. WEEK_4426)",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
        help="Optional cap on chunks to process (for testing)",
    )
    args = parser.parse_args()

    run_tag = args.run_tag or compute_weekly_run_tag()
    return run_node_pipeline(
        notebook_id=args.notebook_id,
        fetch_full_history=False,
        run_tag=run_tag,
        max_chunks=args.max_chunks,
    )


if __name__ == "__main__":
    sys.exit(main())
