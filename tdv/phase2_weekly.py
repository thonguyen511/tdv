"""
Weekly Job (`WEEK_<WWYY>` Scan).

Operates 100% identically to the Initial job (`fetch_full_history=True`, `1,000,000`
initial & `request_more_data` bar loads, universal `1D` studies + quote `last_splits`,
and duplicate `(symbol, time)` rows deduplicated during the final chunk merge step),
differing strictly in:
1. Cookie selection: `(8, free)` (8 Free workers using the node's Free JWT).
2. Yearly CSV log column: `WEEK_<WWYY>` in `log/node_{i}_{yyyy}.csv`.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.config import COOKIE_MODE_FREE, compute_weekly_run_tag
from tdv.pipeline import run_node_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV Phase 2: Weekly Scan")
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
        fetch_full_history=True,
        run_tag=run_tag,
        max_chunks=args.max_chunks,
        cookie_mode=COOKIE_MODE_FREE,
    )


if __name__ == "__main__":
    sys.exit(main())
