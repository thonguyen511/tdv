"""
Initial Job (`INITIAL` Full Historical Scan).

Uses the Premium/Ultimate cookie/JWT when available (both Phase 1 `1D` + studies
and Phase 2 intraday Bar Replay), falling back cleanly to Free workers during
testing. Requests `1,000,000` bars and writes encrypted `.parquet.enc` chunks +
per-node yearly CSV logs (`log/node_{i}_{yyyy}.csv`) to the HF Storage Bucket.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.pipeline import run_node_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="TDV Phase 1: Initial Full Historical Scan")
    parser.add_argument(
        "--notebook-id",
        type=int,
        default=int(os.environ.get("NOTEBOOK_ID", "0")),
        help="Node index (0..19)",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=None,
        help="Optional cap on chunks to process (for testing)",
    )
    args = parser.parse_args()

    return run_node_pipeline(
        notebook_id=args.notebook_id,
        fetch_full_history=True,
        run_tag="INITIAL",
        max_chunks=args.max_chunks,
    )


if __name__ == "__main__":
    sys.exit(main())
