"""
Symbol universe loader and balanced 20-node chunk-aware `map.json` planner.

Supports both:
- String entries `"EXCHANGE.csv"` (all 2,000-symbol chunks of that exchange file)
- Range dict entries `{"file": "EXCHANGE.csv", "chunk_start": 1, "chunk_end": 50}`
  (1-based inclusive chunk slice of an oversized file such as `FINRA.csv`),
  guaranteeing zero bucket file collisions and balanced ~52-chunk (~79.5k symbol)
  workloads across all 20 Kaggle nodes.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.config import CHUNK_SIZE, TOTAL_NOTEBOOKS


@dataclass(frozen=True)
class ChunkAssignment:
    """A single 2,000-symbol chunk assigned to a node."""

    exchange: str
    csv_name: str
    chunk_idx: int          # 1-based chunk index (1..total_file_chunks)
    total_file_chunks: int  # Total chunks in the source CSV
    symbols: list[str]


def load_symbols_from_csv(csv_path: Path) -> list[str]:
    """Load, clean, and deduplicate symbols from an exchange CSV while preserving order."""
    df = pd.read_csv(csv_path, usecols=lambda c: c == "symbol", dtype=str)
    if "symbol" not in df.columns:
        return []
    raw_syms = df["symbol"].dropna().astype(str).str.strip().tolist()
    seen: set[str] = set()
    deduped: list[str] = []
    for s in raw_syms:
        if s and s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def expand_node_assignments(
    map_data: dict[str, list[Any]],
    notebook_id: int,
    data_dir: Path,
    chunk_size: int = CHUNK_SIZE,
) -> list[ChunkAssignment]:
    """
    Expand the `map.json` entries for `notebook_id` into concrete `ChunkAssignment`
    objects (each up to `chunk_size = 2000` symbols).
    """
    entries = map_data.get(str(notebook_id), [])
    assignments: list[ChunkAssignment] = []

    for entry in entries:
        if isinstance(entry, str):
            csv_name = entry
            chunk_start = 1
            chunk_end = None
        elif isinstance(entry, dict):
            csv_name = str(entry["file"])
            chunk_start = int(entry.get("chunk_start", 1))
            chunk_end = int(entry["chunk_end"]) if entry.get("chunk_end") is not None else None
        else:
            continue

        csv_path = data_dir / csv_name
        if not csv_path.exists():
            print(f"[MapPlanner] Warning: CSV file not found: {csv_path}", flush=True)
            continue

        symbols = load_symbols_from_csv(csv_path)
        if not symbols:
            continue

        exchange = csv_path.stem
        all_chunks = [
            symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)
        ]
        total_chunks = len(all_chunks)
        start_idx = max(1, chunk_start)
        end_idx = min(total_chunks, chunk_end if chunk_end is not None else total_chunks)

        for c_idx in range(start_idx, end_idx + 1):
            assignments.append(
                ChunkAssignment(
                    exchange=exchange,
                    csv_name=csv_name,
                    chunk_idx=c_idx,
                    total_file_chunks=total_chunks,
                    symbols=all_chunks[c_idx - 1],
                )
            )

    return assignments


def generate_balanced_map(
    data_dir: Path,
    num_nodes: int = TOTAL_NOTEBOOKS,
    chunk_size: int = CHUNK_SIZE,
    max_whole_file_chunks: int = 45,
) -> dict[str, list[Any]]:
    """
    Partition all `*.csv` files in `data_dir` across `num_nodes` notebooks so
    each node receives an almost identical symbol/chunk count.

    Files with more than `max_whole_file_chunks` chunks (such as `FINRA.csv` with
    149 chunks) are split into contiguous chunk-range dicts
    `{"file": "FINRA.csv", "chunk_start": s, "chunk_end": e}` (~30 chunks each),
    while smaller files are kept intact as plain `"EXCHANGE.csv"` strings.
    """
    csv_files = sorted(data_dir.glob("*.csv"))
    work_units: list[tuple[int, Any]] = []  # (symbol_count, map_entry)

    split_slice_chunks = 30
    for csv_path in csv_files:
        syms = load_symbols_from_csv(csv_path)
        n_syms = len(syms)
        if n_syms == 0:
            continue
        n_chunks = math.ceil(n_syms / chunk_size)
        if n_chunks > max_whole_file_chunks:
            for c_start in range(1, n_chunks + 1, split_slice_chunks):
                c_end = min(n_chunks, c_start + split_slice_chunks - 1)
                slice_syms = len(syms[(c_start - 1) * chunk_size : c_end * chunk_size])
                work_units.append(
                    (
                        slice_syms,
                        {
                            "file": csv_path.name,
                            "chunk_start": c_start,
                            "chunk_end": c_end,
                        },
                    )
                )
        else:
            work_units.append((n_syms, csv_path.name))

    # Greedy largest-processing-time (LPT) assignment across the 20 nodes
    work_units.sort(key=lambda x: x[0], reverse=True)
    node_loads = [0 for _ in range(num_nodes)]
    node_entries: dict[str, list[Any]] = {str(i): [] for i in range(num_nodes)}

    for sym_count, entry in work_units:
        lightest_node = min(range(num_nodes), key=lambda i: node_loads[i])
        node_entries[str(lightest_node)].append(entry)
        node_loads[lightest_node] += sym_count

    return node_entries


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    d_dir = repo_root / "data"
    out_map = repo_root / "map.json"
    balanced = generate_balanced_map(d_dir)
    out_map.write_text(json.dumps(balanced, indent=2), encoding="utf-8")
    print(f"Generated balanced map at {out_map}")
