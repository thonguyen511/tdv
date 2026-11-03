"""
Symbol extraction (`get_symbols_from_file` with `EXCHANGE:SYMBOL` and futures
`contracts` expansion) and `map.json` management (`generate_split_map`,
`update_map_incremental`, `get_assigned_csv_files`).

Preserves the exact ticker extraction and greedy whole-file `map.json` format
from `src/src_1/core/generate_map.py` and `github/tdv_aio/code/initial.py`.
"""

from __future__ import annotations

import ast
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tdv.config import CHUNK_SIZE, TOTAL_NOTEBOOKS

# Increase CSV field size limit so large `contracts` lists in futures CSVs never fail
try:
    csv.field_size_limit(min(sys.maxsize, 2147483647))
except Exception:
    pass


@dataclass(frozen=True)
class ChunkAssignment:
    """A single 2,000-symbol chunk of an exchange CSV assigned to a node."""

    exchange: str
    csv_name: str
    chunk_idx: int          # 1-based chunk index (1..total_file_chunks)
    total_file_chunks: int  # Total chunks in the source CSV
    symbols: list[str]


def get_symbols_from_file(file_path: str | Path) -> list[str]:
    """
    Extract full `EXCHANGE:SYMBOL` tickers from a CSV file, expanding futures
    `contracts` (`f"{prefix}:{c['symbol']}"`) when present.
    """
    symbols: list[str] = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return symbols
            try:
                ex_idx = header.index("exchange_code")
                sym_idx = header.index("symbol")
                contracts_idx = header.index("contracts") if "contracts" in header else -1
            except ValueError:
                return symbols

            for row in reader:
                if len(row) > max(ex_idx, sym_idx):
                    base_symbol = row[sym_idx].strip()
                    if not base_symbol:
                        continue
                    extracted_symbols: list[str] = []

                    if contracts_idx != -1 and len(row) > contracts_idx:
                        contracts_str = row[contracts_idx].strip()
                        if contracts_str.startswith("["):
                            try:
                                contracts_list = ast.literal_eval(contracts_str)
                                if isinstance(contracts_list, list) and len(contracts_list) > 0:
                                    for c in contracts_list:
                                        if isinstance(c, dict) and c.get("symbol"):
                                            prefix = str(c.get("prefix", row[ex_idx].strip())).strip()
                                            extracted_symbols.append(f"{prefix}:{c['symbol']}")
                            except Exception:
                                pass

                    if not extracted_symbols:
                        extracted_symbols.append(f"{row[ex_idx].strip()}:{base_symbol}")

                    for sym in extracted_symbols:
                        symbols.append(sym)
    except Exception as exc:
        print(f"[MapPlanner] ERROR reading {file_path}: {exc}", flush=True)
    return list(dict.fromkeys(symbols))


# Alias for backwards compatibility inside tdv
load_symbols_from_csv = get_symbols_from_file


def get_assigned_csv_files(repo_path: str | Path, notebook_index: int) -> list[Path]:
    """Load `map.json` and return the CSV file paths assigned to `notebook_index`."""
    root = Path(repo_path)
    map_file = root / "map.json"
    if not map_file.exists():
        raise FileNotFoundError(f"map.json not found in {root}")

    node_mappings = json.loads(map_file.read_text(encoding="utf-8"))
    assigned_entry: dict[str, Any] | None = None

    if isinstance(node_mappings, list):
        for entry in node_mappings:
            if isinstance(entry, dict) and int(entry.get("node_index", -1)) == int(notebook_index):
                assigned_entry = entry
                break
        raw_files = assigned_entry.get("files", []) if assigned_entry else []
    elif isinstance(node_mappings, dict):
        raw_files = node_mappings.get(str(notebook_index), [])
    else:
        raw_files = []

    if assigned_entry is None and not raw_files:
        raise ValueError(f"No entry found in map.json for node_index={notebook_index}")

    assigned_files: list[Path] = []
    for item in raw_files:
        fname = item["file"] if isinstance(item, dict) else str(item)
        fpath = root / fname
        if not fpath.exists():
            alt_path = root / "data" / fname
            if alt_path.exists():
                fpath = alt_path
        if fpath.exists():
            assigned_files.append(fpath)
        else:
            print(f"[MapPlanner] Warning: Assigned file {fname} not found in {root}", flush=True)

    return assigned_files


def expand_node_assignments(
    map_data: list[dict[str, Any]] | dict[str, Any],
    notebook_id: int,
    data_dir: Path,
    chunk_size: int = CHUNK_SIZE,
) -> list[ChunkAssignment]:
    """
    Expand `map.json` entries for `notebook_id` into 2,000-symbol `ChunkAssignment`
    objects using `get_symbols_from_file` (`EXCHANGE:SYMBOL` + `contracts` expansion).
    """
    raw_entries: list[Any] = []
    if isinstance(map_data, list):
        for node_obj in map_data:
            if isinstance(node_obj, dict) and int(node_obj.get("node_index", -1)) == int(notebook_id):
                raw_entries = list(node_obj.get("files", []))
                break
    elif isinstance(map_data, dict):
        raw_entries = list(map_data.get(str(notebook_id), []))

    assignments: list[ChunkAssignment] = []
    for entry in raw_entries:
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
            alt = data_dir.parent / csv_name
            if alt.exists():
                csv_path = alt
            else:
                print(f"[MapPlanner] Warning: CSV file not found: {csv_path}", flush=True)
                continue

        symbols = get_symbols_from_file(csv_path)
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


def generate_split_map(
    csv_dir: str | Path,
    map_file: str | Path | None = None,
    total_nodes: int = TOTAL_NOTEBOOKS,
) -> list[dict[str, Any]]:
    """
    Generate the greedy whole-file split `map.json` across `total_nodes` nodes
    (matching `src/src_1/core/generate_map.py`).
    """
    c_dir = Path(csv_dir)
    csv_files = sorted(f.name for f in c_dir.glob("*.csv"))
    file_counts: list[dict[str, Any]] = []

    for fname in csv_files:
        count = len(get_symbols_from_file(c_dir / fname))
        file_counts.append({"path": fname, "symbols": count})

    file_counts.sort(key=lambda x: x["symbols"], reverse=True)
    nodes: list[dict[str, Any]] = [
        {"node_index": i, "total_symbols": 0, "files": []} for i in range(total_nodes)
    ]

    for f_info in file_counts:
        lightest = min(nodes, key=lambda n: n["total_symbols"])
        lightest["files"].append(f_info["path"])
        lightest["total_symbols"] += f_info["symbols"]

    if map_file is not None:
        Path(map_file).write_text(json.dumps(nodes, indent=4), encoding="utf-8")

    return nodes


def update_map_incremental(
    csv_dir: str | Path,
    map_file: str | Path,
    total_nodes: int = TOTAL_NOTEBOOKS,
) -> list[dict[str, Any]]:
    """Incrementally update `map.json` with any new CSV files or updated symbol counts."""
    m_path = Path(map_file)
    c_dir = Path(csv_dir)
    if not m_path.exists():
        return generate_split_map(c_dir, m_path, total_nodes)

    try:
        nodes = json.loads(m_path.read_text(encoding="utf-8"))
        if not isinstance(nodes, list):
            return generate_split_map(c_dir, m_path, total_nodes)
    except Exception:
        return generate_split_map(c_dir, m_path, total_nodes)

    csv_files_set = {f.name for f in c_dir.glob("*.csv")}
    mapped_files: set[str] = set()
    counts_changed = False

    for n in nodes:
        existing_node_files = [f for f in n.get("files", []) if f in csv_files_set]
        if len(existing_node_files) != len(n.get("files", [])):
            n["files"] = existing_node_files
            counts_changed = True
        mapped_files.update(existing_node_files)
        recalculated_total = sum(
            len(get_symbols_from_file(c_dir / fname)) for fname in existing_node_files
        )
        if n.get("total_symbols") != recalculated_total:
            n["total_symbols"] = recalculated_total
            counts_changed = True

    new_files = sorted(f for f in csv_files_set if f not in mapped_files)
    if not new_files and not counts_changed:
        return nodes

    if new_files:
        file_counts = [
            {"path": f, "symbols": len(get_symbols_from_file(c_dir / f))}
            for f in new_files
        ]
        file_counts.sort(key=lambda x: x["symbols"], reverse=True)
        for f_info in file_counts:
            lightest = min(nodes, key=lambda n: n["total_symbols"])
            lightest["files"].append(f_info["path"])
            lightest["total_symbols"] += f_info["symbols"]

    m_path.write_text(json.dumps(nodes, indent=4), encoding="utf-8")
    return nodes


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    d_dir = repo_root / "data"
    out_map = repo_root / "map.json"
    generate_split_map(d_dir, out_map, TOTAL_NOTEBOOKS)
    print(f"Generated greedy whole-file map at {out_map}")
