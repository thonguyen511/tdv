"""
Yearly Per-Node CSV Log Manager (`log/node_{x}_{yyyy}.csv`).

Implements Issue 1 & Issue 3:
1. Single CSV file per node per year in the Hugging Face Bucket:
   `log/node_{notebook_id}_{year}.csv`
2. Schema:
   `symbol,timeframe,chunk,INITIAL,WEEK_4526,WEEK_4626,...`
   - Number of rows = `(number of extracted tickers assigned to node) * 8` (for the 8 timeframes).
   - `chunk` column formatted as `{EXCHANGE} [{chunk_idx}]` (e.g. `ABAXX [1]`, `NASDAQ [6]`).
   - Status codes stored directly in each run column (`INITIAL`, `WEEK_<WWYY>`):
     - `1`: Scanned & saved (>0 bars)
     - `""` (null): Not scanned yet
     - `E1`: Definitive Symbol / Series Error (`symbol_error` / `series_error`)
     - `E2`: Definitive Empty Series (`series_completed` with 0 bars)
     - `E3`: Skipped Intraday (`has_intraday` is False)
     - `E4`: Transient Network / Socket / Timeout Error (automatically retried on resume)
3. Chunk-Preserving Futures Contract Insertion (Issue 3):
   - When new contracts appear for an existing root symbol in `data/{exchange}.csv`,
     their 8 timeframe rows are inserted into `log/node_{x}_{yyyy}.csv` immediately
     after the existing contracts of that symbol with the exact same `chunk` label
     (`{EXCHANGE} [{chunk_idx}]`), allowing that chunk to exceed 2,000 tickers without
     shifting any subsequent tickers across chunk boundaries.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

from tdv.bucket import BucketClient
from tdv.config import (
    ALL_INTERVALS,
    CHUNK_SIZE,
    DEFINITIVE_DONE_STATUSES,
    INTRADAY_INTERVALS,
    STAGE1_INTERVAL,
    STATUS_ERR_EMPTY,
    STATUS_ERR_SYMBOL,
    STATUS_ERR_TRANSIENT,
    STATUS_NULL,
    STATUS_OK,
    STATUS_SKIPPED_NO_INTRADAY,
    format_chunk_label,
    get_node_yearly_log_bucket_path,
    parse_chunk_label,
    resolve_year_from_run_tag,
)
from tdv.map_planner import ChunkAssignment, get_symbol_groups_from_file


class NodeYearlyLog:
    """
    In-memory representation and CSV serializer for `log/node_{x}_{yyyy}.csv`.
    """

    def __init__(
        self,
        notebook_id: int,
        year: int,
        run_columns: list[str] | None = None,
    ) -> None:
        self.notebook_id = int(notebook_id)
        self.year = int(year)
        self.run_columns: list[str] = list(run_columns) if run_columns else ["INITIAL"]
        if "INITIAL" not in self.run_columns:
            self.run_columns.insert(0, "INITIAL")

        # Per-exchange ordered list of tickers: {exchange: [sym1, sym2, ...]}
        self.exchange_tickers: dict[str, list[str]] = {}
        # Ticker -> (exchange, chunk_idx)
        self.ticker_chunk: dict[str, tuple[str, int]] = {}
        # (symbol, timeframe) -> {run_col: status_str}
        self.status_map: dict[tuple[str, str], dict[str, str]] = {}

    @property
    def remote_bucket_path(self) -> str:
        return get_node_yearly_log_bucket_path(self.notebook_id, self.year)

    def ensure_run_column(self, run_tag: str) -> None:
        tag = run_tag.strip()
        if tag and tag not in self.run_columns:
            self.run_columns.append(tag)

    def get_status(self, symbol: str, interval: str, run_tag: str) -> str:
        cell_dict = self.status_map.get((symbol, interval))
        if not cell_dict:
            return STATUS_NULL
        return cell_dict.get(run_tag, STATUS_NULL)

    def set_status(self, symbol: str, interval: str, run_tag: str, status: str) -> None:
        self.ensure_run_column(run_tag)
        cell_dict = self.status_map.setdefault((symbol, interval), {})
        cell_dict[run_tag] = status

    @classmethod
    def from_csv_text(
        cls,
        notebook_id: int,
        year: int,
        csv_text: str,
        carry_over_only_initial: bool = False,
    ) -> NodeYearlyLog:
        """
        Parse `log/node_{x}_{yyyy}.csv` text into a `NodeYearlyLog` instance.
        If `carry_over_only_initial=True` (used when rolling over from `year - 1`
        to `year`), keeps only the `INITIAL` status column from the prior year.
        """
        reader = csv.reader(io.StringIO(csv_text))
        header = next(reader, None)
        if not header or len(header) < 3:
            return cls(notebook_id=notebook_id, year=year)

        raw_run_cols = [c.strip() for c in header[3:] if c.strip()]
        if carry_over_only_initial:
            run_cols = ["INITIAL"]
        else:
            run_cols = raw_run_cols if raw_run_cols else ["INITIAL"]

        log_obj = cls(notebook_id=notebook_id, year=year, run_columns=run_cols)
        seen_tickers_per_ex: dict[str, set[str]] = {}

        for row in reader:
            if len(row) < 3:
                continue
            sym = row[0].strip()
            tf = row[1].strip()
            chunk_str = row[2].strip()
            if not sym or not tf:
                continue

            fallback_ex = sym.split(":", 1)[0] if ":" in sym else ""
            ex_name, c_idx = parse_chunk_label(chunk_str, fallback_exchange=fallback_ex)

            if sym not in log_obj.ticker_chunk:
                log_obj.ticker_chunk[sym] = (ex_name, c_idx)
                ex_list = log_obj.exchange_tickers.setdefault(ex_name, [])
                ex_seen = seen_tickers_per_ex.setdefault(ex_name, set())
                if sym not in ex_seen:
                    ex_seen.add(sym)
                    ex_list.append(sym)

            cell_dict = log_obj.status_map.setdefault((sym, tf), {})
            for col_offset, col_name in enumerate(raw_run_cols):
                if carry_over_only_initial and col_name != "INITIAL":
                    continue
                val_idx = 3 + col_offset
                if val_idx < len(row):
                    val = row[val_idx].strip()
                    if val and val.lower() != "null":
                        cell_dict[col_name] = val

        return log_obj

    def reconcile_with_assigned_csvs(
        self,
        map_data: list[dict[str, Any]] | dict[str, Any],
        data_dir: Path,
        chunk_size: int = CHUNK_SIZE,
    ) -> tuple[list[ChunkAssignment], int]:
        """
        Reconcile the in-memory log structure with the node's assigned `data/*.csv` files:
        - Preserves existing ticker order and chunk assignments.
        - Inserts newly discovered futures contracts immediately after their root symbol's
          existing contracts in the same chunk (allowing that chunk to exceed `chunk_size`
          without shifting subsequent symbols across chunk boundaries).
        - Appends brand-new root symbols at the end of the exchange's chunks.
        Returns `(chunk_assignments, newly_inserted_tickers_count)`.
        """
        raw_entries: list[Any] = []
        if isinstance(map_data, list):
            for node_obj in map_data:
                if isinstance(node_obj, dict) and int(node_obj.get("node_index", -1)) == self.notebook_id:
                    raw_entries = list(node_obj.get("files", []))
                    break
        elif isinstance(map_data, dict):
            raw_entries = list(map_data.get(str(self.notebook_id), []))

        assignments: list[ChunkAssignment] = []
        ordered_exchanges: list[str] = []
        newly_inserted_count = 0

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
                    print(f"[LogManager] Warning: CSV file not found: {csv_path}", flush=True)
                    continue

            exchange = csv_path.stem
            if exchange not in ordered_exchanges:
                ordered_exchanges.append(exchange)

            symbol_groups = get_symbol_groups_from_file(csv_path)
            if not symbol_groups:
                continue

            existing_list = self.exchange_tickers.get(exchange, [])
            if not existing_list:
                # First time initializing this exchange in log/node_{x}_{yyyy}.csv
                flat_syms: list[str] = []
                for _, grp_syms in symbol_groups:
                    flat_syms.extend(grp_syms)

                ex_tickers: list[str] = []
                for idx_0, sym in enumerate(flat_syms):
                    c_idx = (idx_0 // chunk_size) + 1
                    ex_tickers.append(sym)
                    self.ticker_chunk[sym] = (exchange, c_idx)
                    for iv in ALL_INTERVALS:
                        self.status_map.setdefault((sym, iv), {})
                self.exchange_tickers[exchange] = ex_tickers
            else:
                # Reconcile existing exchange tickers with current CSV groups (Issue 3)
                existing_set = set(existing_list)
                # Build chunk ticker counts for appending brand-new root symbols
                chunk_counts: dict[int, int] = {}
                max_chunk_idx = 1
                for sym in existing_list:
                    _, c_idx = self.ticker_chunk.get(sym, (exchange, 1))
                    chunk_counts[c_idx] = chunk_counts.get(c_idx, 0) + 1
                    if c_idx > max_chunk_idx:
                        max_chunk_idx = c_idx

                insertions_after: dict[str, list[str]] = {}
                append_tail: list[str] = []

                for _, grp_syms in symbol_groups:
                    new_in_grp = [s for s in grp_syms if s not in existing_set]
                    if not new_in_grp:
                        continue

                    existing_in_grp = [s for s in grp_syms if s in existing_set]
                    if existing_in_grp:
                        # Issue 3: New contract(s) for an existing root symbol!
                        # Insert right after the last existing contract of this root symbol,
                        # inheriting the exact same chunk index.
                        anchor_sym = existing_in_grp[-1]
                        _, anchor_chunk_idx = self.ticker_chunk.get(anchor_sym, (exchange, 1))
                        bucket_list = insertions_after.setdefault(anchor_sym, [])
                        for new_sym in new_in_grp:
                            bucket_list.append(new_sym)
                            existing_set.add(new_sym)
                            self.ticker_chunk[new_sym] = (exchange, anchor_chunk_idx)
                            chunk_counts[anchor_chunk_idx] = chunk_counts.get(anchor_chunk_idx, 0) + 1
                            for iv in ALL_INTERVALS:
                                self.status_map.setdefault((new_sym, iv), {})
                            newly_inserted_count += 1
                    else:
                        # Brand-new root symbol at the end of the CSV
                        for new_sym in new_in_grp:
                            if chunk_counts.get(max_chunk_idx, 0) >= chunk_size:
                                max_chunk_idx += 1
                            append_tail.append(new_sym)
                            existing_set.add(new_sym)
                            self.ticker_chunk[new_sym] = (exchange, max_chunk_idx)
                            chunk_counts[max_chunk_idx] = chunk_counts.get(max_chunk_idx, 0) + 1
                            for iv in ALL_INTERVALS:
                                self.status_map.setdefault((new_sym, iv), {})
                            newly_inserted_count += 1

                if insertions_after or append_tail:
                    rebuilt: list[str] = []
                    for sym in existing_list:
                        rebuilt.append(sym)
                        if sym in insertions_after:
                            rebuilt.extend(insertions_after[sym])
                    rebuilt.extend(append_tail)
                    existing_list = rebuilt

                self.exchange_tickers[exchange] = existing_list

            # Build ChunkAssignments for this exchange directly from reconciled chunks
            chunks_map: dict[int, list[str]] = {}
            for sym in self.exchange_tickers.get(exchange, []):
                _, c_idx = self.ticker_chunk.get(sym, (exchange, 1))
                chunks_map.setdefault(c_idx, []).append(sym)

            if not chunks_map:
                continue

            total_chunks = max(chunks_map.keys())
            start_idx = max(1, chunk_start)
            end_idx = min(total_chunks, chunk_end if chunk_end is not None else total_chunks)

            for c_idx in range(start_idx, end_idx + 1):
                c_syms = chunks_map.get(c_idx, [])
                if c_syms:
                    assignments.append(
                        ChunkAssignment(
                            exchange=exchange,
                            csv_name=csv_name,
                            chunk_idx=c_idx,
                            total_file_chunks=total_chunks,
                            symbols=c_syms,
                        )
                    )

        # Keep exchange ordering aligned with map.json
        reordered_ex: dict[str, list[str]] = {}
        for ex in ordered_exchanges:
            if ex in self.exchange_tickers:
                reordered_ex[ex] = self.exchange_tickers[ex]
        for ex, syms in self.exchange_tickers.items():
            if ex not in reordered_ex:
                reordered_ex[ex] = syms
        self.exchange_tickers = reordered_ex

        return assignments, newly_inserted_count

    def prepare_run_state(
        self,
        run_tag: str,
        assignments: list[ChunkAssignment],
    ) -> tuple[set[tuple[str, str]], dict[str, bool], set[str]]:
        """
        Inspect the `run_tag` column (and historical columns for `known_intraday`) and:
        1. Clear any Stage 2 statuses for a symbol in `run_tag` if its `1D` status in
           `run_tag` is still incomplete (`""` or `"E4"`), ensuring Phase 1 (`1D`)
           always runs before Phase 2.
        2. Return `(completed_pairs, known_intraday, failed_1d_symbols)` for `run_tag`.
        """
        self.ensure_run_column(run_tag)
        completed_pairs: set[tuple[str, str]] = set()
        known_intraday: dict[str, bool] = {}
        failed_1d_symbols: set[str] = set()

        for chunk in assignments:
            for sym in chunk.symbols:
                # Infer historical has_intraday from any column if available
                for iv in INTRADAY_INTERVALS:
                    cell_dict = self.status_map.get((sym, iv), {})
                    for val in cell_dict.values():
                        if val == STATUS_SKIPPED_NO_INTRADAY:
                            known_intraday[sym] = False
                            break
                        if val == STATUS_OK:
                            known_intraday[sym] = True
                            break
                    if sym in known_intraday:
                        break

                st_1d = self.get_status(sym, STAGE1_INTERVAL, run_tag)
                if st_1d not in DEFINITIVE_DONE_STATUSES:
                    # 1D is not definitively done ("" or "E4") -> ensure all intervals
                    # for this symbol in `run_tag` are reset to "" so Phase 1 + Phase 2 run cleanly
                    for iv in ALL_INTERVALS:
                        if self.get_status(sym, iv, run_tag) != STATUS_NULL:
                            self.set_status(sym, iv, run_tag, STATUS_NULL)
                    continue

                if st_1d in (STATUS_ERR_SYMBOL, STATUS_ERR_EMPTY):
                    failed_1d_symbols.add(sym)
                    known_intraday[sym] = False
                    for iv in ALL_INTERVALS:
                        if self.get_status(sym, iv, run_tag) not in DEFINITIVE_DONE_STATUSES:
                            self.set_status(sym, iv, run_tag, st_1d)
                        completed_pairs.add((sym, iv))
                    continue

                # 1D is STATUS_OK ("1")
                completed_pairs.add((sym, STAGE1_INTERVAL))
                for iv in ALL_INTERVALS:
                    if iv == STAGE1_INTERVAL:
                        continue
                    st_iv = self.get_status(sym, iv, run_tag)
                    if st_iv in DEFINITIVE_DONE_STATUSES:
                        completed_pairs.add((sym, iv))
                        if iv in INTRADAY_INTERVALS:
                            if st_iv == STATUS_SKIPPED_NO_INTRADAY:
                                known_intraday[sym] = False
                            elif st_iv == STATUS_OK:
                                known_intraday[sym] = True
                    elif st_iv == STATUS_ERR_TRANSIENT:
                        # Transient E4 -> will be retried in this session
                        pass

        return completed_pairs, known_intraday, failed_1d_symbols

    def count_run_stats(
        self,
        run_tag: str,
        assignments: list[ChunkAssignment] | None = None,
    ) -> tuple[int, int, float]:
        """
        Return `(missing_tasks_count, total_tasks_count, missing_rate_pct)` for `run_tag`.
        A task `(symbol, timeframe)` is considered complete iff its status in `run_tag`
        is in `DEFINITIVE_DONE_STATUSES` (`"1"`, `"E1"`, `"E2"`, `"E3"`).
        """
        if assignments is not None:
            tickers = [sym for chunk in assignments for sym in chunk.symbols]
        else:
            tickers = [sym for syms in self.exchange_tickers.values() for sym in syms]

        total_tasks = len(tickers) * len(ALL_INTERVALS)
        if total_tasks == 0:
            return 0, 0, 0.0

        done_tasks = 0
        for sym in tickers:
            for iv in ALL_INTERVALS:
                if self.get_status(sym, iv, run_tag) in DEFINITIVE_DONE_STATUSES:
                    done_tasks += 1

        missing_tasks = max(0, total_tasks - done_tasks)
        missing_rate_pct = round((missing_tasks / total_tasks) * 100.0, 4)
        return missing_tasks, total_tasks, missing_rate_pct

    def to_csv_bytes(self) -> bytes:
        """
        Serialize the full `(N_tickers * 8)` table to UTF-8 CSV bytes with columns:
        `symbol,timeframe,chunk,INITIAL,...`
        """
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        header = ["symbol", "timeframe", "chunk"] + self.run_columns
        writer.writerow(header)

        for ex_name, syms in self.exchange_tickers.items():
            for sym in syms:
                ex_label, c_idx = self.ticker_chunk.get(sym, (ex_name, 1))
                chunk_str = format_chunk_label(ex_label, c_idx)
                for iv in ALL_INTERVALS:
                    cell_dict = self.status_map.get((sym, iv), {})
                    row = [sym, iv, chunk_str] + [
                        cell_dict.get(col, STATUS_NULL) for col in self.run_columns
                    ]
                    writer.writerow(row)

        return buf.getvalue().encode("utf-8")


def load_or_create_node_yearly_log(
    bucket: BucketClient,
    notebook_id: int,
    run_tag: str,
    map_data: list[dict[str, Any]] | dict[str, Any],
    data_dir: Path,
    chunk_size: int = CHUNK_SIZE,
) -> tuple[NodeYearlyLog, list[ChunkAssignment]]:
    """
    Download `log/node_{notebook_id}_{year}.csv` from the HF Bucket (or roll over
    from `year - 1` if starting a new year), reconcile with `data/*.csv` (inserting
    any new futures contracts in-place without disrupting chunks), and ensure
    `run_tag` is present as a column.
    """
    year = resolve_year_from_run_tag(run_tag)
    remote_path = get_node_yearly_log_bucket_path(notebook_id, year)
    raw_bytes = bucket.download_bytes(remote_path, must_exist=False)

    if raw_bytes is not None:
        csv_text = raw_bytes.decode("utf-8", errors="replace")
        node_log = NodeYearlyLog.from_csv_text(
            notebook_id=notebook_id,
            year=year,
            csv_text=csv_text,
            carry_over_only_initial=False,
        )
    else:
        # Check if previous year's CSV exists for automatic yearly rollover
        prev_remote_path = get_node_yearly_log_bucket_path(notebook_id, year - 1)
        prev_bytes = bucket.download_bytes(prev_remote_path, must_exist=False)
        if prev_bytes is not None:
            prev_text = prev_bytes.decode("utf-8", errors="replace")
            node_log = NodeYearlyLog.from_csv_text(
                notebook_id=notebook_id,
                year=year,
                csv_text=prev_text,
                carry_over_only_initial=True,
            )
            print(
                f"[LogManager] Initialized new yearly log {remote_path} from {prev_remote_path}.",
                flush=True,
            )
        else:
            node_log = NodeYearlyLog(notebook_id=notebook_id, year=year)

    node_log.ensure_run_column(run_tag)
    assignments, inserted_cnt = node_log.reconcile_with_assigned_csvs(
        map_data=map_data,
        data_dir=data_dir,
        chunk_size=chunk_size,
    )
    if inserted_cnt > 0:
        print(
            f"[LogManager] Inserted {inserted_cnt} new symbol/contract entry(s) into {remote_path} "
            f"(preserving existing chunk indices).",
            flush=True,
        )

    return node_log, assignments
