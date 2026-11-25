# TDV — Distributed TradingView Data Pipeline

Modular, two-stage distributed market data pipeline across 20 Kaggle nodes, GitHub Actions event-driven orchestration (`repository_dispatch`), and Hugging Face Storage Buckets (`thonguyen511/temp`).

## Architecture Highlights

1. **Initial vs Weekly Jobs (100% Identical Worker Execution)**:
   - Both **Initial (`tdv.phase1_initial`)** and **Weekly (`tdv.phase2_weekly`)** run with identical worker configurations (`fetch_full_history=True`, `1,000,000` initial & `request_more_data` bars, universal `1D` chart studies + quote `last_splits`, no ticker or study skipping, and duplicate `(symbol, time)` rows deduplicated during the final chunk merge step), differing strictly in:
     1. **Cookie Selection**: `(8, flex)` for Initial (`5` Ultimate + `3` Free = `8` workers when `TV_ULTIMATE_SESSIONID` is verified Ultimate, flexing to `8` Free workers when unset) vs `(8, free)` for Weekly (`8` Free workers using the node's Free JWT).
     2. **Log Column**: `INITIAL` vs `WEEK_<WWYY>` in `log/node_{x}_{yyyy}.csv`.

2. **Two-Stage Per-Chunk Execution (`2,000` base symbols/chunk, `8` workers/node)**:
   - **Stage 1 (`1D` + Universal Studies + `has_intraday` Filtering)**: Scrapes unadjusted (`adjustment="none"`) `1D` bars alongside `last_splits` (Quote) and all Chart Studies (`Splits`, `Dividends`, `Earnings`, `BarSetContinuousRollDates`) for **every** ticker in the CSV (no asset-type filtering), while capturing `has_intraday` and `is_replayable`.
   - **Stage 2 (`1W`, `1M`, and `has_intraday`-Filtered Intraday)**: Queues `1W` and `1M` for all valid symbols, and queues intraday intervals (`1`, `5`, `15`, `60`, `240`) strictly where `has_intraday == True` (marking daily-only symbols `E3` in the log CSV).

3. **Yearly Per-Node CSV Logging (`log/node_{x}_{yyyy}.csv`)**:
   - Single yearly CSV per node in the HF Bucket (`N_tickers × 8` rows) tracking per-symbol, per-timeframe scan status across `INITIAL` and every weekly `WEEK_<WWYY>` run, with chunk-preserving futures contract insertion (see [Log Table Specification](#log-table-specification-lognode_x_yyyycsv) below).

4. **Authenticated JWT Caching & Event-Driven Orchestration**:
   - Persists encrypted per-node JWTs (`control/jwt/node_{0..19}.json.enc`) and orchestrator state (`control/state.json`) in the HF Bucket.
   - Never falls back to `unauthorized_user_token` (exits with `AUTH_FAILED` code `3` if no authenticated JWT with `user_id > 0` can be obtained).
   - Between weekly runs, `tdv.standby_clock` rotates Kaggle-only 11h 30m standby sessions round-robin across accounts (`Account 0 -> Account 1 -> Account 2 -> Account 3 -> Account 0`) with post-push boot verification, automatic multi-account failover, and handoff confirmation until Monday `00:00:00 UTC`, when it dispatches `TRIGGER_WEEKLY`.

---

## Log Table Specification (`log/node_{x}_{yyyy}.csv`)

Each of the 20 Kaggle nodes (`x = 0..19`) maintains one authoritative yearly CSV log in the Hugging Face Bucket at:

```text
log/node_{x}_{yyyy}.csv
```

- **Row granularity**: `N_tickers × 8` rows per node — one row for each `(symbol, timeframe)` pair across the 8 canonical timeframes (`1D`, `1W`, `1M`, `1`, `5`, `15`, `60`, `240`).
- **Atomic sync**: Uploaded together with each completed chunk's encrypted `.parquet.enc` files (`data/{EXCHANGE}/{TIMEFRAME}/{EXCHANGE}_{TIMEFRAME}_chunk_{N}.parquet.enc`).

### Columns

| Column | Format / Example | Description |
| :--- | :--- | :--- |
| `symbol` | `EXCHANGE:TICKER` (e.g. `NASDAQ:NVDA`, `CME_MINI:ES1!`, `INDEX:BTCUSD`) | Full TradingView symbol assigned to this node from `map.json` and `data/{EXCHANGE}.csv`. |
| `timeframe` | `1D`, `1W`, `1M`, `1`, `5`, `15`, `60`, `240` | Bar interval (`1D` Daily, `1W` Weekly, `1M` Monthly; `1`, `5`, `15`, `60`, `240` = 1m, 5m, 15m, 1H, 4H intraday). |
| `chunk` | `EXCHANGE [N]` (e.g. `ABAXX [1]`, `NASDAQ [6]`) | Authoritative `1`-indexed Parquet chunk label (base `2,000` symbols/chunk) mapping directly to `data/{EXCHANGE}/{TIMEFRAME}/{EXCHANGE}_{TIMEFRAME}_chunk_{N}.parquet.enc`. |
| `INITIAL` | Status code (`1`, `""`, `E1`–`E5`) | Result of the initial full-history scan (`tdv.phase1_initial`). Always kept as the first run column, including across yearly rollovers. |
| `WEEK_<WWYY>` | Status code (e.g. `WEEK_4526`, `WEEK_4626`) | Dynamically appended column for each weekly maintenance run (`%W` UTC week number + `%y` 2-digit year, e.g. UTC week 45 of 2026 → `WEEK_4526`). |

### Status Codes

| Code | Config Constant | Meaning | Counted as Done? | Resume / Retry Behavior |
| :---: | :--- | :--- | :---: | :--- |
| `1` | `STATUS_OK` | Scanned and saved (`> 0` bars merged & encrypted into Parquet chunk). | Yes | Skipped on resume for the same run column. |
| *(empty)* | `STATUS_NULL` | Not scanned yet (empty cell / `null`). | No | Queued for scanning. If a symbol's `1D` row is incomplete, all 8 rows for that symbol in the active run column are reset to empty so Stage 1 (`1D`) always executes before Stage 2. |
| `E1` | `STATUS_ERR_SYMBOL` | Definitive TradingView `symbol_error` or `series_error` (invalid, delisted, or restricted symbol/resolution). | Yes | Skipped on resume. When `1D` fails with `E1`, all 8 timeframe rows for that symbol are marked `E1`. |
| `E2` | `STATUS_ERR_EMPTY` | Definitive empty series (`series_completed` with `0` bars returned). | Yes | Skipped on resume. When `1D` fails with `E2`, all 8 timeframe rows for that symbol are marked `E2`. |
| `E3` | `STATUS_SKIPPED_NO_INTRADAY` | Intraday timeframe (`1`, `5`, `15`, `60`, `240`) skipped because Stage 1 (`1D`) reported `has_intraday=False`. | Yes | Skipped on resume; also remembered as `known_intraday=False` when resuming Stage 2. |
| `E4` | `STATUS_ERR_TRANSIENT` | Transient network, WebSocket, timeout, or auth error after exhausting in-session retries (`MAX_TASK_RETRIES = 6`). | No | Automatically retried on the next continuation/resume run. |
| `E5` | `STATUS_ERR_STUDY` | Reserved code for chart study errors (defined in `tdv/config.py`). | No | Reserved (not part of `DEFINITIVE_DONE_STATUSES`). |

### Example Rows

```csv
symbol,timeframe,chunk,INITIAL,WEEK_4526
NASDAQ:NVDA,1D,NASDAQ [6],1,1
NASDAQ:NVDA,1W,NASDAQ [6],1,1
NASDAQ:NVDA,1M,NASDAQ [6],1,1
NASDAQ:NVDA,1,NASDAQ [6],1,1
NASDAQ:NVDA,5,NASDAQ [6],1,1
NASDAQ:NVDA,15,NASDAQ [6],1,1
NASDAQ:NVDA,60,NASDAQ [6],1,1
NASDAQ:NVDA,240,NASDAQ [6],1,1
INDEX:DAILYONLY,1D,INDEX [1],1,1
INDEX:DAILYONLY,1W,INDEX [1],1,1
INDEX:DAILYONLY,1M,INDEX [1],1,1
INDEX:DAILYONLY,1,INDEX [1],E3,E3
INDEX:DAILYONLY,5,INDEX [1],E3,E3
INDEX:DAILYONLY,15,INDEX [1],E3,E3
INDEX:DAILYONLY,60,INDEX [1],E3,E3
INDEX:DAILYONLY,240,INDEX [1],E3,E3
EXCHANGE:BADSYM,1D,EXCHANGE [1],E1,E1
```

### Lifecycle & Integrity Rules

- **Completion Gate (`2.0%` Missing Threshold)**: A task `(symbol, timeframe)` is considered complete iff its cell is in `DEFINITIVE_DONE_STATUSES` (`1`, `E1`, `E2`, `E3`). Empty cells (`""`) and transient errors (`E4`) count toward `missing_rate_pct`. If `missing_rate_pct > 2.0%` (`MAX_ALLOWED_MISSING_RATE_PCT`), the node exits with `EXIT_ERROR` (`1`) so `fleet_manager.py` triggers a continuation session.
- **Chunk-Preserving Futures Insertion**: When new futures contracts appear for an existing root symbol in `data/{EXCHANGE}.csv`, their 8 timeframe rows are inserted immediately after that root symbol's last existing contract with the exact same `chunk` label (`EXCHANGE [N]`). That chunk may grow past `2,000` symbols without shifting any downstream tickers across chunk boundaries. Brand-new root symbols are appended at the end of the exchange's chunks.
- **Yearly Rollover**: On the first run of a new UTC year (`yyyy`), `tdv.log_manager` initializes `log/node_{x}_{yyyy}.csv` from `log/node_{x}_{yyyy-1}.csv`, carrying over symbol ordering, `chunk` labels, and the `INITIAL` column while starting fresh `WEEK_<WWYY>` columns for the new year.
