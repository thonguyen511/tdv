# TDV — Distributed TradingView Data Pipeline

Modular, two-stage distributed market data pipeline across 20 Kaggle nodes, GitHub Actions event-driven orchestration (`repository_dispatch`), and Hugging Face Storage Buckets (`thonguyen511/temp`).

## Architecture Highlights

1. **Initial vs Weekly Jobs**:
   - **Initial Job (`tdv.phase1_initial`, `INITIAL`)**: Uses the Premium cookie/JWT (`TV_ULTIMATE_SESSIONID` / `TV_ULTIMATE_SESSIONID_SIGN`) when available (both for Phase 1 `1D` + studies and Phase 2 intraday WebSocket Bar Replay), falling back cleanly to Free cookies when no premium cookie is configured yet.
   - **Weekly Job (`tdv.phase2_weekly`, `WEEK_<WWYY>`)**: Uses Free cookies/JWTs only (no premium cookie required after the initial run). Always requests `1,000,000` bars per series; TradingView automatically caps to the JWT limit and returns recent bars that overlap and overwrite-merge in place with the previous week.

2. **Two-Stage Per-Chunk Execution (`2,000` base symbols/chunk, `8` workers/node)**:
   - **Stage 1 (`1D` + Universal Studies + `has_intraday` Filtering)**: Scrapes unadjusted (`adjustment="none"`) `1D` bars alongside `last_splits` (Quote) and all Chart Studies (`Splits`, `Dividends`, `Earnings`, `BarSetContinuousRollDates`) for **every** ticker in the CSV (no asset-type filtering), while capturing `has_intraday` and `is_replayable`.
   - **Stage 2 (`1W`, `1M`, and `has_intraday`-Filtered Intraday)**: Queues `1W` and `1M` for all valid symbols, and queues intraday intervals (`1`, `5`, `15`, `60`, `240`) strictly where `has_intraday == True` (marking daily-only symbols `E3` in the log CSV).

3. **Yearly Per-Node CSV Logging (`log/node_{x}_{yyyy}.csv`)**:
   - Single yearly CSV per node in the HF Bucket (`N_tickers × 8` rows), with schema `symbol,timeframe,chunk,INITIAL,WEEK_4526,...` (`chunk` formatted as `EXCHANGE [1]`, `EXCHANGE [2]`, ...).
   - Status codes: `1` (scanned), `""` (not scanned), `E1` (symbol/series error), `E2` (0 bars/empty), `E3` (intraday skipped), `E4` (transient network/socket error, retried on resume).
   - **Chunk-Preserving Contract Insertion**: Newly added futures contracts for an existing root symbol are inserted immediately after that symbol's existing contracts with the same `chunk` label (`EXCHANGE [N]`), keeping downstream chunks completely stable.

4. **Authenticated JWT Caching & Event-Driven Orchestration**:
   - Persists encrypted per-node JWTs (`control/jwt/node_{0..19}.json.enc`) and orchestrator state (`control/state.json`) in the HF Bucket.
   - Never falls back to `unauthorized_user_token` (exits with `AUTH_FAILED` code `3` if no authenticated JWT with `user_id > 0` can be obtained).
   - Between weekly runs, `tdv.standby_clock` rotates Kaggle-only 11h 30m standby sessions round-robin across accounts (`Account 0 -> Account 1 -> Account 2 -> Account 3 -> Account 0`) with post-push boot verification, automatic multi-account failover, and handoff confirmation until Monday `00:00:00 UTC`, when it dispatches `TRIGGER_WEEKLY`.
