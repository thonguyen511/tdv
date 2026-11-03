# TDV — Distributed TradingView Data Pipeline

Modular, two-phase, two-stage distributed market data pipeline across 20 Kaggle nodes, GitHub Actions event-driven orchestration (`repository_dispatch`), and Hugging Face Storage Buckets (`thonguyen511/temp`).

## Architecture Highlights

1. **Two-Phase Lifecycle**:
   - **Phase 1 (`tdv.phase1_initial`)**: Full historical scan (`INITIAL`) paging backwards via `request_more_data` up to 1,000 batches per series.
   - **Phase 2 (`tdv.phase2_weekly`)**: Weekly incremental maintenance (`WEEK_<WWYY>`) performing in-place encrypted chunk overwrite-merges on the Hugging Face Storage Bucket without git history bloat or `super_squash_history`.

2. **Two-Stage Per-Chunk Execution (`2,000` symbols/chunk, `8` workers/node)**:
   - **Stage 1 (`1D` + Corporate Actions)**: Scrapes unadjusted (`adjustment="none"`) `1D` bars alongside dynamically versioned `Splits`, `Dividends`, `Earnings`, and `BarSetContinuousRollDates` studies, while capturing `has_intraday` and `has_adjustment` from `symbol_resolved`.
   - **Stage 2 (`has_intraday` Filtering)**: Queues `1W` and `1M` for all valid symbols, and queues intraday intervals (`1`, `5`, `15`, `60`, `240`) strictly for symbols with `has_intraday == True`.

3. **Authenticated JWT Caching & Fail-Fast Security**:
   - Persists encrypted per-node JWTs (`control/jwt/node_{0..19}.json.enc`) in the Hugging Face Bucket.
   - Reuses the cached JWT until expiration or server rejection, refreshing via `sessionid` cookies or headless Playwright login only when needed.
   - Never falls back to `unauthorized_user_token` (exits with `AUTH_FAILED` code `3` if no authenticated JWT with `user_id > 0` can be obtained).

4. **Event-Driven GitHub + Kaggle Orchestration (No Cron)**:
   - Orchestrator state lives in `control/state.json` inside the Hugging Face Bucket.
   - Each Kaggle node runs under a `42,300s` (11h 45m) watchdog (`tdv.supervisor`) and dispatches `DONE`, `TIMEOUT`, `RATE_LIMITED`, `AUTH_FAILED`, or `ERROR` to `.github/workflows/fleet_orchestrator.yml`.
   - Between weekly runs, `tdv.standby_clock` chains 11h 45m sessions (`REARM_STANDBY`) until Monday `00:00:00 UTC`, when it dispatches `TRIGGER_WEEKLY`.

