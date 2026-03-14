# MemoryGuard MVP Status Report

## 1. Executive Summary
The MemoryGuard Minimum Viable Product (MVP) has been successfully implemented and verified. The system provides real-time, behavioral anomaly detection for PostgreSQL queries using structural analysis and sliding-window baselines.

The core architecture uses **SQLGlot** for AST parsing, **Redis** for historical behavioral storage, and a dual-engine scoring system (Z-Score + Jaccard Index) to calculate anomaly probabilities.

**Repository:** [https://github.com/KBarathraj/MemoryGuard](https://github.com/KBarathraj/MemoryGuard)

---

## 2. Completed Milestones

### Core Engine (`src/`)
- **Parser (`sql_fingerprint.py`)**: Implemented robust SQL AST parsing. It strips literal values to understand structural intent and extracts normalized operation types, referenced tables, and accessed columns. Supports complex constructs like `JOIN`s and nested subqueries.
- **Behavioral Store (`redis_store.py`)**: Designed a fast Redis-backed memory store. Maintains 30-day daily query volumes and tracks the full set of known tables/columns per user with a 90-day rolling TTL.
- **Drift Engine (`z_score.py`, `jaccard.py`, `scorer.py`)**: 
  - **Volume Drift**: Uses population standard deviation (Z-score) against rolling history to detect query volume spikes.
  - **Column Novelty**: Calculates the Jaccard distance between the current query's columns and the user's historical baseline.
  - **Weighted Scorer**: Synthesizes the signals (default 60% Z-score, 40% Jaccard) into a normalized `0.0–1.0` anomaly score, flagging events above a configurable threshold.
- **Capture & Alerting (`capture/`, `alert/`)**: Implemented parsing for `pg_audit` CSV logs (batch and tail-following modes) and a flexible webhook mechanism supporting Slack (Block Kit) and SIEM HTTP POST endpoints.

### Orchestration & Demo
- Structured a complete `docker-compose.yml` to spin up PostgreSQL (with `pg_audit` extensions loaded), Redis, and the MemoryGuard pipeline.
- Delivered a Python attack simulation (`demo/attack.py`) that steps through normal baseline queries, reconnaissance behavior, and sensitive data exfiltration to empirically demonstrate the scoring engine firing alerts correctly.

### Testing
- Achieved **100% pass rate** across 38 unit tests using `pytest` and `fakeredis`.

---

## 3. Issues Encountered & Resolved during Development
1. **Zero-Variance History Edge Case**: 
   - *Issue*: When testing stable histories (e.g., exactly 100 queries every day), the standard deviation ($\sigma$) was exactly `0.0`, resulting in division-by-zero errors in the Z-score calculation.
   - *Fix*: Implemented a smart sentinel mechanism. If $\sigma=0$, deviations structurally default to a $\pm 4.0$ Z-score while preserving the correct directionality (+4.0 for spikes, -4.0 for drops).
2. **SQLGlot `INSERT` Column Abstraction**:
   - *Issue*: SQLGlot does not classify the structural column definitions in an `INSERT INTO (...)` query as standard `Column` AST nodes. They are represented as `Identifier` nodes inside `Schema` expressions.
   - *Fix*: Augmented the AST walker to locate `Schema` nodes and explicitly extract internal `Identifier` strings, guaranteeing column tracking works across read and write operations.
3. **Repository Merge Conflict**:
   - *Issue*: Pushing the codebase triggered a conflict because the remote GitHub repo contained an unmerged `README.md`.
   - *Fix*: Executed local merge resolution, enforcing the local MemoryGuard architectural `README.md` as the source of truth, and pushed the complete tree to the `main` branch.

---

## 4. Known Limitations & Next Steps
- **Storage Bottlenecks at High Scale**: The current logging design creates independent sorted-set entries per column/table per user. At a scale of tens of thousands of users and schemas, Redis memory consumption may balloon. *Next step: Investigate probabilistic data structures like Bloom Filters for known-column sets.*
- **`pg_audit` Formatting Fragility**: The capture module assumes `pg_audit` is emitting strictly formatted CSVs matching the default column indices. *Next step: Adopt robust CSV dict-reading based on headers, or migrate to native JSON log ingestion if available.*
- **Blind spots to literal modifications**: The parser purposefully strips `VALUES` and `literal` variables to fingerprint structure. This makes MemoryGuard blind to brute-force credential stuffing or data-manipulation queries that execute identically structured queries with rapidly rotating literal parameters. This is by design, but should be noted.
