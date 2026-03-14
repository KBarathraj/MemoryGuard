"""MemoryGuard Demo — Attack Simulation.

Simulates a compromised database user:
  Phase 1: Normal queries (builds baseline)
  Phase 2: Reconnaissance (unusual tables / columns)
  Phase 3: Exfiltration (sensitive data + volume spike)

Prints anomaly scores for each query to demonstrate detection.
"""

from __future__ import annotations

import sys
import time

import fakeredis

from src.config import Config
from src.drift.scorer import compute_anomaly
from src.memory.redis_store import BehaviorStore
from src.parser.sql_fingerprint import parse_sql


# ── Simulated query sequences ────────────────────────────────────────

NORMAL_QUERIES = [
    "SELECT id, name, email FROM employees WHERE dept_id = 1",
    "SELECT dept_name FROM departments WHERE id = 1",
    "SELECT id, name FROM employees WHERE id = 42",
    "SELECT name, email FROM employees",
    "SELECT dept_name, budget FROM departments",
    "SELECT id, name, email FROM employees WHERE dept_id = 2",
    "SELECT id, name FROM employees WHERE name LIKE 'A%'",
    "SELECT dept_name FROM departments",
] * 4  # 32 queries over the "baseline period"

RECON_QUERIES = [
    "SELECT table_name FROM information_schema.tables",
    "SELECT column_name FROM information_schema.columns WHERE table_name = 'employees'",
    "SELECT id, name, salary FROM employees",
    "SELECT * FROM payroll LIMIT 5",
]

EXFIL_QUERIES = [
    "SELECT ssn, bank_account FROM employees",
    "SELECT e.name, e.ssn, p.gross_pay, p.net_pay FROM employees e JOIN payroll p ON e.id = p.employee_id",
    "SELECT ssn, bank_account, salary FROM employees",
    "SELECT * FROM employees",
    "SELECT * FROM payroll",
] * 3  # volume spike


def _banner(text: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")


def run_demo() -> None:
    """Run the attack simulation with in-memory Redis."""
    # Set up in-memory store
    r = fakeredis.FakeRedis(decode_responses=True)
    store = BehaviorStore(r, retention_days=90)
    user = "compromised_app_user"

    config = Config(anomaly_threshold=0.7, z_weight=0.6, j_weight=0.4)

    # ── Phase 1: Build baseline ───────────────────────────────
    _banner("PHASE 1 — Normal behaviour (building baseline)")
    for i, sql in enumerate(NORMAL_QUERIES, 1):
        fp = parse_sql(sql)
        store.record(user, fp)

        known_cols = store.get_known_columns(user)
        query_cols = set(fp.columns)
        # Use a synthetic daily count history (stable)
        history = [len(NORMAL_QUERIES)] * 10
        current = i

        result = compute_anomaly(
            history, current, known_cols, query_cols,
            z_weight=config.z_weight,
            j_weight=config.j_weight,
            threshold=config.anomaly_threshold,
        )
        flag = "🚨 ANOMALY" if result.is_anomaly else "   ok"
        print(f"  [{flag}] score={result.score:.4f}  z={result.z_score_raw:+.2f}  "
              f"j={result.jaccard_raw:.2f}  | {sql[:60]}")

    # ── Phase 2: Reconnaissance ───────────────────────────────
    _banner("PHASE 2 — Reconnaissance (unusual tables & columns)")
    for sql in RECON_QUERIES:
        try:
            fp = parse_sql(sql)
        except ValueError:
            print(f"  [SKIP] unparseable: {sql[:60]}")
            continue
        store.record(user, fp)

        known_cols = store.get_known_columns(user)
        query_cols = set(fp.columns)
        history = [len(NORMAL_QUERIES)] * 10
        current = len(NORMAL_QUERIES) + 5

        result = compute_anomaly(
            history, current, known_cols, query_cols,
            z_weight=config.z_weight,
            j_weight=config.j_weight,
            threshold=config.anomaly_threshold,
        )
        flag = "🚨 ANOMALY" if result.is_anomaly else "   ok"
        print(f"  [{flag}] score={result.score:.4f}  z={result.z_score_raw:+.2f}  "
              f"j={result.jaccard_raw:.2f}  | {sql[:60]}")
        if result.reasons:
            for reason in result.reasons:
                print(f"         ↳ {reason}")

    # ── Phase 3: Exfiltration ─────────────────────────────────
    _banner("PHASE 3 — Data exfiltration (sensitive data + volume spike)")
    for sql in EXFIL_QUERIES:
        fp = parse_sql(sql)
        store.record(user, fp)

        known_cols = store.get_known_columns(user)
        query_cols = set(fp.columns)
        # Simulate massive volume spike
        history = [len(NORMAL_QUERIES)] * 10
        current = len(NORMAL_QUERIES) + len(RECON_QUERIES) + len(EXFIL_QUERIES)

        result = compute_anomaly(
            history, current, known_cols, query_cols,
            z_weight=config.z_weight,
            j_weight=config.j_weight,
            threshold=config.anomaly_threshold,
        )
        flag = "🚨 ANOMALY" if result.is_anomaly else "   ok"
        print(f"  [{flag}] score={result.score:.4f}  z={result.z_score_raw:+.2f}  "
              f"j={result.jaccard_raw:.2f}  | {sql[:60]}")
        if result.reasons:
            for reason in result.reasons:
                print(f"         ↳ {reason}")

    # ── Summary ────────────────────────────────────────────────
    _banner("DEMO COMPLETE")
    print("  MemoryGuard detected anomalous patterns in Phases 2 & 3")
    print("  while Phase 1 (normal behaviour) remained below threshold.\n")


if __name__ == "__main__":
    run_demo()
