"""MemoryGuard pipeline.

Main processing loop:  capture → parse → record → score → alert.
"""

from __future__ import annotations

import logging
from typing import Iterator

import redis

from src.alert.webhook import send_alert
from src.capture.pg_audit_reader import AuditEntry, read_pg_audit_csv, tail_pg_audit_log
from src.config import Config
from src.drift.scorer import AnomalyResult, compute_anomaly
from src.memory.redis_store import BehaviorStore
from src.parser.sql_fingerprint import parse_sql

logger = logging.getLogger("memoryguard")


def _process_entry(
    entry: AuditEntry,
    store: BehaviorStore,
    config: Config,
) -> AnomalyResult | None:
    """Process a single audit entry through the full pipeline.

    Returns the :class:`AnomalyResult` if scoring succeeded, else None.
    """
    # 1. Parse SQL
    try:
        fingerprint = parse_sql(entry.statement)
    except ValueError:
        logger.debug("Skipping unparseable SQL: %.80s…", entry.statement)
        return None

    # 2. Record in behavioral store
    store.record(entry.user, fingerprint, ts=entry.timestamp)

    # 3. Gather baseline data for scoring
    daily_counts = store.get_daily_counts(entry.user, days=config.history_days)
    current_count = store.get_today_count(entry.user)
    known_columns = store.get_known_columns(entry.user)
    query_columns = set(fingerprint.columns)

    # 4. Score
    result = compute_anomaly(
        daily_counts=daily_counts,
        current_count=current_count,
        known_columns=known_columns,
        query_columns=query_columns,
        z_weight=config.z_weight,
        j_weight=config.j_weight,
        threshold=config.anomaly_threshold,
    )

    # 5. Alert if anomaly
    if result.is_anomaly:
        logger.warning(
            "ANOMALY user=%s score=%.4f reasons=%s",
            entry.user,
            result.score,
            result.reasons,
        )
        send_alert(result, entry.user, entry.statement, config.alert_config)

    return result


def run_batch(config: Config | None = None) -> list[AnomalyResult]:
    """Process an entire pg_audit CSV file (batch mode).

    Returns a list of all scoring results.
    """
    cfg = config or Config.from_env()
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    store = BehaviorStore(r, retention_days=cfg.retention_days)

    results: list[AnomalyResult] = []
    for entry in read_pg_audit_csv(cfg.pg_audit_log):
        if not entry.is_dml:
            continue
        result = _process_entry(entry, store, cfg)
        if result:
            results.append(result)

    return results


def run_tail(config: Config | None = None) -> Iterator[AnomalyResult]:
    """Tail the pg_audit log and yield results as they arrive.

    This is a blocking iterator (like ``tail -f``).
    """
    cfg = config or Config.from_env()
    r = redis.Redis.from_url(cfg.redis_url, decode_responses=True)
    store = BehaviorStore(r, retention_days=cfg.retention_days)

    for entry in tail_pg_audit_log(cfg.pg_audit_log):
        if not entry.is_dml:
            continue
        result = _process_entry(entry, store, cfg)
        if result:
            yield result


def main() -> None:
    """CLI entry point — run the pipeline in tail mode."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    cfg = Config.from_env()
    logger.info("MemoryGuard starting — threshold=%.2f", cfg.anomaly_threshold)
    logger.info("Tailing %s …", cfg.pg_audit_log)

    for result in run_tail(cfg):
        if not result.is_anomaly:
            logger.info("OK score=%.4f", result.score)


if __name__ == "__main__":
    main()
