"""Redis-backed behavioral store.

Maintains per-user query profiles with a configurable (default 90-day)
sliding window.  Stores daily query volumes, and sets of known
tables / columns scored by recency.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis

from src.parser.sql_fingerprint import SQLFingerprint


_DAY_SECONDS = 86_400


class BehaviorStore:
    """Per-user behavioral memory backed by Redis.

    Key schema
    ----------
    ``mg:vol:{user}:{YYYY-MM-DD}``  → string (int)   – daily query count
    ``mg:cols:{user}``               → sorted-set      – columns, scored by epoch
    ``mg:tables:{user}``             → sorted-set      – tables,  scored by epoch
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        retention_days: int = 90,
        key_prefix: str = "mg",
    ) -> None:
        self.r = redis_client
        self.retention_days = retention_days
        self.prefix = key_prefix

    # -- key helpers ---------------------------------------------------
    def _vol_key(self, user: str, date_str: str) -> str:
        return f"{self.prefix}:vol:{user}:{date_str}"

    def _cols_key(self, user: str) -> str:
        return f"{self.prefix}:cols:{user}"

    def _tables_key(self, user: str) -> str:
        return f"{self.prefix}:tables:{user}"

    # -- public API ----------------------------------------------------
    def record(
        self,
        user: str,
        fingerprint: SQLFingerprint,
        ts: Optional[datetime] = None,
    ) -> None:
        """Ingest a single query fingerprint for *user*.

        * Increments the daily volume counter.
        * Adds columns and tables to the user's known sets with the
          current timestamp as score (for recency ordering / pruning).
        * Sets TTL on the daily key so Redis auto-evicts after the
          retention window.
        """
        now = ts or datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        epoch = int(now.timestamp())

        pipe = self.r.pipeline(transaction=False)

        # Daily volume
        vol_key = self._vol_key(user, date_str)
        pipe.incr(vol_key)
        pipe.expire(vol_key, self.retention_days * _DAY_SECONDS)

        # Known columns
        if fingerprint.columns:
            cols_key = self._cols_key(user)
            mapping = {col: epoch for col in fingerprint.columns}
            pipe.zadd(cols_key, mapping)  # type: ignore[arg-type]

        # Known tables
        if fingerprint.tables:
            tables_key = self._tables_key(user)
            mapping = {tbl: epoch for tbl in fingerprint.tables}
            pipe.zadd(tables_key, mapping)  # type: ignore[arg-type]

        pipe.execute()

    def get_daily_counts(self, user: str, days: int = 30) -> list[int]:
        """Return the last *days* daily query counts (oldest → newest).

        Missing days are represented as 0.
        """
        today = datetime.now(timezone.utc).date()
        keys = [
            self._vol_key(user, (today - timedelta(days=i)).isoformat())
            for i in range(days - 1, -1, -1)
        ]
        values = self.r.mget(keys)
        return [int(v) if v else 0 for v in values]

    def get_known_columns(self, user: str) -> set[str]:
        """Return the full set of columns this user has historically accessed."""
        members = self.r.zrange(self._cols_key(user), 0, -1)
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    def get_known_tables(self, user: str) -> set[str]:
        """Return the full set of tables this user has historically accessed."""
        members = self.r.zrange(self._tables_key(user), 0, -1)
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    def prune(self, user: str, retention_days: Optional[int] = None) -> int:
        """Remove column/table entries older than the retention window.

        Returns the total number of members removed.
        """
        ret = retention_days or self.retention_days
        cutoff = time.time() - (ret * _DAY_SECONDS)
        removed = 0
        for key in (self._cols_key(user), self._tables_key(user)):
            removed += self.r.zremrangebyscore(key, "-inf", cutoff)
        return removed

    def get_today_count(self, user: str) -> int:
        """Return today's query count for *user*."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        val = self.r.get(self._vol_key(user, date_str))
        return int(val) if val else 0
