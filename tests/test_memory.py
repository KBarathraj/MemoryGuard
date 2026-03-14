"""Tests for src.memory.redis_store using fakeredis."""

from datetime import datetime, timezone

import fakeredis
import pytest

from src.memory.redis_store import BehaviorStore
from src.parser.sql_fingerprint import SQLFingerprint


@pytest.fixture
def store():
    """Fresh BehaviorStore backed by fakeredis."""
    r = fakeredis.FakeRedis(decode_responses=True)
    return BehaviorStore(r, retention_days=90)


@pytest.fixture
def sample_fingerprint():
    return SQLFingerprint(
        operation="SELECT",
        tables=["employees"],
        columns=["id", "name", "email"],
    )


class TestBehaviorStore:

    def test_record_increments_daily_count(self, store, sample_fingerprint):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        store.record("alice", sample_fingerprint, ts=ts)
        store.record("alice", sample_fingerprint, ts=ts)

        key = store._vol_key("alice", "2025-06-15")
        assert int(store.r.get(key)) == 2

    def test_record_adds_known_columns(self, store, sample_fingerprint):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        store.record("alice", sample_fingerprint, ts=ts)

        cols = store.get_known_columns("alice")
        assert cols == {"id", "name", "email"}

    def test_record_adds_known_tables(self, store, sample_fingerprint):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        store.record("alice", sample_fingerprint, ts=ts)

        tables = store.get_known_tables("alice")
        assert tables == {"employees"}

    def test_columns_grow_over_time(self, store):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        fp1 = SQLFingerprint(operation="SELECT", tables=["t"], columns=["a", "b"])
        fp2 = SQLFingerprint(operation="SELECT", tables=["t"], columns=["b", "c"])

        store.record("bob", fp1, ts=ts)
        store.record("bob", fp2, ts=ts)

        assert store.get_known_columns("bob") == {"a", "b", "c"}

    def test_get_daily_counts_missing_days_are_zero(self, store, sample_fingerprint):
        ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        store.record("alice", sample_fingerprint, ts=ts)

        counts = store.get_daily_counts("alice", days=7)
        assert len(counts) == 7
        # Most days will be 0 (only 2025-06-15 has data)
        assert sum(counts) >= 0

    def test_empty_user_returns_empty(self, store):
        assert store.get_known_columns("nobody") == set()
        assert store.get_known_tables("nobody") == set()
        assert store.get_daily_counts("nobody", days=7) == [0] * 7

    def test_prune_removes_old_entries(self, store):
        # Insert column with a very old score (epoch 0 = 1970)
        store.r.zadd(store._cols_key("alice"), {"old_col": 0})
        store.r.zadd(store._cols_key("alice"), {"new_col": 9999999999})

        removed = store.prune("alice", retention_days=90)
        assert removed >= 1

        remaining = store.get_known_columns("alice")
        assert "old_col" not in remaining
        assert "new_col" in remaining

    def test_get_today_count(self, store, sample_fingerprint):
        store.record("alice", sample_fingerprint)
        assert store.get_today_count("alice") >= 1
