"""Tests for src.drift — Z-score, Jaccard, and scorer."""

import pytest

from src.drift.z_score import z_score_drift
from src.drift.jaccard import jaccard_similarity, jaccard_novelty
from src.drift.scorer import AnomalyResult, compute_anomaly, _clamp_z


# ── Z-Score ──────────────────────────────────────────────────────────

class TestZScore:

    def test_stable_history_low_score(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        z = z_score_drift(counts, 102)
        assert abs(z) < 1.5

    def test_spike_high_score(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        z = z_score_drift(counts, 1000)
        assert z > 3.0

    def test_insufficient_data_returns_zero(self):
        assert z_score_drift([10, 20, 30], 100) == 0.0

    def test_zero_std_dev_same_value(self):
        counts = [50] * 10
        assert z_score_drift(counts, 50) == 0.0

    def test_zero_std_dev_different_value(self):
        counts = [50] * 10
        assert z_score_drift(counts, 999) == 4.0

    def test_below_mean(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        z = z_score_drift(counts, 0)
        assert z < 0


# ── Jaccard ──────────────────────────────────────────────────────────

class TestJaccard:

    def test_identical_sets_similarity_one(self):
        s = {"a", "b", "c"}
        assert jaccard_similarity(s, s) == 1.0

    def test_disjoint_sets_similarity_zero(self):
        assert jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0

    def test_empty_sets_similarity_one(self):
        assert jaccard_similarity(set(), set()) == 1.0

    def test_novelty_known_columns(self):
        known = {"id", "name", "email"}
        query = {"id", "name"}
        assert jaccard_novelty(known, query) < 0.5

    def test_novelty_all_new(self):
        known = {"id", "name"}
        query = {"ssn", "salary"}
        assert jaccard_novelty(known, query) == 1.0

    def test_novelty_empty_query(self):
        assert jaccard_novelty({"id"}, set()) == 0.0

    def test_partial_overlap(self):
        known = {"a", "b", "c", "d"}
        query = {"c", "d", "e", "f"}
        # J = 2/6 = 0.333…, novelty = 0.666…
        nov = jaccard_novelty(known, query)
        assert 0.6 < nov < 0.7


# ── Scorer ───────────────────────────────────────────────────────────

class TestScorer:

    def test_clamp_z_within_cap(self):
        assert _clamp_z(2.0) == pytest.approx(0.5)

    def test_clamp_z_exceeds_cap(self):
        assert _clamp_z(8.0) == 1.0

    def test_clamp_z_negative(self):
        assert _clamp_z(-3.0) == pytest.approx(0.75)

    def test_normal_behaviour_low_score(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        known = {"id", "name", "email", "dept"}
        query = {"id", "name"}
        result = compute_anomaly(counts, 101, known, query)
        assert result.score < 0.5
        assert result.is_anomaly is False

    def test_full_anomaly_high_score(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        known = {"id", "name"}
        query = {"ssn", "salary", "bank_account"}
        result = compute_anomaly(counts, 1000, known, query)
        assert result.score > 0.7
        assert result.is_anomaly is True
        assert len(result.reasons) > 0

    def test_custom_weights(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        known = {"id"}
        query = {"ssn"}
        r1 = compute_anomaly(counts, 200, known, query, z_weight=1.0, j_weight=0.0)
        r2 = compute_anomaly(counts, 200, known, query, z_weight=0.0, j_weight=1.0)
        # With z_weight=0, score should be purely Jaccard
        assert r2.score == pytest.approx(r2.jaccard_raw, abs=0.01)

    def test_custom_threshold(self):
        counts = [98, 102, 100, 99, 101, 103, 97, 100, 102, 98,
                  101, 99, 100, 103, 97, 100, 101, 99, 102, 98,
                  100, 103, 97, 101, 99, 100, 102, 98, 101, 100]
        known = {"id"}
        query = {"id"}
        result = compute_anomaly(counts, 105, known, query, threshold=0.01)
        # Very low threshold → flags even small deviations
        assert result.is_anomaly is True

    def test_result_dataclass_fields(self):
        counts = [50] * 10
        result = compute_anomaly(counts, 50, {"a"}, {"a"})
        assert isinstance(result, AnomalyResult)
        assert isinstance(result.reasons, list)
