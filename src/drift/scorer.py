"""Weighted anomaly scorer.

Combines Z-score volume drift and Jaccard column novelty into a single
0–1 anomaly score and decides whether to flag the event.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .jaccard import jaccard_novelty
from .z_score import z_score_drift


@dataclass
class AnomalyResult:
    """Outcome of the anomaly scoring pipeline."""

    score: float                       # 0.0–1.0 combined score
    z_score_raw: float                 # raw Z-score from volume drift
    jaccard_raw: float                 # raw column novelty  (0–1)
    is_anomaly: bool                   # True when score ≥ threshold
    reasons: list[str] = field(default_factory=list)


def _clamp_z(z: float, cap: float = 4.0) -> float:
    """Normalise a raw Z-score into the [0, 1] range.

    ``min(|z| / cap, 1.0)``

    A *cap* of 4.0 means any Z ≥ 4 saturates at 1.0.
    """
    return min(abs(z) / cap, 1.0)


def compute_anomaly(
    daily_counts: list[int],
    current_count: int,
    known_columns: set[str],
    query_columns: set[str],
    *,
    z_weight: float = 0.6,
    j_weight: float = 0.4,
    threshold: float = 0.7,
) -> AnomalyResult:
    """Run the full anomaly scoring pipeline.

    Parameters
    ----------
    daily_counts / current_count:
        Passed to :func:`z_score_drift`.
    known_columns / query_columns:
        Passed to :func:`jaccard_novelty`.
    z_weight / j_weight:
        Relative weights for the two sub-scores.  They are normalised
        internally so they don't need to sum to 1.
    threshold:
        Minimum combined score to flag as anomaly.

    Returns
    -------
    AnomalyResult
    """
    z_raw = z_score_drift(daily_counts, current_count)
    j_raw = jaccard_novelty(known_columns, query_columns)

    z_norm = _clamp_z(z_raw)

    # Normalise weights so they always sum to 1
    total_weight = z_weight + j_weight
    w_z = z_weight / total_weight
    w_j = j_weight / total_weight

    combined = w_z * z_norm + w_j * j_raw

    # Build human-readable reasons
    reasons: list[str] = []
    if z_norm >= 0.5:
        reasons.append(
            f"Volume spike: Z-score {z_raw:+.2f} "
            f"(normalised {z_norm:.2f})"
        )
    if j_raw >= 0.5:
        novel = query_columns - known_columns
        reasons.append(
            f"Column novelty: {j_raw:.2f} — "
            f"new columns: {sorted(novel)}"
        )

    return AnomalyResult(
        score=round(combined, 4),
        z_score_raw=round(z_raw, 4),
        jaccard_raw=round(j_raw, 4),
        is_anomaly=combined >= threshold,
        reasons=reasons,
    )
