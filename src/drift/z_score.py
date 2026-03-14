"""Z-score volume drift detector.

Compares today's query count against a user's rolling daily history
and returns a standard-score indicating how unusual the volume is.
"""

from __future__ import annotations

import math


def z_score_drift(daily_counts: list[int], current_count: int) -> float:
    """Compute Z-score of *current_count* relative to *daily_counts*.

    Parameters
    ----------
    daily_counts:
        Historical daily query counts (oldest → newest).  Must contain
        at least 7 entries for a meaningful baseline; otherwise **0.0**
        is returned (insufficient data).
    current_count:
        Today's observed count.

    Returns
    -------
    float
        The Z-score.  Positive values mean *above* the mean; negative
        values mean *below*.  Returns 0.0 when standard deviation is
        zero or when history is too short.
    """
    min_history = 7
    if len(daily_counts) < min_history:
        return 0.0

    n = len(daily_counts)
    mean = sum(daily_counts) / n
    variance = sum((x - mean) ** 2 for x in daily_counts) / n
    std_dev = math.sqrt(variance)

    if std_dev == 0.0:
        # All historical values identical – any deviation is technically
        # infinite, but we clamp to a safe sentinel.
        if current_count == mean:
            return 0.0
        return 4.0 if current_count > mean else -4.0

    return (current_count - mean) / std_dev
