"""Jaccard column-novelty scorer.

Measures how *new* the columns in a query are relative to the user's
historical baseline.  A score of 1.0 means every column is novel;
0.0 means all columns are already known.
"""

from __future__ import annotations


def jaccard_similarity(set_a: set[str], set_b: set[str]) -> float:
    """Classic Jaccard index: |A ∩ B| / |A ∪ B|.

    Returns 1.0 when both sets are empty (vacuous truth — nothing is
    novel when there is nothing to compare).
    """
    if not set_a and not set_b:
        return 1.0

    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union


def jaccard_novelty(known_columns: set[str], query_columns: set[str]) -> float:
    """Return the *novelty* of *query_columns* against the baseline.

    novelty = 1 − J(known, query)

    Parameters
    ----------
    known_columns:
        The set of columns the user has historically accessed.
    query_columns:
        The set of columns in the current query.

    Returns
    -------
    float
        0.0 (fully known) → 1.0 (entirely novel).
        Returns 0.0 when *query_columns* is empty.
    """
    if not query_columns:
        return 0.0

    return 1.0 - jaccard_similarity(known_columns, query_columns)
