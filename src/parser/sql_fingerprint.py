"""SQL fingerprinting via SQLGlot AST.

Parses raw SQL and extracts a structural fingerprint: operation type,
tables accessed, and columns referenced — stripping all literal values.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class SQLFingerprint:
    """Structural fingerprint of a single SQL statement."""

    operation: str                          # SELECT, INSERT, UPDATE, DELETE, …
    tables: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    fingerprint_hash: str = ""

    def __post_init__(self) -> None:
        if not self.fingerprint_hash:
            self.fingerprint_hash = self._compute_hash()

    # ------------------------------------------------------------------
    def _compute_hash(self) -> str:
        """Deterministic hash of (operation + sorted tables + sorted columns)."""
        parts = [
            self.operation.upper(),
            ",".join(sorted(self.tables)),
            ",".join(sorted(self.columns)),
        ]
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _extract_tables(tree: exp.Expression) -> list[str]:
    """Pull every table reference from the AST."""
    tables: list[str] = []
    for tbl in tree.find_all(exp.Table):
        name = tbl.name
        if name:
            tables.append(name.lower())
    return sorted(set(tables))


def _extract_columns(tree: exp.Expression) -> list[str]:
    """Pull every column reference from the AST.

    Handles both regular Column nodes (SELECT/UPDATE/DELETE) and
    INSERT column-list identifiers (Schema → Identifier).
    """
    columns: list[str] = []

    # Standard column references (SELECT, WHERE, UPDATE SET, etc.)
    for col in tree.find_all(exp.Column):
        name = col.name
        if name:
            columns.append(name.lower())

    # INSERT column lists: INSERT INTO t (col1, col2) VALUES …
    # SQLGlot represents the column list as Identifier nodes inside Schema.
    for schema in tree.find_all(exp.Schema):
        for ident in schema.expressions:
            if isinstance(ident, exp.Identifier) and ident.name:
                columns.append(ident.name.lower())

    return sorted(set(columns))


def _detect_operation(tree: exp.Expression) -> str:
    """Determine the top-level DML operation."""
    op_map: list[tuple[type, str]] = [
        (exp.Select, "SELECT"),
        (exp.Insert, "INSERT"),
        (exp.Update, "UPDATE"),
        (exp.Delete, "DELETE"),
    ]
    for cls, label in op_map:
        if isinstance(tree, cls) or tree.find(cls):
            return label
    return "OTHER"


def parse_sql(sql: str, dialect: str = "postgres") -> SQLFingerprint:
    """Parse a SQL string and return its structural fingerprint.

    Parameters
    ----------
    sql:
        Raw SQL statement.
    dialect:
        SQLGlot dialect for parsing (default ``"postgres"``).

    Returns
    -------
    SQLFingerprint

    Raises
    ------
    ValueError
        When *sql* cannot be parsed at all.
    """
    try:
        trees = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ErrorLevel:
        raise ValueError(f"Cannot parse SQL: {sql!r}")

    if not trees or trees[0] is None:
        raise ValueError(f"Cannot parse SQL: {sql!r}")

    tree = trees[0]
    operation = _detect_operation(tree)
    tables = _extract_tables(tree)
    columns = _extract_columns(tree)

    return SQLFingerprint(
        operation=operation,
        tables=tables,
        columns=columns,
    )
