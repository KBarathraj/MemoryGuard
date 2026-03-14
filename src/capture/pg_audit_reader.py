"""pg_audit CSV log reader.

Reads PostgreSQL pg_audit log entries in CSV format and yields
structured :class:`AuditEntry` records.
"""

from __future__ import annotations

import csv
import io
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional


@dataclass
class AuditEntry:
    """Single pg_audit log record."""

    timestamp: datetime
    user: str
    database: str
    command_tag: str        # SELECT, INSERT, UPDATE, DELETE, …
    statement: str          # raw SQL

    @property
    def is_dml(self) -> bool:
        return self.command_tag.upper() in {"SELECT", "INSERT", "UPDATE", "DELETE"}


# -- Column indices in default pg_audit CSV output -------------------------
# The exact layout depends on pg_audit configuration.  These defaults match
# the common ``pgaudit.log_format = csv`` layout.
_COL_TIMESTAMP = 0
_COL_USER = 1
_COL_DATABASE = 2
_COL_COMMAND_TAG = 4
_COL_STATEMENT = 5


def _parse_row(row: list[str]) -> Optional[AuditEntry]:
    """Parse a single CSV row into an AuditEntry, or None if invalid."""
    try:
        ts = datetime.fromisoformat(row[_COL_TIMESTAMP].strip())
        user = row[_COL_USER].strip()
        database = row[_COL_DATABASE].strip()
        command = row[_COL_COMMAND_TAG].strip()
        stmt = row[_COL_STATEMENT].strip()
        if not stmt:
            return None
        return AuditEntry(
            timestamp=ts,
            user=user,
            database=database,
            command_tag=command,
            statement=stmt,
        )
    except (IndexError, ValueError):
        return None


def read_pg_audit_csv(filepath: str) -> Iterator[AuditEntry]:
    """Read a pg_audit CSV file and yield :class:`AuditEntry` records.

    Silently skips rows that cannot be parsed (comments, headers, or
    malformed lines).
    """
    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for row in reader:
            entry = _parse_row(row)
            if entry is not None:
                yield entry


def read_pg_audit_string(csv_text: str) -> Iterator[AuditEntry]:
    """Parse audit entries from an in-memory CSV string (useful for tests)."""
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        entry = _parse_row(row)
        if entry is not None:
            yield entry


def tail_pg_audit_log(
    filepath: str,
    poll_interval: float = 1.0,
) -> Iterator[AuditEntry]:
    """Tail a pg_audit CSV log file, yielding new entries as they appear.

    This is a blocking iterator that follows the file indefinitely
    (similar to ``tail -f``).
    """
    with open(filepath, newline="", encoding="utf-8") as fh:
        # Seek to end
        fh.seek(0, os.SEEK_END)

        while True:
            line = fh.readline()
            if not line:
                time.sleep(poll_interval)
                continue
            reader = csv.reader(io.StringIO(line))
            for row in reader:
                entry = _parse_row(row)
                if entry is not None:
                    yield entry
