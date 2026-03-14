"""Central configuration.

Reads settings from environment variables with sensible defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    """MemoryGuard runtime configuration."""

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Capture
    pg_audit_log: str = "/var/log/pgaudit/audit.csv"

    # Scoring
    anomaly_threshold: float = 0.7
    z_weight: float = 0.6
    j_weight: float = 0.4

    # Behavioral window
    retention_days: int = 90
    history_days: int = 30

    # Alert channels
    slack_webhook_url: str = ""
    siem_webhook_url: str = ""

    @classmethod
    def from_env(cls) -> "Config":
        """Build a Config from environment variables."""
        return cls(
            redis_url=os.getenv("REDIS_URL", cls.redis_url),
            pg_audit_log=os.getenv("PG_AUDIT_LOG", cls.pg_audit_log),
            anomaly_threshold=float(
                os.getenv("ANOMALY_THRESHOLD", str(cls.anomaly_threshold))
            ),
            z_weight=float(os.getenv("Z_WEIGHT", str(cls.z_weight))),
            j_weight=float(os.getenv("J_WEIGHT", str(cls.j_weight))),
            retention_days=int(
                os.getenv("RETENTION_DAYS", str(cls.retention_days))
            ),
            history_days=int(
                os.getenv("HISTORY_DAYS", str(cls.history_days))
            ),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
            siem_webhook_url=os.getenv("SIEM_WEBHOOK_URL", ""),
        )

    @property
    def alert_config(self) -> dict:
        """Dict suitable for :func:`alert.send_alert`."""
        return {
            "slack_webhook_url": self.slack_webhook_url,
            "siem_webhook_url": self.siem_webhook_url,
        }
