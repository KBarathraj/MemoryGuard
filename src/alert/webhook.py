"""Webhook alert dispatcher.

Sends anomaly alerts to Slack incoming webhooks and/or generic SIEM
HTTP endpoints.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from src.drift.scorer import AnomalyResult

logger = logging.getLogger(__name__)


def _build_payload(
    result: AnomalyResult,
    user: str,
    query: str,
    max_query_len: int = 300,
) -> dict[str, Any]:
    """Build a JSON-serialisable alert payload."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "anomaly_score": result.score,
        "is_anomaly": result.is_anomaly,
        "z_score": result.z_score_raw,
        "jaccard_novelty": result.jaccard_raw,
        "reasons": result.reasons,
        "query_snippet": query[:max_query_len],
    }


def _format_slack_blocks(payload: dict[str, Any]) -> dict[str, Any]:
    """Format the payload as a Slack Block Kit message."""
    score = payload["anomaly_score"]
    user = payload["user"]
    reasons = "\n".join(f"• {r}" for r in payload["reasons"]) or "—"

    return {
        "text": f"🚨 MemoryGuard Alert — score {score:.2f} for user `{user}`",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 MemoryGuard Anomaly Detected",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*User:*\n`{user}`"},
                    {"type": "mrkdwn", "text": f"*Score:*\n`{score:.4f}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Z-Score:*\n`{payload['z_score']:.2f}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Jaccard:*\n`{payload['jaccard_novelty']:.2f}`",
                    },
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Reasons:*\n{reasons}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Query:*\n```{payload['query_snippet']}```",
                },
            },
        ],
    }


def send_slack_alert(
    webhook_url: str,
    result: AnomalyResult,
    user: str,
    query: str,
    timeout: float = 5.0,
) -> bool:
    """Post an anomaly alert to a Slack incoming webhook.

    Returns True on success, False on failure (logged, never raises).
    """
    payload = _build_payload(result, user, query)
    body = _format_slack_blocks(payload)
    try:
        resp = requests.post(webhook_url, json=body, timeout=timeout)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Slack alert failed: %s", exc)
        return False


def send_siem_alert(
    endpoint_url: str,
    result: AnomalyResult,
    user: str,
    query: str,
    timeout: float = 5.0,
    headers: Optional[dict[str, str]] = None,
) -> bool:
    """POST a JSON anomaly payload to a generic SIEM endpoint.

    Returns True on success, False on failure (logged, never raises).
    """
    payload = _build_payload(result, user, query)
    try:
        resp = requests.post(
            endpoint_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json", **(headers or {})},
            timeout=timeout,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("SIEM alert failed: %s", exc)
        return False


def send_alert(
    result: AnomalyResult,
    user: str,
    query: str,
    config: dict[str, Any],
) -> bool:
    """Dispatch alert through all configured channels.

    *config* should contain optional keys ``slack_webhook_url`` and/or
    ``siem_webhook_url``.  Returns True if at least one channel
    succeeded.
    """
    sent = False
    slack_url = config.get("slack_webhook_url")
    siem_url = config.get("siem_webhook_url")

    if slack_url:
        sent = send_slack_alert(slack_url, result, user, query) or sent
    if siem_url:
        sent = send_siem_alert(siem_url, result, user, query) or sent

    if not slack_url and not siem_url:
        logger.warning("No alert channels configured — printing to console")
        payload = _build_payload(result, user, query)
        print(f"[ALERT] {json.dumps(payload, indent=2)}")
        sent = True

    return sent
