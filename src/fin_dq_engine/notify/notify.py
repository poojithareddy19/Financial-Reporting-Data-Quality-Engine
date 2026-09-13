"""Stage 6: distribute the summary. SNS in AWS mode; local mode prints and writes ./out. Slack blocks optional."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import boto3

from fin_dq_engine.config import Settings
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class NotifyResult:
    """Where the notification went."""

    channel: str
    subject: str
    message_id: str | None
    local_path: Path | None


def build_subject(run_date: date, status: str = "SUCCESS") -> str:
    """SNS subject line (max 100 chars)."""
    return f"[fin-dq] {status}: daily financial summary {run_date.isoformat()}"[:100]


def markdown_to_slack_blocks(markdown_text: str, max_blocks: int = 45) -> list[dict[str, Any]]:
    """Alternative renderer: convert the Markdown summary to Slack Block Kit sections.

    Headers become bold section text, tables become preformatted blocks, other paragraphs are passed through.
    """
    blocks: list[dict[str, Any]] = []
    table: list[str] = []

    def flush_table() -> None:
        if table:
            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": "```" + "\n".join(table)[:2900] + "```"}}
            )
            table.clear()

    for line in markdown_text.splitlines():
        if line.startswith("|"):
            if not set(line.replace("|", "").strip()) <= {"-", " "}:
                table.append(" | ".join(c.strip() for c in line.strip().strip("|").split("|")))
            continue
        flush_table()
        if line.startswith("#"):
            blocks.append({"type": "header", "text": {"type": "plain_text", "text": line.lstrip("# ").strip()[:150]}})
        elif line.strip() == "---":
            blocks.append({"type": "divider"})
        elif line.strip():
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": line.replace("**", "*")[:2900]}})
    flush_table()
    return blocks[:max_blocks]


def send_summary(
    settings: Settings, run_date: date, markdown_text: str, *, status: str = "SUCCESS", client: Any | None = None
) -> NotifyResult:
    """Send the Markdown body through SNS, or print + write to ./out in local mode."""
    subject = build_subject(run_date, status)
    out_dir = settings.paths.out_dir / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    slack_path = out_dir / "notification.slack.json"
    slack_path.write_text(json.dumps({"blocks": markdown_to_slack_blocks(markdown_text)}, indent=2))

    if settings.is_local and client is None:
        path = out_dir / "notification.txt"
        path.write_text(f"Subject: {subject}\n\n{markdown_text}", encoding="utf-8")
        print(f"\n===== {subject} =====\n{markdown_text}")
        log.info("notification_written", path=str(path))
        return NotifyResult("local", subject, None, path)

    sns = client or boto3.client("sns", region_name=settings.aws.region)
    resp = sns.publish(TopicArn=settings.aws.sns_topic_arn, Subject=subject, Message=markdown_text[:250_000])
    log.info("notification_sent", topic=settings.aws.sns_topic_arn, message_id=resp.get("MessageId"))
    return NotifyResult("sns", subject, resp.get("MessageId"), None)


def send_alert(settings: Settings, run_date: date, title: str, body: str, *, client: Any | None = None) -> NotifyResult:
    """Critical alert path used when a batch aborts or a stage fails."""
    return send_summary(settings, run_date, f"# {title}\n\n{body}", status="CRITICAL", client=client)
