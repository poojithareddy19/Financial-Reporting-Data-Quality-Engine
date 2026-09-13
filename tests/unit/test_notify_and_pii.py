from __future__ import annotations

from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from moto import mock_aws

from fin_dq_engine.config import Settings
from fin_dq_engine.governance.pii import mask_frame, mask_value
from fin_dq_engine.notify.notify import build_subject, markdown_to_slack_blocks, send_alert, send_summary

MD = "# Title\n\nSome **bold** text\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n---\n\n## Tech\nmore\n"


def test_slack_blocks() -> None:
    blocks = markdown_to_slack_blocks(MD)
    types = [b["type"] for b in blocks]
    assert types[0] == "header" and "divider" in types
    table_block = next(b for b in blocks if b["type"] == "section" and b["text"]["text"].startswith("```"))
    assert "a | b" in table_block["text"]["text"]


def test_local_notify_writes_out(tmp_path: Path, capsys: object) -> None:
    s = Settings(paths={"out_dir": str(tmp_path)})  # type: ignore[arg-type]
    r = send_summary(s, date(2025, 1, 2), MD)
    assert r.channel == "local" and r.local_path is not None and r.local_path.exists()
    assert (tmp_path / "2025-01-02" / "notification.slack.json").exists()
    assert build_subject(date(2025, 1, 2)) == "[fin-dq] SUCCESS: daily financial summary 2025-01-02"


@mock_aws
def test_sns_notify(tmp_path: Path) -> None:
    sns = boto3.client("sns", region_name="us-east-1")
    arn = sns.create_topic(Name="fin-dq-alerts")["TopicArn"]
    s = Settings(environment="aws", paths={"out_dir": str(tmp_path)}, aws={"sns_topic_arn": arn})  # type: ignore[arg-type]
    r = send_summary(s, date(2025, 1, 2), MD, client=sns)
    assert r.channel == "sns" and r.message_id
    a = send_alert(s, date(2025, 1, 2), "Aborted", "details", client=sns)
    assert a.subject.startswith("[fin-dq] CRITICAL")


def test_mask_values() -> None:
    assert mask_value("jane@example.com", "customer_email") == "j***@example.com"
    masked = mask_value("Acme Corp", "customer_name")
    assert masked.startswith("Ac***") and masked != "Acme Corp"
    assert mask_value(None, "customer_name") is None


def test_mask_frame_role() -> None:
    df = pd.DataFrame({"customer_name": ["Acme"], "customer_email": ["a@b.com"], "x": [1]})
    out, cols, masked = mask_frame(df, ["customer_name", "customer_email"], "analyst")
    assert masked and set(cols) == {"customer_name", "customer_email"} and out["x"].tolist() == [1]
    assert out["customer_name"].iloc[0] != "Acme"
    out2, _cols2, masked2 = mask_frame(df, ["customer_name", "customer_email"], "finance_admin")
    assert not masked2 and out2.equals(df)
    _, cols3, masked3 = mask_frame(pd.DataFrame({"x": [1]}), ["customer_name"], "analyst")
    assert cols3 == [] and not masked3
