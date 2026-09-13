"""CDK synth snapshot: resource inventory per stack must match tests/fixtures/cdk_snapshot.json."""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SNAPSHOT = REPO / "tests" / "fixtures" / "cdk_snapshot.json"


@pytest.fixture(scope="module")
def templates() -> dict[str, dict]:  # type: ignore[type-arg]
    sys.path.insert(0, str(REPO / "infra"))
    import aws_cdk as cdk
    from aws_cdk.assertions import Template

    from app import build_app

    app = build_app(cdk.App(context={"prefix": "fin-dq", "image_tag": "test"}))
    return {
        stack.stack_name: Template.from_stack(stack).to_json()
        for stack in app.node.children
        if isinstance(stack, cdk.Stack)
    }


def _inventory(tpl: dict) -> dict[str, int]:  # type: ignore[type-arg]
    return dict(sorted(Counter(r["Type"] for r in tpl.get("Resources", {}).values()).items()))


def test_snapshot(templates: dict[str, dict]) -> None:  # type: ignore[type-arg]
    inventory = {name: _inventory(t) for name, t in templates.items()}
    if os.environ.get("UPDATE_SNAPSHOT") == "1" or not SNAPSHOT.exists():
        SNAPSHOT.write_text(json.dumps(inventory, indent=2) + "\n")
    assert inventory == json.loads(SNAPSHOT.read_text())


def test_key_resources(templates: dict[str, dict]) -> None:  # type: ignore[type-arg]
    storage = _inventory(templates["fin-dq-storage"])
    assert storage["AWS::S3::Bucket"] == 3
    data = _inventory(templates["fin-dq-data"])
    assert data["AWS::RDS::DBInstance"] == 1 and data["AWS::SecretsManager::Secret"] == 1
    compute = _inventory(templates["fin-dq-compute"])
    assert compute["AWS::Lambda::Function"] >= 6 and compute["AWS::StepFunctions::StateMachine"] == 1
    assert compute["AWS::Events::Rule"] == 1 and compute["AWS::SNS::Topic"] == 2
    obs = _inventory(templates["fin-dq-observability"])
    assert obs["AWS::CloudWatch::Alarm"] >= 9 and obs["AWS::CloudWatch::Dashboard"] == 1


def test_schedule_and_least_privilege(templates: dict[str, dict]) -> None:  # type: ignore[type-arg]
    compute = templates["fin-dq-compute"]
    rules = [r for r in compute["Resources"].values() if r["Type"] == "AWS::Events::Rule"]
    assert rules[0]["Properties"]["ScheduleExpression"] == "cron(0 6 * * ? *)"
    buckets = [r for r in templates["fin-dq-storage"]["Resources"].values() if r["Type"] == "AWS::S3::Bucket"]
    for b in buckets:
        assert b["Properties"]["VersioningConfiguration"]["Status"] == "Enabled"
        assert b["Properties"]["BucketEncryption"]
        assert b["Properties"]["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
    policies = json.dumps([r for r in compute["Resources"].values() if r["Type"] == "AWS::IAM::Policy"])
    assert "s3:DeleteObject*" in policies and "cloudwatch:namespace" in policies
    assert '"s3:*"' not in policies and '"Action": "*"' not in policies
    db = next(r for r in templates["fin-dq-data"]["Resources"].values() if r["Type"] == "AWS::RDS::DBInstance")
    assert db["Properties"]["StorageEncrypted"] is True and db["Properties"]["PubliclyAccessible"] is False
    assert db["DeletionPolicy"] == "Snapshot"
