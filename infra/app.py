"""CDK app: StorageStack, DataStack, ComputeStack, ObservabilityStack.

Deploy everything with ``make deploy`` (``cdk deploy --all``). The Lambda container image is built and pushed
by the Makefile before ``cdk deploy`` so that ``cdk synth`` never needs Docker (keeps snapshot tests hermetic).
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.compute_stack import ComputeStack
from stacks.data_stack import DataStack
from stacks.observability_stack import ObservabilityStack
from stacks.storage_stack import StorageStack


def build_app(app: cdk.App | None = None) -> cdk.App:
    """Wire the four stacks together. Exposed for the snapshot test."""
    app = app or cdk.App()
    env = cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT", "111111111111"),
        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
    )
    prefix = app.node.try_get_context("prefix") or "fin-dq"
    alert_email = app.node.try_get_context("alert_email") or "finance-ops@example.com"
    image_tag = app.node.try_get_context("image_tag") or "latest"

    storage = StorageStack(app, f"{prefix}-storage", prefix=prefix, env=env)
    data = DataStack(app, f"{prefix}-data", prefix=prefix, env=env)
    compute = ComputeStack(
        app,
        f"{prefix}-compute",
        prefix=prefix,
        storage=storage,
        data=data,
        image_tag=image_tag,
        alert_email=alert_email,
        env=env,
    )
    ObservabilityStack(app, f"{prefix}-observability", prefix=prefix, compute=compute, alert_email=alert_email, env=env)
    return app


if __name__ == "__main__":
    build_app().synth()
