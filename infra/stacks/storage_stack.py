"""Three S3 buckets (raw, staged, curated) with versioning, lifecycle rules and SSE."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StorageStack(cdk.Stack):
    """S3 storage for the three data layers."""

    def __init__(self, scope: Construct, construct_id: str, *, prefix: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)  # type: ignore[arg-type]
        self.raw_bucket = self._bucket("raw", expire_days=400, ia_days=30)
        self.staged_bucket = self._bucket("staged", expire_days=90, ia_days=None)
        self.curated_bucket = self._bucket("curated", expire_days=None, ia_days=90)
        for name, bucket in (
            ("Raw", self.raw_bucket),
            ("Staged", self.staged_bucket),
            ("Curated", self.curated_bucket),
        ):
            cdk.CfnOutput(
                self, f"{name}BucketName", value=bucket.bucket_name, export_name=f"{prefix}-{name.lower()}-bucket"
            )

    def _bucket(self, layer: str, *, expire_days: int | None, ia_days: int | None) -> s3.Bucket:
        rules: list[s3.LifecycleRule] = [
            s3.LifecycleRule(
                id="expire-noncurrent",
                noncurrent_version_expiration=cdk.Duration.days(30),
                abort_incomplete_multipart_upload_after=cdk.Duration.days(7),
            ),
        ]
        if ia_days:
            rules.append(
                s3.LifecycleRule(
                    id="to-infrequent-access",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS, transition_after=cdk.Duration.days(ia_days)
                        )
                    ],
                )
            )
        if expire_days:
            rules.append(s3.LifecycleRule(id="expire-current", expiration=cdk.Duration.days(expire_days)))
        return s3.Bucket(
            self,
            f"{layer.title()}Bucket",
            bucket_name=f"{self.node.try_get_context('prefix') or 'fin-dq'}-{layer}-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=rules,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
