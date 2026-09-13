"""Secrets Manager lookup for database credentials (never stored in the repo)."""

from __future__ import annotations

import json

import boto3

from fin_dq_engine.config import Settings


def resolve_database_url(settings: Settings) -> str:
    """Build a SQLAlchemy URL from the RDS-style secret ``{username,password,host,port,dbname}``."""
    client = boto3.client("secretsmanager", region_name=settings.aws.region)
    payload = json.loads(client.get_secret_value(SecretId=settings.aws.secret_name)["SecretString"])
    return (
        f"postgresql+psycopg://{payload['username']}:{payload['password']}"
        f"@{payload['host']}:{payload.get('port', 5432)}/{payload.get('dbname', 'fin_dq')}"
    )
