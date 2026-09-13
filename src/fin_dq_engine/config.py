"""Typed configuration loaded from config/settings.yaml with environment variable overrides.

Precedence (highest first): explicit keyword overrides passed to :func:`load_settings` (CLI flags),
environment variables ``FIN_DQ__SECTION__KEY``, then the YAML file. Database credentials are never stored in git: in AWS
mode the connection URL is resolved from Secrets Manager at runtime.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, ClassVar, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource


class PathsConfig(BaseModel):
    """Filesystem locations used in local mode."""

    data_dir: Path = Path("./data")
    out_dir: Path = Path("./out")
    sql_dir: Path = Path("./sql")
    config_dir: Path = Path("./config")


class DatabaseConfig(BaseModel):
    """Warehouse connection settings."""

    url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/fin_dq"
    echo: bool = False


class AwsConfig(BaseModel):
    """AWS resource names. Only used when ``environment == 'aws'``."""

    region: str = "us-east-1"
    raw_bucket: str = "fin-dq-raw"
    staged_bucket: str = "fin-dq-staged"
    curated_bucket: str = "fin-dq-curated"
    sns_topic_arn: str = ""
    secret_name: str = "fin-dq/db-credentials"
    metrics_namespace: str = "FinDQEngine"


class PipelineConfig(BaseModel):
    """Thresholds that govern loading behaviour."""

    batch_failure_threshold_pct: float = 5.0
    late_arrival_days: int = 3
    freshness_days: int = 3
    row_count_tolerance_pct: float = 50.0
    accepted_currencies: list[str] = Field(default_factory=lambda: ["USD", "EUR", "GBP", "JPY"])
    reporting_currency: str = "USD"
    warning_penalty: int = 20
    info_penalty: int = 5


class AnomalyConfig(BaseModel):
    """Statistical anomaly detection parameters."""

    revenue_window_days: int = 28
    zscore_threshold: float = 3.0
    mad_threshold: float = 3.5
    isolation_forest_training_days: int = 90
    isolation_forest_contamination: float = 0.01
    isolation_forest_min_rows: int = 200
    benford_p_value: float = 0.001
    benford_min_rows: int = 300
    fx_jump_pct: float = 5.0


class GovernanceConfig(BaseModel):
    """Retention classes and PII column registry."""

    retention_days: dict[str, int] = Field(
        default_factory=lambda: {"raw": 400, "staged": 90, "curated": 2555, "logs": 730}
    )
    pii_columns: list[str] = Field(default_factory=lambda: ["customer_email", "customer_name"])


class Settings(BaseSettings):
    """Root settings object."""

    model_config = SettingsConfigDict(env_prefix="FIN_DQ__", env_nested_delimiter="__", extra="ignore")
    _yaml_path: ClassVar[Path | None] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """init kwargs > environment > YAML file."""
        sources: list[PydanticBaseSettingsSource] = [init_settings, env_settings]
        if cls._yaml_path is not None and cls._yaml_path.exists():
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=cls._yaml_path))
        return tuple(sources)

    environment: Literal["local", "aws"] = "local"
    actor: str = "pipeline"
    role: str = "analyst"
    paths: PathsConfig = Field(default_factory=PathsConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    aws: AwsConfig = Field(default_factory=AwsConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    anomaly: AnomalyConfig = Field(default_factory=AnomalyConfig)
    governance: GovernanceConfig = Field(default_factory=GovernanceConfig)

    @property
    def is_local(self) -> bool:
        """True when running against Docker Postgres and the local ./data folder."""
        return self.environment == "local"

    @property
    def raw_dir(self) -> Path:
        """Local raw landing folder."""
        return self.paths.data_dir / "raw"

    @property
    def staged_dir(self) -> Path:
        """Local staged (Parquet) folder."""
        return self.paths.data_dir / "staged"

    @property
    def curated_dir(self) -> Path:
        """Local curated folder (report outputs)."""
        return self.paths.data_dir / "curated"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a mapping at the top level")
    return loaded


def load_settings(config_path: Path | str | None = None, **overrides: Any) -> Settings:
    """Load settings from YAML, then apply keyword overrides, then environment variables.

    Args:
        config_path: Path to a settings YAML. Defaults to ``$FIN_DQ_CONFIG`` or ``config/settings.yaml``.
        **overrides: Top-level keys to override programmatically (e.g. ``environment="aws"``).
    """
    path = Path(config_path or os.environ.get("FIN_DQ_CONFIG", "config/settings.yaml"))
    _read_yaml(path)  # validates the file is a mapping before pydantic sees it
    Settings._yaml_path = path
    try:
        settings = Settings(**{k: v for k, v in overrides.items() if v is not None})
    finally:
        Settings._yaml_path = None
    if settings.environment == "aws" and not os.environ.get("FIN_DQ__DATABASE__URL"):
        from fin_dq_engine.governance.secrets import resolve_database_url

        settings.database.url = resolve_database_url(settings)
    return settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings used by the CLI. Tests should call :func:`load_settings` directly."""
    return load_settings()
