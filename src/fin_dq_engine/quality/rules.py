"""Rule definitions and the plugin-style rule registry.

Every rule type is a class registered with :func:`register_rule`. Row-scoped rules express their
check as a SQL predicate that selects failing rows from ``staging.transactions``; batch-scoped rules
(freshness, row_count_delta) evaluate a single condition for the whole batch. Adding a rule type is
three steps: subclass :class:`Rule`, decorate with ``@register_rule("my_type")``, reference it in
``config/dq_rules.yaml``.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, ClassVar, Literal

import yaml
from pydantic import BaseModel, Field
from sqlalchemy import Connection, text

Dimension = Literal["completeness", "uniqueness", "validity", "consistency", "timeliness", "accuracy"]
Severity = Literal["blocking", "warning", "info"]

SAMPLE_LIMIT = 20
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


class RuleDefinition(BaseModel):
    """One entry of config/dq_rules.yaml."""

    id: str
    type: str
    description: str
    dimension: Dimension
    severity: Severity
    owner: str
    policy_section: str
    params: dict[str, Any] = Field(default_factory=dict)

    @property
    def version(self) -> str:
        """Content hash so the registry versions every rule change."""
        payload = json.dumps(self.model_dump(), sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()[:12]


def load_rule_definitions(path: Path) -> list[RuleDefinition]:
    """Parse the governed rule registry YAML."""
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    defs = [RuleDefinition(**item) for item in data.get("rules", [])]
    ids = [d.id for d in defs]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate rule ids in dq_rules.yaml")
    unknown = [d.type for d in defs if d.type not in RULE_REGISTRY]
    if unknown:
        raise ValueError(f"Unknown rule types: {sorted(set(unknown))}")
    return defs


@dataclass(frozen=True)
class RuleContext:
    """Everything a rule needs to run against one batch."""

    batch_id: str
    run_date: date
    table: str = "staging.transactions"
    key: str = "staging_row_id"
    display_key: str = "transaction_id"

    def substitute(self, value: Any) -> Any:
        """Expand ``{run_date}`` style placeholders inside params."""
        if isinstance(value, str):
            return value.replace("{run_date}", self.run_date.isoformat()).replace(
                "{run_date_plus_1}", (self.run_date + timedelta(days=1)).isoformat()
            )
        return value


@dataclass
class RuleOutcome:
    """Result of evaluating one rule on one batch."""

    definition: RuleDefinition
    status: Literal["pass", "fail", "error"]
    rows_checked: int
    rows_failed: int
    sample_keys: list[str]
    failing_keys: set[int] = field(default_factory=set)
    duration_ms: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def failure_rate(self) -> float:
        """Failed / checked (0 when nothing was checked)."""
        return self.rows_failed / self.rows_checked if self.rows_checked else 0.0


def _ident(name: str) -> str:
    """Whitelist SQL identifiers coming from config to keep them out of the injection surface."""
    if not _IDENT.match(name):
        raise ValueError(f"Illegal SQL identifier in rule params: {name!r}")
    return name


_SQL_STATEMENT_BREAK = re.compile(r";|--|/\*")
_SQL_WRITE_KEYWORD = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CREATE|COPY)\b", re.IGNORECASE
)


def _read_only_subquery(sql: str) -> str:
    """Keep a ``custom_sql`` rule body to a single read-only expression.

    ``dq_rules.yaml`` is version-controlled and under the change control in
    ``docs/data_governance_policy.md``, so this is not the security boundary. It is a guard against a
    typo or a thin review: the body is interpolated into a subquery, and neither ending the statement
    nor writing to the warehouse is ever a legitimate thing for a rule to do.
    """
    if _SQL_STATEMENT_BREAK.search(sql):
        raise ValueError("custom_sql must be one expression: ';', '--' and '/*' are not allowed")
    if _SQL_WRITE_KEYWORD.search(sql):
        raise ValueError("custom_sql must be read-only; found a write or DDL keyword")
    return sql


def _coerce(value: Any) -> Any:
    """Turn ISO date strings into dates so bound params compare against DATE columns."""
    if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return date.fromisoformat(value)
    return value


class Rule(ABC):
    """Base class for all rule types."""

    rule_type: ClassVar[str] = ""
    scope: ClassVar[Literal["row", "batch"]] = "row"

    def __init__(self, definition: RuleDefinition) -> None:
        self.definition = definition
        self.params = definition.params

    @abstractmethod
    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        """Return SQL selecting ``(key, display_key)`` of failing rows, plus bound params."""

    def evaluate(self, conn: Connection, ctx: RuleContext) -> RuleOutcome:
        """Run the rule and collect counts, failing keys and up to 20 sample display keys."""
        start = time.perf_counter()
        try:
            rows_checked = int(
                conn.execute(
                    text(f"SELECT count(*) FROM {ctx.table} WHERE batch_id = :batch_id"),
                    {"batch_id": ctx.batch_id},
                ).scalar()
                or 0
            )
            sql, params = self.failing_sql(ctx)
            result = conn.execute(text(sql), {"batch_id": ctx.batch_id, **params}).fetchall()
            failing = {int(r[0]) for r in result}
            samples = [str(r[1]) if r[1] is not None else "<null>" for r in result[:SAMPLE_LIMIT]]
            status: Literal["pass", "fail", "error"] = "fail" if failing else "pass"
            return RuleOutcome(
                self.definition,
                status,
                rows_checked,
                len(failing),
                samples,
                failing,
                int((time.perf_counter() - start) * 1000),
            )
        except Exception as exc:  # a broken rule must not take the pipeline down silently
            return RuleOutcome(
                self.definition,
                "error",
                0,
                0,
                [],
                set(),
                int((time.perf_counter() - start) * 1000),
                {"error": str(exc)},
            )

    def _select(self, ctx: RuleContext, predicate: str) -> str:
        return (
            f"SELECT t.{ctx.key}, t.{ctx.display_key} FROM {ctx.table} t WHERE t.batch_id = :batch_id AND ({predicate})"
        )


RULE_REGISTRY: dict[str, type[Rule]] = {}


def register_rule(rule_type: str) -> Any:
    """Class decorator that registers a rule implementation under ``rule_type``."""

    def deco(cls: type[Rule]) -> type[Rule]:
        cls.rule_type = rule_type
        RULE_REGISTRY[rule_type] = cls
        return cls

    return deco


def build_rule(definition: RuleDefinition) -> Rule:
    """Instantiate the registered class for a definition."""
    return RULE_REGISTRY[definition.type](definition)


@register_rule("not_null")
class NotNullRule(Rule):
    """Column must not be null."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        return self._select(ctx, f"t.{col} IS NULL"), {}


@register_rule("unique")
class UniqueRule(Rule):
    """Column must be unique within the batch (nulls ignored)."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        return self._select(
            ctx,
            f"""t.{col} IN (
            SELECT {col} FROM {ctx.table} WHERE batch_id = :batch_id AND {col} IS NOT NULL
            GROUP BY {col} HAVING count(*) > 1)""",
        ), {}


@register_rule("accepted_values")
class AcceptedValuesRule(Rule):
    """Column must be one of a governed list (nulls handled by not_null)."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        values = list(self.params["values"])
        placeholders = ", ".join(f":v{i}" for i in range(len(values)))
        return (
            self._select(ctx, f"t.{col} IS NOT NULL AND t.{col} NOT IN ({placeholders})"),
            {f"v{i}": v for i, v in enumerate(values)},
        )


@register_rule("regex_match")
class RegexMatchRule(Rule):
    """Column must match a POSIX regex."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        return self._select(ctx, f"t.{col} IS NOT NULL AND t.{col} !~ :pattern"), {"pattern": self.params["pattern"]}


@register_rule("range")
class RangeRule(Rule):
    """Column must fall within [min, max]. Placeholders such as {run_date_plus_1} are expanded."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        params: dict[str, Any] = {}
        clauses: list[str] = []
        if "min" in self.params:
            params["min"] = _coerce(ctx.substitute(self.params["min"]))
            clauses.append(f"t.{col} < :min")
        if "max" in self.params:
            params["max"] = _coerce(ctx.substitute(self.params["max"]))
            clauses.append(f"t.{col} > :max")
        return self._select(ctx, f"t.{col} IS NOT NULL AND ({' OR '.join(clauses)})"), params


@register_rule("referential_integrity")
class ReferentialIntegrityRule(Rule):
    """Foreign key must exist in a dimension (optionally filtered, e.g. ``is_current``)."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        col = _ident(self.params["column"])
        ref_table = _ident(self.params["ref_table"])
        ref_col = _ident(self.params["ref_column"])
        ref_filter = self.params.get("ref_filter")
        extra = f" AND r.{_ident(ref_filter)}" if ref_filter else ""
        sql = (
            f"SELECT t.{ctx.key}, t.{ctx.display_key} FROM {ctx.table} t "
            f"LEFT JOIN {ref_table} r ON r.{ref_col} = t.{col}{extra} "
            f"WHERE t.batch_id = :batch_id AND t.{col} IS NOT NULL AND r.{ref_col} IS NULL"
        )
        return sql, {}


@register_rule("custom_sql")
class CustomSqlRule(Rule):
    """Any SQL returning the failing ``transaction_id`` values; ``:batch_id`` is bound."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        inner = _read_only_subquery(self.params["sql"])
        sql = (
            f"SELECT t.{ctx.key}, t.{ctx.display_key} FROM {ctx.table} t "
            f"WHERE t.batch_id = :batch_id AND t.{ctx.display_key} IN ({inner})"
        )
        return sql, {}


@register_rule("sign_by_account_type")
class SignByAccountTypeRule(Rule):
    """Credit-normal accounts must not carry positive (debit) amounts for the listed event types."""

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:
        types = list(self.params.get("account_types", ["revenue", "liability", "equity"]))
        events = list(self.params.get("event_types", []))
        t_ph = ", ".join(f":t{i}" for i in range(len(types)))
        params: dict[str, Any] = {f"t{i}": v for i, v in enumerate(types)}
        event_clause = ""
        if events:
            e_ph = ", ".join(f":e{i}" for i in range(len(events)))
            params.update({f"e{i}": v for i, v in enumerate(events)})
            event_clause = f" AND t.event_type IN ({e_ph})"
        sql = (
            f"SELECT t.{ctx.key}, t.{ctx.display_key} FROM {ctx.table} t "
            f"JOIN curated.dim_account a ON a.account_id = t.account_id "
            f"WHERE t.batch_id = :batch_id AND a.normal_balance = 'credit' AND a.account_type IN ({t_ph})"
            f"{event_clause} AND t.amount_local > 0"
        )
        return sql, params


class BatchRule(Rule):
    """Base for batch-scoped rules: one condition for the whole batch, no failing rows."""

    scope = "batch"

    def failing_sql(self, ctx: RuleContext) -> tuple[str, dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError("batch rules override evaluate()")

    @abstractmethod
    def check(self, conn: Connection, ctx: RuleContext) -> tuple[bool, dict[str, Any]]:
        """Return (passed, details)."""

    def evaluate(self, conn: Connection, ctx: RuleContext) -> RuleOutcome:
        """Run the batch check."""
        start = time.perf_counter()
        try:
            passed, details = self.check(conn, ctx)
            return RuleOutcome(
                self.definition,
                "pass" if passed else "fail",
                1,
                0 if passed else 1,
                [] if passed else [ctx.batch_id],
                set(),
                int((time.perf_counter() - start) * 1000),
                details,
            )
        except Exception as exc:
            return RuleOutcome(
                self.definition,
                "error",
                0,
                0,
                [],
                set(),
                int((time.perf_counter() - start) * 1000),
                {"error": str(exc)},
            )


@register_rule("freshness")
class FreshnessRule(BatchRule):
    """Newest posted_date must be within ``max_age_days`` of the run date."""

    def check(self, conn: Connection, ctx: RuleContext) -> tuple[bool, dict[str, Any]]:
        col = _ident(self.params.get("column", "posted_date"))
        max_age = int(self.params.get("max_age_days", 3))
        newest = conn.execute(
            text(f"SELECT max({col}) FROM {ctx.table} WHERE batch_id = :batch_id AND {col} <= :cap"),
            {"batch_id": ctx.batch_id, "cap": ctx.run_date + timedelta(days=1)},
        ).scalar()
        if newest is None:
            return False, {"newest": None, "max_age_days": max_age}
        age = (ctx.run_date - newest).days
        return age <= max_age, {
            "newest": newest.isoformat(),
            "age_days": age,
            "max_age_days": max_age,
        }


@register_rule("row_count_delta")
class RowCountDeltaRule(BatchRule):
    """Batch row count must be within ``tolerance_pct`` of the most recent prior batch."""

    def check(self, conn: Connection, ctx: RuleContext) -> tuple[bool, dict[str, Any]]:
        tolerance = float(self.params.get("tolerance_pct", 50.0))
        current = int(
            conn.execute(
                text(f"SELECT count(*) FROM {ctx.table} WHERE batch_id = :batch_id"),
                {"batch_id": ctx.batch_id},
            ).scalar()
            or 0
        )
        prior = conn.execute(
            text("""
            SELECT rows_out FROM dq.batch_run_log
            WHERE stage = 'transform' AND status = 'success' AND run_date < :run_date AND batch_id <> :batch_id
            ORDER BY run_date DESC, started_at DESC LIMIT 1"""),
            {"run_date": ctx.run_date, "batch_id": ctx.batch_id},
        ).scalar()
        if not prior:
            return True, {"current": current, "prior": None, "note": "no prior batch"}
        delta_pct = abs(current - int(prior)) / int(prior) * 100
        return delta_pct <= tolerance, {
            "current": current,
            "prior": int(prior),
            "delta_pct": round(delta_pct, 2),
            "tolerance_pct": tolerance,
        }
