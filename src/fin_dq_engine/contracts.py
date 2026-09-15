"""Schema contracts for the raw feeds.

Every feed has an Avro record schema under ``contracts/`` carrying the promised fields plus the
operational metadata that turns a structural change into a routable incident: the owning team, the
producing system and the delivery window. Ingest validates each source header against its contract
*before* the landing transaction opens, so a producer-side change aborts the batch with nothing
landed instead of arriving as a column full of nulls.

Verdicts:
    ``breaking``    a promised field is absent, or the header repeats a name. The batch is refused.
    ``additive``    the file carries fields the contract does not know about. Lands, recorded for follow-up.
    ``compatible``  the header matches the contract. Column order is irrelevant; files are read by name.

A rename registers as breaking rather than additive: the old name is missing, which is what decides the
verdict, and the new name is reported alongside it so the producer sees both halves of the change.

This module imports neither the database nor object storage, so contract logic can be exercised on a
list of column names alone.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

VerdictName = Literal["compatible", "additive", "breaking"]

VERSION_LENGTH = 12


@dataclass(frozen=True)
class Contract:
    """A parsed ``.avsc`` file: the promised shape of one feed plus who is accountable for it."""

    feed: str
    name: str
    namespace: str
    owner: str
    producing_system: str
    delivery_window: str
    fields: tuple[str, ...]
    nullable_fields: frozenset[str]
    version: str
    path: Path
    definition: dict[str, Any]

    @property
    def fullname(self) -> str:
        """Avro fullname, ``namespace.name``."""
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


@dataclass(frozen=True)
class Verdict:
    """The outcome of checking one file header against one contract."""

    feed: str
    contract: str
    version: str
    owner: str
    verdict: VerdictName
    missing: tuple[str, ...] = ()
    duplicated: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()

    @property
    def is_breaking(self) -> bool:
        """True when the batch must be refused."""
        return self.verdict == "breaking"

    @property
    def message(self) -> str:
        """Human-readable summary naming the feed, the fields and the accountable owner."""
        parts = []
        if self.missing:
            parts.append(f"missing fields {list(self.missing)}")
        if self.duplicated:
            parts.append(f"duplicated fields {list(self.duplicated)}")
        if self.unknown:
            parts.append(f"unknown fields {list(self.unknown)}")
        detail = ", ".join(parts) if parts else "header matches the contract"
        return (
            f"Contract violation on feed '{self.feed}' (contract {self.contract} v {self.version}, "
            f"owner {self.owner}): {detail}."
        )

    def as_log(self) -> dict[str, Any]:
        """Flat dict for the structured logger."""
        return {
            "feed": self.feed,
            "contract": self.contract,
            "version": self.version,
            "owner": self.owner,
            "verdict": self.verdict,
            "missing": list(self.missing),
            "duplicated": list(self.duplicated),
            "unknown": list(self.unknown),
        }


class ContractError(RuntimeError):
    """Base class for contract problems."""


class ContractNotFoundError(ContractError):
    """A feed is being landed with no contract to validate it against."""


class SchemaDriftError(ContractError):
    """One or more feeds broke their contract. Raised before anything is landed."""

    def __init__(self, verdicts: Sequence[Verdict]) -> None:
        self.verdicts = tuple(verdicts)
        super().__init__(" ".join(v.message for v in self.verdicts) + " Nothing was landed.")


def validate_columns(contract: Contract, header: Sequence[str]) -> Verdict:
    """Compare a source file header against a contract and return the verdict.

    Presence is what is checked, not nullability: a nullable field still owes a column, it is the
    values inside it that may be empty. Order is ignored because files are read by name.

    Args:
        contract: The contract the feed promised to deliver.
        header: Column names in the order they appear in the file.

    Returns:
        A :class:`Verdict`. ``breaking`` wins over ``additive`` when a rename produces both.
    """
    seen: dict[str, int] = {}
    for column in header:
        seen[column] = seen.get(column, 0) + 1
    duplicated = tuple(name for name, count in seen.items() if count > 1)
    missing = tuple(f for f in contract.fields if f not in seen)
    unknown = tuple(name for name in seen if name not in contract.fields)
    if missing or duplicated:
        name: VerdictName = "breaking"
    elif unknown:
        name = "additive"
    else:
        name = "compatible"
    return Verdict(
        feed=contract.feed,
        contract=contract.name,
        version=contract.version,
        owner=contract.owner,
        verdict=name,
        missing=missing,
        duplicated=duplicated,
        unknown=unknown,
    )


def validate_feeds(contracts: Mapping[str, Contract], headers: Mapping[str, Sequence[str]]) -> dict[str, Verdict]:
    """Validate several feeds at once, keyed by feed name.

    Raises:
        ContractNotFoundError: A header was supplied for a feed with no contract file.
    """
    verdicts: dict[str, Verdict] = {}
    for feed, header in headers.items():
        if feed not in contracts:
            raise ContractNotFoundError(f"No contract for feed '{feed}'; expected contracts/{feed}.avsc")
        verdicts[feed] = validate_columns(contracts[feed], header)
    return verdicts


def canonical_form(definition: Mapping[str, Any]) -> str:
    """Structural fingerprint input: fullname and ordered field name/type pairs, nothing else.

    Documentation, ownership and delivery metadata are deliberately excluded so that correcting a
    doc string does not present itself to operators as a new schema version.
    """
    namespace = definition.get("namespace", "")
    name = definition.get("name", "")
    fullname = f"{namespace}.{name}" if namespace else name
    fields = [[f["name"], _type_form(f["type"])] for f in definition.get("fields", [])]
    return json.dumps({"name": fullname, "type": "record", "fields": fields}, separators=(",", ":"), sort_keys=False)


def _type_form(type_value: Any) -> Any:
    """Normalise an Avro type so union member order does not change the fingerprint."""
    if isinstance(type_value, list):
        return sorted(_type_form(t) for t in type_value)
    if isinstance(type_value, dict):
        return json.dumps(type_value, separators=(",", ":"), sort_keys=True)
    return type_value


def contract_version(definition: Mapping[str, Any]) -> str:
    """Short sha256 fingerprint of the contract's structure."""
    digest = hashlib.sha256(canonical_form(definition).encode("utf-8")).hexdigest()
    return digest[:VERSION_LENGTH]


def load_contract(path: Path, feed: str | None = None) -> Contract:
    """Parse one ``.avsc`` file. The feed name defaults to the file stem.

    Raises:
        ContractError: The file is not an Avro record, or omits required metadata.
    """
    definition = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(definition, dict) or definition.get("type") != "record":
        raise ContractError(f"{path} is not an Avro record schema")
    try:
        fields = tuple(str(f["name"]) for f in definition["fields"])
        owner = str(definition["owner"])
    except (KeyError, TypeError) as exc:
        raise ContractError(f"{path} is missing a required contract attribute: {exc}") from exc
    nullable = frozenset(
        str(f["name"]) for f in definition["fields"] if isinstance(f.get("type"), list) and "null" in f["type"]
    )
    return Contract(
        feed=feed or path.stem,
        name=str(definition.get("name", path.stem)),
        namespace=str(definition.get("namespace", "")),
        owner=owner,
        producing_system=str(definition.get("producing_system", "unknown")),
        delivery_window=str(definition.get("delivery_window", "unspecified")),
        fields=fields,
        nullable_fields=nullable,
        version=contract_version(definition),
        path=path,
        definition=definition,
    )


def load_contracts(contracts_dir: Path) -> dict[str, Contract]:
    """Load every ``*.avsc`` in a directory, keyed by feed name.

    Raises:
        ContractNotFoundError: The directory does not exist or holds no contracts.
    """
    directory = Path(contracts_dir)
    paths = sorted(directory.glob("*.avsc")) if directory.is_dir() else []
    if not paths:
        raise ContractNotFoundError(f"No contracts found in {directory.resolve()}")
    return {p.stem: load_contract(p) for p in paths}


def header_from_bytes(blob: bytes) -> list[str]:
    """Read the column names from the first line of a CSV blob.

    Returns an empty list for an empty file, which validates as every field missing.
    """
    if not blob.strip():
        return []
    text = blob.decode("utf-8-sig")
    first_line = text.splitlines()[0]
    return [c.strip() for c in next(csv.reader(io.StringIO(first_line)))]
