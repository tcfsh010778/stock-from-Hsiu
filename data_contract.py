from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_REGISTRY_PATH = ROOT / "contracts" / "taiwan_stock_data_contracts.json"
DEFAULT_MANIFEST_PATH = ROOT / "data" / "freshness_manifest.json"

MANIFEST_SCHEMA_VERSION = "1.0.0"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FRESHNESS_STATUSES = {
    "fresh",
    "expected_lag",
    "stale",
    "missing",
    "fallback_fresh",
    "fallback_stale",
    "schema_error",
}
MISSING_STATUSES = {"complete", "partial", "missing"}
REQUIRED_MANIFEST_FIELDS = {
    "manifest_schema_version",
    "dataset_id",
    "dataset_schema_version",
    "source_id",
    "source_tier",
    "source_url",
    "market_coverage",
    "update_frequency",
    "data_date",
    "trading_date",
    "expected_data_date",
    "fetched_at",
    "row_count",
    "sha256",
    "fallback",
    "missing",
    "freshness",
    "schema_validation",
}


class ContractError(ValueError):
    """Raised when a registry or artifact manifest violates its contract."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_rows(rows: Sequence[dict[str, Any]]) -> str:
    return sha256_bytes(canonical_json_bytes(rows))


def _parse_date(value: str | date | None, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError as exc:
        raise ContractError(f"{field} must be an ISO date: {value!r}") from exc


def _parse_datetime(value: str | datetime | None, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ContractError(f"{field} must be an ISO datetime with timezone: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ContractError(f"{field} must include a timezone offset")
    return parsed


def _iso_datetime(value: str | datetime) -> str:
    return _parse_datetime(value, "datetime").isoformat(timespec="seconds")


def business_day_lag(
    data_date: str | date,
    expected_data_date: str | date,
    holidays: Iterable[str | date] = (),
) -> int:
    """Count expected trading weekdays after data_date through expected_data_date."""

    observed = _parse_date(data_date, "data_date")
    expected = _parse_date(expected_data_date, "expected_data_date")
    if observed >= expected:
        return 0
    holiday_dates = {_parse_date(item, "holiday") for item in holidays}
    lag = 0
    current = observed + timedelta(days=1)
    while current <= expected:
        if current.weekday() < 5 and current not in holiday_dates:
            lag += 1
        current += timedelta(days=1)
    return lag


def trading_session_lag(
    data_date: str | date,
    expected_data_date: str | date,
    trading_sessions: Iterable[str | date],
) -> int:
    """Count official session dates after data_date through expected_data_date."""

    observed = _parse_date(data_date, "data_date")
    expected = _parse_date(expected_data_date, "expected_data_date")
    if observed >= expected:
        return 0
    sessions = {_parse_date(item, "trading_session") for item in trading_sessions}
    return sum(observed < session <= expected for session in sessions)


def load_registry(path: Path = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    try:
        registry = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ContractError(f"contract registry not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ContractError(f"contract registry is not valid JSON: {exc}") from exc
    validate_registry(registry)
    return registry


def _require_https(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.startswith("https://"):
        raise ContractError(f"{label} must be an https URL")


def validate_registry(registry: dict[str, Any]) -> None:
    if registry.get("registry_schema_version") != "1.0.0":
        raise ContractError("registry_schema_version must be 1.0.0")
    if registry.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ContractError(f"manifest_schema_version must be {MANIFEST_SCHEMA_VERSION}")

    required_metadata = set(registry.get("required_artifact_metadata") or [])
    if not REQUIRED_MANIFEST_FIELDS.issubset(required_metadata):
        missing = sorted(REQUIRED_MANIFEST_FIELDS - required_metadata)
        raise ContractError(f"required_artifact_metadata is missing: {missing}")

    sources = registry.get("sources")
    datasets = registry.get("datasets")
    if not isinstance(sources, dict) or not sources:
        raise ContractError("sources must be a non-empty object")
    if not isinstance(datasets, dict) or not datasets:
        raise ContractError("datasets must be a non-empty object")

    for source_id, source in sources.items():
        if source.get("source_id") != source_id:
            raise ContractError(f"source key/id mismatch: {source_id}")
        _require_https(source.get("endpoint"), f"sources.{source_id}.endpoint")
        _require_https(source.get("documentation_url"), f"sources.{source_id}.documentation_url")
        if source.get("official"):
            if not source.get("terms_checked_on"):
                raise ContractError(f"official source {source_id} lacks terms_checked_on")
            if not source.get("terms_url") and source.get("terms_status") != "owner_openapi_without_explicit_terms_link":
                raise ContractError(f"official source {source_id} lacks terms_url or explicit review status")

    allowed_modes = {"trading_day", "calendar_day", "weekly", "monthly", "quarterly", "event_driven", "annual"}
    for dataset_id, contract in datasets.items():
        if contract.get("dataset_id") != dataset_id:
            raise ContractError(f"dataset key/id mismatch: {dataset_id}")
        if not re.fullmatch(r"\d+\.\d+\.\d+", str(contract.get("schema_version") or "")):
            raise ContractError(f"dataset {dataset_id} needs semantic schema_version")
        fields = contract.get("canonical_fields")
        if not isinstance(fields, list) or not fields:
            raise ContractError(f"dataset {dataset_id} must define canonical_fields")
        names = [field.get("name") for field in fields if isinstance(field, dict)]
        if len(names) != len(set(names)) or any(not name for name in names):
            raise ContractError(f"dataset {dataset_id} has invalid/duplicate canonical field names")
        routes = list(contract.get("primary_sources") or []) + list(contract.get("fallback_sources") or [])
        if not contract.get("primary_sources"):
            raise ContractError(f"dataset {dataset_id} must have primary_sources")
        unknown = [route.get("source_id") for route in routes if route.get("source_id") not in sources]
        if unknown:
            raise ContractError(f"dataset {dataset_id} references unknown sources: {unknown}")
        freshness = contract.get("freshness") or {}
        if freshness.get("mode") not in allowed_modes:
            raise ContractError(f"dataset {dataset_id} has invalid freshness mode")


def _dataset_contract(registry: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    try:
        return registry["datasets"][dataset_id]
    except KeyError as exc:
        raise ContractError(f"unknown dataset_id: {dataset_id}") from exc


def _source_contract(registry: dict[str, Any], source_id: str) -> dict[str, Any]:
    try:
        return registry["sources"][source_id]
    except KeyError as exc:
        raise ContractError(f"unknown source_id: {source_id}") from exc


def _source_tier(contract: dict[str, Any], source_id: str) -> str:
    if any(route.get("source_id") == source_id for route in contract.get("primary_sources") or []):
        return "primary"
    if any(route.get("source_id") == source_id for route in contract.get("fallback_sources") or []):
        return "fallback"
    raise ContractError(f"source {source_id} is not allowed for dataset {contract.get('dataset_id')}")


def _calendar_source_ids(registry: dict[str, Any]) -> set[str]:
    calendar_contract = _dataset_contract(registry, "trading_calendar")
    return {
        str(route.get("source_id"))
        for route in calendar_contract.get("primary_sources") or []
        if route.get("source_id")
    }


def evaluate_freshness(
    contract: dict[str, Any],
    *,
    data_date: str | date | None,
    expected_data_date: str | date,
    fetched_at: str | datetime,
    row_count: int,
    fallback_used: bool = False,
    evaluated_at: str | datetime | None = None,
    trading_sessions: Iterable[str | date] | None = None,
    calendar_source_ids: Sequence[str] = (),
) -> dict[str, Any]:
    policy = contract.get("freshness") or {}
    expected = _parse_date(expected_data_date, "expected_data_date")
    fetched = _parse_datetime(fetched_at, "fetched_at")
    evaluated = _parse_datetime(evaluated_at, "evaluated_at") if evaluated_at else fetched
    result: dict[str, Any] = {
        "status": "missing",
        "evaluated_at": evaluated.isoformat(timespec="seconds"),
        "expected_data_date": expected.isoformat(),
        "age_trading_days": None,
        "age_calendar_days": None,
        "sla": policy,
    }
    empty_result_is_valid = bool(policy.get("empty_result_is_valid"))
    if not data_date or (row_count <= 0 and not empty_result_is_valid):
        return result

    observed = _parse_date(data_date, "data_date")
    calendar_lag = max(0, (expected - observed).days)
    result["age_calendar_days"] = calendar_lag
    mode = policy.get("mode")
    base_status = "fresh"

    if mode == "trading_day":
        if trading_sessions is None or not calendar_source_ids:
            raise ContractError(
                "trading-day freshness requires official trading_sessions and calendar_source_ids"
            )
        lag = trading_session_lag(observed, expected, trading_sessions)
        result["age_trading_days"] = lag
        result["calendar_basis"] = "official_trading_sessions"
        result["calendar_source_ids"] = sorted(set(calendar_source_ids))
        max_lag = int(policy.get("max_lag_trading_days", 0))
        if lag > max_lag:
            base_status = "stale"
        elif lag > 0:
            base_status = "expected_lag"
    elif mode in {"calendar_day", "weekly", "monthly", "quarterly", "annual"}:
        max_lag = int(policy.get("max_lag_calendar_days", 0))
        if calendar_lag > max_lag:
            base_status = "stale"
        elif calendar_lag > 0:
            base_status = "expected_lag"
    elif mode == "event_driven":
        fetch_age_hours = max(0.0, (evaluated - fetched).total_seconds() / 3600)
        result["fetch_age_hours"] = round(fetch_age_hours, 2)
        if fetch_age_hours > float(policy.get("max_fetch_age_hours", 24)):
            base_status = "stale"

    if fallback_used:
        result["status"] = "fallback_stale" if base_status == "stale" else "fallback_fresh"
    else:
        result["status"] = base_status
    return result


def build_manifest(
    dataset_id: str,
    source_id: str,
    rows: Sequence[dict[str, Any]],
    *,
    data_date: str,
    expected_data_date: str,
    fetched_at: str | datetime,
    trading_date: str | None = None,
    payload: bytes | None = None,
    fallback_from_source_id: str | None = None,
    fallback_reason: str | None = None,
    missing_fields: Sequence[str] = (),
    missing_partitions: Sequence[str] = (),
    evaluated_at: str | datetime | None = None,
    trading_sessions: Iterable[str | date] | None = None,
    calendar_source_ids: Sequence[str] = (),
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = registry or load_registry()
    contract = _dataset_contract(registry, dataset_id)
    source = _source_contract(registry, source_id)
    source_tier = _source_tier(contract, source_id)
    fallback_used = source_tier == "fallback"
    if fallback_from_source_id and not fallback_used:
        raise ContractError("fallback_from_source_id is only valid when a fallback source is used")
    if fallback_used:
        if not fallback_from_source_id or not fallback_reason:
            raise ContractError("fallback source requires fallback_from_source_id and fallback_reason")
        if _source_tier(contract, fallback_from_source_id) != "primary":
            raise ContractError("fallback_from_source_id must identify a primary source for the dataset")
    allowed_calendar_sources = _calendar_source_ids(registry)
    for calendar_source_id in calendar_source_ids:
        calendar_source = _source_contract(registry, calendar_source_id)
        if not calendar_source.get("official") or calendar_source_id not in allowed_calendar_sources:
            raise ContractError(f"calendar source must be an official trading-calendar route: {calendar_source_id}")
    fetched_iso = _iso_datetime(fetched_at)
    row_count = len(rows)
    payload_bytes = payload if payload is not None else canonical_json_bytes(rows)

    observed_fields = sorted({key for row in rows for key in row})
    required_fields = [
        field["name"]
        for field in contract["canonical_fields"]
        if field.get("required")
    ]
    field_violations = {
        field: [index for index, row in enumerate(rows) if field not in row]
        for field in required_fields
    }
    field_violations = {field: indexes for field, indexes in field_violations.items() if indexes}
    empty_result_is_valid = bool((contract.get("freshness") or {}).get("empty_result_is_valid"))
    absent_required = sorted(field_violations) if rows else ([] if empty_result_is_valid else required_fields)
    missing_fields_all = sorted(set(missing_fields) | set(absent_required))
    if row_count == 0 and not empty_result_is_valid:
        missing_status = "missing"
    elif missing_fields_all or missing_partitions:
        missing_status = "partial"
    else:
        missing_status = "complete"

    freshness = evaluate_freshness(
        contract,
        data_date=data_date or None,
        expected_data_date=expected_data_date,
        fetched_at=fetched_iso,
        row_count=row_count,
        fallback_used=fallback_used,
        evaluated_at=evaluated_at,
        trading_sessions=trading_sessions,
        calendar_source_ids=calendar_source_ids,
    )
    if row_count and absent_required:
        freshness["status"] = "schema_error"

    manifest = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "dataset_schema_version": contract["schema_version"],
        "source_id": source_id,
        "source_tier": source_tier,
        "source_url": source["endpoint"],
        "market_coverage": source.get("market_coverage") or [],
        "update_frequency": contract["freshness"]["frequency"],
        "data_date": data_date or None,
        "trading_date": trading_date,
        "expected_data_date": expected_data_date,
        "fetched_at": fetched_iso,
        "row_count": row_count,
        "sha256": sha256_bytes(payload_bytes),
        "fallback": {
            "used": fallback_used,
            "from_source_id": fallback_from_source_id,
            "reason": fallback_reason,
        },
        "missing": {
            "status": missing_status,
            "missing_fields": missing_fields_all,
            "missing_partitions": sorted(set(missing_partitions)),
        },
        "freshness": freshness,
        "schema_validation": {
            "status": "error" if absent_required else "ok",
            "required_fields": required_fields,
            "observed_fields": observed_fields,
            "missing_required_fields": absent_required,
            "missing_required_field_rows": field_violations,
        },
    }
    validate_manifest(manifest, registry=registry, payload=payload_bytes, rows=rows)
    return manifest


def prepare_artifact_manifest(
    payload: dict[str, Any],
    *,
    dataset_id: str,
    source_id: str,
    rows: Sequence[dict[str, Any]],
    data_date: str,
    expected_data_date: str,
    fetched_at: str | datetime,
    trading_date: str | None = None,
    fallback_from_source_id: str | None = None,
    fallback_reason: str | None = None,
    missing_fields: Sequence[str] = (),
    missing_partitions: Sequence[str] = (),
    evaluated_at: str | datetime | None = None,
    trading_sessions: Iterable[str | date] | None = None,
    calendar_source_ids: Sequence[str] = (),
    registry: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    """Attach visible freshness metadata and manifest the exact JSON bytes.

    The returned bytes are the only bytes callers should write for the artifact;
    this guarantees the manifest SHA-256 covers the published payload including
    its visible freshness field.
    """

    registry = registry or load_registry()
    preview = build_manifest(
        dataset_id,
        source_id,
        rows,
        data_date=data_date,
        expected_data_date=expected_data_date,
        fetched_at=fetched_at,
        trading_date=trading_date,
        payload=canonical_json_bytes(payload),
        fallback_from_source_id=fallback_from_source_id,
        fallback_reason=fallback_reason,
        missing_fields=missing_fields,
        missing_partitions=missing_partitions,
        evaluated_at=evaluated_at,
        trading_sessions=trading_sessions,
        calendar_source_ids=calendar_source_ids,
        registry=registry,
    )
    prepared = copy.deepcopy(payload)
    prepared["freshness"] = {
        "dataset_id": preview["dataset_id"],
        "dataset_schema_version": preview["dataset_schema_version"],
        "source_id": preview["source_id"],
        "source_tier": preview["source_tier"],
        "status": preview["freshness"]["status"],
        "data_date": preview["data_date"],
        "expected_data_date": preview["expected_data_date"],
        "fetched_at": preview["fetched_at"],
        "row_count": preview["row_count"],
        "sla": preview["freshness"]["sla"],
        "age_trading_days": preview["freshness"].get("age_trading_days"),
        "age_calendar_days": preview["freshness"].get("age_calendar_days"),
        "calendar_basis": preview["freshness"].get("calendar_basis"),
        "calendar_source_ids": preview["freshness"].get("calendar_source_ids") or [],
        "fallback": preview["fallback"],
        "missing": preview["missing"],
    }
    artifact_bytes = (json.dumps(prepared, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    manifest = build_manifest(
        dataset_id,
        source_id,
        rows,
        data_date=data_date,
        expected_data_date=expected_data_date,
        fetched_at=fetched_at,
        trading_date=trading_date,
        payload=artifact_bytes,
        fallback_from_source_id=fallback_from_source_id,
        fallback_reason=fallback_reason,
        missing_fields=missing_fields,
        missing_partitions=missing_partitions,
        evaluated_at=evaluated_at,
        trading_sessions=trading_sessions,
        calendar_source_ids=calendar_source_ids,
        registry=registry,
    )
    return prepared, manifest, artifact_bytes


def validate_manifest(
    manifest: dict[str, Any],
    *,
    registry: dict[str, Any] | None = None,
    payload: bytes | None = None,
    rows: Sequence[dict[str, Any]] | None = None,
) -> None:
    registry = registry or load_registry()
    missing_keys = REQUIRED_MANIFEST_FIELDS - set(manifest)
    if missing_keys:
        raise ContractError(f"manifest missing required fields: {sorted(missing_keys)}")
    if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ContractError("unsupported manifest_schema_version")

    contract = _dataset_contract(registry, str(manifest.get("dataset_id")))
    source = _source_contract(registry, str(manifest.get("source_id")))
    expected_tier = _source_tier(contract, str(manifest.get("source_id")))
    if manifest.get("source_tier") != expected_tier:
        raise ContractError("source_tier does not match the dataset route")
    if manifest.get("dataset_schema_version") != contract.get("schema_version"):
        raise ContractError("dataset_schema_version does not match registry")
    if manifest.get("source_url") != source.get("endpoint"):
        raise ContractError("source_url does not match registry")
    if manifest.get("market_coverage") != (source.get("market_coverage") or []):
        raise ContractError("market_coverage does not match registry")
    if manifest.get("update_frequency") != (contract.get("freshness") or {}).get("frequency"):
        raise ContractError("update_frequency does not match registry")

    _parse_date(manifest.get("expected_data_date"), "expected_data_date")
    if manifest.get("data_date"):
        _parse_date(manifest.get("data_date"), "data_date")
    if manifest.get("trading_date"):
        _parse_date(manifest.get("trading_date"), "trading_date")
    _parse_datetime(manifest.get("fetched_at"), "fetched_at")

    row_count = manifest.get("row_count")
    if not isinstance(row_count, int) or row_count < 0:
        raise ContractError("row_count must be a non-negative integer")
    if rows is not None and row_count != len(rows):
        raise ContractError(f"row_count mismatch: manifest={row_count}, observed={len(rows)}")
    digest = str(manifest.get("sha256") or "")
    if not SHA256_RE.fullmatch(digest):
        raise ContractError("sha256 must be 64 lowercase hexadecimal characters")
    if payload is not None and digest != sha256_bytes(payload):
        raise ContractError("sha256 does not match payload")

    fallback = manifest.get("fallback") or {}
    if not isinstance(fallback.get("used"), bool):
        raise ContractError("fallback.used must be boolean")
    if expected_tier == "fallback" and not fallback.get("used"):
        raise ContractError("fallback source must set fallback.used=true")
    if expected_tier == "fallback":
        fallback_from = fallback.get("from_source_id")
        if not fallback_from or not fallback.get("reason"):
            raise ContractError("fallback source must identify the failed primary and reason")
        if _source_tier(contract, str(fallback_from)) != "primary":
            raise ContractError("fallback.from_source_id must be a primary source")
    elif fallback.get("used") or fallback.get("from_source_id") or fallback.get("reason"):
        raise ContractError("primary source manifest cannot contain fallback state")
    missing = manifest.get("missing") or {}
    if missing.get("status") not in MISSING_STATUSES:
        raise ContractError("missing.status is invalid")
    freshness = manifest.get("freshness") or {}
    if freshness.get("status") not in FRESHNESS_STATUSES:
        raise ContractError("freshness.status is invalid")
    if (contract.get("freshness") or {}).get("mode") == "trading_day":
        calendar_sources = freshness.get("calendar_source_ids")
        if freshness.get("status") != "missing" and (
            freshness.get("calendar_basis") != "official_trading_sessions"
            or not isinstance(calendar_sources, list)
            or not calendar_sources
        ):
            raise ContractError("trading-day freshness must record official calendar provenance")
        for calendar_source_id in calendar_sources or []:
            if calendar_source_id not in _calendar_source_ids(registry):
                raise ContractError(
                    f"freshness calendar source is not an official trading-calendar route: {calendar_source_id}"
                )


def update_manifest_file(
    manifest: dict[str, Any],
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = registry or load_registry()
    validate_manifest(manifest, registry=registry)
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    else:
        payload = {}
    if not isinstance(payload, dict) or payload.get("manifest_schema_version") not in (None, MANIFEST_SCHEMA_VERSION):
        raise ContractError(f"existing manifest file has incompatible schema: {path}")
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        artifacts = {}
    key = f"{manifest['dataset_id']}:{manifest['source_id']}"
    artifacts[key] = manifest
    output = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": manifest["fetched_at"],
        "artifacts": dict(sorted(artifacts.items())),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    return output


def _main() -> None:
    parser = argparse.ArgumentParser(description="Validate Taiwan-stock data contracts and freshness manifests.")
    parser.add_argument("command", choices=("validate-registry", "validate-manifest"), nargs="?", default="validate-registry")
    parser.add_argument("path", nargs="?", type=Path)
    args = parser.parse_args()

    if args.command == "validate-registry":
        path = args.path or DEFAULT_REGISTRY_PATH
        registry = load_registry(path)
        print(f"[data_contract] registry OK: sources={len(registry['sources'])} datasets={len(registry['datasets'])}")
        return

    path = args.path or DEFAULT_MANIFEST_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    artifacts = payload.get("artifacts") if isinstance(payload, dict) else None
    if not isinstance(artifacts, dict):
        raise ContractError("manifest document must contain an artifacts object")
    registry = load_registry()
    for artifact in artifacts.values():
        validate_manifest(artifact, registry=registry)
    print(f"[data_contract] manifest OK: artifacts={len(artifacts)}")


if __name__ == "__main__":
    _main()
