"""FHIR temporal normalization shared by denormalization and query compilation.

FHIR JSON keeps ``date``, ``dateTime`` and ``instant`` values as strings.  MongoDB
cannot compare those strings to BSON dates, and lexical comparison is not safe
across precision and timezone variants.  Kehrnel therefore keeps the canonical
FHIR value untouched and writes an internal, indexed interval projection under
``_search._dates``.

Intervals are half-open: ``start`` is inclusive and ``end`` is exclusive.  This
preserves FHIR's implicit precision (a year, month, day, minute, second, etc.)
without losing information in the canonical resource.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from typing import Any, Iterable

from dateutil import parser as date_parser


DATE_PROJECTION_ROOT = "_search._dates"
DATE_PROJECTION_INDEX_NAME = "idx_search_dates_wildcard"

_YEAR = re.compile(r"^\d{4}$")
_MONTH = re.compile(r"^\d{4}-\d{2}$")
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def date_projection_field(parameter_name: str) -> str:
    """Return the stable Mongo path for one FHIR date search parameter."""

    return f"{DATE_PROJECTION_ROOT}.{parameter_name}"


def date_projection_index_spec() -> dict[str, Any]:
    """One wildcard index covers every date parameter for a resource type."""

    return {
        "fields": {f"{DATE_PROJECTION_ROOT}.$**": 1},
        "options": {"name": DATE_PROJECTION_INDEX_NAME},
    }


def has_date_search_parameters(config: dict[str, Any]) -> bool:
    return any(
        isinstance(spec, dict) and spec.get("type") in {"date", "datetime"}
        for spec in (config.get("search_parameters") or {}).values()
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_fhir_temporal_range(value: Any) -> tuple[datetime, datetime]:
    """Parse a FHIR temporal primitive into an inclusive/exclusive UTC range."""

    if isinstance(value, datetime):
        start = _utc(value)
        return start, start + timedelta(milliseconds=1)
    if isinstance(value, date):
        start = datetime(value.year, value.month, value.day, tzinfo=UTC)
        return start, start + timedelta(days=1)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("FHIR temporal value must be a non-empty string or date")

    raw = value.strip()
    if _YEAR.fullmatch(raw):
        year = int(raw)
        start = datetime(year, 1, 1, tzinfo=UTC)
        end = datetime(year + 1, 1, 1, tzinfo=UTC) if year < 9999 else datetime.max.replace(tzinfo=UTC)
        return start, end
    if _MONTH.fullmatch(raw):
        year, month = (int(part) for part in raw.split("-"))
        start = datetime(year, month, 1, tzinfo=UTC)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            end = datetime(year, month + 1, 1, tzinfo=UTC)
        return start, end
    if _DAY.fullmatch(raw):
        parsed = date.fromisoformat(raw)
        start = datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
        return start, start + timedelta(days=1)

    parsed = _utc(date_parser.isoparse(raw))
    time_part = raw.split("T", 1)[1] if "T" in raw else ""
    time_without_zone = re.split(r"Z|[+-]\d{2}:?\d{2}$", time_part, maxsplit=1)[0]
    if "." in time_without_zone:
        # BSON dates have millisecond precision.  A more precise FHIR instant is
        # represented by the smallest interval MongoDB can persist faithfully.
        step = timedelta(milliseconds=1)
    elif time_without_zone.count(":") >= 2:
        step = timedelta(seconds=1)
    elif time_without_zone.count(":") == 1:
        step = timedelta(minutes=1)
    else:
        step = timedelta(hours=1)
    return parsed, parsed + step


def _path_values(value: Any, dotted_path: str) -> list[Any]:
    """Resolve a dotted Mongo-style path through objects and arrays."""

    parts = [part for part in dotted_path.split(".") if part]

    def walk(current: Any, index: int) -> list[Any]:
        if index == len(parts):
            if isinstance(current, list):
                return [item for item in current if item is not None]
            return [] if current is None else [current]
        if isinstance(current, list):
            values: list[Any] = []
            for item in current:
                values.extend(walk(item, index))
            return values
        if not isinstance(current, dict) or parts[index] not in current:
            return []
        return walk(current[parts[index]], index + 1)

    return walk(value, 0)


def _field_specs(spec: dict[str, Any]) -> Iterable[dict[str, Any]]:
    fields = spec.get("fields") or []
    if isinstance(fields, dict):
        fields = fields.get("default") or []
    if not isinstance(fields, list):
        fields = [fields]
    for field in fields:
        if isinstance(field, str):
            yield {"field": field, "type": "date"}
        elif isinstance(field, dict) and field.get("field"):
            yield field


def _period_range(value: Any) -> tuple[datetime, datetime] | None:
    if not isinstance(value, dict):
        return None
    low = datetime.min.replace(tzinfo=UTC)
    high = datetime.max.replace(tzinfo=UTC)
    if value.get("start") is not None:
        low = parse_fhir_temporal_range(value["start"])[0]
    if value.get("end") is not None:
        high = parse_fhir_temporal_range(value["end"])[1]
    if low >= high:
        return None
    return low, high


def build_date_projections(
    document: dict[str, Any], config: dict[str, Any]
) -> dict[str, list[dict[str, datetime]]]:
    """Build date interval projections from an already-denormalized document."""

    projections: dict[str, list[dict[str, datetime]]] = {}
    for parameter_name, spec in (config.get("search_parameters") or {}).items():
        if not isinstance(spec, dict) or spec.get("type") not in {"date", "datetime"}:
            continue
        intervals: list[dict[str, datetime]] = []
        seen: set[tuple[datetime, datetime]] = set()
        for field_spec in _field_specs(spec):
            field_name = str(field_spec["field"])
            for raw in _path_values(document, field_name):
                try:
                    interval = (
                        _period_range(raw)
                        if field_spec.get("type") == "period"
                        else parse_fhir_temporal_range(raw)
                    )
                except (TypeError, ValueError, OverflowError):
                    continue
                if interval is None or interval in seen:
                    continue
                seen.add(interval)
                intervals.append({"start": interval[0], "end": interval[1]})
        if intervals:
            projections[str(parameter_name)] = intervals
    return projections
