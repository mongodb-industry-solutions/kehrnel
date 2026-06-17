"""Map FHIR release labels to bundled JSON Schema files."""

from __future__ import annotations

from pathlib import Path

from fhir_gen.config import SCHEMA_PATH, V6_SCHEMA_PATH

_VERSION_ALIASES: dict[str, str] = {
    "R5": "R5",
    "5": "R5",
    "V5": "R5",
    "FHIR5": "R5",
    "R6": "R6",
    "6": "R6",
    "V6": "R6",
    "FHIR6": "R6",
}

_SCHEMA_BY_VERSION: dict[str, Path] = {
    "R5": SCHEMA_PATH,
    "R6": V6_SCHEMA_PATH,
}


def normalize_schema_version(version: str) -> str:
    key = version.strip().upper().replace("FHIR", "")
    normalized = _VERSION_ALIASES.get(key)
    if normalized is None:
        supported = ", ".join(sorted({"R5", "R6"}))
        raise ValueError(
            f"Unknown FHIR schema version {version!r}. Supported: {supported}"
        )
    return normalized


def resolve_schema_path(
    *,
    schema_version: str | None = "R5",
    schema_path: Path | str | None = None,
) -> Path:
    """
    Resolve the JSON Schema file for generation.

    ``schema_path`` wins when set (advanced override). Otherwise ``schema_version``
    selects the bundled ``fhir.schema.v5.json`` or ``fhir.schema.v6.json``.
    """
    if schema_path is not None:
        path = Path(schema_path)
        if not path.is_file():
            raise FileNotFoundError(f"Schema file not found: {path}")
        return path
    version = normalize_schema_version(schema_version or "R5")
    return _SCHEMA_BY_VERSION[version]


def supported_schema_versions() -> tuple[str, ...]:
    return ("R5", "R6")
