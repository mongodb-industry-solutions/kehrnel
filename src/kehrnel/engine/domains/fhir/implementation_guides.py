"""Deterministic compilation of customer FHIR implementation-guide packages.

The compiler consumes a local FHIR NPM package (directory or ``.tgz``) during
strategy activation.  It never downloads packages and it never interprets an IG
on a request path.  Its output is immutable, checksum-addressed evidence that a
customer can layer over the shared FHIR strategy without copying that strategy.

This module intentionally does not implement a FHIR validator or silently claim
support for arbitrary FHIRPath.  It inventories conformance resources and emits
candidate search paths for the simple subset; expressions outside that subset
are explicitly marked as requiring a reviewed override.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tarfile
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

COMPILER_VERSION = "fhir-ig-compiler.v1"
MAX_PACKAGE_FILES = 50_000
MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_PACKAGE_UNCOMPRESSED_BYTES = 256 * 1024 * 1024

_CONFORMANCE_TYPES = frozenset(
    {
        "CapabilityStatement",
        "CodeSystem",
        "CompartmentDefinition",
        "ConceptMap",
        "ImplementationGuide",
        "NamingSystem",
        "OperationDefinition",
        "SearchParameter",
        "StructureDefinition",
        "ValueSet",
    }
)
_SEARCH_TYPES = frozenset(
    {
        "number",
        "date",
        "string",
        "token",
        "reference",
        "composite",
        "quantity",
        "uri",
        "special",
    }
)
_SIMPLE_FHIR_PATH = re.compile(
    r"^(?P<base>[A-Z][A-Za-z0-9]*)\.(?P<path>[A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)*)$"
)
_SAFE_ID = re.compile(r"[^A-Za-z0-9._-]+")


class ImplementationGuideError(ValueError):
    """Raised when a configured IG cannot be compiled safely."""


@dataclass(frozen=True)
class _PackageContent:
    source: Path
    digest: str
    files: dict[str, bytes]


def _safe_member_name(name: str) -> str:
    normalized = str(PurePosixPath(name))
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts:
        raise ImplementationGuideError(f"Unsafe package member path: {name!r}")
    return normalized


def _directory_content(source: Path) -> _PackageContent:
    package_root = (
        source / "package"
        if (source / "package" / "package.json").is_file()
        else source
    )
    if not (package_root / "package.json").is_file():
        raise ImplementationGuideError(
            f"FHIR package.json was not found under {source}"
        )
    paths = sorted(path for path in package_root.rglob("*") if path.is_file())
    if len(paths) > MAX_PACKAGE_FILES:
        raise ImplementationGuideError(
            f"FHIR package has {len(paths)} files; limit is {MAX_PACKAGE_FILES}"
        )
    files: dict[str, bytes] = {}
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(package_root).as_posix()
        if path.stat().st_size > MAX_JSON_BYTES and relative.endswith(".json"):
            raise ImplementationGuideError(
                f"FHIR package JSON file is too large: {relative}"
            )
        data = path.read_bytes()
        files[relative] = data
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(data)
    return _PackageContent(source=source, digest=digest.hexdigest(), files=files)


def _archive_content(source: Path) -> _PackageContent:
    files: dict[str, bytes] = {}
    with tarfile.open(source, mode="r:*") as archive:
        members = [member for member in archive.getmembers() if member.isfile()]
        if len(members) > MAX_PACKAGE_FILES:
            raise ImplementationGuideError(
                f"FHIR package has {len(members)} files; limit is {MAX_PACKAGE_FILES}"
            )
        unpacked_bytes = sum(int(member.size or 0) for member in members)
        if unpacked_bytes > MAX_PACKAGE_UNCOMPRESSED_BYTES:
            raise ImplementationGuideError(
                "FHIR package expands beyond the permitted uncompressed size "
                f"({unpacked_bytes} > {MAX_PACKAGE_UNCOMPRESSED_BYTES} bytes)"
            )
        for member in members:
            name = _safe_member_name(member.name)
            relative = (
                name.removeprefix("package/") if name.startswith("package/") else name
            )
            if not relative:
                continue
            if member.size > MAX_JSON_BYTES and relative.endswith(".json"):
                raise ImplementationGuideError(
                    f"FHIR package JSON file is too large: {name}"
                )
            handle = archive.extractfile(member)
            if handle is not None:
                files[relative] = handle.read()
    if "package.json" not in files:
        raise ImplementationGuideError(f"FHIR package.json was not found in {source}")
    return _PackageContent(
        source=source,
        digest=hashlib.sha256(source.read_bytes()).hexdigest(),
        files=files,
    )


def _load_content(source: str | Path) -> _PackageContent:
    path = Path(source).resolve()
    if path.is_dir():
        return _directory_content(path)
    if path.is_file() and tarfile.is_tarfile(path):
        return _archive_content(path)
    raise ImplementationGuideError(
        f"FHIR IG source must be a package directory or tar archive: {path}"
    )


def _json_object(data: bytes, name: str) -> dict[str, Any]:
    if len(data) > MAX_JSON_BYTES:
        raise ImplementationGuideError(f"FHIR package JSON file is too large: {name}")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImplementationGuideError(
            f"Invalid JSON in FHIR package file {name}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ImplementationGuideError(
            f"FHIR package file must contain a JSON object: {name}"
        )
    return value


def _simple_search_paths(
    expression: Any, bases: Iterable[str]
) -> tuple[list[dict[str, str]], str]:
    text = str(expression or "").strip()
    if not text:
        return [], "missing-expression"
    base_set = {str(value) for value in bases if value}
    candidates: list[dict[str, str]] = []
    for raw_term in text.split("|"):
        term = raw_term.strip().strip("() ")
        match = _SIMPLE_FHIR_PATH.fullmatch(term)
        if not match or (base_set and match.group("base") not in base_set):
            return [], "reviewed-override-required"
        candidates.append({"base": match.group("base"), "path": match.group("path")})
    return candidates, "candidate"


def inspect_implementation_guide(
    source: str | Path,
    *,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Compile one local FHIR NPM package into deterministic in-memory evidence."""

    content = _load_content(source)
    if expected_sha256 and content.digest.lower() != expected_sha256.strip().lower():
        raise ImplementationGuideError(
            f"FHIR package checksum mismatch for {content.source}: expected {expected_sha256}, got {content.digest}"
        )
    package = _json_object(content.files["package.json"], "package.json")
    name = str(package.get("name") or "").strip()
    version = str(package.get("version") or "").strip()
    if not name or not version:
        raise ImplementationGuideError("FHIR package.json requires name and version")

    resources: list[tuple[str, dict[str, Any]]] = []
    invalid_files: list[dict[str, str]] = []
    for filename, data in sorted(content.files.items()):
        if filename == "package.json" or not filename.endswith(".json"):
            continue
        try:
            resource = _json_object(data, filename)
        except ImplementationGuideError as exc:
            invalid_files.append({"file": filename, "error": str(exc)})
            continue
        if isinstance(resource.get("resourceType"), str):
            resources.append((filename, resource))

    by_type: dict[str, list[dict[str, Any]]] = {}
    for filename, resource in resources:
        by_type.setdefault(str(resource["resourceType"]), []).append(
            {
                "file": filename,
                "url": resource.get("url"),
                "version": resource.get("version"),
                "id": resource.get("id"),
            }
        )

    profiles: list[dict[str, Any]] = []
    for filename, resource in resources:
        if resource.get("resourceType") != "StructureDefinition":
            continue
        profiles.append(
            {
                "file": filename,
                "id": resource.get("id"),
                "url": resource.get("url"),
                "version": resource.get("version"),
                "type": resource.get("type"),
                "kind": resource.get("kind"),
                "derivation": resource.get("derivation"),
                "base_definition": resource.get("baseDefinition"),
                "has_snapshot": isinstance(resource.get("snapshot"), dict),
                "has_differential": isinstance(resource.get("differential"), dict),
            }
        )

    search_parameters: list[dict[str, Any]] = []
    for filename, resource in resources:
        if resource.get("resourceType") != "SearchParameter":
            continue
        bases = resource.get("base") if isinstance(resource.get("base"), list) else []
        candidates, status = _simple_search_paths(resource.get("expression"), bases)
        parameter_type = str(resource.get("type") or "")
        if parameter_type not in _SEARCH_TYPES:
            status = "unsupported-search-type"
            candidates = []
        search_parameters.append(
            {
                "file": filename,
                "id": resource.get("id"),
                "url": resource.get("url"),
                "version": resource.get("version"),
                "code": resource.get("code"),
                "type": parameter_type,
                "base": sorted(str(value) for value in bases),
                "target": sorted(
                    str(value) for value in (resource.get("target") or [])
                ),
                "expression": resource.get("expression"),
                "candidate_paths": candidates,
                "compilation_status": status,
            }
        )

    fhir_versions = package.get("fhirVersions") or package.get("fhirVersion") or []
    if isinstance(fhir_versions, str):
        fhir_versions = [fhir_versions]
    dependencies = (
        package.get("dependencies")
        if isinstance(package.get("dependencies"), dict)
        else {}
    )
    compiled_id = "-".join(
        (
            _SAFE_ID.sub("-", name).strip("-") or "fhir-package",
            _SAFE_ID.sub("-", version).strip("-") or "unknown",
            content.digest[:12],
        )
    )
    return {
        "compiler_version": COMPILER_VERSION,
        "compiled_id": compiled_id,
        "package": {
            "name": name,
            "version": version,
            "canonical": package.get("canonical"),
            "fhir_versions": sorted(str(value) for value in fhir_versions),
            "dependencies": dict(
                sorted((str(k), str(v)) for k, v in dependencies.items())
            ),
            "sha256": content.digest,
        },
        "inventory": {
            "resource_count": len(resources),
            "by_resource_type": {key: value for key, value in sorted(by_type.items())},
            "profiles": sorted(
                profiles,
                key=lambda value: (str(value.get("url")), str(value.get("file"))),
            ),
            "search_parameters": sorted(
                search_parameters,
                key=lambda value: (str(value.get("code")), str(value.get("url"))),
            ),
            "compartment_definitions": by_type.get("CompartmentDefinition", []),
            "examples": [
                {
                    "file": filename,
                    "resource_type": resource.get("resourceType"),
                    "id": resource.get("id"),
                }
                for filename, resource in resources
                if resource.get("resourceType") not in _CONFORMANCE_TYPES
            ],
            "invalid_json_files": invalid_files,
        },
        "evidence": {
            "candidate_search_parameters": sum(
                1
                for item in search_parameters
                if item["compilation_status"] == "candidate"
            ),
            "manual_override_required": [
                item["url"] or item["id"] or item["file"]
                for item in search_parameters
                if item["compilation_status"] != "candidate"
            ],
            "profile_validation_enabled": False,
            "activation_ready": not invalid_files,
        },
    }


def _assert_release_compatibility(
    compilation: dict[str, Any], expected_release: str
) -> None:
    configured_release = str(expected_release or "").strip().upper()
    release_major = {"R4": "4.", "R5": "5.", "R6": "6."}.get(configured_release)
    declared_versions = compilation["package"].get("fhir_versions") or []
    if (
        release_major
        and declared_versions
        and not any(str(value).startswith(release_major) for value in declared_versions)
    ):
        raise ImplementationGuideError(
            f"FHIR package {compilation['package']['name']} does not declare "
            f"compatibility with activated release {configured_release}"
        )


def stage_implementation_guide(
    data: bytes,
    *,
    filename: str,
    environment_id: str,
    staging_root: str | Path,
    expected_release: str,
    max_upload_bytes: int,
    max_environment_bytes: int,
) -> dict[str, Any]:
    """Validate and checksum-address a user-uploaded FHIR NPM archive.

    Staging is intentionally separate from activation. The returned source can
    be reviewed and added to ``implementation_guides.packages`` voluntarily.
    """
    if not data:
        raise ImplementationGuideError("FHIR package upload is empty")
    if len(data) > max_upload_bytes:
        raise ImplementationGuideError(
            f"FHIR package upload exceeds {max_upload_bytes} bytes"
        )
    safe_filename = Path(filename or "package.tgz").name
    if not safe_filename.lower().endswith((".tgz", ".tar.gz", ".tar")):
        raise ImplementationGuideError("FHIR package upload must be a .tgz or .tar archive")

    root = Path(staging_root).resolve()
    safe_environment = _SAFE_ID.sub("-", environment_id).strip("-") or "environment"
    environment_root = root / safe_environment
    environment_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    existing_bytes = sum(
        path.stat().st_size for path in environment_root.iterdir() if path.is_file()
    )
    digest = hashlib.sha256(data).hexdigest()
    destination = environment_root / f"{digest}.tgz"
    additional_bytes = 0 if destination.exists() else len(data)
    if existing_bytes + additional_bytes > max_environment_bytes:
        raise ImplementationGuideError(
            "FHIR package staging quota exceeded for this environment"
        )

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".tgz.upload", dir=environment_root, delete=False
        ) as handle:
            handle.write(data)
            temporary_path = Path(handle.name)
        compilation = inspect_implementation_guide(temporary_path)
        _assert_release_compatibility(compilation, expected_release)
        if compilation["package"]["sha256"] != digest:
            raise ImplementationGuideError("FHIR package checksum changed during staging")
        if destination.exists():
            temporary_path.unlink(missing_ok=True)
        else:
            os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return {
        "staged_id": digest,
        "filename": safe_filename,
        "source": str(destination),
        "sha256": digest,
        "size_bytes": len(data),
        "package": compilation["package"],
        "inventory": {
            "resource_count": compilation["inventory"]["resource_count"],
            "profile_count": len(compilation["inventory"]["profiles"]),
            "search_parameter_count": len(
                compilation["inventory"]["search_parameters"]
            ),
            "invalid_json_files": compilation["inventory"]["invalid_json_files"],
        },
        "evidence": compilation["evidence"],
        "activation_entry": {
            "enabled": True,
            "source": str(destination),
            "sha256": digest,
        },
        "activated": False,
        "profiles_selected": False,
        "profile_validation_enabled": False,
    }


def write_compiled_implementation_guide(
    compilation: dict[str, Any],
    output_root: str | Path,
) -> Path:
    """Persist immutable compiler evidence under its checksum-addressed id."""

    root = Path(output_root).resolve()
    destination = root / str(compilation["compiled_id"])
    destination.mkdir(parents=True, exist_ok=True)
    outputs = {
        "package.lock.json": {
            "compiler_version": compilation["compiler_version"],
            "compiled_id": compilation["compiled_id"],
            "package": compilation["package"],
        },
        "catalog.json": compilation["inventory"],
        "search-plan.json": {
            "compiler_version": compilation["compiler_version"],
            "compiled_id": compilation["compiled_id"],
            "search_parameters": compilation["inventory"]["search_parameters"],
            "evidence": compilation["evidence"],
        },
        "compiled-package.json": compilation,
    }
    for filename, value in outputs.items():
        output = destination / filename
        serialized = (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        )
        if output.exists() and output.read_text(encoding="utf-8") != serialized:
            raise ImplementationGuideError(
                f"Compiled IG output is immutable and already differs: {output}"
            )
        output.write_text(serialized, encoding="utf-8")
    return destination / "compiled-package.json"


def compile_configured_implementation_guides(
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Compile every IG declared by a customer activation configuration."""

    ig_config = config.get("implementation_guides")
    if not isinstance(ig_config, dict):
        return []
    packages = [
        item
        for item in (ig_config.get("packages") or [])
        if not isinstance(item, dict) or item.get("enabled", True)
    ]
    if not packages:
        return []
    output_root = ig_config.get("compiled_root")
    if not isinstance(output_root, str) or not output_root.strip():
        raise ImplementationGuideError(
            "implementation_guides.compiled_root is required when packages are configured"
        )

    planned = inspect_configured_implementation_guides(config)
    results: list[dict[str, Any]] = []
    for compiled in planned:
        output = write_compiled_implementation_guide(compiled, output_root)
        results.append(
            {
                "compiled_id": compiled["compiled_id"],
                "package": compiled["package"],
                "evidence": compiled["evidence"],
                "output": str(output),
            }
        )
    return results


def inspect_configured_implementation_guides(
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Validate and inspect configured packages without writing compiler output."""

    ig_config = config.get("implementation_guides")
    if not isinstance(ig_config, dict):
        return []
    packages = [
        item
        for item in (ig_config.get("packages") or [])
        if not isinstance(item, dict) or item.get("enabled", True)
    ]
    if not packages:
        return []
    output_root = ig_config.get("compiled_root")
    if not isinstance(output_root, str) or not output_root.strip():
        raise ImplementationGuideError(
            "implementation_guides.compiled_root is required when packages are configured"
        )

    results: list[dict[str, Any]] = []
    configured_release = str(config.get("schema_version") or "").strip().upper()
    for position, item in enumerate(packages):
        if not isinstance(item, dict) or not isinstance(item.get("source"), str):
            raise ImplementationGuideError(
                f"implementation_guides.packages[{position}].source is required"
            )
        inspected = inspect_implementation_guide(
            item["source"], expected_sha256=item.get("sha256")
        )
        _assert_release_compatibility(inspected, configured_release)
        results.append(inspected)
    return results


def resolve_active_profiles(
    config: dict[str, Any],
    inspected: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Resolve an optional profile selection against configured package catalogs.

    An empty selection is deliberately valid and means that the store remains
    unconstrained by profiles.  Selection does not imply profile validation;
    that capability remains separately reported until a validator is wired.
    """

    ig_config = config.get("implementation_guides")
    if not isinstance(ig_config, dict):
        return []
    requested = ig_config.get("active_profiles") or []
    if not isinstance(requested, list):
        raise ImplementationGuideError(
            "implementation_guides.active_profiles must be an array"
        )
    requested_urls = [str(value).strip() for value in requested if str(value).strip()]
    if len(requested_urls) != len(set(requested_urls)):
        raise ImplementationGuideError(
            "implementation_guides.active_profiles cannot contain duplicates"
        )
    if not requested_urls:
        return []

    packages = inspected
    if packages is None:
        packages = inspect_configured_implementation_guides(config)
    available: dict[str, dict[str, Any]] = {}
    for package in packages:
        package_name = str((package.get("package") or {}).get("name") or "")
        for profile in (package.get("inventory") or {}).get("profiles") or []:
            url = str(profile.get("url") or "").strip()
            if url:
                if url in available:
                    raise ImplementationGuideError(
                        "Active FHIR packages define the same profile canonical URL more than once: "
                        f"{url}"
                    )
                available[url] = {**profile, "package": package_name}
    missing = sorted(set(requested_urls) - set(available))
    if missing:
        raise ImplementationGuideError(
            "Configured active profiles are not present in the enabled packages: "
            + ", ".join(missing)
        )
    return [available[url] for url in requested_urls]
