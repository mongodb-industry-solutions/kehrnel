"""Deterministic content identity for strategy packages.

``packageDigest`` identifies the canonical file manifest, not a mutable source
checkout and not an HTTP representation. All distributable regular files are
included, including presentation metadata. Only explicitly listed VCS,
cache, build and temporary artifacts are ignored.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EXCLUDED_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".ruff_cache",
        ".svn",
        ".tox",
        "__pycache__",
        "build",
        "dist",
    }
)
EXCLUDED_FILE_NAMES = frozenset({".DS_Store"})
EXCLUDED_SUFFIXES = frozenset({".pyc", ".pyo", ".swp", ".tmp"})
FORBIDDEN_SECRET_NAMES = frozenset(
    {
        ".env",
        ".env.local",
        ".env.production",
        ".env.staging",
        "credentials.json",
        "secrets.json",
    }
)
PACKAGE_EXCLUSIONS = sorted(
    [
        *(f"directory:{name}" for name in EXCLUDED_DIRECTORY_NAMES),
        *(f"file:{name}" for name in EXCLUDED_FILE_NAMES),
        *(f"suffix:{name}" for name in EXCLUDED_SUFFIXES),
        "directory-suffix:.egg-info",
    ]
)


class PackageLayoutError(ValueError):
    """The package cannot be assigned a trusted deterministic digest."""


class PackageFileEntry(BaseModel):
    path: str = Field(
        min_length=1,
        pattern=r"^[^/\\]+(?:/[^/\\]+)*$",
        description="NFC-normalized relative POSIX path without dot segments.",
        json_schema_extra={
            "pattern": (
                r"^(?!/)(?![A-Za-z]:/)(?!\.{1,2}(?:/|$))"
                r"(?!.*\/\.{1,2}(?:/|$))[^/\\]+(?:/[^/\\]+)*$"
            )
        },
    )
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("path")
    @classmethod
    def path_must_be_canonical_relative_posix(cls, value: str) -> str:
        normalized = unicodedata.normalize("NFC", value)
        parts = value.split("/")
        if (
            not value
            or value != normalized
            or value.startswith("/")
            or "\\" in value
            or any(part in {"", ".", ".."} for part in parts)
            or (parts and parts[0].endswith(":"))
        ):
            raise ValueError("path must be an NFC-normalized relative POSIX path")
        return value


class PackageFileManifest(BaseModel):
    contract: Literal["strategy-package-file-manifest/v1"] = "strategy-package-file-manifest/v1"
    scope: Literal["strategy-directory/v1"] = "strategy-directory/v1"
    hash_algorithm: Literal["sha256"] = Field(default="sha256", alias="hashAlgorithm")
    path_encoding: Literal["utf-8-nfc-posix"] = Field(
        default="utf-8-nfc-posix", alias="pathEncoding"
    )
    byte_policy: Literal["exact"] = Field(default="exact", alias="bytePolicy")
    presentation_metadata: Literal["included"] = Field(
        default="included", alias="presentationMetadata"
    )
    exclusions: list[str] = Field(
        default_factory=lambda: list(PACKAGE_EXCLUSIONS)
    )
    files: list[PackageFileEntry]

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="after")
    def policy_and_file_order_are_canonical(self) -> "PackageFileManifest":
        if self.exclusions != PACKAGE_EXCLUSIONS:
            raise ValueError("exclusions must match the strategy-directory/v1 policy")
        paths = [entry.path for entry in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("file paths must be unique")
        if paths != sorted(paths, key=lambda value: value.encode("utf-8")):
            raise ValueError("file paths must be sorted by UTF-8 bytes")
        return self

    @classmethod
    def model_json_schema(cls, *args, **kwargs) -> dict[str, Any]:
        schema = super().model_json_schema(*args, **kwargs)
        properties = schema.get("properties", {})
        exclusions = properties.get("exclusions")
        if isinstance(exclusions, dict):
            exclusions["const"] = PACKAGE_EXCLUSIONS
        files = properties.get("files")
        if isinstance(files, dict):
            files["uniqueItems"] = True
            files["description"] = (
                "Entries are unique by path and sorted by UTF-8 path bytes; "
                "use the contract verifier for these semantic constraints."
            )
        return schema


def canonical_json_bytes(value: BaseModel | Mapping[str, Any] | Sequence[Any]) -> bytes:
    """Return RFC 8785-compatible bytes for the contract's JSON subset.

    Package manifests contain only objects, arrays, strings and integers, so
    Python's sorted compact encoding is equivalent to RFC 8785 for these
    values. ``allow_nan=False`` prevents non-I-JSON numeric values.
    """

    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", by_alias=True, exclude_none=True)
    _assert_canonical_subset(value)
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _assert_canonical_subset(value: Any, path: str = "$") -> None:
    """Constrain inputs to the RFC 8785 subset used by Kehrnel contracts.

    Object keys are ASCII and numeric values are integers. Unicode is allowed
    in values. This removes the cross-runtime key-order and floating-point
    ambiguities that otherwise require a dedicated JCS implementation.
    """

    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        raise TypeError(f"floating-point values are outside the canonical subset at {path}")
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise TypeError(f"object keys must be ASCII strings at {path}")
            _assert_canonical_subset(child, f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _assert_canonical_subset(child, f"{path}[{index}]")
        return
    raise TypeError(f"unsupported canonical JSON value at {path}: {type(value).__name__}")


def _is_excluded(relative: Path) -> bool:
    if any(
        part in EXCLUDED_DIRECTORY_NAMES or part.endswith(".egg-info")
        for part in relative.parts[:-1]
    ):
        return True
    name = relative.name
    return name in EXCLUDED_FILE_NAMES or relative.suffix.lower() in EXCLUDED_SUFFIXES


def _iter_package_files(root: Path) -> Iterable[tuple[str, Path]]:
    def raise_walk_error(error: OSError) -> None:
        location = error.filename or str(root)
        raise PackageLayoutError(f"cannot inspect package path {location}: {error}") from error

    seen_paths: set[str] = set()
    for current, directory_names, file_names in os.walk(
        root,
        topdown=True,
        followlinks=False,
        onerror=raise_walk_error,
    ):
        current_path = Path(current)
        retained_directories: list[str] = []
        for name in sorted(directory_names):
            candidate = current_path / name
            relative = candidate.relative_to(root)
            folded_name = name.casefold()
            if candidate.is_symlink():
                raise PackageLayoutError(f"symlink is not allowed: {relative.as_posix()}")
            if folded_name in FORBIDDEN_SECRET_NAMES or (
                folded_name.startswith(".env") and folded_name != ".env.example"
            ):
                raise PackageLayoutError(
                    f"secret-like directory is not allowed: {relative.as_posix()}"
                )
            if name in EXCLUDED_DIRECTORY_NAMES or name.endswith(".egg-info"):
                continue
            retained_directories.append(name)
        directory_names[:] = retained_directories

        for name in sorted(file_names):
            candidate = current_path / name
            relative = candidate.relative_to(root)
            relative_posix = unicodedata.normalize("NFC", relative.as_posix())
            folded_name = name.casefold()
            if folded_name in FORBIDDEN_SECRET_NAMES or (
                folded_name.startswith(".env") and folded_name != ".env.example"
            ):
                raise PackageLayoutError(f"secret-like file is not allowed: {relative_posix}")
            if candidate.is_symlink():
                raise PackageLayoutError(f"symlink is not allowed: {relative_posix}")
            if _is_excluded(relative):
                continue
            try:
                mode = candidate.stat().st_mode
            except OSError as exc:
                raise PackageLayoutError(
                    f"cannot inspect package file {relative_posix}: {exc}"
                ) from exc
            if not stat.S_ISREG(mode):
                raise PackageLayoutError(f"special file is not allowed: {relative_posix}")
            if relative_posix in seen_paths:
                raise PackageLayoutError(f"normalized path collision: {relative_posix}")
            seen_paths.add(relative_posix)
            yield relative_posix, candidate


def validate_package_layout(pack_root: str | Path) -> tuple[tuple[str, Path], ...]:
    """Validate the complete package tree before any package file is opened."""

    root = Path(pack_root)
    if root.is_symlink():
        raise PackageLayoutError("package root must not be a symlink")
    if not root.exists() or not root.is_dir():
        raise PackageLayoutError(f"package root is not a directory: {root}")
    return tuple(_iter_package_files(root))


def build_package_file_manifest(pack_root: str | Path) -> PackageFileManifest:
    root = Path(pack_root)
    # Two passes are intentional: reject every symlink, secret-like path and
    # special file before opening even the first package file.
    validated_files = validate_package_layout(root)
    entries: list[PackageFileEntry] = []
    for relative_path, path in validated_files:
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise PackageLayoutError(
                f"cannot read package file {relative_path}: {exc}"
            ) from exc
        entries.append(
            PackageFileEntry(
                path=relative_path,
                size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    entries.sort(key=lambda item: item.path.encode("utf-8"))
    return PackageFileManifest(files=entries)


def compute_package_digest(pack_root: str | Path) -> str:
    manifest = build_package_file_manifest(pack_root)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
