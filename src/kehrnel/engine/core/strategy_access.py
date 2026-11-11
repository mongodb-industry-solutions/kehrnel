"""Server-owned exact-package access policy, independent of HTTP and JWT."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path, PurePosixPath
import re
from types import MappingProxyType
from typing import Mapping

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.strategy_identity import package_reference


@dataclass(frozen=True)
class PackageRef:
    id: str
    version: str
    package_digest: str

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or set(value) != {"id", "version", "packageDigest"}:
            raise ValueError("Exact package reference required")
        if any(not isinstance(value[k], str) or not value[k] or len(value[k]) > 256 for k in ("id", "version")):
            raise ValueError("Invalid package id/version")
        if not isinstance(value["packageDigest"], str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value["packageDigest"]):
            raise ValueError("Invalid package digest")
        return cls(value["id"], value["version"], value["packageDigest"])

    def as_dict(self):
        return {"id": self.id, "version": self.version, "packageDigest": self.package_digest}


@dataclass(frozen=True)
class Owner:
    kind: str
    id: str

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or set(value) != {"kind", "id"} or value["kind"] not in {"team", "individual"}:
            raise ValueError("Invalid owner scope")
        if not isinstance(value["id"], str) or not value["id"] or len(value["id"]) > 256:
            raise ValueError("Invalid owner id")
        return cls(value["kind"], value["id"])


@dataclass(frozen=True)
class Authority:
    owner: Owner | None = None
    refs: frozenset[PackageRef] = field(default_factory=frozenset)
    actions: frozenset[str] = field(default_factory=frozenset)
    environments: frozenset[str] = field(default_factory=frozenset)
    delegated: bool = False
    actor: str | None = None
    environment_id: str | None = None
    runtime_id: str | None = None


@dataclass(frozen=True)
class AccessRecord:
    ref: PackageRef
    owner: Owner | None
    visibility: str
    approved: bool
    dependencies: tuple[PackageRef, ...]
    assets: frozenset[str]


def canonical_asset_path(value):
    if not isinstance(value, str) or not value or any(c in value for c in ("\\", "%", "\x00", ":", "?", "#")):
        raise ValueError("Invalid asset path")
    path = PurePosixPath(value)
    if value.startswith("/") or any(p in {"", ".", ".."} for p in value.split("/")) or str(path) != value:
        raise ValueError("Invalid asset path")
    return value


class StrategyAccessPolicy:
    """Immutable operator configuration. Missing records are denied/private."""
    def __init__(self, records=()):
        parsed = {}
        for data in records:
            if not isinstance(data, dict) or set(data) - {"ref", "owner", "publication", "dependencies", "assets"}:
                raise ValueError("Invalid strategy access record")
            ref = PackageRef.parse(data.get("ref"))
            owner = Owner.parse(data["owner"]) if data.get("owner") is not None else None
            publication = data.get("publication") or {}
            if not isinstance(publication, dict) or set(publication) - {"visibility", "approved"}:
                raise ValueError("Invalid publication record")
            visibility = publication.get("visibility", "private")
            if visibility not in {"public", "internal", "private"}:
                raise ValueError("Invalid publication visibility")
            deps = data.get("dependencies", [])
            assets = data.get("assets", [])
            if not isinstance(deps, list) or not isinstance(assets, list):
                raise ValueError("Invalid dependency/asset declarations")
            record = AccessRecord(ref, owner, visibility, publication.get("approved") is True,
                                  tuple(PackageRef.parse(d) for d in deps), frozenset(canonical_asset_path(a) for a in assets))
            if ref in parsed:
                raise ValueError("Duplicate strategy access record")
            parsed[ref] = record
        self.records: Mapping[PackageRef, AccessRecord] = MappingProxyType(parsed)

    @classmethod
    def from_file(cls, path):
        if not path:
            return cls()
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) != {"contract", "records"} or data["contract"] != "strategy-access/v1" or not isinstance(data["records"], list):
            raise ValueError("Invalid strategy-access/v1 sidecar")
        return cls(data["records"])

    def reference(self, manifest):
        return PackageRef.parse(package_reference(manifest))

    def _current(self, registry, ref):
        manifest = registry.get_manifest(ref.id)
        try:
            if manifest is None or self.reference(manifest) != ref:
                return False
            recorded = getattr(registry, "manifest_identities", {}).get(ref.id)
            if recorded is not None and recorded != ref.as_dict():
                return False
            declared = manifest.model_dump().get("dependencies", [])
            if not isinstance(declared, list):
                return False
            record = self.records.get(ref)
            if declared and (record is None or set(PackageRef.parse(d) for d in declared) != set(record.dependencies)):
                return False
            return True
        except (ValueError, OSError, KehrnelError):
            return False

    def _public(self, registry, ref, trail=frozenset()):
        record = self.records.get(ref)
        return bool(record and ref not in trail and self._current(registry, ref)
                    and record.approved and record.visibility == "public"
                    and all(self._public(registry, dep, trail | {ref}) for dep in record.dependencies))

    def allowed(self, registry, ref, authority=Authority(), action="strategy:read", trail=frozenset()):
        record = self.records.get(ref)
        if not record or ref in trail or not self._current(registry, ref):
            return False
        # Anonymous public reads are separate from scoped use/compilation/query.
        if action == "strategy:read" and self._public(registry, ref):
            return True
        entitled = (action in authority.actions and ref in authority.refs and
                    ((record.owner is not None and record.owner == authority.owner) or
                     (record.approved and record.visibility in {"public", "internal"})))
        return bool(entitled and all(self.allowed(registry, dep, authority, action, trail | {ref}) for dep in record.dependencies))

    def require(self, registry, strategy_id, authority=Authority(), action="strategy:read"):
        manifest = registry.get_manifest(strategy_id)
        try:
            ref = self.reference(manifest) if manifest else None
        except (ValueError, OSError, KehrnelError):
            ref = None
        if ref is None or not self.allowed(registry, ref, authority, action):
            raise KehrnelError("STRATEGY_NOT_FOUND", 404, "Strategy not found")
        return manifest

    def asset(self, registry, strategy_id, asset_path, authority=Authority()):
        manifest = self.require(registry, strategy_id, authority)
        ref = self.reference(manifest)
        try:
            relative = canonical_asset_path(asset_path)
            root = Path(manifest.pack_root).resolve(strict=True)
            candidate = root / relative
            # Reject symlinks at every level, including links back into the pack.
            cursor = root
            for part in relative.split("/"):
                cursor = cursor / part
                if cursor.is_symlink():
                    raise ValueError("Symlinked asset")
            resolved = candidate.resolve(strict=True)
            if relative not in self.records[ref].assets or root not in resolved.parents or not resolved.is_file():
                raise ValueError("Undeclared asset")
            return resolved
        except (ValueError, OSError):
            raise KehrnelError("ASSET_NOT_FOUND", 404, "Asset not found") from None
