"""Immutable registration identity; uses the existing package digest contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from kehrnel.contracts.package_digest import compute_package_digest
from kehrnel.engine.core.errors import KehrnelError


def manifest_only_identity_bytes(manifest) -> bytes:
    """Deterministic SDK JSON identity, separate from package canonicalization.

    JSON config/schema values may contain fractional numbers and Unicode keys.
    This encoding is local to the Python registry, not a cross-runtime package
    digest contract. Sorting object keys makes insertion order immaterial, while
    allow_nan=False retains valid JSON's restriction to finite numeric values.
    """
    return json.dumps(manifest.model_dump(mode="json"), sort_keys=True,
                      separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def registration_identity(manifest) -> dict:
    if manifest.pack_root:
        root = Path(manifest.pack_root).resolve(strict=True)
        digest = compute_package_digest(root)  # Reject symlinked layout before reading package metadata.
        manifest_path = root / "manifest.json"
        if not manifest_path.is_file():
            manifest_path = root / "specification" / "manifest.json"
        source = json.loads(manifest_path.read_text(encoding="utf-8"))
        if source.get("id") != manifest.id or source.get("version") != manifest.version:
            raise KehrnelError("STRATEGY_PACKAGE_UNRESOLVED", 422, "Installed package identity does not match its manifest")
        return {"id": manifest.id, "version": manifest.version, "packageDigest": digest}
    # SDK-created manifests remain usable in the Python registry, but this
    # manifest-only identity cannot be admitted as an HTTP package reference.
    return {"id": manifest.id, "version": manifest.version, "packageDigest": None,
            "manifestContentDigest": "sha256:" + hashlib.sha256(manifest_only_identity_bytes(manifest)).hexdigest()}


def package_reference(manifest) -> dict:
    ref = registration_identity(manifest)
    if not ref["packageDigest"]:
        raise KehrnelError("STRATEGY_PACKAGE_UNRESOLVED", 422, "Installed package provenance is required")
    return ref
