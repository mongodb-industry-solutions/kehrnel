"""Filesystem-backed bundle store."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from kehrnel.core.bundles import validate_bundle, compute_bundle_digest
from kehrnel.core.errors import KehrnelError


def _sanitize_bundle_id(bundle_id: str) -> str:
    return bundle_id.replace("/", "__")


class BundleStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, bundle_id: str) -> Path:
        return self.root / f"{_sanitize_bundle_id(bundle_id)}.json"

    def list_bundles(self) -> List[Dict[str, Any]]:
        bundles: List[Dict[str, Any]] = []
        for path in sorted(self.root.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                digest = compute_bundle_digest(data)
                bundles.append(
                    {
                        "bundle_id": data.get("bundle_id"),
                        "domain": data.get("domain"),
                        "kind": data.get("kind"),
                        "version": data.get("version"),
                        "digest": digest,
                    }
                )
            except Exception:
                continue
        return bundles

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        path = self._path_for(bundle_id)
        if not path.exists():
            raise KehrnelError(code="BUNDLE_NOT_FOUND", status=404, message=f"Bundle not found: {bundle_id}")
        data = json.loads(path.read_text(encoding="utf-8"))
        data["_digest"] = compute_bundle_digest(data)
        return data

    def save_bundle(self, bundle: Dict[str, Any], mode: str = "error") -> Dict[str, Any]:
        errs = validate_bundle(bundle)
        if errs:
            raise KehrnelError(code="BUNDLE_INVALID", status=400, message="Bundle validation failed", details={"errors": errs})
        bundle_id = bundle.get("bundle_id")
        path = self._path_for(bundle_id)
        if path.exists() and mode != "upsert":
            raise KehrnelError(code="BUNDLE_EXISTS", status=409, message=f"Bundle already exists: {bundle_id}")
        path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
        return {"bundle_id": bundle_id, "digest": compute_bundle_digest(bundle)}

    def delete_bundle(self, bundle_id: str) -> None:
        path = self._path_for(bundle_id)
        if not path.exists():
            raise KehrnelError(code="BUNDLE_NOT_FOUND", status=404, message=f"Bundle not found: {bundle_id}")
        path.unlink()
