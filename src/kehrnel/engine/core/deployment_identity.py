"""Exact installed package identity for the stateless deployment compiler."""
import hashlib
import json
from pathlib import Path
from kehrnel.contracts.package_digest import canonical_json_bytes, compute_package_digest
from kehrnel.engine.core.errors import KehrnelError

SUPPORTED_STRATEGIES = {"openehr.rps_dual", "openehr.rps_dual_ibm"}
RPS_ROOT = Path(__file__).resolve().parents[1] / "strategies/openehr/rps_dual"

def content_digest(value):
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()

def compute_manifest_digest(manifest):
    payload = {"id": manifest.id, "version": manifest.version, "entrypoint": manifest.entrypoint,
               "domain": manifest.domain, "capabilities": sorted(manifest.capabilities or []),
               "ops": sorted([{"name": op.name, "kind": op.kind} for op in manifest.ops or []], key=lambda item: item["name"])}
    if getattr(manifest, "pack_format", None): payload["pack_format"] = manifest.pack_format
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def strategy_package_root(manifest):
    if manifest.id not in SUPPORTED_STRATEGIES:
        raise ValueError("Unsupported deployment strategy")
    if not manifest.pack_root:
        raise KehrnelError(code="DEPLOYMENT_PACKAGE_UNRESOLVED", status=422,
                           message="Reload the strategy from its installed package before exporting")
    root = Path(manifest.pack_root).resolve()
    source = root / "manifest.json"
    if not source.is_file() or json.loads(source.read_text()).get("id") != manifest.id:
        raise KehrnelError(code="DEPLOYMENT_PACKAGE_UNRESOLVED", status=422,
                           message="Loaded strategy package provenance is unavailable or mismatched")
    return root

def package_identity(manifest):
    return {"packageDigest": compute_package_digest(strategy_package_root(manifest)),
            "manifestDigest": compute_manifest_digest(manifest)}
