"""Pure integrity loader for private operator-authored design-reference inputs.

No default record, entrypoint execution, HTTP, identity-provider or activation
is involved. Integrity authenticates configured bytes, not capability truth;
the operator is responsible for reviewing the referenced evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.architecture_foundation import (
    ArchitectureResolutionInputCatalog, SemanticExecutionBindingV1,
    TargetCapabilityProfile,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import SourceProfileCatalog
from kehrnel.engine.core.source_profiles import load_source_profile_catalog
from kehrnel.engine.core.strategy_access import Owner, PackageRef


class ArchitectureInputCatalogError(ValueError):
    """Unavailable or invalid operator inputs; deliberately bounded text."""


@dataclass(frozen=True)
class ArchitectureInputRecord:
    artifact_ref: ArtifactRef
    owner: Owner
    required_package_refs: tuple[PackageRef, ...]
    source_profile_refs: tuple[ArtifactRef, ...]
    inputs: ArchitectureResolutionInputCatalog
    evidence_refs: tuple[ArtifactRef, ...]

    def to_wire(self):
        return {
            "artifactRef": self.artifact_ref.to_wire(),
            "owner": {"kind": self.owner.kind, "id": self.owner.id},
            "requiredPackageRefs": [ref.as_dict() for ref in self.required_package_refs],
            "sourceProfileRefs": [ref.to_wire() for ref in self.source_profile_refs],
            "evidenceBasis": "design_reference",
            "evidenceRefs": [ref.to_wire() for ref in self.evidence_refs],
            **{k: v for k, v in self.inputs.to_wire().items() if k != "contract"},
        }


def _digest(payload):
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def target_capability_content_digest(value: TargetCapabilityProfile | dict) -> str:
    """Maintained target seed convention: exclude artifactRef and contract."""
    try:
        model = value if isinstance(value, TargetCapabilityProfile) else TargetCapabilityProfile.model_validate(value)
        return _digest({k: v for k, v in model.to_wire().items() if k not in {"artifactRef", "contract"}})
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ArchitectureInputCatalogError("Architecture inputs unavailable") from exc


def semantic_binding_content_digest(value: SemanticExecutionBindingV1 | dict) -> str:
    """Binding convention: exclude artifactRef; retain contract and all links."""
    try:
        model = value if isinstance(value, SemanticExecutionBindingV1) else SemanticExecutionBindingV1.model_validate(value)
        return _digest({k: v for k, v in model.to_wire().items() if k != "artifactRef"})
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ArchitectureInputCatalogError("Architecture inputs unavailable") from exc


def _refs(raw, key, parser):
    values = raw[key]
    if not isinstance(values, list) or not values:
        raise ValueError("Nonempty exact references required")
    refs = tuple(parser(v) for v in values)
    if len(refs) != len(set(refs)):
        raise ValueError("Duplicate reference")
    return refs


def _record(raw) -> ArchitectureInputRecord:
    if not isinstance(raw, dict) or set(raw) != {
        "artifactRef", "owner", "requiredPackageRefs", "sourceProfileRefs",
        "evidenceBasis", "evidenceRefs", "targetCapabilityProfiles", "semanticExecutionBindings",
    } or raw["evidenceBasis"] != "design_reference":
        raise ValueError("Closed design-reference record required")
    ref = ArtifactRef.model_validate(raw["artifactRef"])
    if ref.kind != "architecture-input-record":
        raise ValueError("Invalid record reference kind")
    profiles = _refs(raw, "sourceProfileRefs", ArtifactRef.model_validate)
    if any(ref.kind != "source-profile" for ref in profiles):
        raise ValueError("Invalid profile reference kind")
    return ArchitectureInputRecord(
        ref, Owner.parse(raw["owner"]), _refs(raw, "requiredPackageRefs", PackageRef.parse), profiles,
        ArchitectureResolutionInputCatalog.model_validate({
            "contract": "architecture-resolution-input-catalog/v1",
            "targetCapabilityProfiles": raw["targetCapabilityProfiles"],
            "semanticExecutionBindings": raw["semanticExecutionBindings"],
        }), _refs(raw, "evidenceRefs", ArtifactRef.model_validate),
    )


def architecture_input_record_content_digest(value: ArchitectureInputRecord | dict) -> str:
    """Normalize the complete private record, excluding only its own ref."""
    try:
        model = value if isinstance(value, ArchitectureInputRecord) else _record(value)
        return _digest({k: v for k, v in model.to_wire().items() if k != "artifactRef"})
    except (ValueError, TypeError, UnicodeError, KeyError) as exc:
        raise ArchitectureInputCatalogError("Architecture inputs unavailable") from exc


def _operator_path(path, forbidden_roots):
    if not path:
        raise ValueError("Explicit operator path required")
    candidate = Path(path).absolute()
    if any(p.is_symlink() for p in (candidate, *candidate.parents)):
        raise ValueError("Symlinked operator path")
    resolved = candidate.resolve(strict=True)
    roots = (Path(__file__).resolve().parents[2], *(Path(p).resolve(strict=True) for p in forbidden_roots))
    if not resolved.is_file() or any(resolved == root or root in resolved.parents for root in roots):
        raise ValueError("Operator file must be outside executable package roots")
    return resolved


def load_architecture_resolution_context(
    path: str | Path | None, *, forbidden_roots: Iterable[str | Path] = (),
    source_profiles: SourceProfileCatalog | None = None,
) -> tuple[ArchitectureInputRecord, ...]:
    """Load explicit private records; an empty envelope grants no inputs.

    Supply every externally installed executable pack root in forbidden_roots.
    The SDK's own package root is always excluded. No request-selected path or
    bundled qualification is used. API authority is applied separately.
    """
    try:
        file = _operator_path(path, forbidden_roots)
        raw = json.loads(file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or set(raw) != {"contract", "records"} or raw["contract"] != "architecture-resolution-context/v1" or not isinstance(raw["records"], list):
            raise ValueError("Invalid context envelope")
        records = tuple(_record(v) for v in raw["records"])
        profiles = source_profiles if source_profiles is not None else load_source_profile_catalog()
        # Verify profile content even when the caller supplies models directly.
        from kehrnel.engine.core.source_profiles import source_profile_content_digest
        if any(p.artifact_ref.digest != source_profile_content_digest(p) for p in profiles.profiles):
            raise ValueError("Invalid source profile body")
        known_profiles = {p.artifact_ref for p in profiles.profiles}
        immutable = {}
        record_refs = set()
        for record in records:
            if record.artifact_ref in record_refs or record.artifact_ref.digest != architecture_input_record_content_digest(record):
                raise ValueError("Duplicate or altered record")
            record_refs.add(record.artifact_ref)
            if not set(record.source_profile_refs) <= known_profiles:
                raise ValueError("Unknown full profile reference")
            package_artifacts = {ArtifactRef(kind="strategy", id=p.id, version=p.version, digest=p.package_digest) for p in record.required_package_refs}
            definitions = [record, *record.inputs.target_capability_profiles, *record.inputs.semantic_execution_bindings]
            for definition in definitions:
                ref = definition.artifact_ref
                key = (ref.kind, ref.id, ref.version)
                body = definition.to_wire()
                if key in immutable and immutable[key] != body:
                    raise ValueError("Conflicting immutable identity")
                immutable[key] = body
            for target in record.inputs.target_capability_profiles:
                if target.artifact_ref.digest != target_capability_content_digest(target):
                    raise ValueError("Altered target")
            for binding in record.inputs.semantic_execution_bindings:
                if binding.artifact_ref.digest != semantic_binding_content_digest(binding) or binding.strategy_ref not in package_artifacts:
                    raise ValueError("Altered or unassociated binding")
                if not {binding.semantic_authority_ref, *(c.component_ref for c in binding.consumers)} <= set(record.evidence_refs):
                    raise ValueError("Missing authority/component evidence references")
        return records
    except (OSError, ValueError, TypeError, UnicodeError, KeyError, RuntimeError) as exc:
        raise ArchitectureInputCatalogError("Architecture inputs unavailable") from exc
