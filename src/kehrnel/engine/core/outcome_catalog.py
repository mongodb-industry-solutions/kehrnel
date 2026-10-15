"""Load and integrity-check the industry outcome catalog.

The catalog is a portfolio and discovery surface, not an execution shortcut.
Its reference architectures may point at executable Kehrnel artifacts, so the
loader resolves those references exactly and fails closed when they become
stale.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.outcome_catalog import (
    ArchitectureCapabilityDefinition,
    DataProductDefinition,
    DemoExperience,
    IndustryOutcomeCatalog,
    OutcomePattern,
    PortfolioDomainDefinition,
    ReferenceArchitecture,
    VisualPatternDefinition,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.blueprint_templates import (
    BlueprintTemplateCatalogError,
    assert_blueprint_template_refs,
)
from kehrnel.engine.core.mappings import load_mapping_catalog
from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy_manifest,
)
from kehrnel.engine.core.source_categories import load_source_category_catalog
from kehrnel.engine.core.source_profiles import load_source_profile_catalog


DEFAULT_OUTCOME_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "outcome_catalog" / "catalog.json"
)
DEFAULT_STRATEGY_ROOT = Path(__file__).resolve().parents[1] / "strategies"

CatalogArtifact = (
    PortfolioDomainDefinition
    | DataProductDefinition
    | ArchitectureCapabilityDefinition
    | VisualPatternDefinition
    | OutcomePattern
    | ReferenceArchitecture
    | DemoExperience
    | IndustryOutcomeCatalog
)


class IndustryOutcomeCatalogError(ValueError):
    """The outcome catalog is malformed or contains stale artifact links."""


def outcome_catalog_artifact_digest(
    artifact: CatalogArtifact | dict[str, Any],
) -> str:
    """Digest an atomic catalog artifact without its self-referential digest."""

    if isinstance(
        artifact,
        (
            PortfolioDomainDefinition,
            DataProductDefinition,
            ArchitectureCapabilityDefinition,
            VisualPatternDefinition,
            OutcomePattern,
            ReferenceArchitecture,
            DemoExperience,
            IndustryOutcomeCatalog,
        ),
    ):
        payload = artifact.to_wire()
    else:
        payload = deepcopy(artifact)
    artifact_ref = payload.get("artifactRef") or payload.get("artifact_ref")
    if not isinstance(artifact_ref, dict):
        raise IndustryOutcomeCatalogError("catalog artifact is missing artifactRef")
    artifact_ref.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _exact_ref_key(ref: Any) -> tuple[str, str, str, str]:
    return (ref.kind, ref.id, ref.version, ref.digest)


def _assert_atomic_digests(catalog: IndustryOutcomeCatalog) -> None:
    for artifact in (
        *catalog.domains,
        *catalog.data_products,
        *catalog.architecture_capabilities,
        *catalog.visual_patterns,
        *catalog.outcomes,
        *catalog.reference_architectures,
        *catalog.demo_experiences,
    ):
        expected = outcome_catalog_artifact_digest(artifact)
        if artifact.artifact_ref.digest != expected:
            raise IndustryOutcomeCatalogError(
                "outcome catalog digest mismatch for "
                f"{artifact.artifact_ref.kind}:{artifact.artifact_ref.id}"
                f"@{artifact.artifact_ref.version}"
            )
    expected_catalog_digest = outcome_catalog_artifact_digest(catalog)
    if catalog.artifact_ref.digest != expected_catalog_digest:
        raise IndustryOutcomeCatalogError(
            "outcome catalog digest mismatch for "
            f"{catalog.artifact_ref.id}@{catalog.artifact_ref.version}"
        )


def assert_outcome_catalog_links(
    catalog: IndustryOutcomeCatalog,
    *,
    strategy_root: str | Path = DEFAULT_STRATEGY_ROOT,
    blueprint_templates: tuple[Any, ...] | None = None,
) -> None:
    """Resolve every Kehrnel-owned external reference used by an architecture."""

    source_categories = load_source_category_catalog()
    source_category_ids = {
        category.artifact_ref.id for category in source_categories.categories
    }
    source_category_refs = {
        _exact_ref_key(category.artifact_ref)
        for category in source_categories.categories
    }
    requested_category_ids = {
        category_id
        for outcome in catalog.outcomes
        for category_id in outcome.source_category_ids
    }
    missing_categories = sorted(requested_category_ids - source_category_ids)
    if missing_categories:
        raise IndustryOutcomeCatalogError(
            f"outcome catalog references unknown source categories: {missing_categories}"
        )

    requested_category_refs = {
        _exact_ref_key(reference)
        for architecture in catalog.reference_architectures
        for reference in architecture.source_category_refs
    }
    missing_category_refs = sorted(requested_category_refs - source_category_refs)
    if missing_category_refs:
        raise IndustryOutcomeCatalogError(
            "outcome catalog contains stale source category refs: "
            f"{missing_category_refs}"
        )

    evidence_refs = []
    for outcome in catalog.outcomes:
        evidence_refs.extend(outcome.evidence_refs)
    for architecture in catalog.reference_architectures:
        evidence_refs.extend(architecture.evidence_refs)
        evidence_refs.extend(architecture.semantic_asset_refs)
    for experience in catalog.demo_experiences:
        evidence_refs.extend(experience.evidence_refs)
        for step in experience.storyboard_steps:
            evidence_refs.extend(step.evidence_refs)

    requested_profile_refs = {
        _exact_ref_key(reference)
        for architecture in catalog.reference_architectures
        for reference in architecture.source_profile_refs
    }
    requested_profile_refs.update(
        _exact_ref_key(reference)
        for reference in evidence_refs
        if reference.kind == "source-profile"
    )
    profile_by_ref = {}
    if requested_profile_refs:
        profile_catalog = load_source_profile_catalog()
        profile_by_ref = {
            _exact_ref_key(profile.artifact_ref): profile
            for profile in profile_catalog.profiles
        }
        profile_refs = set(profile_by_ref)
        missing_profiles = sorted(requested_profile_refs - profile_refs)
        if missing_profiles:
            raise IndustryOutcomeCatalogError(
                f"outcome catalog contains stale source profile refs: {missing_profiles}"
            )

    requested_mapping_refs = {
        _exact_ref_key(reference)
        for architecture in catalog.reference_architectures
        for reference in architecture.mapping_refs
    }
    requested_mapping_refs.update(
        _exact_ref_key(reference)
        for reference in evidence_refs
        if reference.kind == "mapping"
    )
    mapping_by_ref = {}
    if requested_mapping_refs:
        mapping_catalog = load_mapping_catalog()
        mapping_by_ref = {
            _exact_ref_key(mapping.artifact_ref): mapping
            for mapping in mapping_catalog.mappings
        }
        mapping_refs = set(mapping_by_ref)
        missing_mappings = sorted(requested_mapping_refs - mapping_refs)
        if missing_mappings:
            raise IndustryOutcomeCatalogError(
                f"outcome catalog contains stale mapping refs: {missing_mappings}"
            )

    requested_strategy_refs = {
        _exact_ref_key(reference)
        for architecture in catalog.reference_architectures
        for reference in architecture.strategy_refs
    }
    requested_strategy_refs.update(
        _exact_ref_key(reference)
        for reference in evidence_refs
        if reference.kind == "strategy"
    )
    strategy_by_ref = {}
    if requested_strategy_refs:
        for manifest_path in iter_strategy_manifest_paths(strategy_root):
            try:
                manifest = load_strategy_manifest(manifest_path)
            except Exception:
                # Discovery may contain unrelated preview or locally-authored packs.
                # A broken pack must only make this catalog unavailable when one of
                # its exact references remains unresolved below.
                continue
            if manifest.package_digest:
                key = (
                    "strategy",
                    manifest.id,
                    manifest.version,
                    manifest.package_digest,
                )
                strategy_by_ref[key] = manifest
        installed_strategy_refs = set(strategy_by_ref)
        missing_strategies = sorted(
            requested_strategy_refs - installed_strategy_refs
        )
        if missing_strategies:
            raise IndustryOutcomeCatalogError(
                f"outcome catalog contains stale strategy refs: {missing_strategies}"
            )

    for architecture in catalog.reference_architectures:
        architecture_categories = {
            reference.id for reference in architecture.source_category_refs
        }
        architecture_profiles = {
            _exact_ref_key(reference) for reference in architecture.source_profile_refs
        }
        architecture_strategies = {
            reference.id for reference in architecture.strategy_refs
        }

        for profile_ref in architecture.source_profile_refs:
            profile = profile_by_ref[_exact_ref_key(profile_ref)]
            if not architecture_categories.intersection(profile.category_ids):
                raise IndustryOutcomeCatalogError(
                    f"reference architecture '{architecture.artifact_ref.id}' "
                    f"source profile '{profile_ref.id}' does not match any declared "
                    "source category"
                )

        for mapping_ref in architecture.mapping_refs:
            mapping = mapping_by_ref[_exact_ref_key(mapping_ref)]
            source_key = _exact_ref_key(mapping.source.profile_ref)
            if source_key not in architecture_profiles:
                raise IndustryOutcomeCatalogError(
                    f"reference architecture '{architecture.artifact_ref.id}' mapping "
                    f"'{mapping_ref.id}' requires undeclared source profile "
                    f"'{mapping.source.profile_ref.id}'"
                )
            if not architecture_strategies.intersection(mapping.target.strategy_ids):
                raise IndustryOutcomeCatalogError(
                    f"reference architecture '{architecture.artifact_ref.id}' mapping "
                    f"'{mapping_ref.id}' has no compatible declared target strategy"
                )

        consumption_kinds = {kind.value for kind in architecture.consumption_kinds}
        for strategy_ref in architecture.strategy_refs:
            manifest = strategy_by_ref[_exact_ref_key(strategy_ref)]
            normalized_domain = str(manifest.domain).strip().lower()
            if normalized_domain in {"openehr", "fhir", "cdisc"} and (
                normalized_domain not in consumption_kinds
            ):
                raise IndustryOutcomeCatalogError(
                    f"reference architecture '{architecture.artifact_ref.id}' strategy "
                    f"'{strategy_ref.id}' has no matching consumption kind"
                )

    known_external_refs = (
        source_category_refs
        | set(profile_by_ref)
        | set(mapping_by_ref)
        | set(strategy_by_ref)
    )
    resolvable_kinds = {"source-category", "source-profile", "mapping", "strategy"}
    stale_evidence = sorted(
        _exact_ref_key(reference)
        for reference in evidence_refs
        if reference.kind in resolvable_kinds
        and _exact_ref_key(reference) not in known_external_refs
    )
    if stale_evidence:
        raise IndustryOutcomeCatalogError(
            f"outcome catalog contains stale evidence refs: {stale_evidence}"
        )

    try:
        assert_blueprint_template_refs(catalog, templates=blueprint_templates)
    except BlueprintTemplateCatalogError as exc:
        raise IndustryOutcomeCatalogError(str(exc)) from exc


def load_industry_outcome_catalog(
    path: str | Path = DEFAULT_OUTCOME_CATALOG_PATH,
    *,
    strategy_root: str | Path = DEFAULT_STRATEGY_ROOT,
) -> IndustryOutcomeCatalog:
    """Read the catalog and verify its graph, digests, and executable links."""

    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = IndustryOutcomeCatalog.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise IndustryOutcomeCatalogError(
            f"invalid industry outcome catalog at {catalog_path}: {exc}"
        ) from exc

    try:
        _assert_atomic_digests(catalog)
        assert_outcome_catalog_links(catalog, strategy_root=strategy_root)
    except IndustryOutcomeCatalogError:
        raise
    except Exception as exc:
        raise IndustryOutcomeCatalogError(
            f"industry outcome catalog dependencies are unavailable: {exc}"
        ) from exc
    return catalog


@lru_cache(maxsize=1)
def _load_bundled_industry_outcome_catalog_cached() -> IndustryOutcomeCatalog:
    """Keep the verified catalog private so callers cannot poison the cache."""

    return load_industry_outcome_catalog()


def load_bundled_industry_outcome_catalog() -> IndustryOutcomeCatalog:
    """Return an isolated copy of the verified packaged catalog."""

    return deepcopy(_load_bundled_industry_outcome_catalog_cached())
