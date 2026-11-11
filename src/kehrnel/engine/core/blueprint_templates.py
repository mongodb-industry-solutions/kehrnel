"""Load immutable Solution Blueprint templates and instantiate them safely.

Reference Architectures are editorial recommendations.  They become an
editable customer ``SolutionBlueprint`` only through an exact, content-
addressed template.  This module deliberately performs no persistence,
activation, connector calls, or runtime mutation.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.outcome_catalog import IndustryOutcomeCatalog, ReferenceArchitecture
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import (
    BlueprintAssumptionDecisionStatus,
    BlueprintInstantiationStatus,
    CandidateStatus,
    CompilationIssue,
    IssueSeverity,
    ReferenceArchitectureInstantiationRequest,
    ReferenceArchitectureInstantiationResult,
    LegacySourceBlueprint,
    SolutionBlueprintTemplate,
    SolutionBlueprintTemplateCatalog,
    StrategyCompilationRequest,
)
from kehrnel.engine.core.strategy_candidate_compiler import (
    StrategyCompilationError,
    compile_strategy_candidate,
)
from kehrnel.engine.core.mappings import MappingCatalogError
from kehrnel.engine.core.source_categories import SourceCategoryCatalogError
from kehrnel.engine.core.source_profiles import SourceProfileCatalogError
from kehrnel.engine.core.strategy_portfolio import StrategyPortfolioError
from kehrnel.engine.core.blueprint_patching import (
    apply_blueprint_override,
    pointer_exists,
    pointers_overlap,
)


DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "blueprint_templates" / "catalog.json"
)


class BlueprintTemplateCatalogError(ValueError):
    """The immutable template catalog is malformed or fails integrity checks."""


class BlueprintTemplateDependencyError(BlueprintTemplateCatalogError):
    """A catalog required to validate a Blueprint template is unavailable."""


class ReferenceArchitectureInstantiationError(ValueError):
    """An exact Reference Architecture cannot be selected for instantiation."""

    def __init__(self, code: str, message: str, *, status: int) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def blueprint_template_content_digest(
    template: SolutionBlueprintTemplate | dict[str, Any],
) -> str:
    """Digest a template without its self-referential artifact digest."""

    if isinstance(template, SolutionBlueprintTemplate):
        payload = template.to_wire()
    else:
        payload = deepcopy(template)
    artifact_ref = payload.get("artifactRef") or payload.get("artifact_ref")
    if not isinstance(artifact_ref, dict):
        raise BlueprintTemplateCatalogError(
            "solution blueprint template is missing artifactRef"
        )
    artifact_ref.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def load_solution_blueprint_templates(
    path: str | Path = DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
) -> tuple[SolutionBlueprintTemplate, ...]:
    """Read and integrity-check every bundled template."""

    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = SolutionBlueprintTemplateCatalog.model_validate(payload)
        templates = tuple(catalog.templates)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise BlueprintTemplateCatalogError(
            "invalid solution blueprint template catalog"
        ) from exc

    exact_refs: set[tuple[str, str, str, str]] = set()
    versions: set[tuple[str, str]] = set()
    for template in templates:
        expected = blueprint_template_content_digest(template)
        if template.artifact_ref.digest != expected:
            raise BlueprintTemplateCatalogError(
                "solution blueprint template digest mismatch for "
                f"{template.artifact_ref.id}@{template.artifact_ref.version}"
            )
        exact_key = _ref_key(template.artifact_ref)
        version_key = (template.artifact_ref.id, template.artifact_ref.version)
        if exact_key in exact_refs or version_key in versions:
            raise BlueprintTemplateCatalogError(
                "solution blueprint template catalog contains duplicate versions"
            )
        exact_refs.add(exact_key)
        versions.add(version_key)
    return templates


@lru_cache(maxsize=1)
def _load_bundled_solution_blueprint_templates_cached(
) -> tuple[SolutionBlueprintTemplate, ...]:
    """Keep private originals so callers cannot poison the process cache."""

    return load_solution_blueprint_templates()


def load_bundled_solution_blueprint_templates() -> tuple[SolutionBlueprintTemplate, ...]:
    """Return an isolated copy of the immutable packaged template set."""

    return deepcopy(_load_bundled_solution_blueprint_templates_cached())


def solution_blueprint_template_index(
    templates: tuple[SolutionBlueprintTemplate, ...] | None = None,
) -> dict[tuple[str, str, str, str], SolutionBlueprintTemplate]:
    if templates is None:
        templates = load_bundled_solution_blueprint_templates()
    return {_ref_key(template.artifact_ref): template for template in templates}


def assert_blueprint_template_refs(
    catalog: IndustryOutcomeCatalog,
    *,
    templates: tuple[SolutionBlueprintTemplate, ...] | None = None,
) -> None:
    """Fail closed when a Reference Architecture pins a stale template."""

    by_ref = solution_blueprint_template_index(templates)
    linked = [
        architecture
        for architecture in catalog.reference_architectures
        if architecture.blueprint_template_ref is not None
    ]
    missing = sorted(
        _ref_key(architecture.blueprint_template_ref)
        for architecture in linked
        if _ref_key(architecture.blueprint_template_ref) not in by_ref
    )
    if missing:
        raise BlueprintTemplateCatalogError(
            f"outcome catalog contains stale solution blueprint template refs: {missing}"
        )

    for architecture in linked:
        template = by_ref[_ref_key(architecture.blueprint_template_ref)]
        _assert_template_matches_architecture(template, architecture)


def instantiate_reference_architecture(
    request: ReferenceArchitectureInstantiationRequest,
    *,
    catalog: IndustryOutcomeCatalog,
    templates: tuple[SolutionBlueprintTemplate, ...] | None = None,
) -> ReferenceArchitectureInstantiationResult:
    """Create one deterministic, reviewable Blueprint without side effects."""

    architecture = _resolve_reference_architecture(
        request.reference_architecture_ref,
        catalog,
    )
    if architecture.blueprint_template_ref is None:
        return _result(
            request,
            status=BlueprintInstantiationStatus.NOT_INSTANTIABLE,
            reference_architecture_ref=architecture.artifact_ref,
            requires_review=True,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_TEMPLATE_REQUIRED",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "This Reference Architecture is a portfolio recommendation and "
                        "does not yet publish a complete Solution Blueprint template."
                    ),
                    refs=[architecture.artifact_ref.id],
                )
            ],
        )

    by_ref = solution_blueprint_template_index(templates)
    template = by_ref.get(_ref_key(architecture.blueprint_template_ref))
    if template is None:
        raise ReferenceArchitectureInstantiationError(
            "BLUEPRINT_TEMPLATE_UNAVAILABLE",
            "The exact Solution Blueprint template pinned by this Reference "
            "Architecture is unavailable.",
            status=503,
        )

    try:
        if template.artifact_ref.digest != blueprint_template_content_digest(template):
            raise BlueprintTemplateCatalogError(
                f"template '{template.artifact_ref.id}' fails its content digest"
            )
        _assert_template_matches_architecture(template, architecture)
    except BlueprintTemplateDependencyError as exc:
        raise ReferenceArchitectureInstantiationError(
            "BLUEPRINT_COMPILER_DEPENDENCY_UNAVAILABLE",
            "A required source, mapping, or strategy catalog is unavailable.",
            status=503,
        ) from exc
    except BlueprintTemplateCatalogError as exc:
        raise ReferenceArchitectureInstantiationError(
            "BLUEPRINT_TEMPLATE_INCOMPATIBLE",
            "The pinned Solution Blueprint template is invalid or incompatible "
            "with its Reference Architecture.",
            status=503,
        ) from exc

    known_assumptions = {
        assumption.assumption_id: assumption for assumption in template.assumptions
    }
    unknown_decisions = sorted(
        {
            decision.assumption_id
            for decision in request.assumption_decisions
            if decision.assumption_id not in known_assumptions
        }
    )
    unknown_override_assumptions = sorted(
        {
            assumption_id
            for override in request.overrides
            for assumption_id in override.assumption_ids
            if assumption_id not in known_assumptions
        }
    )
    if unknown_decisions or unknown_override_assumptions:
        unknown = sorted(set(unknown_decisions) | set(unknown_override_assumptions))
        raise ReferenceArchitectureInstantiationError(
            "UNKNOWN_BLUEPRINT_ASSUMPTION",
            f"The request references unknown template assumptions: {unknown}",
            status=422,
        )

    for override in request.overrides:
        for assumption_id in override.assumption_ids:
            assumption = known_assumptions[assumption_id]
            if not any(
                pointers_overlap(override.path, assumed_path)
                for assumed_path in assumption.applies_to_paths
            ):
                raise ReferenceArchitectureInstantiationError(
                    "IRRELEVANT_ASSUMPTION_CITATION",
                    "An override may cite only an assumption whose affected path "
                    "overlaps the override path.",
                    status=422,
                )

    decisions = {
        decision.assumption_id: decision for decision in request.assumption_decisions
    }
    accepted_contradictions = sorted(
        assumption_id
        for assumption_id, decision in decisions.items()
        if decision.status is BlueprintAssumptionDecisionStatus.ACCEPTED
        and any(
            pointers_overlap(override.path, assumed_path)
            for override in request.overrides
            for assumed_path in known_assumptions[assumption_id].applies_to_paths
        )
    )
    if accepted_contradictions:
        raise ReferenceArchitectureInstantiationError(
            "ACCEPTED_ASSUMPTION_OVERRIDDEN",
            "An accepted template assumption cannot be changed by an overlapping "
            "Blueprint override.",
            status=422,
        )
    unresolved_rejections = sorted(
        assumption_id
        for assumption_id, decision in decisions.items()
        if decision.status is BlueprintAssumptionDecisionStatus.REJECTED
        and not any(
            assumption_id in override.assumption_ids
            for override in request.overrides
        )
    )
    if unresolved_rejections:
        return _result(
            request,
            status=BlueprintInstantiationStatus.NOT_INSTANTIABLE,
            reference_architecture_ref=architecture.artifact_ref,
            blueprint_template_ref=template.artifact_ref,
            assumptions=list(template.assumptions),
            assumption_decisions=list(request.assumption_decisions),
            overrides=list(request.overrides),
            requires_review=True,
            issues=[
                CompilationIssue(
                    code="REJECTED_ASSUMPTION_REQUIRES_OVERRIDE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "Every rejected template assumption must be resolved by an "
                        "explicit Blueprint override that cites it."
                    ),
                    refs=unresolved_rejections,
                )
            ],
        )

    blueprint_payload = _template_blueprint_payload(
        template,
        blueprint_id=request.blueprint_id,
        revision=request.revision,
    )

    try:
        for override in request.overrides:
            apply_blueprint_override(blueprint_payload, override)
        blueprint = LegacySourceBlueprint.model_validate(blueprint_payload)
    except (KeyError, IndexError, TypeError, ValueError, ValidationError) as exc:
        return _result(
            request,
            status=BlueprintInstantiationStatus.NOT_INSTANTIABLE,
            reference_architecture_ref=architecture.artifact_ref,
            blueprint_template_ref=template.artifact_ref,
            assumptions=list(template.assumptions),
            assumption_decisions=list(request.assumption_decisions),
            overrides=list(request.overrides),
            requires_review=True,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_OVERRIDE_NOT_APPLICABLE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "The requested overrides do not produce a valid Solution "
                        "Blueprint. Review the cited paths and required inputs."
                    ),
                    refs=[override.override_id for override in request.overrides],
                )
            ],
        )

    architecture_strategy_ids = {
        reference.id for reference in architecture.strategy_refs
    }
    outside_strategies = sorted(
        set(blueprint.preferred_strategy_ids) - architecture_strategy_ids
    )
    if outside_strategies:
        raise ReferenceArchitectureInstantiationError(
            "PREFERRED_STRATEGY_OUTSIDE_REFERENCE_ARCHITECTURE",
            "The derived Blueprint requests a preferred strategy that is not "
            "pinned by the Reference Architecture.",
            status=422,
        )

    try:
        candidate = compile_strategy_candidate(
            StrategyCompilationRequest(
                contract="strategy-compilation-request/v1",
                blueprint=blueprint,
                discoveredProfiles=[],
            )
        )
    except StrategyCompilationError:
        return _result(
            request,
            status=BlueprintInstantiationStatus.NOT_INSTANTIABLE,
            reference_architecture_ref=architecture.artifact_ref,
            blueprint_template_ref=template.artifact_ref,
            assumptions=list(template.assumptions),
            assumption_decisions=list(request.assumption_decisions),
            overrides=list(request.overrides),
            requires_review=True,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_NOT_COMPILABLE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "The derived Blueprint does not resolve against the current "
                        "source profile and supported-strategy catalogs."
                    ),
                    refs=[architecture.artifact_ref.id, template.artifact_ref.id],
                )
            ],
        )
    except (
        MappingCatalogError,
        SourceCategoryCatalogError,
        SourceProfileCatalogError,
        StrategyPortfolioError,
    ) as exc:
        raise ReferenceArchitectureInstantiationError(
            "BLUEPRINT_COMPILER_DEPENDENCY_UNAVAILABLE",
            "A required source, mapping, or strategy catalog is unavailable.",
            status=503,
        ) from exc

    if candidate.status is CandidateStatus.BLOCKED:
        return _result(
            request,
            status=BlueprintInstantiationStatus.NOT_INSTANTIABLE,
            reference_architecture_ref=architecture.artifact_ref,
            blueprint_template_ref=template.artifact_ref,
            assumptions=list(template.assumptions),
            assumption_decisions=list(request.assumption_decisions),
            overrides=list(request.overrides),
            requires_review=True,
            issues=list(candidate.issues),
        )

    unresolved = sorted(set(known_assumptions) - set(decisions))
    issues: list[CompilationIssue] = []
    if unresolved:
        issues.append(
            CompilationIssue(
                code="BLUEPRINT_ASSUMPTIONS_REQUIRE_REVIEW",
                severity=IssueSeverity.REVIEW,
                message=(
                    "The Blueprint was instantiated from deterministic defaults, but "
                    "its unresolved assumptions require customer review."
                ),
                refs=unresolved,
            )
        )
    if request.overrides:
        issues.append(
            CompilationIssue(
                code="BLUEPRINT_OVERRIDES_REQUIRE_REVIEW",
                severity=IssueSeverity.REVIEW,
                message=(
                    "Customer overrides are preserved for review before compilation "
                    "or any later activation workflow."
                ),
                refs=[override.override_id for override in request.overrides],
            )
        )
    issues.extend(candidate.issues)

    return _result(
        request,
        status=BlueprintInstantiationStatus.INSTANTIATED,
        reference_architecture_ref=architecture.artifact_ref,
        blueprint_template_ref=template.artifact_ref,
        blueprint=blueprint,
        assumptions=list(template.assumptions),
        assumption_decisions=list(request.assumption_decisions),
        overrides=list(request.overrides),
        requires_review=bool(
            unresolved or request.overrides or candidate.requires_approval
        ),
        issues=issues,
    )


def _resolve_reference_architecture(
    reference: ArtifactRef,
    catalog: IndustryOutcomeCatalog,
) -> ReferenceArchitecture:
    exact = {
        _ref_key(architecture.artifact_ref): architecture
        for architecture in catalog.reference_architectures
    }
    architecture = exact.get(_ref_key(reference))
    if architecture is not None:
        return architecture

    same_version = any(
        architecture.artifact_ref.id == reference.id
        and architecture.artifact_ref.version == reference.version
        for architecture in catalog.reference_architectures
    )
    if same_version:
        raise ReferenceArchitectureInstantiationError(
            "REFERENCE_ARCHITECTURE_REF_STALE",
            "The supplied Reference Architecture digest is stale.",
            status=409,
        )
    raise ReferenceArchitectureInstantiationError(
        "REFERENCE_ARCHITECTURE_NOT_FOUND",
        "The exact Reference Architecture is not present in the outcome catalog.",
        status=404,
    )


def reference_architecture_instantiation_request_digest(
    request: ReferenceArchitectureInstantiationRequest,
) -> str:
    """Bind every result, including failures, to the complete request."""

    return "sha256:" + hashlib.sha256(
        canonical_json_bytes(request.to_wire())
    ).hexdigest()


def _result(
    request: ReferenceArchitectureInstantiationRequest,
    **payload: Any,
) -> ReferenceArchitectureInstantiationResult:
    unsigned = {
        "contract": "reference-architecture-instantiation-result/v1",
        "requestDigest": reference_architecture_instantiation_request_digest(request),
        "requestedBlueprintId": request.blueprint_id,
        "requestedRevision": request.revision,
        **payload,
    }
    wire = ReferenceArchitectureInstantiationResult(
        **unsigned,
        derivationDigest="sha256:" + "0" * 64,
    ).to_wire()
    wire.pop("derivationDigest", None)
    digest = "sha256:" + hashlib.sha256(canonical_json_bytes(wire)).hexdigest()
    return ReferenceArchitectureInstantiationResult(
        **unsigned,
        derivationDigest=digest,
    )


def _assert_template_matches_architecture(
    template: SolutionBlueprintTemplate,
    architecture: ReferenceArchitecture,
) -> None:
    industries = {path.industry for path in architecture.catalog.paths}
    if template.industry not in industries:
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' industry does not match "
            f"Reference Architecture '{architecture.artifact_ref.id}'"
        )

    architecture_profiles = {
        _ref_key(reference) for reference in architecture.source_profile_refs
    }
    template_profiles = {
        _ref_key(source.profile_ref) for source in template.sources
    }
    if not template_profiles.issubset(architecture_profiles):
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' uses source profiles outside "
            f"Reference Architecture '{architecture.artifact_ref.id}'"
        )

    architecture_consumptions = set(architecture.consumption_kinds)
    template_consumptions = {
        consumption.kind for consumption in template.consumptions
    }
    if not template_consumptions.issubset(architecture_consumptions):
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' uses consumption kinds outside "
            f"Reference Architecture '{architecture.artifact_ref.id}'"
        )

    architecture_strategy_ids = {
        reference.id for reference in architecture.strategy_refs
    }
    if not set(template.preferred_strategy_ids).issubset(
        architecture_strategy_ids
    ):
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' prefers strategies outside "
            f"Reference Architecture '{architecture.artifact_ref.id}'"
        )

    allowed_evidence = {
        _ref_key(reference)
        for references in (
            architecture.outcome_pattern_refs,
            architecture.source_category_refs,
            architecture.source_profile_refs,
            architecture.strategy_refs,
            architecture.mapping_refs,
            architecture.semantic_asset_refs,
            architecture.evidence_refs,
        )
        for reference in references
    }
    template_evidence = {
        _ref_key(reference)
        for assumption in template.assumptions
        for reference in assumption.evidence_refs
    }
    if not template_evidence.issubset(allowed_evidence):
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' cites evidence "
            f"outside Reference Architecture '{architecture.artifact_ref.id}'"
        )

    blueprint_payload = _template_blueprint_payload(
        template,
        blueprint_id=f"validate.{template.artifact_ref.id}",
        revision=1,
    )
    for assumption in template.assumptions:
        missing_paths = [
            path
            for path in assumption.applies_to_paths
            if not pointer_exists(blueprint_payload, path)
        ]
        if missing_paths:
            raise BlueprintTemplateCatalogError(
                f"template '{template.artifact_ref.id}' assumption "
                f"'{assumption.assumption_id}' references missing paths"
            )

    validation_blueprint = LegacySourceBlueprint.model_validate(blueprint_payload)
    try:
        candidate = compile_strategy_candidate(
            StrategyCompilationRequest(
                contract="strategy-compilation-request/v1",
                blueprint=validation_blueprint,
                discoveredProfiles=[],
            )
        )
    except StrategyCompilationError as exc:
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' does not compile"
        ) from exc
    except (
        MappingCatalogError,
        SourceCategoryCatalogError,
        SourceProfileCatalogError,
        StrategyPortfolioError,
    ) as exc:
        raise BlueprintTemplateDependencyError(
            "a template compiler dependency is unavailable"
        ) from exc
    if candidate.status is CandidateStatus.BLOCKED:
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' compiles to a blocked candidate"
        )
    selected_strategy_ids = {
        selection.strategy_id for selection in candidate.strategy_selections
    }
    if not selected_strategy_ids.issubset(architecture_strategy_ids):
        raise BlueprintTemplateCatalogError(
            f"template '{template.artifact_ref.id}' selects strategies outside "
            f"Reference Architecture '{architecture.artifact_ref.id}'"
        )


def _ref_key(reference: ArtifactRef) -> tuple[str, str, str, str]:
    return (
        reference.kind,
        reference.id,
        reference.version,
        reference.digest,
    )


def _template_blueprint_payload(
    template: SolutionBlueprintTemplate,
    *,
    blueprint_id: str,
    revision: int,
) -> dict[str, Any]:
    return {
      "contract": "legacy-source-blueprint/internal",
        "blueprintId": blueprint_id,
        "industry": template.industry,
        "revision": revision,
        "sources": [item.to_wire() for item in template.sources],
        "context": [item.to_wire() for item in template.context],
        "consumptions": [item.to_wire() for item in template.consumptions],
        "constraints": template.constraints.to_wire(),
        "preferredStrategyIds": list(template.preferred_strategy_ids),
    }
