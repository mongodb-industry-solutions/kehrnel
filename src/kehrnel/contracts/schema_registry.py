"""Versioned JSON Schema registry for Kehrnel's foundation contracts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, Type

from pydantic import BaseModel

from .artifacts import (
    ActionAvailability,
    ArtifactRef,
    CapabilityClaim,
    ContentDigests,
    ProblemDetails,
)
from .architecture_foundation import (
    ArchitectureResolutionInputCatalog,
    ArchitectureResolutionRequest,
    ArchitectureResolutionResult,
    BenchmarkEvidence,
    ImplementationPackage,
    PerformanceForecast,
    PerformanceModel,
    PreparationPlan,
    ProjectInteractionContract,
    RecordAuthorityPolicy,
    ResolvedArchitecturePlan,
    RuntimeQueryPlanTrace,
    RuntimeExecutionPlan,
    SolutionBlueprint,
    SemanticExecutionBindingV1,
    TargetCapabilityProfile,
    WorkloadProfile,
)
from .blueprint_canonicalization import SolutionBlueprintCanonicalizationResult
from .implementation_package_build import ImplementationPackageBuildRequest
from .implementation_package_activation import (
    ApprovedStrategyActivationSpecification,
    ImplementationPackageActivationReceipt,
    ImplementationPackageActivationRequest,
)
from .implementation_package_installation import (
    ImplementationPackageInstallationRequest,
    ImplementationPackageInstallationStage,
)
from .implementation_package_materialization import (
    ImplementationPackageAssembly,
    ImplementationPackageMaterializationReceipt,
    ImplementationPackageMaterializationRequest,
)
from .implementation_package_preparation import (
    ImplementationPackagePreparationReadiness,
    ImplementationPackagePreparationReadinessRequest,
)
from .implementation_package_preparation_execution import (
    ImplementationPackagePreparationExecutionReceipt,
    ImplementationPackagePreparationExecutionRequest,
)
from .implementation_package_runtime_execution import (
    ImplementationPackageRuntimeExecutionReceipt,
    ImplementationPackageRuntimeExecutionRequest,
    ImplementationPackageRuntimeExecutionResponse,
    RuntimePolicyDecision,
    RuntimeResultEvidence,
)
from .implementation_package_promotion import (
    ImplementationPackagePromotionReceipt,
    ImplementationPackagePromotionRequest,
    InteractionPromotionEvidence,
)
from .implementation_package_operational_readiness import (
    ImplementationPackageOperationalReadinessReceipt,
    ImplementationPackageOperationalReadinessRequest,
    OperationalReadinessCheck,
)
from .implementation_package_deployment import (
    ImplementationPackageDeploymentReceipt,
    ImplementationPackageDeploymentRequest,
    ImplementationPackageDeploymentStatus,
    ImplementationPackageRollbackReceipt,
    ImplementationPackageRollbackRequest,
)
from .implementation_package_observation import (
    ImplementationPackageOperationalObservationHistory,
    ImplementationPackageOperationalObservationReceipt,
    ImplementationPackageOperationalObservationRequest,
    ImplementationPackageOperationalObservationTriggerRequest,
    OperationalObservationCheck,
    StrategyOperationalProbeCheck,
    StrategyOperationalProbeEvidence,
)
from .implementation_package_proof import (
    ImplementationPackageProofEvidence,
    ImplementationPackageProofRequest,
    ImplementationPackageProofRun,
)
from .cohort_intent import ClinicalCohortIntent, ClinicalCohortResolution
from .manifest import StrategyManifestSource
from .mapping_ir import (
    MappingCatalog,
    MappingDefinition,
    MappingSimulationRequest,
    MappingSimulationResult,
)
from .modeler_assistant import (
    ModelerAgenticExecutionReceipt,
    ModelerAgenticInvocation,
    ModelerBlueprintProposal,
    ModelerProposalApprovalRequest,
    ModelerProposalValidationRequest,
    ModelerProposalValidationResult,
)
from .outcome_catalog import (
    ArchitectureCapabilityDefinition,
    DataProductDefinition,
    DemoExperience,
    IndustryOutcomeCatalog,
    OutcomePattern,
    PortfolioDomainDefinition,
    ReferenceArchitecture,
    VisualPatternDefinition,
)
from .package_digest import PackageFileManifest, canonical_json_bytes
from .semantic_qualification import (
    SemanticExecutionQualificationResult,
    SemanticExecutionQualificationSpec,
)
from .semantic_execution import ContextObjectDefinitionCatalog, ContextObjectDefinitionSummary
from .strategy_portfolio import StrategyPortfolio
from .source_architecture import (
    SourceCategory,
    SourceCategoryCatalog,
    SourceProfile,
    SourceProfileCatalog,
)


SCHEMA_BASE_URI = "urn:mongodb:data-lab:contracts:v1"
PUBLISHED_SCHEMA_DIR = Path(__file__).with_name("schemas") / "v1"
CONTRACT_MODELS: Dict[str, Type[BaseModel]] = {
    "action-availability": ActionAvailability,
    "architecture-capability": ArchitectureCapabilityDefinition,
    "architecture-resolution-input-catalog": ArchitectureResolutionInputCatalog,
    "architecture-resolution-request": ArchitectureResolutionRequest,
    "architecture-resolution-result": ArchitectureResolutionResult,
    "artifact-ref": ArtifactRef,
    "benchmark-evidence": BenchmarkEvidence,
    "capability-claim": CapabilityClaim,
    "clinical-cohort-intent": ClinicalCohortIntent,
    "clinical-cohort-resolution": ClinicalCohortResolution,
    "content-digests": ContentDigests,
    "data-product": DataProductDefinition,
    "demo-experience": DemoExperience,
    "industry-outcome-catalog": IndustryOutcomeCatalog,
    "implementation-package": ImplementationPackage,
    "approved-strategy-activation-specification": ApprovedStrategyActivationSpecification,
    "implementation-package-activation-request": ImplementationPackageActivationRequest,
    "implementation-package-activation-receipt": ImplementationPackageActivationReceipt,
    "implementation-package-build-request": ImplementationPackageBuildRequest,
    "implementation-package-installation-request": ImplementationPackageInstallationRequest,
    "implementation-package-installation-stage": ImplementationPackageInstallationStage,
    "implementation-package-assembly": ImplementationPackageAssembly,
    "implementation-package-materialization-request": ImplementationPackageMaterializationRequest,
    "implementation-package-materialization-receipt": ImplementationPackageMaterializationReceipt,
    "implementation-package-preparation-readiness-request": ImplementationPackagePreparationReadinessRequest,
    "implementation-package-preparation-readiness": ImplementationPackagePreparationReadiness,
    "implementation-package-preparation-execution-request": ImplementationPackagePreparationExecutionRequest,
    "implementation-package-preparation-execution-receipt": ImplementationPackagePreparationExecutionReceipt,
    "implementation-package-runtime-execution-request": ImplementationPackageRuntimeExecutionRequest,
    "implementation-package-runtime-execution-receipt": ImplementationPackageRuntimeExecutionReceipt,
    "implementation-package-runtime-execution-response": ImplementationPackageRuntimeExecutionResponse,
    "implementation-package-promotion-request": ImplementationPackagePromotionRequest,
    "implementation-package-promotion-receipt": ImplementationPackagePromotionReceipt,
    "interaction-promotion-evidence": InteractionPromotionEvidence,
    "implementation-package-operational-readiness-request": ImplementationPackageOperationalReadinessRequest,
    "implementation-package-operational-readiness-receipt": ImplementationPackageOperationalReadinessReceipt,
    "operational-readiness-check": OperationalReadinessCheck,
    "implementation-package-deployment-request": ImplementationPackageDeploymentRequest,
    "implementation-package-deployment-receipt": ImplementationPackageDeploymentReceipt,
    "implementation-package-deployment-status": ImplementationPackageDeploymentStatus,
    "implementation-package-rollback-request": ImplementationPackageRollbackRequest,
    "implementation-package-rollback-receipt": ImplementationPackageRollbackReceipt,
    "implementation-package-operational-observation-request": ImplementationPackageOperationalObservationRequest,
    "implementation-package-operational-observation-trigger-request": ImplementationPackageOperationalObservationTriggerRequest,
    "implementation-package-operational-observation-receipt": ImplementationPackageOperationalObservationReceipt,
    "implementation-package-operational-observation-history": ImplementationPackageOperationalObservationHistory,
    "operational-observation-check": OperationalObservationCheck,
    "strategy-operational-probe-check": StrategyOperationalProbeCheck,
    "strategy-operational-probe-evidence": StrategyOperationalProbeEvidence,
    "implementation-package-proof-evidence": ImplementationPackageProofEvidence,
    "implementation-package-proof-request": ImplementationPackageProofRequest,
    "implementation-package-proof-run": ImplementationPackageProofRun,
    "interaction-contract": ProjectInteractionContract,
    "mapping-catalog": MappingCatalog,
    "mapping-definition": MappingDefinition,
    "mapping-simulation-request": MappingSimulationRequest,
    "mapping-simulation-result": MappingSimulationResult,
    "modeler-agentic-execution-receipt": ModelerAgenticExecutionReceipt,
    "modeler-agentic-invocation": ModelerAgenticInvocation,
    "modeler-blueprint-proposal": ModelerBlueprintProposal,
    "modeler-proposal-approval-request": ModelerProposalApprovalRequest,
    "modeler-proposal-validation-request": ModelerProposalValidationRequest,
    "modeler-proposal-validation-result": ModelerProposalValidationResult,
    "outcome-pattern": OutcomePattern,
    "portfolio-domain": PortfolioDomainDefinition,
    "performance-forecast": PerformanceForecast,
    "performance-model": PerformanceModel,
    "preparation-plan": PreparationPlan,
    "problem-details": ProblemDetails,
    "reference-architecture": ReferenceArchitecture,
    "record-authority-policy": RecordAuthorityPolicy,
    "resolved-architecture-plan": ResolvedArchitecturePlan,
    "runtime-query-plan-trace": RuntimeQueryPlanTrace,
    "runtime-policy-decision": RuntimePolicyDecision,
    "runtime-result-evidence": RuntimeResultEvidence,
    "runtime-execution-plan": RuntimeExecutionPlan,
    "semantic-execution-binding": SemanticExecutionBindingV1,
    "solution-blueprint": SolutionBlueprint,
    "solution-blueprint-canonicalization-result": SolutionBlueprintCanonicalizationResult,
    "semantic-execution-qualification-result": SemanticExecutionQualificationResult,
    "semantic-execution-qualification-spec": SemanticExecutionQualificationSpec,
    "context-object-definition": ContextObjectDefinitionSummary,
    "context-object-definition-catalog": ContextObjectDefinitionCatalog,
    "source-profile": SourceProfile,
    "source-profile-catalog": SourceProfileCatalog,
    "source-category": SourceCategory,
    "source-category-catalog": SourceCategoryCatalog,
    "strategy-manifest": StrategyManifestSource,
    "strategy-package-file-manifest": PackageFileManifest,
    "strategy-portfolio": StrategyPortfolio,
    "target-capability-profile": TargetCapabilityProfile,
    "visual-pattern": VisualPatternDefinition,
    "workload-profile": WorkloadProfile,
}

STRATEGY_PACK_SPEC_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": f"{SCHEMA_BASE_URI}:strategy-pack-spec",
    "title": "Strategy Pack Specification",
    "type": "object",
    "minProperties": 1,
    "properties": {
        "$schema": {"type": "string"},
        "_description": {"type": "string"},
        "_schemaVersion": {"type": "string", "minLength": 1},
        "_strategy": {"type": "string", "minLength": 1},
        "meta": {
            "type": "object",
            "required": ["strategyId", "specVersion"],
            "properties": {
                "strategyId": {"type": "string", "minLength": 1},
                "specVersion": {"type": "string", "minLength": 1},
            },
            "additionalProperties": True,
        },
        "glossary": {},
        "logicalModel": {"type": "object"},
        "instanceShape": {"type": "object"},
        "storageModel": {"type": "object"},
        "storage": {"type": "object"},
        "queryModel": {"type": "object"},
        "transformModel": {"type": "object"},
        "visualization": {"type": "object"},
    },
    "allOf": [
        {
            "if": {"required": ["meta"]},
            "then": {
                "properties": {
                    "meta": {
                        "type": "object",
                        "required": ["strategyId", "specVersion"],
                    }
                }
            },
        },
        {
            "if": {"required": ["_strategy"]},
            "then": {"required": ["_schemaVersion"]},
        },
    ],
    "anyOf": [{"required": ["meta"]}, {"required": ["_strategy"]}],
    "required": ["queryModel"],
    "additionalProperties": True,
}


def contract_schemas() -> Dict[str, dict]:
    schemas: Dict[str, dict] = {}
    for name, model in sorted(CONTRACT_MODELS.items()):
        generated = model.model_json_schema(by_alias=True, mode="validation")
        if name == "strategy-manifest":
            properties = generated.get("properties") or {}
            for runtime_field in ("packageDigest", "contractDigest", "pack_spec"):
                properties.pop(runtime_field, None)
        schemas[f"{name}.schema.json"] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"{SCHEMA_BASE_URI}:{name}",
            **generated,
        }
    schemas["strategy-pack-spec.schema.json"] = STRATEGY_PACK_SPEC_SCHEMA
    return schemas


def compute_contract_digest(schemas: Dict[str, dict] | None = None) -> str:
    payload = schemas if schemas is not None else contract_schemas()
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def published_contract_digest(*, verify: bool = True) -> str:
    """Read the immutable committed digest instead of regenerating at runtime."""

    manifest_path = PUBLISHED_SCHEMA_DIR / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    digest = str(manifest.get("contractDigest") or "")
    if not verify:
        return digest

    schemas: Dict[str, dict] = {}
    for entry in manifest.get("files") or []:
        name = str(entry["path"])
        path = PUBLISHED_SCHEMA_DIR / name
        payload = path.read_bytes()
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != entry.get("sha256") or len(payload) != entry.get("size"):
            raise RuntimeError(f"published contract schema integrity failure: {name}")
        schemas[name] = json.loads(payload)
    actual_digest = compute_contract_digest(schemas)
    if actual_digest != digest:
        raise RuntimeError("published contract digest does not match its schema set")
    return digest


def _render_schema(schema: dict) -> bytes:
    return (json.dumps(schema, indent=2, sort_keys=True) + "\n").encode("utf-8")


def schema_file_manifest(schemas: Dict[str, dict] | None = None) -> dict:
    payload = schemas if schemas is not None else contract_schemas()
    files = []
    for name, schema in sorted(payload.items()):
        encoded = _render_schema(schema)
        files.append(
            {
                "path": name,
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "size": len(encoded),
            }
        )
    return {
        "contract": "contract-schema-manifest/v1",
        "contractDigest": compute_contract_digest(payload),
        "files": files,
    }


def write_contract_schemas(output_dir: str | Path) -> None:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    schemas = contract_schemas()
    for name, schema in schemas.items():
        (output / name).write_bytes(_render_schema(schema))
    (output / "manifest.json").write_text(
        json.dumps(schema_file_manifest(schemas), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
