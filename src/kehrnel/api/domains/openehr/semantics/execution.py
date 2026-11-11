"""Authenticated execution boundary for governed openEHR semantic queries.

The caller still supplies a typed semantic intent.  Terminology expansion,
OPT validation, AQL rendering, strategy selection, compilation, and execution
all happen on the server against one authenticated environment.  This keeps
the execution route from trusting caller-issued terminology receipts or a
caller-selected persistence strategy.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
from typing import Any, Literal, Mapping, Optional, Protocol

from bson import json_util
from pydantic import Field

from kehrnel.contracts.artifacts import ContractModel, Sha256Digest
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.openehr.semantic_query import OpenEhrSemanticQueryPlan
from kehrnel.engine.strategies.openehr.rps_dual.query.pagination import (
    MAX_PAGE_LIMIT,
)
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config

from .service import StoredAdl14Opt
from .terminology_planning import (
    OpenEhrTerminologyPlanningService,
    OpenEhrTerminologyQueryPlanRequest,
    TerminologyResolutionEvidence,
)


TERMINOLOGY_QUERY_EXECUTION_RESULT_CONTRACT = (
    "openehr-terminology-query-execution-result/v1"
)
SEMANTIC_PROJECTION_MANIFEST_CONTRACT = (
    "openehr-semantic-projection-manifest/v1"
)
SEMANTIC_PROJECTION_READINESS_CONTRACT = (
    "openehr-semantic-projection-readiness/v1"
)

SUPPORTED_EXECUTION_STRATEGIES = frozenset(
    {"openehr.rps_dual"}
)

# This route is an interactive, evidence-returning boundary, not an export API.
# The strategy's governed page ceiling is 100.  Reserve one row for a
# look-ahead probe so the public result can distinguish a complete window from
# a truncated one without silently relying on the strategy's limit cap.
MAX_SEMANTIC_EXECUTION_ROWS = MAX_PAGE_LIMIT - 1
MAX_SEMANTIC_EXECUTION_FETCH_ROWS = MAX_PAGE_LIMIT
MAX_SEMANTIC_EXECUTION_EXPANSION_CODES = 1024


class SemanticExecutionRuntime(Protocol):
    """Minimum runtime seam required by semantic execution."""

    registry: Any

    async def dispatch(
        self,
        environment_id: str,
        operation: str,
        payload: dict[str, Any],
    ) -> Any: ...


class OpenEhrRuntimeQueryResult(ContractModel):
    """Stable public view of the RPS Dual query result."""

    result_kind: Literal["composition_match_rows"] = "composition_match_rows"
    engine_used: str = Field(min_length=1, max_length=128)
    rows: list[dict[str, Any]] = Field(
        max_length=MAX_SEMANTIC_EXECUTION_FETCH_ROWS
    )
    requested_limit: int = Field(default=1, ge=1, le=MAX_SEMANTIC_EXECUTION_ROWS)
    offset: int = Field(default=0, ge=0)
    has_more: bool = False
    next_offset: Optional[int] = Field(default=None, ge=1)
    explain: Optional[dict[str, Any]] = None


class OpenEhrSemanticProjectionManifest(ContractModel):
    """Deterministic inputs that make an empty semantic result meaningful."""

    contract: Literal["openehr-semantic-projection-manifest/v1"] = (
        SEMANTIC_PROJECTION_MANIFEST_CONTRACT
    )
    environment_id: str = Field(min_length=1, max_length=256)
    strategy_id: Literal["openehr.rps_dual"] = "openehr.rps_dual"
    template_id: str = Field(min_length=1, max_length=512)
    template_source_digest: Sha256Digest
    catalog_digest: Sha256Digest
    plan_digest: Sha256Digest
    mapping_digest: Sha256Digest
    projection_revision: str = Field(min_length=1, max_length=256)
    required_paths: list[str] = Field(min_length=1)
    covered_paths: list[str]
    missing_paths: list[str]
    atlas_index_name: str = Field(min_length=1, max_length=256)
    atlas_index_definition_digest: Sha256Digest
    manifest_digest: Sha256Digest

    def verify_digest(self) -> bool:
        payload = self.to_wire()
        payload.pop("manifestDigest", None)
        return self.manifest_digest == _digest_payload(payload)


class OpenEhrSemanticProjectionReadiness(ContractModel):
    """Server-proven readiness of the exact projection used by one plan."""

    contract: Literal["openehr-semantic-projection-readiness/v1"] = (
        SEMANTIC_PROJECTION_READINESS_CONTRACT
    )
    ready: bool
    manifest: OpenEhrSemanticProjectionManifest
    rebuild_state: Literal[
        "ready",
        "building",
        "index_pending",
        "missing",
        "stale",
    ]
    rebuild_complete: bool
    rebuild_in_progress: bool
    rebuild_watermark: Optional[dict[str, Any]] = None
    projection_scope: Optional[dict[str, Any]] = None
    reconciliation: dict[str, Any] = Field(default_factory=dict)
    consistency: dict[str, Any] = Field(default_factory=dict)
    atlas_index_state: Literal[
        "verified_ready",
        "configured_unverified",
        "missing",
        "stale",
        "adapter_unavailable",
    ]
    atlas_index_live_verified: bool
    reasons: list[str] = Field(default_factory=list)


class OpenEhrSemanticExecutionEvidence(ContractModel):
    """Evidence binding the returned rows to the reviewed semantic plan."""

    environment_id: str = Field(min_length=1, max_length=256)
    strategy_id: str = Field(min_length=1, max_length=256)
    activation_id: Optional[str] = Field(default=None, max_length=256)
    strategy_version: Optional[str] = Field(default=None, max_length=256)
    manifest_digest: Optional[str] = Field(default=None, max_length=256)
    config_hash: Optional[str] = Field(default=None, max_length=256)
    bindings_hash: Optional[str] = Field(default=None, max_length=256)
    data_target_digest: Sha256Digest
    plan_digest: Sha256Digest
    semantic_query_digest: Sha256Digest
    catalog_digest: Sha256Digest
    execution_input_digest: Sha256Digest
    compiled_pipeline_digest: Sha256Digest
    terminology_expansion_digests: list[Sha256Digest]
    projection_readiness: OpenEhrSemanticProjectionReadiness
    require_exact_path_correlation: Literal[True] = True
    engine_used: str = Field(min_length=1, max_length=128)
    row_count: int = Field(ge=0, le=MAX_SEMANTIC_EXECUTION_ROWS)
    requested_limit: int = Field(ge=1, le=MAX_SEMANTIC_EXECUTION_ROWS)
    offset: int = Field(ge=0)
    has_more: bool
    next_offset: Optional[int] = Field(default=None, ge=1)
    projection_mutation_sequence: int = Field(ge=0)


class OpenEhrTerminologyQueryExecutionResult(ContractModel):
    contract: Literal["openehr-terminology-query-execution-result/v1"] = (
        TERMINOLOGY_QUERY_EXECUTION_RESULT_CONTRACT
    )
    environment_id: str
    strategy_id: str
    plan: OpenEhrSemanticQueryPlan
    terminology_resolution: list[TerminologyResolutionEvidence]
    result: OpenEhrRuntimeQueryResult
    execution_evidence: OpenEhrSemanticExecutionEvidence


class OpenEhrTerminologyExecutionService:
    """Plan and execute one bounded semantic query through active RPS Dual."""

    def __init__(
        self,
        *,
        planning_service: OpenEhrTerminologyPlanningService,
        runtime: SemanticExecutionRuntime,
        environment_id: str,
    ) -> None:
        self._planning_service = planning_service
        self._runtime = runtime
        self._environment_id = environment_id

    async def execute_query(
        self,
        stored: StoredAdl14Opt,
        request: OpenEhrTerminologyQueryPlanRequest,
    ) -> OpenEhrTerminologyQueryExecutionResult:
        activation = self._active_supported_activation()
        active_strategy_config = normalize_config(
            getattr(activation, "config", {}) or {}
        )
        if not stored.data_target_digest:
            raise KehrnelError(
                code="OPENEHR_DATA_TARGET_UNVERIFIED",
                status=409,
                message=(
                    "Semantic execution requires the stored OPT and runtime "
                    "query to be pinned to one resolved persistence target."
                ),
            )
        if request.query.limit > MAX_SEMANTIC_EXECUTION_ROWS:
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_EXECUTION_LIMIT_EXCEEDED",
                status=422,
                message=(
                    "Semantic query execution is limited to "
                    f"{MAX_SEMANTIC_EXECUTION_ROWS} rows per request."
                ),
                details={
                    "requestedLimit": request.query.limit,
                    "maximumLimit": MAX_SEMANTIC_EXECUTION_ROWS,
                },
            )
        requested_expansion_capacity = sum(
            expansion.max_codes for expansion in request.expansion_requests
        )
        if requested_expansion_capacity > MAX_SEMANTIC_EXECUTION_EXPANSION_CODES:
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_EXECUTION_EXPANSION_LIMIT_EXCEEDED",
                status=422,
                message=(
                    "Semantic query execution permits at most "
                    f"{MAX_SEMANTIC_EXECUTION_EXPANSION_CODES} requested "
                    "terminology codes across all expansions."
                ),
                details={
                    "requestedCapacity": requested_expansion_capacity,
                    "maximumCapacity": MAX_SEMANTIC_EXECUTION_EXPANSION_CODES,
                },
            )

        planned = await self._planning_service.plan_query(stored, request)
        if planned.environment_id != self._environment_id:
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_EXECUTION_ENVIRONMENT_MISMATCH",
                status=500,
                message="The terminology plan was issued for a different environment.",
                details={
                    "executionEnvironmentId": self._environment_id,
                    "planningEnvironmentId": planned.environment_id,
                },
            )

        plan = planned.plan
        if not plan.verify_digest():
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_PLAN_DIGEST_INVALID",
                status=500,
                message="The semantic query plan failed its content-digest check.",
                details={"planDigest": plan.plan_digest},
            )
        if plan.execution_request.require_exact_path_correlation is not True:
            raise KehrnelError(
                code="OPENEHR_EXACT_PATH_CORRELATION_REQUIRED",
                status=500,
                message=(
                    "Semantic openEHR execution requires exact path and "
                    "archetype-instance correlation."
                ),
            )
        if (
            plan.requested_limit != request.query.limit
            or plan.execution_limit != request.query.limit + 1
            or plan.offset != request.query.offset
        ):
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_RESULT_WINDOW_MISMATCH",
                status=500,
                message="The semantic plan does not preserve the requested result window.",
                details={
                    "requestedLimit": request.query.limit,
                    "plannedRequestedLimit": plan.requested_limit,
                    "plannedExecutionLimit": plan.execution_limit,
                    "requestedOffset": request.query.offset,
                    "plannedOffset": plan.offset,
                },
            )

        projection_readiness_before = await self._require_projection_readiness(
            plan,
            activation,
            data_target_digest=stored.data_target_digest,
        )

        strategy_query = deepcopy(plan.execution_request.to_strategy_payload())
        # Defense in depth: this must remain a literal true in the payload that
        # reaches the strategy compiler, even if the adapter changes later.
        if strategy_query.get("require_exact_path_correlation") is not True:
            raise KehrnelError(
                code="OPENEHR_EXACT_PATH_CORRELATION_REQUIRED",
                status=500,
                message=(
                    "The strategy handoff did not preserve exact path correlation."
                ),
            )

        raw_result = await self._runtime.dispatch(
            self._environment_id,
            "query",
            {
                "domain": "openEHR",
                "query": strategy_query,
                "__activation_precondition": _activation_snapshot(
                    activation,
                    data_target_digest=stored.data_target_digest,
                ),
            },
        )
        result = self._validate_runtime_result(
            raw_result,
            request=request,
            plan=plan,
        )
        _validate_execution_activation(
            result,
            activation,
            data_target_digest=stored.data_target_digest,
        )
        compiled_pipeline_digest, execution_input_digest = (
            _validate_compiled_execution_evidence(
                result,
                expected_request=strategy_query,
                expected_plan=plan,
                expected_collection=active_strategy_config.collections.search.name,
                expected_comp_id_field=active_strategy_config.fields.document.comp_id,
            )
        )
        _validate_result_aliases(result, request=request)
        projection_readiness = await self._require_projection_readiness(
            plan,
            activation,
            data_target_digest=stored.data_target_digest,
        )
        projection_mutation_sequence = _validate_projection_stability(
            projection_readiness_before,
            projection_readiness,
        )
        strategy_id = str(getattr(activation, "strategy_id", "")).strip()

        evidence = OpenEhrSemanticExecutionEvidence(
            environment_id=self._environment_id,
            strategy_id=strategy_id,
            activation_id=_optional_text(getattr(activation, "activation_id", None)),
            strategy_version=_optional_text(getattr(activation, "version", None)),
            manifest_digest=_optional_text(
                getattr(activation, "manifest_digest", None)
            ),
            config_hash=_optional_text(getattr(activation, "config_hash", None)),
            bindings_hash=_optional_text(
                getattr(activation, "bindings_hash", None)
            ),
            data_target_digest=stored.data_target_digest,
            plan_digest=plan.plan_digest,
            semantic_query_digest=plan.semantic_query_digest,
            catalog_digest=plan.catalog_ref.catalog_digest,
            execution_input_digest=execution_input_digest,
            compiled_pipeline_digest=compiled_pipeline_digest,
            terminology_expansion_digests=[
                receipt.expansion_digest
                for receipt in plan.terminology_expansion_receipts
            ],
            projection_readiness=projection_readiness,
            require_exact_path_correlation=True,
            engine_used=result.engine_used,
            row_count=len(result.rows),
            requested_limit=result.requested_limit,
            offset=result.offset,
            has_more=result.has_more,
            next_offset=result.next_offset,
            projection_mutation_sequence=projection_mutation_sequence,
        )
        return OpenEhrTerminologyQueryExecutionResult(
            environment_id=self._environment_id,
            strategy_id=strategy_id,
            plan=plan,
            terminology_resolution=planned.terminology_resolution,
            result=result,
            execution_evidence=evidence,
        )

    def _active_supported_activation(self) -> Any:
        registry = getattr(self._runtime, "registry", None)
        activation = (
            registry.get_activation(self._environment_id, "openehr")
            if registry is not None
            else None
        )
        if activation is None:
            raise KehrnelError(
                code="OPENEHR_ACTIVATION_REQUIRED",
                status=409,
                message=(
                    "Semantic query execution requires an active openEHR strategy."
                ),
                details={"environmentId": self._environment_id},
            )
        strategy_id = str(getattr(activation, "strategy_id", "")).strip().lower()
        domain = str(getattr(activation, "domain", "")).strip().lower()
        if domain != "openehr" or strategy_id not in SUPPORTED_EXECUTION_STRATEGIES:
            raise KehrnelError(
                code="OPENEHR_SEMANTIC_EXECUTION_STRATEGY_UNSUPPORTED",
                status=409,
                message=(
                    "Semantic query execution currently requires an active "
                    "openehr.rps_dual strategy with chain-valued path-instance "
                    "metadata. The IBM compatibility strategy remains available "
                    "for its legacy query surface, but cannot safely execute "
                    "correlated semantic queries until its scalar lineage is "
                    "migrated."
                ),
                details={
                    "environmentId": self._environment_id,
                    "activeDomain": domain or None,
                    "activeStrategyId": strategy_id or None,
                    "supportedStrategyIds": sorted(SUPPORTED_EXECUTION_STRATEGIES),
                },
            )
        return activation

    async def _require_projection_readiness(
        self,
        plan: OpenEhrSemanticQueryPlan,
        activation: Any,
        *,
        data_target_digest: str,
    ) -> OpenEhrSemanticProjectionReadiness:
        required_paths = sorted(
            {
                path
                for field in plan.resolved_fields
                for path in field.projection_paths
            }
        )
        if not required_paths:
            raise _projection_not_ready(
                "the semantic plan declares no physical projection paths"
            )

        try:
            raw = await self._runtime.dispatch(
                self._environment_id,
                "op",
                {
                    "domain": "openEHR",
                    "op": "semantic_projection_preflight",
                    "payload": {
                        "contract": "openehr-semantic-projection-preflight/v1",
                        "templateId": plan.catalog_ref.template_id,
                        "templateSourceDigest": plan.catalog_ref.source_digest,
                        "catalogDigest": plan.catalog_ref.catalog_digest,
                        "planDigest": plan.plan_digest,
                        "requiredPaths": required_paths,
                    },
                    "__activation_precondition": _activation_snapshot(
                        activation,
                        data_target_digest=data_target_digest,
                    ),
                },
            )
        except KehrnelError as exc:
            if exc.code == "OPENEHR_SEMANTIC_PROJECTION_NOT_READY":
                raise
            raise _projection_not_ready(
                "the active strategy could not prove projection readiness",
                details={"causeCode": exc.code},
            ) from exc
        except Exception as exc:
            raise _projection_not_ready(
                "the active strategy could not prove projection readiness"
            ) from exc

        try:
            readiness = OpenEhrSemanticProjectionReadiness.model_validate(raw)
        except (TypeError, ValueError) as exc:
            raise _projection_not_ready(
                "the active strategy returned an invalid projection-readiness receipt"
            ) from exc

        manifest = readiness.manifest
        expected = {
            "environmentId": self._environment_id,
            "strategyId": str(getattr(activation, "strategy_id", "")).strip(),
            "templateId": plan.catalog_ref.template_id,
            "templateSourceDigest": plan.catalog_ref.source_digest,
            "catalogDigest": plan.catalog_ref.catalog_digest,
            "planDigest": plan.plan_digest,
            "requiredPaths": required_paths,
        }
        actual = {
            "environmentId": manifest.environment_id,
            "strategyId": manifest.strategy_id,
            "templateId": manifest.template_id,
            "templateSourceDigest": manifest.template_source_digest,
            "catalogDigest": manifest.catalog_digest,
            "planDigest": manifest.plan_digest,
            "requiredPaths": manifest.required_paths,
        }
        mismatches = {
            key: {"expected": expected[key], "actual": actual[key]}
            for key in expected
            if expected[key] != actual[key]
        }
        if not manifest.verify_digest():
            mismatches["manifestDigest"] = {
                "expected": _digest_payload(
                    {
                        key: value
                        for key, value in manifest.to_wire().items()
                        if key != "manifestDigest"
                    }
                ),
                "actual": manifest.manifest_digest,
            }
        if mismatches or not readiness.ready:
            raise _projection_not_ready(
                "the active semantic projection is absent, stale, or not queryable",
                details={
                    "mismatches": mismatches,
                    "missingPaths": manifest.missing_paths,
                    "rebuildState": readiness.rebuild_state,
                    "rebuildComplete": readiness.rebuild_complete,
                    "atlasIndexState": readiness.atlas_index_state,
                    "atlasIndexLiveVerified": readiness.atlas_index_live_verified,
                    "reasons": readiness.reasons,
                },
            )
        _validate_projection_integrity_receipt(readiness)
        return readiness

    @staticmethod
    def _validate_runtime_result(
        raw_result: Any,
        *,
        request: OpenEhrTerminologyQueryPlanRequest,
        plan: OpenEhrSemanticQueryPlan,
    ) -> OpenEhrRuntimeQueryResult:
        if not isinstance(raw_result, Mapping):
            raise _invalid_runtime_result("the strategy returned a non-object result")
        if raw_result.get("ok") is False:
            raise _invalid_runtime_result("the strategy reported an unsuccessful result")
        try:
            result = OpenEhrRuntimeQueryResult.model_validate(raw_result)
        except (TypeError, ValueError) as exc:
            raise _invalid_runtime_result(
                "the strategy result does not satisfy the query-result contract"
            ) from exc
        if len(result.rows) > plan.execution_limit:
            raise _invalid_runtime_result(
                "the strategy returned more rows than the governed look-ahead limit",
                details={
                    "returnedRows": len(result.rows),
                    "requestedLimit": request.query.limit,
                    "executionLimit": plan.execution_limit,
                },
            )
        explain = result.explain or {}
        if explain.get("error"):
            raise KehrnelError(
                code="OPENEHR_QUERY_EXECUTION_FAILED",
                status=502,
                message="The openEHR query could not be executed.",
                details={"engine": result.engine_used},
            )
        has_more = len(result.rows) > request.query.limit
        next_offset = (
            request.query.offset + request.query.limit if has_more else None
        )
        return result.model_copy(
            update={
                "rows": result.rows[: request.query.limit],
                "requested_limit": request.query.limit,
                "offset": request.query.offset,
                "has_more": has_more,
                "next_offset": next_offset,
            }
        )


def _invalid_runtime_result(
    reason: str,
    *,
    details: Optional[dict[str, Any]] = None,
) -> KehrnelError:
    return KehrnelError(
        code="OPENEHR_QUERY_RESULT_INVALID",
        status=502,
        message="The active openEHR strategy returned an invalid query result.",
        details={"reason": reason, **(details or {})},
    )


def _projection_not_ready(
    reason: str,
    *,
    details: Optional[dict[str, Any]] = None,
) -> KehrnelError:
    return KehrnelError(
        code="OPENEHR_SEMANTIC_PROJECTION_NOT_READY",
        status=409,
        message=(
            "The active openEHR projection cannot safely answer this semantic query. "
            "Rebuild or repair the projection and Atlas Search index before retrying."
        ),
        details={"reason": reason, **(details or {})},
    )


def _validate_projection_integrity_receipt(
    readiness: OpenEhrSemanticProjectionReadiness,
) -> None:
    reconciliation = readiness.reconciliation
    consistency = readiness.consistency
    integrity_proven = bool(
        reconciliation.get("complete") is True
        and reconciliation.get("safe") is True
        and reconciliation.get("sourceToTargetCoverage") == "verified"
        and reconciliation.get("targetToSourceCoverage") == "verified"
        and reconciliation.get("mappedEmptyCount") == 0
        and consistency.get("model")
        == "governed-writer-mutation-fence/v1"
        and consistency.get("activeMutations") == 0
        and consistency.get("governedWritersOnly") is True
        and consistency.get("publication") == "compare-and-set"
        and readiness.rebuild_in_progress is False
        and isinstance(consistency.get("mutationSequence"), int)
        and not isinstance(consistency.get("mutationSequence"), bool)
        and consistency.get("mutationSequence") >= 0
    )
    if not integrity_proven:
        raise _projection_not_ready(
            "the projection readiness receipt lacks complete reconciliation or "
            "a closed governed-writer mutation fence",
            details={
                "reconciliation": reconciliation,
                "consistency": consistency,
            },
        )


def _validate_projection_stability(
    before: OpenEhrSemanticProjectionReadiness,
    after: OpenEhrSemanticProjectionReadiness,
) -> int:
    before_sequence = before.consistency.get("mutationSequence")
    after_sequence = after.consistency.get("mutationSequence")
    if before_sequence != after_sequence:
        raise KehrnelError(
            code="OPENEHR_SEMANTIC_PROJECTION_CHANGED_DURING_QUERY",
            status=409,
            message=(
                "The semantic projection changed while the query was running; "
                "discard the rows and retry against the new projection state."
            ),
            details={
                "beforeMutationSequence": before_sequence,
                "afterMutationSequence": after_sequence,
            },
        )
    return int(after_sequence)


def _digest_payload(payload: Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _optional_text(value: Any) -> Optional[str]:
    normalized = str(value or "").strip()
    return normalized or None


def _activation_snapshot(
    activation: Any,
    *,
    data_target_digest: Optional[str] = None,
) -> dict[str, Optional[str]]:
    """Return the immutable activation facts required by dispatch."""

    return {
        "activationId": _optional_text(getattr(activation, "activation_id", None)),
        "strategyId": _optional_text(getattr(activation, "strategy_id", None)),
        "configHash": _optional_text(getattr(activation, "config_hash", None)),
        "manifestDigest": _optional_text(
            getattr(activation, "manifest_digest", None)
        ),
        "bindingsHash": _optional_text(getattr(activation, "bindings_hash", None)),
        "dataTargetDigest": data_target_digest,
    }


def _validate_execution_activation(
    result: OpenEhrRuntimeQueryResult,
    activation: Any,
    *,
    data_target_digest: str,
) -> None:
    """Verify that runtime evidence is for the activation we authorized."""

    explain = result.explain or {}
    expected = _activation_snapshot(
        activation,
        data_target_digest=data_target_digest,
    )
    explain_keys = {
        "activationId": "activation_id",
        "strategyId": "strategy_id",
        "configHash": "config_hash",
        "manifestDigest": "manifest_digest",
        "dataTargetDigest": "data_target_digest",
    }
    mismatches: dict[str, dict[str, Optional[str]]] = {}
    for public_name, explain_name in explain_keys.items():
        expected_value = expected.get(public_name)
        if expected_value is None:
            continue
        actual_value = _optional_text(explain.get(explain_name))
        if actual_value != expected_value:
            mismatches[public_name] = {
                "expected": expected_value,
                "actual": actual_value,
            }
    if mismatches:
        raise KehrnelError(
            code="OPENEHR_EXECUTION_ACTIVATION_MISMATCH",
            status=409,
            message=(
                "The query result was produced by a different strategy "
                "activation than the one authorized for the semantic plan."
            ),
            details={"mismatches": mismatches},
        )


def _validate_compiled_execution_evidence(
    result: OpenEhrRuntimeQueryResult,
    *,
    expected_request: Mapping[str, Any],
    expected_plan: OpenEhrSemanticQueryPlan,
    expected_collection: str,
    expected_comp_id_field: str,
) -> tuple[str, str]:
    """Bind the receipt to the exact authorized MongoDB execution shape."""

    explain = result.explain or {}
    expected_aql = expected_request.get("raw_aql")
    expected_parameters = expected_request.get("params")
    actual_aql = explain.get("rawAql")
    actual_parameters = explain.get("parameters")
    mismatches: dict[str, dict[str, Any]] = {}
    if explain.get("mode") != "raw_aql_strategy":
        mismatches["mode"] = {
            "expected": "raw_aql_strategy",
            "actual": explain.get("mode"),
        }
    if actual_aql != expected_aql:
        mismatches["rawAql"] = {
            "expectedDigest": _digest_text(expected_aql),
            "actualDigest": _digest_text(actual_aql),
        }
    if actual_parameters != expected_parameters:
        mismatches["parameters"] = {
            "expectedDigest": _digest_value(expected_parameters),
            "actualDigest": _digest_value(actual_parameters),
        }
    if mismatches:
        raise KehrnelError(
            code="OPENEHR_EXECUTION_INPUT_MISMATCH",
            status=502,
            message=(
                "The active strategy did not prove that it compiled the exact "
                "AQL and parameters authorized by the semantic plan."
            ),
            details={"mismatches": mismatches},
        )

    builder = explain.get("builder")
    if (
        not isinstance(builder, Mapping)
        or builder.get("exact_path_correlation_required") is not True
    ):
        raise KehrnelError(
            code="OPENEHR_EXACT_PATH_CORRELATION_UNVERIFIED",
            status=502,
            message=(
                "The active strategy did not attest exact path correlation "
                "for the executed semantic query."
            ),
        )
    pipeline = explain.get("pipeline")
    if not isinstance(pipeline, list) or not pipeline:
        raise KehrnelError(
            code="OPENEHR_EXECUTION_PIPELINE_EVIDENCE_MISSING",
            status=502,
            message=(
                "The active strategy did not return the compiled pipeline "
                "needed to verify execution evidence."
            ),
        )

    execution_mismatches: dict[str, dict[str, Any]] = {}
    expected_engine = "text_search_dual"
    if result.engine_used != expected_engine:
        execution_mismatches["engine"] = {
            "expected": expected_engine,
            "actual": result.engine_used,
        }

    actual_collection = explain.get("collection")
    if actual_collection != expected_collection:
        execution_mismatches["collection"] = {
            "expected": expected_collection,
            "actual": actual_collection,
        }

    expected_builder = {
        "chosen": "search_pipeline_builder",
        "compiler_engine": "search_pipeline_builder",
        "execution_engine": expected_engine,
        "scope": "cross_patient",
    }
    if isinstance(builder, Mapping):
        for key, expected_value in expected_builder.items():
            if builder.get(key) != expected_value:
                execution_mismatches[f"builder.{key}"] = {
                    "expected": expected_value,
                    "actual": builder.get(key),
                }
    if explain.get("stage0") != "$search":
        execution_mismatches["stage0"] = {
            "expected": "$search",
            "actual": explain.get("stage0"),
        }

    pagination = explain.get("pagination")
    actual_page_size = (
        pagination.get("pageSize") if isinstance(pagination, Mapping) else None
    )
    if actual_page_size != expected_plan.execution_limit:
        execution_mismatches["pagination.pageSize"] = {
            "expected": expected_plan.execution_limit,
            "actual": actual_page_size,
        }

    limit_values = [
        stage.get("$limit")
        for stage in pipeline
        if isinstance(stage, Mapping) and "$limit" in stage
    ]
    if limit_values != [expected_plan.execution_limit]:
        execution_mismatches["pipeline.$limit"] = {
            "expected": [expected_plan.execution_limit],
            "actual": limit_values,
        }

    skip_values = [
        stage.get("$skip")
        for stage in pipeline
        if isinstance(stage, Mapping) and "$skip" in stage
    ]
    expected_skip_values = [expected_plan.offset] if expected_plan.offset else []
    if skip_values != expected_skip_values:
        execution_mismatches["pipeline.$skip"] = {
            "expected": expected_skip_values,
            "actual": skip_values,
        }

    first_stage = pipeline[0] if isinstance(pipeline[0], Mapping) else {}
    search = first_stage.get("$search")
    if not isinstance(search, Mapping):
        execution_mismatches["pipeline.$search"] = {
            "expected": "first top-level stage",
            "actual": "missing",
        }
    else:
        expected_sort = {expected_comp_id_field: 1}
        if search.get("sort") != expected_sort:
            execution_mismatches["pipeline.$search.sort"] = {
                "expected": expected_sort,
                "actual": search.get("sort"),
            }
        pagination_tokens = {
            key: search.get(key)
            for key in ("searchAfter", "searchBefore")
            if key in search
        }
        if pagination_tokens:
            execution_mismatches["pipeline.$search.paginationTokens"] = {
                "expected": {},
                "actual": pagination_tokens,
            }

    if execution_mismatches:
        raise KehrnelError(
            code="OPENEHR_COMPILED_EXECUTION_MISMATCH",
            status=502,
            message=(
                "The active strategy compiled an execution shape that does "
                "not preserve the authorized semantic query plan."
            ),
            details={"mismatches": execution_mismatches},
        )

    compiled_execution = {
        "engine": result.engine_used,
        "collection": actual_collection,
        "pipeline": pipeline,
    }
    encoded = json_util.dumps(
        compiled_execution,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return (
        "sha256:" + hashlib.sha256(encoded).hexdigest(),
        _digest_payload(dict(expected_request)),
    )


def _validate_result_aliases(
    result: OpenEhrRuntimeQueryResult,
    *,
    request: OpenEhrTerminologyQueryPlanRequest,
) -> None:
    """Reject rows that do not satisfy the typed projection contract."""

    expected_aliases = set(request.query.select)
    expected_aliases.update(
        projection.alias for projection in request.query.projections
    )
    for index, row in enumerate(result.rows):
        missing = sorted(expected_aliases - set(row))
        if missing:
            raise _invalid_runtime_result(
                "a returned row is missing a requested projection alias",
                details={"rowIndex": index, "missingAliases": missing},
            )


def _digest_text(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _digest_value(value: Any) -> Optional[str]:
    if not isinstance(value, Mapping):
        return None
    return _digest_payload(dict(value))


__all__ = [
    "MAX_SEMANTIC_EXECUTION_EXPANSION_CODES",
    "MAX_SEMANTIC_EXECUTION_FETCH_ROWS",
    "MAX_SEMANTIC_EXECUTION_ROWS",
    "OpenEhrRuntimeQueryResult",
    "OpenEhrSemanticExecutionEvidence",
    "OpenEhrSemanticProjectionManifest",
    "OpenEhrSemanticProjectionReadiness",
    "OpenEhrTerminologyExecutionService",
    "OpenEhrTerminologyQueryExecutionResult",
    "SUPPORTED_EXECUTION_STRATEGIES",
    "SEMANTIC_PROJECTION_MANIFEST_CONTRACT",
    "SEMANTIC_PROJECTION_READINESS_CONTRACT",
    "TERMINOLOGY_QUERY_EXECUTION_RESULT_CONTRACT",
]
