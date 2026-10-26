"""Governed SNOMED expansion to a bounded FHIR Condition resource search.

This module deliberately stops short of natural-language interpretation.  A
caller supplies a typed, version-pinned terminology selector and ordinary FHIR
criteria.  Kehrnel resolves the selector through the tenant terminology
gateway, compiles the resulting exact codes through ``fhir.resource_store``,
and executes the query against the same pinned activation.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
from typing import Any, Literal, Mapping, Optional, Protocol

from bson import json_util
from pydantic import Field, field_validator, model_validator

from kehrnel.contracts.artifacts import ContractModel, Sha256Digest
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.strategies.fhir.resource_store.scripts.serialization import (
    canonical_resources,
)


FHIR_TERMINOLOGY_COHORT_REQUEST_CONTRACT = (
    "fhir-terminology-cohort-query-request/v1"
)
FHIR_TERMINOLOGY_COHORT_RESULT_CONTRACT = (
    "fhir-terminology-cohort-query-result/v1"
)
FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT = (
    "fhir-resource-store-projection-readiness/v1"
)
FHIR_COMPILED_QUERY_EVIDENCE_CONTRACT = "fhir-compiled-query-evidence/v1"
SNOMED_SYSTEM_URI = "http://snomed.info/sct"
SUPPORTED_EXECUTION_STRATEGIES = frozenset({"fhir.resource_store"})
MAX_FHIR_COHORT_ROWS = 200
MAX_FHIR_COHORT_OFFSET = 10_000
MAX_FHIR_COHORT_EXPANSION_CODES = 256
MAX_FHIR_COHORT_CRITERIA = 20
MAX_FHIR_CRITERION_LENGTH = 2_048


class TerminologyGateway(Protocol):
    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]: ...


class FhirCohortRuntime(Protocol):
    registry: Any

    async def dispatch(
        self,
        environment_id: str,
        operation: str,
        payload: dict[str, Any],
    ) -> Any: ...


class FhirSnomedExpansionRequest(ContractModel):
    """One bounded, immutable SNOMED expansion request."""

    expansion_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    provider_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    system_uri: Literal["http://snomed.info/sct"] = SNOMED_SYSTEM_URI
    version: str = Field(min_length=1, max_length=512)
    release_id: Optional[str] = Field(default=None, max_length=128)
    expression: Optional[str] = Field(default=None, min_length=1, max_length=2_048)
    value_set_uri: Optional[str] = Field(default=None, min_length=1, max_length=2_048)
    value_set_version: Optional[str] = Field(default=None, min_length=1, max_length=256)
    max_codes: int = Field(default=128, ge=1, le=MAX_FHIR_COHORT_EXPANSION_CODES)

    @model_validator(mode="after")
    def selector_is_exactly_one_and_pinned(self) -> "FhirSnomedExpansionRequest":
        values = (
            self.version,
            self.release_id,
            self.expression,
            self.value_set_uri,
            self.value_set_version,
        )
        for value in values:
            if value is None:
                continue
            if value != value.strip() or any(
                ord(character) < 0x20 or ord(character) == 0x7F
                for character in value
            ):
                raise ValueError(
                    "terminology selector values must be normalized and contain "
                    "no control characters"
                )
        if bool(self.expression) == bool(self.value_set_uri):
            raise ValueError("exactly one of expression or valueSetUri is required")
        if bool(self.value_set_uri) != bool(self.value_set_version):
            raise ValueError(
                "valueSetUri and valueSetVersion must be supplied together"
            )
        if self.release_id and "/version/" in self.version.rstrip("/"):
            version_release = self.version.rstrip("/").rsplit("/version/", 1)[-1]
            if self.release_id != version_release:
                raise ValueError(
                    "releaseId must match the release encoded in the SNOMED version URI"
                )
        return self


class FhirTerminologyCohortQueryRequest(ContractModel):
    """A deliberately narrow terminology-backed FHIR resource-search contract."""

    contract: Literal["fhir-terminology-cohort-query-request/v1"] = (
        FHIR_TERMINOLOGY_COHORT_REQUEST_CONTRACT
    )
    resource_type: Literal["Condition"] = "Condition"
    code_search_parameter: Literal["code"] = "code"
    criteria: dict[str, str | list[str]] = Field(
        default_factory=dict,
        max_length=MAX_FHIR_COHORT_CRITERIA,
    )
    sort: str = Field(default="_id", min_length=1, max_length=256)
    limit: int = Field(default=50, ge=1, le=MAX_FHIR_COHORT_ROWS)
    offset: int = Field(default=0, ge=0, le=MAX_FHIR_COHORT_OFFSET)
    expansion_request: FhirSnomedExpansionRequest

    @field_validator("criteria")
    @classmethod
    def criteria_are_bounded_and_do_not_override_semantics(
        cls,
        criteria: dict[str, str | list[str]],
    ) -> dict[str, str | list[str]]:
        reserved = {"code", "_count", "_offset", "_sort"}
        for name, raw_value in criteria.items():
            if (
                not name
                or name != name.strip()
                or name.split(":", 1)[0] in reserved
                or any(ord(character) < 0x20 for character in name)
            ):
                raise ValueError(
                    "criteria must use normalized non-control FHIR parameters and "
                    "cannot override code or result controls"
                )
            values = raw_value if isinstance(raw_value, list) else [raw_value]
            if not values:
                raise ValueError("criterion value arrays must not be empty")
            for value in values:
                if (
                    not isinstance(value, str)
                    or not value
                    or value != value.strip()
                    or len(value) > MAX_FHIR_CRITERION_LENGTH
                    or any(ord(character) < 0x20 for character in value)
                ):
                    raise ValueError(
                        "criterion values must be bounded, normalized strings "
                        "without control characters"
                    )
        return criteria


class FhirTerminologyResolutionEvidence(ContractModel):
    expansion_id: str
    provider_id: str
    provider_type: str
    system_uri: str
    version: str
    release_id: Optional[str] = None
    selector: str
    codes: list[str] = Field(
        min_length=1,
        max_length=MAX_FHIR_COHORT_EXPANSION_CODES,
    )
    correlation_id: str
    request_digest: str
    result_digest: str
    expansion_digest: Sha256Digest


class FhirCohortRuntimeResult(ContractModel):
    result_kind: Literal["condition_resource_search"] = "condition_resource_search"
    engine_used: Literal["fhir_mql"]
    resources: list[dict[str, Any]] = Field(max_length=MAX_FHIR_COHORT_ROWS)
    total_matching_resources: int = Field(ge=0)
    returned_resource_count: int = Field(ge=0, le=MAX_FHIR_COHORT_ROWS)
    returned_subject_references: list[str] = Field(
        default_factory=list,
        max_length=MAX_FHIR_COHORT_ROWS,
    )
    explain: Optional[dict[str, Any]] = None


class FhirProjectionReadinessEvidence(ContractModel):
    resource_type: Literal["Condition"] = "Condition"
    data_target_digest: Sha256Digest
    database: str = Field(min_length=1)
    collection: str = Field(min_length=1)
    collection_exists: Literal[True] = True
    document_count: int = Field(ge=0)
    contract_current_count: int = Field(ge=0)
    canonical_projection_mismatch_count: Literal[0] = 0
    storage_schema_version: str
    projection_contract_version: str
    resource_projection_version: str
    search_index_present: Literal[True] = True
    exact_code_index_present: Literal[True] = True
    readiness_receipt_contract: Literal[
        "fhir-resource-store-projection-readiness/v1"
    ] = FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT
    readiness_receipt_digest: Sha256Digest


class FhirCohortExecutionEvidence(ContractModel):
    environment_id: str
    strategy_id: str
    activation_id: Optional[str] = None
    strategy_version: Optional[str] = None
    manifest_digest: Optional[str] = None
    config_hash: Optional[str] = None
    bindings_hash: Optional[str] = None
    data_target_digest: Sha256Digest
    query_digest: Sha256Digest
    compiled_query_digest: Sha256Digest
    compiled_filter_digest: Sha256Digest
    terminology_expansion_digest: Sha256Digest
    exact_token_projection: str
    projection_readiness: FhirProjectionReadinessEvidence
    expansion_version_pinned: Literal[True] = True
    stored_coding_version_enforced: Literal[False] = False
    row_limit: int = Field(ge=1, le=MAX_FHIR_COHORT_ROWS)
    row_count: int = Field(ge=0, le=MAX_FHIR_COHORT_ROWS)


class FhirTerminologyCohortQueryResult(ContractModel):
    contract: Literal["fhir-terminology-cohort-query-result/v1"] = (
        FHIR_TERMINOLOGY_COHORT_RESULT_CONTRACT
    )
    environment_id: str
    strategy_id: str
    resource_type: Literal["Condition"] = "Condition"
    terminology_resolution: FhirTerminologyResolutionEvidence
    result: FhirCohortRuntimeResult
    execution_evidence: FhirCohortExecutionEvidence


class FhirTerminologyCohortExecutionService:
    """Resolve one SNOMED selector and execute it through Resource Store."""

    def __init__(
        self,
        *,
        terminology_gateway: TerminologyGateway,
        runtime: FhirCohortRuntime,
        environment_id: str,
    ) -> None:
        self._terminology_gateway = terminology_gateway
        self._runtime = runtime
        self._environment_id = environment_id

    async def execute_query(
        self,
        request: FhirTerminologyCohortQueryRequest,
    ) -> FhirTerminologyCohortQueryResult:
        activation = self._active_supported_activation()
        resolution = await self._resolve_expansion(request.expansion_request)
        projection_readiness = await self._require_projection_readiness(
            request.resource_type,
            activation,
        )
        query = {
            "resource_type": request.resource_type,
            "criteria": deepcopy(request.criteria),
            "_count": request.limit,
            "_offset": request.offset,
            "_sort": request.sort,
            "debug": True,
            "terminology_code_filter": {
                "parameter": request.code_search_parameter,
                "system": resolution.system_uri,
                "codes": resolution.codes,
            },
        }
        raw_result = await self._runtime.dispatch(
            self._environment_id,
            "query",
            {
                "domain": "fhir",
                "query": query,
                "__activation_precondition": _activation_snapshot(
                    activation,
                    data_target_digest=projection_readiness.data_target_digest,
                ),
            },
        )
        query_digest = _bson_digest(query)
        result, compiled_filter, compiled_query_digest = self._validate_runtime_result(
            raw_result,
            request=request,
            resolution=resolution,
            projection_readiness=projection_readiness,
            expected_source_query_digest=query_digest,
        )
        _validate_execution_activation(
            result.explain or {},
            activation,
            data_target_digest=projection_readiness.data_target_digest,
        )
        strategy_id = str(getattr(activation, "strategy_id", "")).strip()
        return FhirTerminologyCohortQueryResult(
            environment_id=self._environment_id,
            strategy_id=strategy_id,
            terminology_resolution=resolution,
            result=result,
            execution_evidence=FhirCohortExecutionEvidence(
                environment_id=self._environment_id,
                strategy_id=strategy_id,
                activation_id=_optional_text(
                    getattr(activation, "activation_id", None)
                ),
                strategy_version=_optional_text(getattr(activation, "version", None)),
                manifest_digest=_optional_text(
                    getattr(activation, "manifest_digest", None)
                ),
                config_hash=_optional_text(getattr(activation, "config_hash", None)),
                bindings_hash=_optional_text(
                    getattr(activation, "bindings_hash", None)
                ),
                data_target_digest=projection_readiness.data_target_digest,
                query_digest=query_digest,
                compiled_query_digest=compiled_query_digest,
                # FHIR date search compilation intentionally produces BSON
                # datetimes.  Digest the executed filter with the canonical
                # BSON encoder rather than the JSON-only package encoder.
                compiled_filter_digest=_bson_digest(compiled_filter),
                terminology_expansion_digest=resolution.expansion_digest,
                exact_token_projection="_search.code_systemCode",
                projection_readiness=projection_readiness,
                row_limit=request.limit,
                row_count=result.returned_resource_count,
            ),
        )

    def _active_supported_activation(self) -> Any:
        registry = getattr(self._runtime, "registry", None)
        activation = (
            registry.get_activation(self._environment_id, "fhir")
            if registry is not None
            else None
        )
        if activation is None:
            raise KehrnelError(
                code="FHIR_ACTIVATION_REQUIRED",
                status=409,
                message="Terminology-backed resource search requires an active FHIR strategy.",
                details={"environmentId": self._environment_id},
            )
        strategy_id = str(getattr(activation, "strategy_id", "")).strip().lower()
        domain = str(getattr(activation, "domain", "")).strip().lower()
        if domain != "fhir" or strategy_id not in SUPPORTED_EXECUTION_STRATEGIES:
            raise KehrnelError(
                code="FHIR_TERMINOLOGY_COHORT_STRATEGY_UNSUPPORTED",
                status=409,
                message=(
                    "Terminology-backed FHIR resource search currently requires "
                    "an active fhir.resource_store strategy."
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
        resource_type: str,
        activation: Any,
    ) -> FhirProjectionReadinessEvidence:
        try:
            raw = await self._runtime.dispatch(
                self._environment_id,
                "op",
                {
                    "domain": "fhir",
                    "op": "fhir_stats",
                    "payload": {"resource_types": [resource_type]},
                    "__activation_precondition": _activation_snapshot(activation),
                },
            )
        except KehrnelError as exc:
            if exc.code in {
                "FHIR_SEARCH_PROJECTION_NOT_READY",
                "ACTIVATION_PRECONDITION_FAILED",
                "ACTIVATION_PRECONDITION_INVALID",
            }:
                raise
            raise _projection_not_ready(
                "the active strategy could not prove FHIR projection readiness",
                details={"causeCode": exc.code},
            ) from exc
        except Exception as exc:
            raise _projection_not_ready(
                "the active strategy could not prove FHIR projection readiness"
            ) from exc

        if not isinstance(raw, Mapping) or raw.get("ok") is not True:
            raise _projection_not_ready(
                "the active strategy returned an invalid projection-readiness result"
            )
        rows = raw.get("collections")
        matches = (
            [
                row
                for row in rows
                if isinstance(row, Mapping)
                and row.get("resource_type") == resource_type
            ]
            if isinstance(rows, list)
            else []
        )
        if len(matches) != 1:
            raise _projection_not_ready(
                "the active strategy did not report exactly one requested resource collection"
            )
        row = matches[0]
        document_count = row.get("document_count")
        current_count = row.get("contract_current_count")
        collection_exists = row.get("collection_exists")
        mismatch_count = row.get("canonical_projection_mismatch_count")
        storage_version = _required_string(row.get("storage_schema_version"))
        projection_version = _required_string(raw.get("projection_contract_version"))
        database = _required_string(raw.get("database"))
        data_target_digest = _required_string(raw.get("data_target_digest"))
        resource_version = _required_string(row.get("resource_projection_version"))
        if (
            isinstance(document_count, bool)
            or not isinstance(document_count, int)
            or document_count < 0
            or isinstance(current_count, bool)
            or not isinstance(current_count, int)
            or current_count < 0
            or collection_exists is not True
            or isinstance(mismatch_count, bool)
            or not isinstance(mismatch_count, int)
            or mismatch_count < 0
            or row.get("search_configured") is not True
            or not _required_string(row.get("collection"))
            or not database
            or not _is_sha256_digest(data_target_digest)
            or not storage_version
            or not projection_version
            or not resource_version
        ):
            raise _projection_not_ready(
                "the active strategy returned incomplete projection-version evidence"
            )
        if mismatch_count != 0:
            raise _projection_not_ready(
                "canonical Condition.code codings do not match the exact search-token projection",
                details={
                    "resourceType": resource_type,
                    "canonicalProjectionMismatchCount": mismatch_count,
                    "projectionField": "_search.code_systemCode",
                },
            )
        if current_count != document_count:
            raise _projection_not_ready(
                "the Condition collection contains missing or stale search projections",
                details={
                    "resourceType": resource_type,
                    "documentCount": document_count,
                    "contractCurrentCount": current_count,
                    "missingOrStaleCount": document_count - current_count,
                    "resourceProjectionVersion": resource_version,
                },
            )
        index_present = row.get("search_index_present")
        if index_present is not True:
            raise _projection_not_ready(
                "the exact Condition search-token index is missing or could not be verified"
            )
        exact_code_index_present = row.get("exact_code_index_present")
        if exact_code_index_present is not True:
            raise _projection_not_ready(
                "the _search.code_systemCode index is missing or could not be verified"
            )
        receipt = row.get("projection_readiness_receipt")
        if not isinstance(receipt, Mapping):
            raise _projection_not_ready(
                "the active strategy returned no governed projection-readiness receipt"
            )
        receipt_digest = _required_string(receipt.get("receiptDigest"))
        receipt_payload = {
            str(key): value
            for key, value in receipt.items()
            if key != "receiptDigest"
        }
        expected_receipt = {
            "contract": FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT,
            "state": "ready",
            "resourceType": resource_type,
            "database": database,
            "collection": _required_string(row.get("collection")),
            "collectionExists": True,
            "documentCount": document_count,
            "contractCurrentCount": current_count,
            "canonicalProjectionMismatchCount": mismatch_count,
            "storageSchemaVersion": storage_version,
            "projectionContractVersion": projection_version,
            "resourceProjectionVersion": resource_version,
            "searchConfigured": True,
            "searchIndexPresent": True,
            "exactCodeIndexPresent": True,
        }
        if receipt_payload != expected_receipt or receipt_digest != _digest(
            expected_receipt
        ):
            raise _projection_not_ready(
                "the governed projection-readiness receipt is missing, stale, or invalid"
            )
        return FhirProjectionReadinessEvidence(
            resource_type="Condition",
            data_target_digest=data_target_digest,
            database=database,
            collection=_required_string(row.get("collection")),
            collection_exists=True,
            document_count=document_count,
            contract_current_count=current_count,
            canonical_projection_mismatch_count=0,
            storage_schema_version=storage_version,
            projection_contract_version=projection_version,
            resource_projection_version=resource_version,
            search_index_present=True,
            exact_code_index_present=True,
            readiness_receipt_contract=FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT,
            readiness_receipt_digest=receipt_digest,
        )

    async def _resolve_expansion(
        self,
        request: FhirSnomedExpansionRequest,
    ) -> FhirTerminologyResolutionEvidence:
        payload: dict[str, Any] = {
            "provider_id": request.provider_id,
            "system": request.system_uri,
            "version": request.version,
            "offset": 0,
            # One extra member detects silent truncation at the accepted bound.
            "count": request.max_codes + 1,
        }
        if request.expression:
            payload["expression"] = request.expression
        if request.release_id:
            payload["release_id"] = request.release_id
        else:
            payload["release_id"] = _snomed_release_from_version(request.version)
        if request.value_set_uri:
            payload["url"] = request.value_set_uri
            payload["value_set_version"] = request.value_set_version

        result = await self._terminology_gateway.execute("expand", payload)
        if not isinstance(result, Mapping):
            raise _invalid_terminology_result(
                request,
                "the terminology gateway returned a non-object result",
            )
        return _resolution_from_result(request, result)

    @staticmethod
    def _validate_runtime_result(
        raw_result: Any,
        *,
        request: FhirTerminologyCohortQueryRequest,
        resolution: FhirTerminologyResolutionEvidence,
        projection_readiness: FhirProjectionReadinessEvidence,
        expected_source_query_digest: str,
    ) -> tuple[FhirCohortRuntimeResult, dict[str, Any], str]:
        if not isinstance(raw_result, Mapping):
            raise _invalid_query_result("the strategy returned a non-object result")
        if raw_result.get("ok") is False:
            raise _invalid_query_result("the strategy reported an unsuccessful result")
        engine_used = _required_string(raw_result.get("engine_used"))
        rows = raw_result.get("rows")
        explain = raw_result.get("explain")
        if (
            engine_used != "fhir_mql"
            or not isinstance(rows, list)
            or not isinstance(explain, Mapping)
        ):
            raise _invalid_query_result(
                "the strategy result does not satisfy the FHIR query-result contract"
            )
        if len(rows) > request.limit:
            raise _invalid_query_result(
                "the strategy returned more resources than the governed query limit",
                details={"returnedRows": len(rows), "queryLimit": request.limit},
            )

        builder = explain.get("builder")
        filter_evidence = (
            builder.get("terminology_code_filter")
            if isinstance(builder, Mapping)
            else None
        )
        expected_tokens = [
            f"{resolution.system_uri}|{code}" for code in resolution.codes
        ]
        expected_token_digest = _digest(expected_tokens)
        if not isinstance(filter_evidence, Mapping) or any(
            (
                filter_evidence.get("parameter") != request.code_search_parameter,
                filter_evidence.get("system") != resolution.system_uri,
                filter_evidence.get("projection_field")
                != "_search.code_systemCode",
                filter_evidence.get("code_count") != len(resolution.codes),
                filter_evidence.get("token_digest") != expected_token_digest,
            )
        ):
            raise _invalid_query_result(
                "the strategy did not attest the exact terminology expansion filter"
            )
        compiled_filter = explain.get("filter")
        if not isinstance(compiled_filter, dict) or not _has_mandatory_exact_token_filter(
            compiled_filter,
            field="_search.code_systemCode",
            tokens=expected_tokens,
        ):
            raise _invalid_query_result(
                "the compiled FHIR filter is missing the exact system|code expansion"
            )

        compiler_evidence = (
            builder.get("compiler_evidence")
            if isinstance(builder, Mapping)
            else None
        )
        execution = explain.get("execution")
        if not isinstance(compiler_evidence, Mapping) or not isinstance(
            execution, Mapping
        ):
            raise _invalid_query_result(
                "the strategy returned no complete compiler/runtime query evidence"
            )
        compiled_sort = compiler_evidence.get("sort")
        if not isinstance(compiled_sort, list) or not compiled_sort:
            raise _invalid_query_result(
                "the strategy did not compile the requested deterministic sort"
            )
        compiled_query_contract = {
            "contract": FHIR_COMPILED_QUERY_EVIDENCE_CONTRACT,
            "resourceType": request.resource_type,
            "database": projection_readiness.database,
            "collection": projection_readiness.collection,
            "filter": compiled_filter,
            "sort": compiled_sort,
            "limit": request.limit,
            "offset": request.offset,
        }
        expected_compiled_query_digest = _bson_digest(compiled_query_contract)
        evidence_mismatch = any(
            (
                compiler_evidence.get("contract")
                != FHIR_COMPILED_QUERY_EVIDENCE_CONTRACT,
                compiler_evidence.get("source_query_digest")
                != expected_source_query_digest,
                compiler_evidence.get("compiled_query_digest")
                != expected_compiled_query_digest,
                compiler_evidence.get("resource_type") != request.resource_type,
                compiler_evidence.get("database") != projection_readiness.database,
                compiler_evidence.get("collection") != projection_readiness.collection,
                compiler_evidence.get("limit") != request.limit,
                compiler_evidence.get("offset") != request.offset,
                execution.get("database") != projection_readiness.database,
                execution.get("collection") != projection_readiness.collection,
                execution.get("sort") != compiled_sort,
                execution.get("limit") != request.limit,
                execution.get("skip") != request.offset,
                execution.get("source_query_digest")
                != expected_source_query_digest,
                execution.get("compiled_query_digest")
                != expected_compiled_query_digest,
            )
        )
        if evidence_mismatch:
            raise _invalid_query_result(
                "the executed collection, filter, sort, or pagination does not match the complete typed query",
                details={
                    "expectedSourceQueryDigest": expected_source_query_digest,
                    "expectedCompiledQueryDigest": expected_compiled_query_digest,
                    "expectedDatabase": projection_readiness.database,
                    "expectedCollection": projection_readiness.collection,
                    "expectedLimit": request.limit,
                    "expectedOffset": request.offset,
                },
            )

        canonical = canonical_resources(rows)
        allowed_codes = set(resolution.codes)
        for resource in canonical:
            if resource.get("resourceType") != request.resource_type:
                raise _invalid_query_result(
                    "the strategy returned a resource of the wrong FHIR type"
                )
            codings = ((resource.get("code") or {}).get("coding") or [])
            if not any(
                isinstance(coding, Mapping)
                and _required_string(coding.get("system")).rstrip("/")
                == resolution.system_uri.rstrip("/")
                and _required_string(coding.get("code")) in allowed_codes
                for coding in codings
            ):
                raise _invalid_query_result(
                    "a returned Condition does not contain a coding from the resolved expansion"
                )

        total = explain.get("total")
        returned = explain.get("returned")
        if (
            isinstance(total, bool)
            or not isinstance(total, int)
            or total < len(canonical)
            or isinstance(returned, bool)
            or not isinstance(returned, int)
            or returned != len(canonical)
        ):
            raise _invalid_query_result(
                "the strategy returned inconsistent resource-count evidence"
            )
        subject_refs = sorted(
            {
                reference
                for resource in canonical
                for reference in [
                    _required_string((resource.get("subject") or {}).get("reference"))
                ]
                if reference
            }
        )
        validated = FhirCohortRuntimeResult(
            engine_used="fhir_mql",
            resources=canonical,
            total_matching_resources=total,
            returned_resource_count=len(canonical),
            returned_subject_references=subject_refs,
            explain=dict(explain),
        )
        return validated, compiled_filter, expected_compiled_query_digest


def _resolution_from_result(
    request: FhirSnomedExpansionRequest,
    result: Mapping[str, Any],
) -> FhirTerminologyResolutionEvidence:
    if result.get("ok") is False:
        raise _invalid_terminology_result(
            request,
            "the provider reported an unsuccessful expansion",
        )
    provider = result.get("provider")
    evidence = result.get("evidence")
    if not isinstance(provider, Mapping) or not isinstance(evidence, Mapping):
        raise _invalid_terminology_result(
            request,
            "the expansion is missing tenant-gateway provider evidence",
        )
    provider_id = _required_string(provider.get("id"))
    evidence_provider_id = _required_string(evidence.get("providerId"))
    provider_type = _required_string(
        evidence.get("providerType") or provider.get("type")
    )
    if provider_type not in {"kehrnel-strategy", "fhir-terminology"}:
        raise _invalid_terminology_result(
            request,
            "the provider type is missing or unsupported",
        )
    if provider_id != request.provider_id or evidence_provider_id != request.provider_id:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_PROVIDER_MISMATCH",
            status=422,
            message="The terminology result does not match the requested provider.",
            details={
                "requestedProviderId": request.provider_id,
                "resultProviderId": provider_id or None,
                "evidenceProviderId": evidence_provider_id or None,
            },
        )
    if provider_type == "fhir-terminology" and request.expression:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_SELECTOR_UNSUPPORTED",
            status=422,
            message=(
                "External FHIR terminology expansion requires a pinned ValueSet; "
                "native ECL expressions are not supported by that adapter."
            ),
        )

    result_system = _required_string(result.get("system") or evidence.get("system"))
    if result_system.rstrip("/") != request.system_uri.rstrip("/"):
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_SYSTEM_MISMATCH",
            status=422,
            message="The terminology result does not match the requested SNOMED system.",
            details={
                "requestedSystemUri": request.system_uri,
                "resultSystemUri": result_system or None,
            },
        )
    _validate_value_set_identity(request, result)
    codes, observed_versions = _extract_snomed_codes(request, result)
    _validate_expansion_completeness(
        request,
        result,
        returned_code_count=len(codes),
    )
    version, release_id = _resolve_version_evidence(
        request,
        result,
        provider_type=provider_type,
        observed_versions=observed_versions,
    )
    correlation_id = _required_string(evidence.get("correlationId"))
    request_digest = _required_string(evidence.get("requestDigest"))
    result_digest = _required_string(evidence.get("resultDigest"))
    if not correlation_id or not request_digest or not result_digest:
        raise _invalid_terminology_result(
            request,
            "the expansion is missing gateway correlation or digest evidence",
        )
    selector = (
        request.expression
        if request.expression
        else f"value-set:{request.value_set_uri}|{request.value_set_version}"
    )
    expansion_payload = {
        "expansionId": request.expansion_id,
        "providerId": request.provider_id,
        "providerType": provider_type,
        "systemUri": request.system_uri,
        "version": version,
        "releaseId": release_id,
        "selector": selector,
        "codes": codes,
        "correlationId": correlation_id,
        "requestDigest": request_digest,
        "resultDigest": result_digest,
    }
    return FhirTerminologyResolutionEvidence(
        **expansion_payload,
        expansionDigest=_digest(expansion_payload),
    )


def _extract_snomed_codes(
    request: FhirSnomedExpansionRequest,
    result: Mapping[str, Any],
) -> tuple[list[str], set[str]]:
    code_sets: list[set[str]] = []
    observed_versions: set[str] = set()
    for name in ("concepts", "matches"):
        collection = result.get(name)
        if collection is None:
            continue
        if not isinstance(collection, list):
            raise _invalid_terminology_result(request, f"{name} must be an array")
        parsed: set[str] = set()
        for item in collection:
            if not isinstance(item, Mapping):
                raise _invalid_terminology_result(
                    request,
                    f"{name} contains a non-object member",
                )
            _collect_snomed_member(request, item, parsed, observed_versions)
        code_sets.append(parsed)

    value_set = result.get("valueSet")
    if value_set is not None:
        if not isinstance(value_set, Mapping) or value_set.get("resourceType") != "ValueSet":
            raise _invalid_terminology_result(
                request,
                "valueSet must be a FHIR ValueSet resource",
            )
        expansion = value_set.get("expansion")
        if not isinstance(expansion, Mapping):
            raise _invalid_terminology_result(
                request,
                "the FHIR ValueSet has no expansion",
            )
        fhir_codes: set[str] = set()
        _collect_fhir_contains(
            request,
            expansion.get("contains"),
            fhir_codes,
            observed_versions,
        )
        code_sets.append(fhir_codes)

    if not code_sets:
        raise _invalid_terminology_result(
            request,
            "the expansion has no recognized member collection",
        )
    if any(code_set != code_sets[0] for code_set in code_sets[1:]):
        raise _invalid_terminology_result(
            request,
            "provider member collections disagree",
        )
    codes = sorted(code_sets[0])
    if not codes:
        raise _invalid_terminology_result(request, "the expansion contains no codes")
    if len(codes) > request.max_codes:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_EXPANSION_TOO_LARGE",
            status=422,
            message="The SNOMED expansion exceeds its declared code bound.",
            details={
                "maxCodes": request.max_codes,
                "returnedCodes": len(codes),
            },
        )
    return codes, observed_versions


def _collect_snomed_member(
    request: FhirSnomedExpansionRequest,
    item: Mapping[str, Any],
    codes: set[str],
    versions: set[str],
) -> None:
    code = _required_string(item.get("conceptId") or item.get("code"))
    if not code or not code.isdigit() or not (6 <= len(code) <= 18):
        raise _invalid_terminology_result(
            request,
            "an expansion member has an invalid SNOMED CT identifier",
        )
    system = _required_string(item.get("system"))
    if system and system.rstrip("/") != request.system_uri.rstrip("/"):
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_SYSTEM_MISMATCH",
            status=422,
            message="A terminology expansion member uses a different code system.",
            details={"memberSystemUri": system, "code": code},
        )
    version = _required_string(item.get("version"))
    if version:
        versions.add(version)
    codes.add(code)


def _collect_fhir_contains(
    request: FhirSnomedExpansionRequest,
    contains: Any,
    codes: set[str],
    versions: set[str],
) -> None:
    if not isinstance(contains, list):
        raise _invalid_terminology_result(
            request,
            "ValueSet.expansion.contains must be an array",
        )
    stack = list(contains)
    inspected = 0
    while stack:
        inspected += 1
        if inspected > request.max_codes + 1:
            raise KehrnelError(
                code="FHIR_TERMINOLOGY_EXPANSION_TOO_LARGE",
                status=422,
                message="The SNOMED expansion exceeds its declared code bound.",
                details={"maxCodes": request.max_codes},
            )
        item = stack.pop()
        if not isinstance(item, Mapping):
            raise _invalid_terminology_result(
                request,
                "ValueSet.expansion.contains has a non-object member",
            )
        children = item.get("contains")
        if children is not None:
            if not isinstance(children, list):
                raise _invalid_terminology_result(
                    request,
                    "nested ValueSet.expansion.contains must be an array",
                )
            stack.extend(children)
        if item.get("code") is None and children:
            continue
        _collect_snomed_member(request, item, codes, versions)


def _strict_non_negative_int(
    request: FhirSnomedExpansionRequest,
    value: Any,
    *,
    label: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _invalid_terminology_result(
            request,
            f"{label} must be a non-negative integer",
        )
    return value


def _validate_expansion_completeness(
    request: FhirSnomedExpansionRequest,
    result: Mapping[str, Any],
    *,
    returned_code_count: int,
) -> None:
    truncated = result.get("truncated")
    if truncated is not None and not isinstance(truncated, bool):
        raise _invalid_terminology_result(request, "truncated must be a boolean")
    if truncated is True:
        raise _incomplete_expansion(request, "provider marked the expansion as truncated")

    authoritative_complete = False
    declared_totals: list[tuple[str, int]] = []
    page_has_more: Optional[bool] = None
    page_next_offset: Optional[int] = None
    page_limit: Optional[int] = None
    page = result.get("page")
    if page is not None:
        if not isinstance(page, Mapping):
            raise _invalid_terminology_result(request, "page metadata must be an object")
        offset = _strict_non_negative_int(
            request,
            page.get("offset", 0),
            label="page.offset",
        )
        if offset != 0:
            raise _incomplete_expansion(request, "provider expansion starts after offset zero")

        if "limit" in page:
            page_limit = _strict_non_negative_int(
                request,
                page.get("limit"),
                label="page.limit",
            )
            if page_limit < 1:
                raise _invalid_terminology_result(
                    request,
                    "page.limit must be greater than zero",
                )
        if "returned" in page:
            page_returned = _strict_non_negative_int(
                request,
                page.get("returned"),
                label="page.returned",
            )
            if page_returned != returned_code_count:
                raise _invalid_terminology_result(
                    request,
                    "page.returned does not match the expansion member count",
                )
            if page_limit is not None and page_returned > page_limit:
                raise _invalid_terminology_result(
                    request,
                    "page.returned exceeds page.limit",
                )
        else:
            page_returned = None

        if "hasMore" in page:
            if not isinstance(page.get("hasMore"), bool):
                raise _invalid_terminology_result(
                    request,
                    "page.hasMore must be a boolean",
                )
            page_has_more = page["hasMore"]
        if "nextOffset" in page and page.get("nextOffset") is not None:
            page_next_offset = _strict_non_negative_int(
                request,
                page.get("nextOffset"),
                label="page.nextOffset",
            )
            if page_next_offset <= offset:
                raise _invalid_terminology_result(
                    request,
                    "page.nextOffset must advance beyond page.offset",
                )
            if (
                page_returned is not None
                and page_next_offset != offset + page_returned
            ):
                raise _invalid_terminology_result(
                    request,
                    "page.nextOffset does not follow page.returned",
                )
        if page_has_more is False and page_next_offset is not None:
            raise _invalid_terminology_result(
                request,
                "page.hasMore=false contradicts a non-null page.nextOffset",
            )
        if page_has_more is True and page_next_offset is None:
            raise _invalid_terminology_result(
                request,
                "page.hasMore=true requires a non-null page.nextOffset",
            )
        if "total" in page and page.get("total") is not None:
            declared_totals.append(
                (
                    "page.total",
                    _strict_non_negative_int(
                        request,
                        page.get("total"),
                        label="page.total",
                    ),
                )
            )
        if (
            "returned" in page
            and page_has_more is False
            and "nextOffset" in page
            and page.get("nextOffset") is None
        ):
            authoritative_complete = True

    value_set = result.get("valueSet")
    if isinstance(value_set, Mapping) and isinstance(value_set.get("expansion"), Mapping):
        expansion = value_set["expansion"]
        offset = _strict_non_negative_int(
            request,
            expansion.get("offset", 0),
            label="ValueSet.expansion.offset",
        )
        if offset != 0:
            raise _incomplete_expansion(request, "FHIR expansion starts after offset zero")
        if expansion.get("total") is not None:
            declared_totals.append(
                (
                    "ValueSet.expansion.total",
                    _strict_non_negative_int(
                        request,
                        expansion.get("total"),
                        label="ValueSet.expansion.total",
                    ),
                )
            )

    distinct_totals = {total for _, total in declared_totals}
    if len(distinct_totals) > 1:
        raise _invalid_terminology_result(
            request,
            "terminology pagination totals contradict one another",
        )
    if declared_totals:
        total = declared_totals[0][1]
        if total < returned_code_count:
            raise _invalid_terminology_result(
                request,
                "the declared expansion total is smaller than the returned member count",
            )
        if page_has_more is False and total > returned_code_count:
            raise _invalid_terminology_result(
                request,
                "page.hasMore=false contradicts the declared expansion total",
            )
        if page_has_more is True and total <= returned_code_count:
            raise _invalid_terminology_result(
                request,
                "page.hasMore=true contradicts the declared expansion total",
            )
        if page_next_offset is not None and total <= returned_code_count:
            raise _invalid_terminology_result(
                request,
                "page.nextOffset contradicts the declared expansion total",
            )
        if total > returned_code_count:
            raise _incomplete_expansion(
                request,
                "the provider returned fewer members than its declared total",
            )
        authoritative_complete = True

    if page_has_more is True or page_next_offset is not None:
        raise _incomplete_expansion(request, "provider returned another page")

    at_unproven_boundary = returned_code_count >= request.max_codes or (
        page_limit is not None and returned_code_count >= page_limit
    )
    if at_unproven_boundary and not authoritative_complete:
        raise _incomplete_expansion(
            request,
            "a boundary-sized expansion lacks authoritative completeness evidence",
        )


def _validate_value_set_identity(
    request: FhirSnomedExpansionRequest,
    result: Mapping[str, Any],
) -> None:
    if not request.value_set_uri:
        return
    value_set = result.get("valueSet")
    if value_set is not None and not isinstance(value_set, Mapping):
        raise _invalid_terminology_result(request, "valueSet must be an object")
    value_set = value_set if isinstance(value_set, Mapping) else {}
    result_uri = _required_string(value_set.get("url") or result.get("url"))
    result_version = _required_string(
        value_set.get("version") or result.get("valueSetVersion")
    )
    if result_uri != request.value_set_uri or result_version != request.value_set_version:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_VALUE_SET_MISMATCH",
            status=422,
            message="The terminology result does not match the requested ValueSet definition.",
            details={
                "requestedValueSetUri": request.value_set_uri,
                "requestedValueSetVersion": request.value_set_version,
                "resultValueSetUri": result_uri or None,
                "resultValueSetVersion": result_version or None,
            },
        )


def _resolve_version_evidence(
    request: FhirSnomedExpansionRequest,
    result: Mapping[str, Any],
    *,
    provider_type: str,
    observed_versions: set[str],
) -> tuple[str, Optional[str]]:
    release_id = _required_string(
        result.get("releaseId") or (result.get("evidence") or {}).get("releaseId")
    )
    result_version = _required_string(result.get("version"))
    if provider_type == "kehrnel-strategy":
        expected_release = request.release_id or _snomed_release_from_version(
            request.version
        )
        if release_id != expected_release or (
            result_version and result_version != request.version
        ):
            raise KehrnelError(
                code="FHIR_TERMINOLOGY_VERSION_MISMATCH",
                status=422,
                message=(
                    "The native terminology result does not prove the requested "
                    "SNOMED release."
                ),
                details={
                    "requestedVersion": request.version,
                    "requestedReleaseId": expected_release,
                    "resultVersion": result_version or None,
                    "resultReleaseId": release_id or None,
                },
            )
        return request.version, release_id

    if result_version:
        observed_versions.add(result_version)
    observed_versions.update(_fhir_expansion_versions(result.get("valueSet")))
    if observed_versions != {request.version}:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_VERSION_MISMATCH",
            status=422,
            message=(
                "The external terminology response does not prove the requested "
                "SNOMED version."
            ),
            details={
                "requestedVersion": request.version,
                "observedVersions": sorted(observed_versions),
            },
        )
    return request.version, release_id or None


def _fhir_expansion_versions(value_set: Any) -> set[str]:
    if not isinstance(value_set, Mapping):
        return set()
    expansion = value_set.get("expansion")
    if not isinstance(expansion, Mapping):
        return set()
    versions: set[str] = set()
    for parameter in expansion.get("parameter") or []:
        if not isinstance(parameter, Mapping):
            continue
        name = str(parameter.get("name") or "").replace("-", "").lower()
        if name not in {"version", "systemversion"}:
            continue
        value = next(
            (
                candidate
                for key, candidate in parameter.items()
                if str(key).startswith("value") and candidate is not None
            ),
            None,
        )
        normalized = _required_string(value)
        if normalized:
            versions.add(normalized)
    return versions


def _has_mandatory_exact_token_filter(
    value: Any,
    *,
    field: str,
    tokens: list[str],
) -> bool:
    """Return true only when the exact token clause is conjunctively required.

    A recursive "contains" check is unsafe: the expected clause inside ``$or``
    or ``$nor`` does not constrain every returned document.  MongoDB object
    fields are implicit conjunctions, while only explicit ``$and`` children
    preserve that guarantee recursively.
    """

    if not isinstance(value, Mapping):
        return False
    expected = sorted(tokens)
    clause = value.get(field)
    if (
        isinstance(clause, Mapping)
        and set(clause) == {"$in"}
        and isinstance(clause.get("$in"), list)
        and sorted(clause["$in"]) == expected
    ):
        return True
    conjunction = value.get("$and")
    if isinstance(conjunction, list):
        for nested in conjunction:
            if isinstance(nested, Mapping) and _has_mandatory_exact_token_filter(
                nested,
                field=field,
                tokens=tokens,
            ):
                return True
    return False


def _validate_execution_activation(
    explain: Mapping[str, Any],
    activation: Any,
    *,
    data_target_digest: str,
) -> None:
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
            code="FHIR_EXECUTION_ACTIVATION_MISMATCH",
            status=409,
            message=(
                "The FHIR resource search was produced by a different strategy activation "
                "than the one authorized before terminology resolution."
            ),
            details={"mismatches": mismatches},
        )


def _activation_snapshot(
    activation: Any,
    *,
    data_target_digest: Optional[str] = None,
) -> dict[str, Optional[str]]:
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


def _is_sha256_digest(value: str) -> bool:
    return (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _snomed_release_from_version(version: str) -> str:
    normalized = version.rstrip("/")
    if "/version/" in normalized:
        release_id = normalized.rsplit("/version/", 1)[-1]
    elif "://" not in normalized:
        release_id = normalized
    else:
        release_id = ""
    if not release_id:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_VERSION_UNSUPPORTED",
            status=422,
            message=(
                "A SNOMED expansion requires releaseId or a version URI with a "
                "concrete release identifier."
            ),
            details={"version": version},
        )
    return release_id


def _invalid_terminology_result(
    request: FhirSnomedExpansionRequest,
    reason: str,
) -> KehrnelError:
    return KehrnelError(
        code="FHIR_TERMINOLOGY_RESULT_INVALID",
        status=502,
        message="The terminology provider returned an invalid SNOMED expansion.",
        details={"expansionId": request.expansion_id, "reason": reason},
    )


def _incomplete_expansion(
    request: FhirSnomedExpansionRequest,
    reason: str,
) -> KehrnelError:
    return KehrnelError(
        code="FHIR_TERMINOLOGY_EXPANSION_INCOMPLETE",
        status=422,
        message="A paginated or truncated SNOMED expansion cannot drive a FHIR resource search.",
        details={"expansionId": request.expansion_id, "reason": reason},
    )


def _invalid_query_result(
    reason: str,
    *,
    details: Optional[dict[str, Any]] = None,
) -> KehrnelError:
    return KehrnelError(
        code="FHIR_COHORT_QUERY_RESULT_INVALID",
        status=502,
        message="The active FHIR strategy returned an invalid resource-search result.",
        details={"reason": reason, **(details or {})},
    )


def _projection_not_ready(
    reason: str,
    *,
    details: Optional[dict[str, Any]] = None,
) -> KehrnelError:
    return KehrnelError(
        code="FHIR_SEARCH_PROJECTION_NOT_READY",
        status=409,
        message=(
            "FHIR terminology resource search requires complete, current "
            "Resource Store search projections."
        ),
        details={"reason": reason, **(details or {})},
    )


def _required_string(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _optional_text(value: Any) -> Optional[str]:
    normalized = str(value or "").strip()
    return normalized or None


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _bson_digest(value: Any) -> str:
    encoded = json_util.dumps(
        value,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


__all__ = [
    "FHIR_TERMINOLOGY_COHORT_REQUEST_CONTRACT",
    "FHIR_TERMINOLOGY_COHORT_RESULT_CONTRACT",
    "FhirSnomedExpansionRequest",
    "FhirTerminologyCohortExecutionService",
    "FhirTerminologyCohortQueryRequest",
    "FhirTerminologyCohortQueryResult",
    "FhirProjectionReadinessEvidence",
    "MAX_FHIR_COHORT_EXPANSION_CODES",
    "MAX_FHIR_COHORT_ROWS",
]
