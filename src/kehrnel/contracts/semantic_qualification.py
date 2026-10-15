"""Contracts and verifier for deployed semantic-execution qualification.

The qualification artifact deliberately contains immutable execution evidence,
not query results.  It is therefore safe to retain as a deployment receipt and
does not duplicate clinical records returned by the qualified endpoints.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Annotated, Any, Callable, Literal, Mapping, Optional, Union
from urllib.parse import quote, urlsplit, urlunsplit

from pydantic import Field, field_validator, model_validator

from .artifacts import ContractModel, Sha256Digest
from .package_digest import canonical_json_bytes


OPEN_EHR_RESULT_CONTRACT = "openehr-terminology-query-execution-result/v1"
FHIR_RESULT_CONTRACT = "fhir-terminology-cohort-query-result/v1"
QUALIFICATION_SPEC_CONTRACT = "semantic-execution-qualification-spec/v1"
QUALIFICATION_RESULT_CONTRACT = "semantic-execution-qualification-result/v1"
_SAFE_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")


def content_digest(value: Mapping[str, Any] | list[Any]) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _response_content_digest(value: Mapping[str, Any]) -> str:
    """Digest valid JSON response evidence, including finite timing numbers.

    Kehrnel package contracts deliberately prohibit floating-point values, but
    live endpoint responses legitimately include latency measurements.  Keep
    that stricter contract intact and use a response-only deterministic JSON
    encoding for the opaque response digest.
    """

    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _safe_endpoint_error_code(payload: Any) -> str:
    """Extract only a bounded machine code from known API error envelopes."""

    candidates: list[Any] = []
    if isinstance(payload, Mapping):
        error = payload.get("error")
        if isinstance(error, Mapping):
            candidates.append(error.get("code"))
        issues = payload.get("issue")
        if isinstance(issues, list) and issues and isinstance(issues[0], Mapping):
            details = issues[0].get("details")
            if isinstance(details, Mapping):
                candidates.append(details.get("text"))
    for candidate in candidates:
        if isinstance(candidate, str) and _SAFE_ERROR_CODE.fullmatch(candidate):
            return candidate
    return "ENDPOINT_REJECTED_PROBE"


class OpenEhrQualificationProbe(ContractModel):
    probe_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    kind: Literal["openehr-terminology-query"]
    template_id: str = Field(min_length=1, max_length=512)
    request: dict[str, Any]

    @field_validator("template_id")
    @classmethod
    def normalized_template_id(cls, value: str) -> str:
        if value != value.strip() or any(
            ord(character) < 0x20 for character in value
        ):
            raise ValueError(
                "templateId must be normalized and contain no control characters"
            )
        return value

    @property
    def endpoint_path(self) -> str:
        template_id = quote(self.template_id, safe="")
        return (
            f"/api/domains/openehr/semantics/templates/{template_id}/"
            "terminology-query-executions"
        )


class FhirQualificationProbe(ContractModel):
    probe_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    kind: Literal["fhir-terminology-cohort"]
    request: dict[str, Any]

    @property
    def endpoint_path(self) -> str:
        return "/api/domains/fhir/semantic/terminology-query-executions"


SemanticQualificationProbe = Annotated[
    Union[OpenEhrQualificationProbe, FhirQualificationProbe],
    Field(discriminator="kind"),
]


class SemanticExecutionQualificationSpec(ContractModel):
    contract: Literal["semantic-execution-qualification-spec/v1"] = (
        QUALIFICATION_SPEC_CONTRACT
    )
    qualification_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    environment_id: str = Field(min_length=1, max_length=256)
    base_url: str = Field(min_length=1, max_length=2048)
    probes: list[SemanticQualificationProbe] = Field(min_length=1, max_length=32)

    @field_validator("environment_id")
    @classmethod
    def normalized_environment_id(cls, value: str) -> str:
        if value != value.strip() or any(
            ord(character) < 0x20 for character in value
        ):
            raise ValueError(
                "environmentId must be normalized and contain no control characters"
            )
        return value

    @field_validator("base_url")
    @classmethod
    def safe_base_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "baseUrl must be an HTTP(S) origin or path without credentials, query, or fragment"
            )
        normalized_path = parsed.path.rstrip("/")
        return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))

    @model_validator(mode="after")
    def unique_probe_ids(self) -> "SemanticExecutionQualificationSpec":
        probe_ids = [probe.probe_id for probe in self.probes]
        if len(probe_ids) != len(set(probe_ids)):
            raise ValueError("probeId values must be unique")
        return self

    @property
    def target_origin_digest(self) -> str:
        parsed = urlsplit(self.base_url)
        origin = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
        return content_digest({"origin": origin})


class SemanticExecutionEvidenceSummary(ContractModel):
    environment_id: str = Field(min_length=1)
    strategy_id: str = Field(min_length=1)
    activation_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    manifest_digest: Sha256Digest
    config_hash: Sha256Digest
    bindings_hash: Sha256Digest
    data_target_digest: Sha256Digest
    terminology_expansion_digests: list[Sha256Digest] = Field(min_length=1)
    compiled_artifact_digests: dict[str, Sha256Digest] = Field(min_length=1)
    projection_readiness_digest: Sha256Digest
    projection_live_verified: Literal[True] = True
    engine_used: str = Field(min_length=1)
    row_count: int = Field(ge=0)


class SemanticQualificationFailure(ContractModel):
    code: str = Field(min_length=1, max_length=128, pattern=r"^[A-Z][A-Z0-9_]*$")
    message: str = Field(min_length=1, max_length=512)


class SemanticQualificationProbeResult(ContractModel):
    probe_id: str
    kind: Literal["openehr-terminology-query", "fhir-terminology-cohort"]
    status: Literal["qualified", "failed"]
    endpoint_path: str = Field(pattern=r"^/api/")
    http_status: Optional[int] = Field(default=None, ge=100, le=599)
    request_digest: Sha256Digest
    response_digest: Optional[Sha256Digest] = None
    result_contract: Optional[str] = None
    evidence: Optional[SemanticExecutionEvidenceSummary] = None
    failure: Optional[SemanticQualificationFailure] = None

    @model_validator(mode="after")
    def status_has_matching_detail(self) -> "SemanticQualificationProbeResult":
        if self.status == "qualified":
            if self.http_status != 200 or self.evidence is None or self.failure is not None:
                raise ValueError("qualified probes require HTTP 200 evidence and no failure")
        elif self.failure is None or self.evidence is not None:
            raise ValueError("failed probes require a failure and cannot include evidence")
        return self


class SemanticExecutionQualificationResult(ContractModel):
    contract: Literal["semantic-execution-qualification-result/v1"] = (
        QUALIFICATION_RESULT_CONTRACT
    )
    qualification_id: str
    environment_id: str
    target_origin_digest: Sha256Digest
    started_at: datetime
    completed_at: datetime
    status: Literal["qualified", "failed"]
    probes: list[SemanticQualificationProbeResult] = Field(min_length=1)
    artifact_digest: Sha256Digest

    @field_validator("started_at", "completed_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("qualification timestamps must include a timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def aggregate_status_is_consistent(self) -> "SemanticExecutionQualificationResult":
        expected = (
            "qualified"
            if all(probe.status == "qualified" for probe in self.probes)
            else "failed"
        )
        if self.status != expected:
            raise ValueError("status must reflect all probe results")
        if self.completed_at < self.started_at:
            raise ValueError("completedAt cannot precede startedAt")
        return self

    def expected_artifact_digest(self) -> str:
        payload = self.to_wire()
        payload.pop("artifactDigest", None)
        return content_digest(payload)

    def verify_artifact_digest(self) -> bool:
        return self.artifact_digest == self.expected_artifact_digest()


class QualificationHttpResponse(ContractModel):
    status: int = Field(ge=100, le=599)
    payload: Optional[dict[str, Any]] = None


QualificationTransport = Callable[
    [str, dict[str, Any], Mapping[str, str], float], QualificationHttpResponse
]


class _EvidenceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message


def run_semantic_execution_qualification(
    spec: SemanticExecutionQualificationSpec,
    transport: QualificationTransport,
    *,
    api_key: Optional[str] = None,
    timeout_seconds: float = 30.0,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> SemanticExecutionQualificationResult:
    """Execute every probe and return a redacted, digest-bound receipt."""

    if timeout_seconds <= 0 or timeout_seconds > 300:
        raise ValueError("timeout_seconds must be greater than zero and at most 300")
    started_at = clock()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Active-Env": spec.environment_id,
    }
    if api_key:
        headers["X-API-Key"] = api_key

    probe_results = [
        _run_probe(
            spec,
            probe,
            transport,
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
        for probe in spec.probes
    ]
    completed_at = clock()
    result = SemanticExecutionQualificationResult(
        qualification_id=spec.qualification_id,
        environment_id=spec.environment_id,
        target_origin_digest=spec.target_origin_digest,
        started_at=started_at,
        completed_at=completed_at,
        status=(
            "qualified"
            if all(probe.status == "qualified" for probe in probe_results)
            else "failed"
        ),
        probes=probe_results,
        artifact_digest="sha256:" + "0" * 64,
    )
    return result.model_copy(
        update={"artifact_digest": result.expected_artifact_digest()}
    )


def _run_probe(
    spec: SemanticExecutionQualificationSpec,
    probe: OpenEhrQualificationProbe | FhirQualificationProbe,
    transport: QualificationTransport,
    *,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> SemanticQualificationProbeResult:
    endpoint_path = probe.endpoint_path
    request_digest = content_digest(probe.request)
    url = f"{spec.base_url}{endpoint_path}"
    try:
        response = transport(url, probe.request, headers, timeout_seconds)
    except Exception:
        return _failed_probe(
            probe,
            endpoint_path,
            request_digest,
            code="TRANSPORT_ERROR",
            message="The semantic endpoint could not be reached.",
        )

    response_digest = (
        _response_content_digest(response.payload)
        if response.payload is not None
        else None
    )
    if response.status != 200 or response.payload is None:
        return _failed_probe(
            probe,
            endpoint_path,
            request_digest,
            http_status=response.status,
            response_digest=response_digest,
            code=_safe_endpoint_error_code(response.payload),
            message="The semantic endpoint did not return a successful JSON result.",
        )

    try:
        evidence, result_contract = _summarize_response(
            probe,
            response.payload,
            expected_environment_id=spec.environment_id,
        )
    except _EvidenceError as exc:
        return _failed_probe(
            probe,
            endpoint_path,
            request_digest,
            http_status=response.status,
            response_digest=response_digest,
            code=exc.code,
            message=exc.safe_message,
        )
    return SemanticQualificationProbeResult(
        probe_id=probe.probe_id,
        kind=probe.kind,
        status="qualified",
        endpoint_path=endpoint_path,
        http_status=response.status,
        request_digest=request_digest,
        response_digest=response_digest,
        result_contract=result_contract,
        evidence=evidence,
    )


def _failed_probe(
    probe: OpenEhrQualificationProbe | FhirQualificationProbe,
    endpoint_path: str,
    request_digest: str,
    *,
    code: str,
    message: str,
    http_status: Optional[int] = None,
    response_digest: Optional[str] = None,
) -> SemanticQualificationProbeResult:
    return SemanticQualificationProbeResult(
        probe_id=probe.probe_id,
        kind=probe.kind,
        status="failed",
        endpoint_path=endpoint_path,
        http_status=http_status,
        request_digest=request_digest,
        response_digest=response_digest,
        failure=SemanticQualificationFailure(code=code, message=message),
    )


def _summarize_response(
    probe: OpenEhrQualificationProbe | FhirQualificationProbe,
    payload: Mapping[str, Any],
    *,
    expected_environment_id: str,
) -> tuple[SemanticExecutionEvidenceSummary, str]:
    if probe.kind == "openehr-terminology-query":
        return _summarize_openehr(payload, expected_environment_id)
    return _summarize_fhir(payload, expected_environment_id)


def _summarize_openehr(
    payload: Mapping[str, Any], expected_environment_id: str
) -> tuple[SemanticExecutionEvidenceSummary, str]:
    contract = _required_equal(payload, "contract", OPEN_EHR_RESULT_CONTRACT)
    evidence = _required_mapping(payload, "executionEvidence")
    result = _required_mapping(payload, "result")
    readiness = _required_mapping(evidence, "projectionReadiness")
    manifest = _required_mapping(readiness, "manifest")
    _validate_common_identity(payload, evidence, expected_environment_id, "openehr.rps_dual")
    if (
        readiness.get("ready") is not True
        or readiness.get("rebuildComplete") is not True
        or readiness.get("rebuildInProgress") is not False
        or readiness.get("atlasIndexState") != "verified_ready"
        or readiness.get("atlasIndexLiveVerified") is not True
        or manifest.get("missingPaths") != []
    ):
        raise _EvidenceError(
            "PROJECTION_NOT_LIVE_VERIFIED",
            "The openEHR projection and Atlas Search index are not live verified.",
        )
    manifest_digest = _required_digest(manifest, "manifestDigest")
    manifest_payload = dict(manifest)
    manifest_payload.pop("manifestDigest", None)
    if content_digest(manifest_payload) != manifest_digest:
        raise _EvidenceError(
            "INVALID_PROJECTION_RECEIPT",
            "The openEHR projection manifest digest is invalid.",
        )
    if (
        manifest.get("environmentId") != expected_environment_id
        or manifest.get("strategyId") != "openehr.rps_dual"
        or manifest.get("planDigest") != evidence.get("planDigest")
        or manifest.get("catalogDigest") != evidence.get("catalogDigest")
        or _required_mapping(payload, "plan").get("planDigest")
        != evidence.get("planDigest")
    ):
        raise _EvidenceError(
            "EVIDENCE_IDENTITY_MISMATCH",
            "The openEHR projection receipt does not match the executed plan.",
        )
    row_count = _required_integer(evidence, "rowCount")
    rows = result.get("rows")
    if not isinstance(rows, list) or row_count != len(rows):
        raise _EvidenceError(
            "RESULT_COUNT_MISMATCH",
            "The openEHR result count does not match its execution evidence.",
        )
    expansion_digests = evidence.get("terminologyExpansionDigests")
    if not isinstance(expansion_digests, list) or not expansion_digests:
        raise _EvidenceError(
            "MISSING_TERMINOLOGY_EVIDENCE",
            "The openEHR result has no terminology expansion evidence.",
        )
    resolution = payload.get("terminologyResolution")
    if (
        not isinstance(resolution, list)
        or [
            item.get("expansionDigest")
            for item in resolution
            if isinstance(item, Mapping)
        ]
        != expansion_digests
        or evidence.get("requireExactPathCorrelation") is not True
        or result.get("engineUsed") != evidence.get("engineUsed")
    ):
        raise _EvidenceError(
            "EVIDENCE_IDENTITY_MISMATCH",
            "The openEHR terminology, correlation, or engine evidence is inconsistent.",
        )
    return (
        SemanticExecutionEvidenceSummary(
            **_common_evidence(evidence),
            terminology_expansion_digests=[
                _digest_value(value) for value in expansion_digests
            ],
            compiled_artifact_digests={
                "plan": _required_digest(evidence, "planDigest"),
                "semanticQuery": _required_digest(evidence, "semanticQueryDigest"),
                "catalog": _required_digest(evidence, "catalogDigest"),
                "executionInput": _required_digest(evidence, "executionInputDigest"),
                "compiledPipeline": _required_digest(evidence, "compiledPipelineDigest"),
            },
            projection_readiness_digest=content_digest(dict(readiness)),
            projection_live_verified=True,
            engine_used=_required_text(evidence, "engineUsed"),
            row_count=row_count,
        ),
        contract,
    )


def _summarize_fhir(
    payload: Mapping[str, Any], expected_environment_id: str
) -> tuple[SemanticExecutionEvidenceSummary, str]:
    contract = _required_equal(payload, "contract", FHIR_RESULT_CONTRACT)
    evidence = _required_mapping(payload, "executionEvidence")
    result = _required_mapping(payload, "result")
    readiness = _required_mapping(evidence, "projectionReadiness")
    _validate_common_identity(
        payload,
        evidence,
        expected_environment_id,
        "fhir.resource_store",
    )
    if (
        readiness.get("collectionExists") is not True
        or readiness.get("canonicalProjectionMismatchCount") != 0
        or readiness.get("searchIndexPresent") is not True
        or readiness.get("exactCodeIndexPresent") is not True
        or readiness.get("dataTargetDigest") != evidence.get("dataTargetDigest")
    ):
        raise _EvidenceError(
            "PROJECTION_NOT_LIVE_VERIFIED",
            "The FHIR canonical projection and exact-code indexes are not ready.",
        )
    if (
        readiness.get("readinessReceiptContract")
        != "fhir-resource-store-projection-readiness/v1"
    ):
        raise _EvidenceError(
            "INVALID_PROJECTION_RECEIPT",
            "The FHIR projection-readiness receipt contract is invalid.",
        )
    readiness_receipt_digest = _required_digest(readiness, "readinessReceiptDigest")
    expected_readiness_receipt = {
        "contract": "fhir-resource-store-projection-readiness/v1",
        "state": "ready",
        "resourceType": readiness.get("resourceType"),
        "database": readiness.get("database"),
        "collection": readiness.get("collection"),
        "collectionExists": True,
        "documentCount": readiness.get("documentCount"),
        "contractCurrentCount": readiness.get("contractCurrentCount"),
        "canonicalProjectionMismatchCount": 0,
        "storageSchemaVersion": readiness.get("storageSchemaVersion"),
        "projectionContractVersion": readiness.get("projectionContractVersion"),
        "resourceProjectionVersion": readiness.get("resourceProjectionVersion"),
        "searchConfigured": True,
        "searchIndexPresent": True,
        "exactCodeIndexPresent": True,
    }
    if content_digest(expected_readiness_receipt) != readiness_receipt_digest:
        raise _EvidenceError(
            "INVALID_PROJECTION_RECEIPT",
            "The FHIR projection-readiness receipt digest is invalid.",
        )
    row_count = _required_integer(evidence, "rowCount")
    returned_count = result.get("returnedResourceCount")
    resources = result.get("resources")
    if (
        not isinstance(returned_count, int)
        or isinstance(returned_count, bool)
        or returned_count != row_count
        or not isinstance(resources, list)
        or returned_count != len(resources)
    ):
        raise _EvidenceError(
            "RESULT_COUNT_MISMATCH",
            "The FHIR result count does not match its execution evidence.",
        )
    terminology_resolution = _required_mapping(payload, "terminologyResolution")
    if (
        terminology_resolution.get("expansionDigest")
        != evidence.get("terminologyExpansionDigest")
        or evidence.get("expansionVersionPinned") is not True
        or evidence.get("exactTokenProjection") != "_search.code_systemCode"
        or result.get("engineUsed") != "fhir_mql"
    ):
        raise _EvidenceError(
            "EVIDENCE_IDENTITY_MISMATCH",
            "The FHIR terminology, token projection, or engine evidence is inconsistent.",
        )
    return (
        SemanticExecutionEvidenceSummary(
            **_common_evidence(evidence),
            terminology_expansion_digests=[
                _required_digest(evidence, "terminologyExpansionDigest")
            ],
            compiled_artifact_digests={
                "query": _required_digest(evidence, "queryDigest"),
                "compiledQuery": _required_digest(evidence, "compiledQueryDigest"),
                "compiledFilter": _required_digest(evidence, "compiledFilterDigest"),
            },
            projection_readiness_digest=content_digest(dict(readiness)),
            projection_live_verified=True,
            engine_used=_required_text(result, "engineUsed"),
            row_count=row_count,
        ),
        contract,
    )


def _validate_common_identity(
    payload: Mapping[str, Any],
    evidence: Mapping[str, Any],
    environment_id: str,
    strategy_id: str,
) -> None:
    if (
        payload.get("environmentId") != environment_id
        or evidence.get("environmentId") != environment_id
        or payload.get("strategyId") != strategy_id
        or evidence.get("strategyId") != strategy_id
    ):
        raise _EvidenceError(
            "EVIDENCE_IDENTITY_MISMATCH",
            "The response identity does not match the qualified environment and strategy.",
        )


def _common_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "environment_id": _required_text(evidence, "environmentId"),
        "strategy_id": _required_text(evidence, "strategyId"),
        "activation_id": _required_text(evidence, "activationId"),
        "strategy_version": _required_text(evidence, "strategyVersion"),
        # Runtime activation records historically store these three SHA-256
        # identities as bare lowercase hex.  Normalize their wire form into the
        # qualification contract; execution-artifact digests remain strict.
        "manifest_digest": _required_identity_digest(evidence, "manifestDigest"),
        "config_hash": _required_identity_digest(evidence, "configHash"),
        "bindings_hash": _required_identity_digest(evidence, "bindingsHash"),
        "data_target_digest": _required_digest(evidence, "dataTargetDigest"),
    }


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise _EvidenceError(
            "MISSING_EXECUTION_EVIDENCE",
            f"The response is missing required {key} evidence.",
        )
    return value


def _required_equal(payload: Mapping[str, Any], key: str, expected: str) -> str:
    if payload.get(key) != expected:
        raise _EvidenceError(
            "UNEXPECTED_RESULT_CONTRACT",
            "The semantic endpoint returned an unexpected result contract.",
        )
    return expected


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _EvidenceError(
            "MISSING_EXECUTION_EVIDENCE",
            f"The response is missing required {key} evidence.",
        )
    return value


def _digest_value(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise _EvidenceError(
            "INVALID_EXECUTION_DIGEST",
            "The response contains a missing or malformed execution digest.",
        )
    return value


def _required_digest(payload: Mapping[str, Any], key: str) -> str:
    return _digest_value(payload.get(key))


def _required_identity_digest(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    ):
        return f"sha256:{value}"
    return _digest_value(value)


def _required_integer(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _EvidenceError(
            "MISSING_EXECUTION_EVIDENCE",
            f"The response is missing required {key} evidence.",
        )
    return value


__all__ = [
    "FhirQualificationProbe",
    "OpenEhrQualificationProbe",
    "QualificationHttpResponse",
    "SemanticExecutionQualificationResult",
    "SemanticExecutionQualificationSpec",
    "SemanticQualificationProbeResult",
    "content_digest",
    "run_semantic_execution_qualification",
]
