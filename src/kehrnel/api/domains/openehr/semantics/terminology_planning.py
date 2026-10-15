"""Server-orchestrated terminology expansion for governed openEHR plans.

This boundary is deliberately narrower than natural-language interpretation:
the caller supplies an already typed semantic query and bounded terminology
expansion requests.  Kehrnel resolves those requests through the tenant's
configured terminology gateway, creates the receipts itself, and only then
hands the enriched query to the deterministic OPT-to-AQL planner.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Literal, Mapping, Optional, Protocol

from pydantic import Field, model_validator
from starlette.concurrency import run_in_threadpool

from kehrnel.contracts.artifacts import ContractModel, Sha256Digest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.openehr.semantic_query import (
    MAX_ID_LENGTH,
    MAX_QUERY_EXPANSIONS,
    MAX_QUERY_TEXT_LENGTH,
    MAX_TERMINOLOGY_EXPANSION_CODES,
    MAX_TERMINOLOGY_ID_LENGTH,
    MAX_URI_LENGTH,
    OpenEhrSemanticQuery,
    OpenEhrSemanticQueryPlan,
    TerminologyExpansionReceipt,
    create_terminology_expansion_receipt,
)

from .service import OpenEhrSemanticService, StoredAdl14Opt


TERMINOLOGY_QUERY_PLAN_REQUEST_CONTRACT = (
    "openehr-terminology-query-plan-request/v1"
)
TERMINOLOGY_QUERY_PLAN_RESULT_CONTRACT = (
    "openehr-terminology-query-plan-result/v1"
)
SNOMED_SYSTEM_URI = "http://snomed.info/sct"

# openEHR terminology identifiers are part of the stored RM value and are not
# interchangeable with canonical code-system URIs.  Keep this association on
# the server so a caller cannot expand one system and make the resulting AQL
# match a different terminology identifier.  Deployments can extend this
# registry through ``OpenEhrTerminologyPlanningService`` without weakening the
# built-in bindings.
DEFAULT_OPENEHR_TERMINOLOGY_ID_BINDINGS: Mapping[str, str] = MappingProxyType(
    {SNOMED_SYSTEM_URI: "SNOMED-CT"}
)


class TerminologyGateway(Protocol):
    """Minimum tenant terminology service used by the orchestration seam."""

    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]: ...


class OpenEhrTerminologyExpansionRequest(ContractModel):
    """A bounded and explicitly pinned terminology expansion request."""

    expansion_id: str = Field(
        min_length=1,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    provider_id: str = Field(
        min_length=1,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    system_uri: str = Field(min_length=1, max_length=MAX_URI_LENGTH)
    terminology_id: str = Field(
        min_length=1,
        max_length=MAX_TERMINOLOGY_ID_LENGTH,
    )
    version: str = Field(min_length=1, max_length=MAX_ID_LENGTH)
    release_id: Optional[str] = Field(default=None, max_length=MAX_ID_LENGTH)
    expression: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=MAX_QUERY_TEXT_LENGTH,
    )
    value_set_uri: Optional[str] = Field(default=None, max_length=MAX_URI_LENGTH)
    value_set_version: Optional[str] = Field(default=None, max_length=MAX_ID_LENGTH)
    max_codes: int = Field(
        default=MAX_TERMINOLOGY_EXPANSION_CODES,
        ge=1,
        le=MAX_TERMINOLOGY_EXPANSION_CODES,
    )

    @model_validator(mode="after")
    def request_is_pinned_and_canonical(
        self,
    ) -> "OpenEhrTerminologyExpansionRequest":
        for label, value in (
            ("systemUri", self.system_uri),
            ("terminologyId", self.terminology_id),
            ("version", self.version),
            ("releaseId", self.release_id),
            ("expression", self.expression),
            ("valueSetUri", self.value_set_uri),
            ("valueSetVersion", self.value_set_version),
        ):
            if value is None:
                continue
            if value != value.strip():
                raise ValueError(
                    f"{label} must not have leading or trailing whitespace"
                )
            if any(
                ord(character) < 0x20 or ord(character) == 0x7F
                for character in value
            ):
                raise ValueError(f"{label} must not contain control characters")
        if self.value_set_uri and not self.value_set_version:
            raise ValueError(
                "valueSetVersion is required when valueSetUri is supplied"
            )
        if self.value_set_version and not self.value_set_uri:
            raise ValueError(
                "valueSetUri is required when valueSetVersion is supplied"
            )
        if bool(self.expression) == bool(self.value_set_uri):
            raise ValueError(
                "exactly one of expression or valueSetUri is required"
            )
        return self


class OpenEhrTerminologyQueryPlanRequest(ContractModel):
    contract: Literal["openehr-terminology-query-plan-request/v1"] = (
        TERMINOLOGY_QUERY_PLAN_REQUEST_CONTRACT
    )
    query: OpenEhrSemanticQuery
    expansion_requests: list[OpenEhrTerminologyExpansionRequest] = Field(
        min_length=1,
        max_length=MAX_QUERY_EXPANSIONS,
    )

    @model_validator(mode="after")
    def expansion_requests_match_query(
        self,
    ) -> "OpenEhrTerminologyQueryPlanRequest":
        if self.query.terminology_expansions:
            raise ValueError(
                "terminologyExpansions must be empty; this endpoint issues receipts server-side"
            )
        request_ids = [request.expansion_id for request in self.expansion_requests]
        if len(request_ids) != len(set(request_ids)):
            raise ValueError("expansion request ids must be unique")
        referenced_ids = {
            predicate.terminology_expansion_ref
            for predicate in self.query.predicates
            if predicate.terminology_expansion_ref is not None
        }
        requested_ids = set(request_ids)
        missing = sorted(referenced_ids - requested_ids)
        unused = sorted(requested_ids - referenced_ids)
        if missing or unused:
            parts = []
            if missing:
                parts.append("missing requests: " + ", ".join(missing))
            if unused:
                parts.append("unused requests: " + ", ".join(unused))
            raise ValueError(
                "terminology expansion requests must exactly match query references ("
                + "; ".join(parts)
                + ")"
            )
        return self


class TerminologyResolutionEvidence(ContractModel):
    """Tenant-gateway evidence retained next to the deterministic plan."""

    expansion_id: str
    provider_id: str
    provider_type: str
    system_uri: str
    version: str
    release_id: Optional[str] = None
    correlation_id: str
    request_digest: str
    result_digest: str
    expansion_digest: Sha256Digest


class OpenEhrTerminologyQueryPlanResult(ContractModel):
    contract: Literal["openehr-terminology-query-plan-result/v1"] = (
        TERMINOLOGY_QUERY_PLAN_RESULT_CONTRACT
    )
    environment_id: str
    plan: OpenEhrSemanticQueryPlan
    terminology_resolution: list[TerminologyResolutionEvidence]


class OpenEhrTerminologyPlanningService:
    """Resolve governed expansion requests and invoke the existing planner."""

    def __init__(
        self,
        *,
        semantic_service: OpenEhrSemanticService,
        terminology_gateway: TerminologyGateway,
        environment_id: str,
        terminology_id_bindings: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._semantic_service = semantic_service
        self._terminology_gateway = terminology_gateway
        self._environment_id = environment_id
        self._terminology_id_bindings = _normalized_terminology_id_bindings(
            terminology_id_bindings
        )

    async def plan_query(
        self,
        stored: StoredAdl14Opt,
        request: OpenEhrTerminologyQueryPlanRequest,
    ) -> OpenEhrTerminologyQueryPlanResult:
        receipts: list[TerminologyExpansionReceipt] = []
        evidence: list[TerminologyResolutionEvidence] = []
        for expansion_request in request.expansion_requests:
            terminology_id = _validated_terminology_id(
                expansion_request,
                self._terminology_id_bindings,
            )
            result = await self._resolve(expansion_request)
            receipt, resolution = _receipt_from_result(
                expansion_request,
                result,
                terminology_id=terminology_id,
            )
            receipts.append(receipt)
            evidence.append(resolution)

        enriched_query = OpenEhrSemanticQuery.model_validate(
            {
                **request.query.to_wire(),
                "terminologyExpansions": [receipt.to_wire() for receipt in receipts],
            }
        )
        plan = await run_in_threadpool(
            self._semantic_service.plan_query,
            stored,
            enriched_query,
        )
        return OpenEhrTerminologyQueryPlanResult(
            environment_id=self._environment_id,
            plan=plan,
            terminology_resolution=evidence,
        )

    async def _resolve(
        self,
        request: OpenEhrTerminologyExpansionRequest,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "provider_id": request.provider_id,
            "system": request.system_uri,
            "version": request.version,
            "offset": 0,
            # Request one extra row so providers without explicit page metadata
            # still cannot silently truncate a receipt at the accepted bound.
            "count": request.max_codes + 1,
        }
        if request.expression:
            payload["expression"] = request.expression
        if request.release_id:
            payload["release_id"] = request.release_id
        elif request.system_uri.rstrip("/") == "http://snomed.info/sct":
            payload["release_id"] = _snomed_release_from_version(request.version)
        if request.value_set_uri:
            payload["url"] = request.value_set_uri
            payload["value_set_version"] = request.value_set_version
        result = await self._terminology_gateway.execute("expand", payload)
        if not isinstance(result, dict):
            raise _malformed_provider(
                request,
                "the terminology gateway returned a non-object result",
            )
        return result


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
            code="OPENEHR_TERMINOLOGY_VERSION_UNSUPPORTED",
            status=422,
            message=(
                "A SNOMED CT expansion requires a releaseId or a version that "
                "contains a concrete release identifier."
            ),
            details={"version": version},
        )
    return release_id


def _receipt_from_result(
    request: OpenEhrTerminologyExpansionRequest,
    result: Mapping[str, Any],
    *,
    terminology_id: str,
) -> tuple[TerminologyExpansionReceipt, TerminologyResolutionEvidence]:
    if result.get("ok") is False:
        raise _malformed_provider(request, "the provider reported an unsuccessful expansion")

    provider = result.get("provider")
    gateway_evidence = result.get("evidence")
    if not isinstance(provider, Mapping) or not isinstance(gateway_evidence, Mapping):
        raise _malformed_provider(
            request,
            "the expansion is missing tenant-gateway provider evidence",
        )
    provider_id = _required_string(provider.get("id"))
    evidence_provider_id = _required_string(gateway_evidence.get("providerId"))
    provider_type = _required_string(
        gateway_evidence.get("providerType") or provider.get("type")
    )
    if provider_type not in {"kehrnel-strategy", "fhir-terminology"}:
        raise _malformed_provider(request, "the provider type is missing or unsupported")
    if provider_type == "fhir-terminology" and request.expression:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_SELECTOR_UNSUPPORTED",
            status=422,
            message=(
                "External FHIR terminology expansion requires a pinned ValueSet; "
                "native expressions cannot be represented by the current gateway."
            ),
            details={
                "expansionId": request.expansion_id,
                "providerId": request.provider_id,
            },
        )
    if provider_id != request.provider_id or evidence_provider_id != request.provider_id:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_PROVIDER_MISMATCH",
            status=422,
            message="The terminology result does not match the requested provider.",
            details={
                "expansionId": request.expansion_id,
                "requestedProviderId": request.provider_id,
                "resultProviderId": provider_id or None,
                "evidenceProviderId": evidence_provider_id or None,
            },
        )

    result_system = _required_string(
        result.get("system") or gateway_evidence.get("system")
    )
    if result_system.rstrip("/") != request.system_uri.rstrip("/"):
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_SYSTEM_MISMATCH",
            status=422,
            message="The terminology result does not match the requested code system.",
            details={
                "expansionId": request.expansion_id,
                "requestedSystemUri": request.system_uri,
                "resultSystemUri": result_system or None,
            },
        )

    _validate_value_set_identity(request, result)
    codes, observed_versions = _extract_codes(request, result)
    _validate_expansion_completeness(
        request,
        result,
        returned_code_count=len(codes),
    )
    resolved_version, release_id = _resolve_version_evidence(
        request,
        result,
        provider_type=provider_type,
        observed_versions=observed_versions,
    )

    correlation_id = _required_string(gateway_evidence.get("correlationId"))
    request_digest = _required_string(gateway_evidence.get("requestDigest"))
    result_digest = _required_string(gateway_evidence.get("resultDigest"))
    if not correlation_id or not request_digest or not result_digest:
        raise _malformed_provider(
            request,
            "the expansion is missing gateway correlation or digest evidence",
        )

    try:
        receipt = create_terminology_expansion_receipt(
            expansion_id=request.expansion_id,
            provider_id=request.provider_id,
            system_uri=request.system_uri,
            terminology_id=terminology_id,
            value_set_uri=request.value_set_uri,
            version=resolved_version,
            expression=_selector_expression(request),
            codes=codes,
            truncated=False,
        )
    except (TypeError, ValueError) as exc:
        raise _malformed_provider(
            request,
            f"the expansion cannot form a valid receipt: {exc}",
        ) from exc

    return receipt, TerminologyResolutionEvidence(
        expansion_id=request.expansion_id,
        provider_id=request.provider_id,
        provider_type=provider_type,
        system_uri=request.system_uri,
        version=resolved_version,
        release_id=release_id,
        correlation_id=correlation_id,
        request_digest=request_digest,
        result_digest=result_digest,
        expansion_digest=receipt.expansion_digest,
    )


def _extract_codes(
    request: OpenEhrTerminologyExpansionRequest,
    result: Mapping[str, Any],
) -> tuple[list[str], set[str]]:
    native_collections: list[tuple[str, Any]] = [
        (name, result.get(name))
        for name in ("concepts", "matches")
        if result.get(name) is not None
    ]
    native_code_sets: list[set[str]] = []
    observed_versions: set[str] = set()
    for name, collection in native_collections:
        if not isinstance(collection, list):
            raise _malformed_provider(request, f"{name} must be an array")
        parsed: set[str] = set()
        for item in collection:
            if not isinstance(item, Mapping):
                raise _malformed_provider(request, f"{name} contains a non-object item")
            code = _required_string(item.get("conceptId") or item.get("code"))
            if not code:
                raise _malformed_provider(request, f"{name} contains an item without a code")
            item_system = _required_string(item.get("system"))
            if item_system and item_system.rstrip("/") != request.system_uri.rstrip("/"):
                raise KehrnelError(
                    code="OPENEHR_TERMINOLOGY_SYSTEM_MISMATCH",
                    status=422,
                    message="A terminology expansion member uses a different code system.",
                    details={
                        "expansionId": request.expansion_id,
                        "requestedSystemUri": request.system_uri,
                        "memberSystemUri": item_system,
                        "code": code,
                    },
                )
            item_version = _required_string(item.get("version"))
            if item_version:
                observed_versions.add(item_version)
            parsed.add(code)
        native_code_sets.append(parsed)

    if len(native_code_sets) == 2 and native_code_sets[0] != native_code_sets[1]:
        raise _malformed_provider(
            request,
            "concepts and matches disagree about the expansion members",
        )

    fhir_value_set = result.get("valueSet")
    fhir_codes: set[str] = set()
    if fhir_value_set is not None:
        if (
            not isinstance(fhir_value_set, Mapping)
            or fhir_value_set.get("resourceType") != "ValueSet"
        ):
            raise _malformed_provider(request, "valueSet must be a FHIR ValueSet resource")
        expansion = fhir_value_set.get("expansion")
        if not isinstance(expansion, Mapping):
            raise _malformed_provider(request, "the FHIR ValueSet has no expansion")
        _collect_fhir_contains(
            request,
            expansion.get("contains"),
            fhir_codes,
            observed_versions,
        )

    available_sets = [*native_code_sets, *([fhir_codes] if fhir_value_set is not None else [])]
    if not available_sets:
        raise _malformed_provider(request, "the expansion has no recognized member collection")
    if any(code_set != available_sets[0] for code_set in available_sets[1:]):
        raise _malformed_provider(request, "provider member collections disagree")
    codes = sorted(available_sets[0])
    if not codes:
        raise _malformed_provider(request, "the expansion contains no codes")
    if len(codes) > request.max_codes:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_EXPANSION_TOO_LARGE",
            status=422,
            message="The terminology expansion exceeds its declared code bound.",
            details={
                "expansionId": request.expansion_id,
                "maxCodes": request.max_codes,
                "returnedCodes": len(codes),
            },
        )
    return codes, observed_versions


def _collect_fhir_contains(
    request: OpenEhrTerminologyExpansionRequest,
    contains: Any,
    codes: set[str],
    versions: set[str],
) -> None:
    if not isinstance(contains, list):
        raise _malformed_provider(request, "ValueSet.expansion.contains must be an array")
    stack = list(contains)
    inspected = 0
    while stack:
        inspected += 1
        if inspected > request.max_codes + 1:
            raise KehrnelError(
                code="OPENEHR_TERMINOLOGY_EXPANSION_TOO_LARGE",
                status=422,
                message="The terminology expansion exceeds its declared code bound.",
                details={
                    "expansionId": request.expansion_id,
                    "maxCodes": request.max_codes,
                },
            )
        item = stack.pop()
        if not isinstance(item, Mapping):
            raise _malformed_provider(request, "ValueSet.expansion.contains has a non-object item")
        children = item.get("contains")
        if children is not None:
            if not isinstance(children, list):
                raise _malformed_provider(request, "nested expansion.contains must be an array")
            stack.extend(children)
        code = _required_string(item.get("code"))
        if not code:
            if children:
                continue
            raise _malformed_provider(request, "an expansion member is missing code")
        system = _required_string(item.get("system"))
        if not system or system.rstrip("/") != request.system_uri.rstrip("/"):
            raise KehrnelError(
                code="OPENEHR_TERMINOLOGY_SYSTEM_MISMATCH",
                status=422,
                message="A terminology expansion member uses a different or missing code system.",
                details={
                    "expansionId": request.expansion_id,
                    "requestedSystemUri": request.system_uri,
                    "memberSystemUri": system or None,
                    "code": code,
                },
            )
        version = _required_string(item.get("version"))
        if version:
            versions.add(version)
        codes.add(code)


def _normalized_terminology_id_bindings(
    additional: Optional[Mapping[str, str]],
) -> dict[str, str]:
    bindings = {
        system_uri.rstrip("/"): terminology_id
        for system_uri, terminology_id in DEFAULT_OPENEHR_TERMINOLOGY_ID_BINDINGS.items()
    }
    for raw_system_uri, raw_terminology_id in (additional or {}).items():
        system_uri = _required_string(raw_system_uri).rstrip("/")
        terminology_id = _required_string(raw_terminology_id)
        if not system_uri or not terminology_id:
            raise ValueError(
                "openEHR terminology bindings require non-empty system URIs and identifiers"
            )
        built_in = DEFAULT_OPENEHR_TERMINOLOGY_ID_BINDINGS.get(system_uri)
        if built_in is not None and terminology_id != built_in:
            raise ValueError(
                f"the built-in terminology binding for {system_uri} cannot be overridden"
            )
        bindings[system_uri] = terminology_id
    return bindings


def _validated_terminology_id(
    request: OpenEhrTerminologyExpansionRequest,
    bindings: Mapping[str, str],
) -> str:
    system_uri = request.system_uri.rstrip("/")
    terminology_id = bindings.get(system_uri)
    if terminology_id is None:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_BINDING_REQUIRED",
            status=422,
            message=(
                "The code system has no server-owned openEHR terminology binding."
            ),
            details={
                "expansionId": request.expansion_id,
                "systemUri": request.system_uri,
            },
        )
    if request.terminology_id != terminology_id:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_ID_MISMATCH",
            status=422,
            message=(
                "The openEHR terminology identifier does not match the "
                "server-owned code-system binding."
            ),
            details={
                "expansionId": request.expansion_id,
                "systemUri": request.system_uri,
                "requestedTerminologyId": request.terminology_id,
                "expectedTerminologyId": terminology_id,
            },
        )
    return terminology_id


def _strict_non_negative_int(
    request: OpenEhrTerminologyExpansionRequest,
    value: Any,
    *,
    label: str,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _malformed_provider(request, f"{label} must be a non-negative integer")
    return value


def _validate_expansion_completeness(
    request: OpenEhrTerminologyExpansionRequest,
    result: Mapping[str, Any],
    *,
    returned_code_count: int,
) -> None:
    truncated = result.get("truncated")
    if truncated is not None and not isinstance(truncated, bool):
        raise _malformed_provider(request, "truncated must be a boolean")
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
            raise _malformed_provider(request, "page metadata must be an object")
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
                raise _malformed_provider(request, "page.limit must be greater than zero")
        if "returned" in page:
            page_returned = _strict_non_negative_int(
                request,
                page.get("returned"),
                label="page.returned",
            )
            if page_returned != returned_code_count:
                raise _malformed_provider(
                    request,
                    "page.returned does not match the expansion member count",
                )
            if page_limit is not None and page_returned > page_limit:
                raise _malformed_provider(
                    request,
                    "page.returned exceeds page.limit",
                )
        else:
            page_returned = None

        if "hasMore" in page:
            if not isinstance(page.get("hasMore"), bool):
                raise _malformed_provider(request, "page.hasMore must be a boolean")
            page_has_more = page["hasMore"]
        if "nextOffset" in page and page.get("nextOffset") is not None:
            page_next_offset = _strict_non_negative_int(
                request,
                page.get("nextOffset"),
                label="page.nextOffset",
            )
            if page_next_offset <= offset:
                raise _malformed_provider(
                    request,
                    "page.nextOffset must advance beyond page.offset",
                )
            if (
                page_returned is not None
                and page_next_offset != offset + page_returned
            ):
                raise _malformed_provider(
                    request,
                    "page.nextOffset does not follow page.returned",
                )
        if page_has_more is False and page_next_offset is not None:
            raise _malformed_provider(
                request,
                "page.hasMore=false contradicts a non-null page.nextOffset",
            )
        if page_has_more is True and page_next_offset is None:
            raise _malformed_provider(
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
        raise _malformed_provider(
            request,
            "terminology pagination totals contradict one another",
        )
    if declared_totals:
        total = declared_totals[0][1]
        if total < returned_code_count:
            raise _malformed_provider(
                request,
                "the declared expansion total is smaller than the returned member count",
            )
        if page_has_more is False and total > returned_code_count:
            raise _malformed_provider(
                request,
                "page.hasMore=false contradicts the declared expansion total",
            )
        if page_has_more is True and total <= returned_code_count:
            raise _malformed_provider(
                request,
                "page.hasMore=true contradicts the declared expansion total",
            )
        if page_next_offset is not None and total <= returned_code_count:
            raise _malformed_provider(
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
    request: OpenEhrTerminologyExpansionRequest,
    result: Mapping[str, Any],
) -> None:
    """Ensure a named ValueSet response is the exact requested definition."""

    if not request.value_set_uri:
        return
    value_set = result.get("valueSet")
    if value_set is not None and not isinstance(value_set, Mapping):
        raise _malformed_provider(request, "valueSet must be an object")
    value_set = value_set if isinstance(value_set, Mapping) else {}
    result_uri = _required_string(value_set.get("url") or result.get("url"))
    result_version = _required_string(
        value_set.get("version") or result.get("valueSetVersion")
    )
    if result_uri != request.value_set_uri or result_version != request.value_set_version:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_VALUE_SET_MISMATCH",
            status=422,
            message="The terminology result does not match the requested ValueSet definition.",
            details={
                "expansionId": request.expansion_id,
                "requestedValueSetUri": request.value_set_uri,
                "requestedValueSetVersion": request.value_set_version,
                "resultValueSetUri": result_uri or None,
                "resultValueSetVersion": result_version or None,
            },
        )


def _selector_expression(request: OpenEhrTerminologyExpansionRequest) -> str:
    if request.expression:
        return request.expression
    return f"value-set:{request.value_set_uri}|{request.value_set_version}"


def _resolve_version_evidence(
    request: OpenEhrTerminologyExpansionRequest,
    result: Mapping[str, Any],
    *,
    provider_type: str,
    observed_versions: set[str],
) -> tuple[str, Optional[str]]:
    release_id = _required_string(
        result.get("releaseId")
        or (result.get("evidence") or {}).get("releaseId")
    )
    explicit_result_version = _required_string(result.get("version"))
    if provider_type == "kehrnel-strategy":
        expected_release = request.release_id
        if not expected_release and request.system_uri.rstrip("/") == "http://snomed.info/sct":
            expected_release = _snomed_release_from_version(request.version)
        if not release_id or not expected_release or release_id != expected_release:
            raise KehrnelError(
                code="OPENEHR_TERMINOLOGY_VERSION_MISMATCH",
                status=422,
                message="The native terminology result does not prove the requested release.",
                details={
                    "expansionId": request.expansion_id,
                    "requestedVersion": request.version,
                    "requestedReleaseId": expected_release,
                    "resultReleaseId": release_id or None,
                },
            )
        if explicit_result_version and explicit_result_version != request.version:
            raise KehrnelError(
                code="OPENEHR_TERMINOLOGY_VERSION_MISMATCH",
                status=422,
                message="The terminology result does not match the requested version.",
                details={
                    "expansionId": request.expansion_id,
                    "requestedVersion": request.version,
                    "resultVersion": explicit_result_version,
                },
            )
        return request.version, release_id

    # External FHIR terminology servers must attest the code-system version in
    # the provider response itself.  The gateway evidence version can be an
    # echo of the request, so it is intentionally not accepted as proof.
    if explicit_result_version:
        observed_versions.add(explicit_result_version)
    if not observed_versions:
        observed_versions.update(_fhir_expansion_versions(result.get("valueSet")))
    if observed_versions != {request.version}:
        raise KehrnelError(
            code="OPENEHR_TERMINOLOGY_VERSION_MISMATCH",
            status=422,
            message="The external terminology response does not prove the requested version.",
            details={
                "expansionId": request.expansion_id,
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


def _required_string(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _malformed_provider(
    request: OpenEhrTerminologyExpansionRequest,
    reason: str,
) -> KehrnelError:
    return KehrnelError(
        code="OPENEHR_TERMINOLOGY_RESULT_INVALID",
        status=502,
        message="The terminology provider returned an invalid expansion result.",
        details={"expansionId": request.expansion_id, "reason": reason},
    )


def _incomplete_expansion(
    request: OpenEhrTerminologyExpansionRequest,
    reason: str,
) -> KehrnelError:
    return KehrnelError(
        code="OPENEHR_TERMINOLOGY_EXPANSION_INCOMPLETE",
        status=422,
        message="A paginated or truncated terminology expansion cannot be used in AQL.",
        details={"expansionId": request.expansion_id, "reason": reason},
    )


__all__ = [
    "OpenEhrTerminologyExpansionRequest",
    "OpenEhrTerminologyPlanningService",
    "OpenEhrTerminologyQueryPlanRequest",
    "OpenEhrTerminologyQueryPlanResult",
    "TerminologyResolutionEvidence",
]
