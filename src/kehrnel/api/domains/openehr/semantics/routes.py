"""HTTP routes for deterministic semantics derived from stored openEHR OPTs."""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from kehrnel.api.domains.openehr.semantics.execution import (
    OpenEhrTerminologyExecutionService,
    OpenEhrTerminologyQueryExecutionResult,
)
from kehrnel.api.domains.openehr.semantics.service import (
    OpenEhrSemanticService,
    StoredAdl14OptLoader,
    get_open_ehr_semantic_service,
    get_stored_adl14_opt_loader,
)
from kehrnel.api.domains.openehr.semantics.terminology_planning import (
    OpenEhrTerminologyPlanningService,
    OpenEhrTerminologyQueryPlanRequest,
    OpenEhrTerminologyQueryPlanResult,
)
from kehrnel.api.terminology.routes import _context as _terminology_context
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.openehr.semantic_query import (
    OpenEhrSemanticQuery,
    OpenEhrSemanticQueryPlan,
)
from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    OpenEhrOptSemanticCatalog,
)
from kehrnel.engine.terminology import TerminologyService


router = APIRouter(prefix="/semantics", tags=["openEHR - OPT semantics"])


@router.get(
    "/templates/{template_id}/catalog",
    response_model=OpenEhrOptSemanticCatalog,
    response_model_by_alias=True,
    summary="Compile the semantic catalog for a stored ADL 1.4 OPT",
    description=(
        "Retrieves the existing ADL 1.4 OPT by template id and deterministically "
        "derives its archetype-scoped, queryable semantic catalog."
    ),
)
async def get_stored_opt_semantic_catalog(
    template_id: str,
    loader: StoredAdl14OptLoader = Depends(get_stored_adl14_opt_loader),
    service: OpenEhrSemanticService = Depends(get_open_ehr_semantic_service),
):
    try:
        stored = await loader.load(template_id)
        return await run_in_threadpool(service.compile_catalog, stored)
    except KehrnelError as exc:
        return _semantic_error_response(exc)


@router.post(
    "/templates/{template_id}/query-plans",
    response_model=OpenEhrSemanticQueryPlan,
    response_model_by_alias=True,
    summary="Plan a typed semantic query against a stored ADL 1.4 OPT",
    description=(
        "Validates a typed semantic query against the exact pinned OPT catalog "
        "and renders AQL. This endpoint performs no NLP, terminology expansion, "
        "or query execution."
    ),
)
async def plan_stored_opt_semantic_query(
    template_id: str,
    query: OpenEhrSemanticQuery = Body(...),
    loader: StoredAdl14OptLoader = Depends(get_stored_adl14_opt_loader),
    service: OpenEhrSemanticService = Depends(get_open_ehr_semantic_service),
):
    try:
        stored = await loader.load(template_id)
        return await run_in_threadpool(service.plan_query, stored, query)
    except KehrnelError as exc:
        return _semantic_error_response(exc)


def get_open_ehr_terminology_planning_service(
    request: Request,
    semantic_service: OpenEhrSemanticService = Depends(
        get_open_ehr_semantic_service
    ),
) -> OpenEhrTerminologyPlanningService:
    """Bind semantic planning to the authenticated tenant terminology gateway."""

    runtime, environment_id = _terminology_context(request)
    return OpenEhrTerminologyPlanningService(
        semantic_service=semantic_service,
        terminology_gateway=TerminologyService(runtime, environment_id),
        environment_id=environment_id,
    )


def get_open_ehr_terminology_execution_service(
    request: Request,
    planning_service: OpenEhrTerminologyPlanningService = Depends(
        get_open_ehr_terminology_planning_service
    ),
) -> OpenEhrTerminologyExecutionService:
    """Bind execution to the same authorized tenant and active runtime."""

    runtime, environment_id = _terminology_context(request)
    return OpenEhrTerminologyExecutionService(
        planning_service=planning_service,
        runtime=runtime,
        environment_id=environment_id,
    )


@router.post(
    "/templates/{template_id}/terminology-query-plans",
    response_model=OpenEhrTerminologyQueryPlanResult,
    response_model_by_alias=True,
    summary="Resolve terminology and plan a typed query against a stored OPT",
    description=(
        "Resolves bounded, pinned terminology expansion requests through the "
        "active tenant terminology gateway, issues server-side evidence receipts, "
        "and renders AQL. This endpoint performs no NLP or query execution. The "
        "sibling /query-plans endpoint remains compile-only and accepts caller-"
        "supplied receipts."
    ),
)
async def plan_stored_opt_query_with_terminology(
    template_id: str,
    body: OpenEhrTerminologyQueryPlanRequest = Body(...),
    loader: StoredAdl14OptLoader = Depends(get_stored_adl14_opt_loader),
    planning_service: OpenEhrTerminologyPlanningService = Depends(
        get_open_ehr_terminology_planning_service
    ),
):
    try:
        stored = await loader.load(template_id)
        return await planning_service.plan_query(stored, body)
    except KehrnelError as exc:
        return _semantic_error_response(exc)


@router.post(
    "/templates/{template_id}/terminology-query-executions",
    response_model=OpenEhrTerminologyQueryExecutionResult,
    response_model_by_alias=True,
    summary="Resolve terminology, plan, and execute a typed query",
    description=(
        "Resolves bounded terminology through the authenticated tenant gateway, "
        "validates the exact stored OPT and plan digest, then executes through "
        "the active openEHR RPS Dual strategy with exact path correlation. "
        "The endpoint accepts typed semantic intent only; it performs no NLP."
    ),
)
async def execute_stored_opt_query_with_terminology(
    template_id: str,
    body: OpenEhrTerminologyQueryPlanRequest = Body(...),
    loader: StoredAdl14OptLoader = Depends(get_stored_adl14_opt_loader),
    execution_service: OpenEhrTerminologyExecutionService = Depends(
        get_open_ehr_terminology_execution_service
    ),
):
    try:
        stored = await loader.load(template_id)
        return await execution_service.execute_query(stored, body)
    except KehrnelError as exc:
        return _semantic_error_response(exc)


def _semantic_error_response(exc: KehrnelError) -> JSONResponse:
    """Keep domain errors stable even when this router is mounted standalone."""

    return JSONResponse(
        status_code=exc.status,
        content={
            "error": {
                "code": exc.code,
                "message": str(exc),
                "details": exc.details,
            }
        },
    )


__all__ = [
    "get_open_ehr_terminology_execution_service",
    "get_open_ehr_terminology_planning_service",
    "router",
]
