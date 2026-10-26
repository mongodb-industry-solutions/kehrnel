"""Application service for the openEHR OPT semantic-query boundary.

The service intentionally owns no template persistence.  It receives the raw
ADL 1.4 OPT retrieved by the existing template service, recompiles its semantic
catalog deterministically, and plans typed queries against that exact catalog.
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db
from kehrnel.api.domains.openehr.template.models import TemplateFormat
from kehrnel.api.domains.openehr.template.service import (
    retrieve_template_by_id_and_format,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.openehr.semantic_query import (
    OpenEhrSemanticQuery,
    OpenEhrSemanticQueryPlan,
    SemanticQueryPlanningError,
    plan_open_ehr_semantic_query,
)
from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    OpenEhrOptSemanticCatalog,
    OptSemanticCatalogError,
    compile_opt_semantic_catalog_xml,
)


@dataclass(frozen=True, slots=True)
class StoredAdl14Opt:
    """The minimum immutable input needed by the semantic compiler."""

    template_id: str
    content: str
    data_target_digest: str | None = None


async def load_stored_adl14_opt(
    template_id: str,
    db: AsyncIOMotorDatabase,
    *,
    data_target_digest: str | None = None,
) -> StoredAdl14Opt:
    """Load an OPT through the existing openEHR template persistence service."""

    try:
        result = await retrieve_template_by_id_and_format(
            db=db,
            template_format=TemplateFormat.ADL_1_4,
            template_id=template_id,
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            raise KehrnelError(
                code="OPENEHR_TEMPLATE_NOT_FOUND",
                status=404,
                message=f"Stored ADL 1.4 OPT '{template_id}' was not found.",
                details={"templateId": template_id, "templateFormat": "adl1.4"},
            ) from exc
        raise KehrnelError(
            code="OPENEHR_TEMPLATE_RETRIEVAL_FAILED",
            status=exc.status_code if 400 <= exc.status_code < 500 else 500,
            message="The stored ADL 1.4 OPT could not be retrieved.",
            details={"templateId": template_id, "templateFormat": "adl1.4"},
        ) from exc

    template = result["template"]
    return StoredAdl14Opt(
        template_id=template.template_id,
        content=template.content,
        data_target_digest=data_target_digest,
    )


class StoredAdl14OptLoader:
    """Request-scoped adapter over the existing template persistence service."""

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        *,
        data_target_digest: str | None = None,
    ):
        self._db = db
        self._data_target_digest = data_target_digest

    async def load(self, template_id: str) -> StoredAdl14Opt:
        return await load_stored_adl14_opt(
            template_id,
            self._db,
            data_target_digest=self._data_target_digest,
        )


def get_stored_adl14_opt_loader(
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
) -> StoredAdl14OptLoader:
    """Dependency seam; API tests can replace storage without running MongoDB."""

    context = getattr(request.state, "_openehr_context", {})
    return StoredAdl14OptLoader(
        db,
        data_target_digest=(
            context.get("data_target_digest")
            if isinstance(context, dict)
            else None
        ),
    )


class OpenEhrSemanticService:
    """Compile catalogs and query plans without inference or execution."""

    def compile_catalog(self, stored: StoredAdl14Opt) -> OpenEhrOptSemanticCatalog:
        try:
            catalog = compile_opt_semantic_catalog_xml(stored.content)
        except OptSemanticCatalogError as exc:
            raise KehrnelError(
                code="OPENEHR_OPT_SEMANTIC_CATALOG_INVALID",
                status=422,
                message="The stored OPT cannot be compiled into a semantic catalog.",
                details={"templateId": stored.template_id, "reason": str(exc)},
            ) from exc

        if catalog.template.template_id != stored.template_id:
            raise KehrnelError(
                code="OPENEHR_TEMPLATE_ID_MISMATCH",
                status=409,
                message="The stored template identifier does not match its OPT content.",
                details={
                    "storedTemplateId": stored.template_id,
                    "optTemplateId": catalog.template.template_id,
                },
            )
        return catalog

    def plan_query(
        self,
        stored: StoredAdl14Opt,
        query: OpenEhrSemanticQuery,
    ) -> OpenEhrSemanticQueryPlan:
        catalog = self.compile_catalog(stored)
        try:
            return plan_open_ehr_semantic_query(catalog, query)
        except SemanticQueryPlanningError as exc:
            status = 409 if exc.code == "TEMPLATE_REF_MISMATCH" else 422
            raise KehrnelError(
                code=exc.code,
                status=status,
                message=str(exc),
                details=exc.details,
            ) from exc


def get_open_ehr_semantic_service() -> OpenEhrSemanticService:
    """FastAPI dependency seam for tests and alternate service implementations."""

    return OpenEhrSemanticService()


__all__ = [
    "OpenEhrSemanticService",
    "StoredAdl14Opt",
    "StoredAdl14OptLoader",
    "get_open_ehr_semantic_service",
    "get_stored_adl14_opt_loader",
    "load_stored_adl14_opt",
]
