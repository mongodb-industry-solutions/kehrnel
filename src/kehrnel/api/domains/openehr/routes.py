"""Domain-scoped openEHR API surface shared by all openEHR strategies."""
from __future__ import annotations

from fastapi import APIRouter

from kehrnel.api.domains.openehr.aql.routes import router as aql_router
from kehrnel.api.domains.openehr.ehr.routes import router as ehr_router
from kehrnel.api.domains.openehr.ehr_status.routes import router as ehr_status_router
from kehrnel.api.domains.openehr.composition.routes import router as composition_router
from kehrnel.api.domains.openehr.contribution.routes import router as contribution_router
from kehrnel.api.domains.openehr.directory.routes import router as directory_router
from kehrnel.api.domains.openehr.template.routes import router as template_router


router = APIRouter(prefix="/api/domains/openehr")

router.include_router(ehr_router)
router.include_router(ehr_status_router)
router.include_router(composition_router)
router.include_router(contribution_router)
router.include_router(directory_router)
router.include_router(template_router)
router.include_router(aql_router)
