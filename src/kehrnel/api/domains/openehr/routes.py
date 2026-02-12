"""Domain-scoped openEHR API surface shared by all openEHR strategies."""
from __future__ import annotations

from fastapi import APIRouter

from kehrnel.api.domains.openehr.aql.routes import router as legacy_aql_router
from kehrnel.api.domains.openehr.ehr.routes import router as legacy_ehr_router
from kehrnel.api.domains.openehr.ehr_status.routes import router as legacy_ehr_status_router
from kehrnel.api.domains.openehr.composition.routes import router as legacy_composition_router
from kehrnel.api.domains.openehr.contribution.routes import router as legacy_contribution_router
from kehrnel.api.domains.openehr.directory.routes import router as legacy_directory_router
from kehrnel.api.domains.openehr.template.routes import router as legacy_template_router


router = APIRouter(prefix="/api/domains/openehr")

router.include_router(legacy_ehr_router)
router.include_router(legacy_ehr_status_router)
router.include_router(legacy_composition_router)
router.include_router(legacy_contribution_router)
router.include_router(legacy_directory_router)
router.include_router(legacy_template_router)
router.include_router(legacy_aql_router)

