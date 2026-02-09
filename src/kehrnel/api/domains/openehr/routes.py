"""Domain-scoped openEHR API surface shared by all openEHR strategies."""
from __future__ import annotations

from fastapi import APIRouter

from kehrnel.api.legacy.v1.aql.routes import router as legacy_aql_router
from kehrnel.api.legacy.v1.ehr.routes import router as legacy_ehr_router
from kehrnel.api.legacy.v1.ehr_status.routes import router as legacy_ehr_status_router
from kehrnel.api.legacy.v1.composition.routes import router as legacy_composition_router
from kehrnel.api.legacy.v1.contribution.routes import router as legacy_contribution_router
from kehrnel.api.legacy.v1.directory.routes import router as legacy_directory_router
from kehrnel.api.legacy.v1.template.routes import router as legacy_template_router


router = APIRouter(prefix="/api/domains/openehr")

router.include_router(legacy_ehr_router)
router.include_router(legacy_ehr_status_router)
router.include_router(legacy_composition_router)
router.include_router(legacy_contribution_router)
router.include_router(legacy_directory_router)
router.include_router(legacy_template_router)
router.include_router(legacy_aql_router)

