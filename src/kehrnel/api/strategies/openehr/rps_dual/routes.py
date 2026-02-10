"""Strategy-scoped API surface for openEHR RPS dual."""
from __future__ import annotations

from fastapi import APIRouter

from kehrnel.api.legacy.v1.ingest.routes import router as legacy_ingest_router
from kehrnel.api.legacy.v1.config.routes import router as legacy_config_router
from kehrnel.api.legacy.v1.synthetic.routes import router as legacy_synthetic_router


router = APIRouter(
    prefix="/api/strategies/openehr/rps_dual",
)

# Keep only strategy-specific endpoints here.
router.include_router(legacy_ingest_router, prefix="/ingest", tags=["Ingest"])
router.include_router(legacy_config_router, prefix="/config", tags=["Config"])
router.include_router(legacy_synthetic_router)
