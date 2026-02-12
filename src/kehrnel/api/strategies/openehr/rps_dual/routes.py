"""Strategy-scoped API surface for openEHR RPS dual."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from kehrnel.api.strategies.openehr.rps_dual.ingest.routes import router as ingest_router
from kehrnel.api.strategies.openehr.rps_dual.config.routes import router as config_router
from kehrnel.api.strategies.openehr.rps_dual.synthetic.routes import router as synthetic_router


router = APIRouter(
    prefix="/api/strategies/openehr/rps_dual",
)

# Strategy-owned endpoints.
router.include_router(ingest_router, prefix="/ingest", tags=["Ingest"])
router.include_router(config_router, prefix="/config", tags=["Config"])
router.include_router(synthetic_router)


# Legacy client compatibility:
# Some old UI builds incorrectly call domain APIs under the strategy prefix
# (/api/strategies/openehr/rps_dual/*). Redirect known domain resources to
# canonical domain-scoped routes.
_DOMAIN_FIRST_SEGMENTS = {
    "ehr",
    "query",
    "definition",
}


@router.api_route(
    "/{legacy_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    include_in_schema=False,
)
async def redirect_legacy_domain_calls(legacy_path: str, request: Request):
    first = (legacy_path.split("/", 1)[0] or "").strip().lower()
    if first in _DOMAIN_FIRST_SEGMENTS:
        target = f"/api/domains/openehr/{legacy_path}"
        query = str(request.url.query or "").strip()
        if query:
            target = f"{target}?{query}"
        return RedirectResponse(url=target, status_code=307)
    raise HTTPException(
        status_code=404,
        detail=f"Not Found. Use domain routes under /api/domains/openehr/* for clinical APIs, or strategy routes under /api/strategies/openehr/rps_dual/* for strategy operations.",
    )
