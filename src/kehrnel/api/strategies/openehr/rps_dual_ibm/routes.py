"""Strategy-scoped API surface for openEHR RPS dual IBM."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from kehrnel.api.strategies.openehr.rps_dual_ibm.synthetic.routes import router as synthetic_router


router = APIRouter(
    prefix="/api/strategies/openehr/rps_dual_ibm",
)

router.include_router(synthetic_router)


_DOMAIN_FIRST_SEGMENTS = {
    "ehr",
    "query",
    "definition",
}


@router.api_route(
    "/{path_suffix:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    include_in_schema=False,
)
async def redirect_domain_calls(path_suffix: str, request: Request):
    first = (path_suffix.split("/", 1)[0] or "").strip().lower()
    if first in _DOMAIN_FIRST_SEGMENTS:
        target = f"/api/domains/openehr/{path_suffix}"
        query = str(request.url.query or "").strip()
        if query:
            target = f"{target}?{query}"
        return RedirectResponse(url=target, status_code=307)
    raise HTTPException(
        status_code=404,
        detail=(
            "Not Found. Use domain routes under /api/domains/openehr/* for clinical APIs, "
            "or strategy routes under /api/strategies/openehr/rps_dual_ibm/* for strategy operations."
        ),
    )
