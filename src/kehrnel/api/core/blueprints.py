"""Non-persisting structural Blueprint validation; no planning or runtime access."""
from fastapi import APIRouter, Response

from kehrnel.contracts.architecture_foundation import SolutionBlueprint
from kehrnel.contracts.blueprint_canonicalization import SolutionBlueprintCanonicalizationResult
from kehrnel.engine.core.blueprint_canonicalization import canonicalize_solution_blueprint

router = APIRouter()


@router.post(
    "/solution-blueprints/canonicalize",
    response_model=SolutionBlueprintCanonicalizationResult,
    response_model_exclude_none=True,
)
async def canonicalize_blueprint(body: SolutionBlueprint, response: Response):
    """Validate the supplied document without resolving any named artifact."""
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "X-API-Key, X-HDL-Principal-Assertion"
    return canonicalize_solution_blueprint(body)
