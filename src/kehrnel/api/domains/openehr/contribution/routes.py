from fastapi import APIRouter, Depends, status, Response
from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db
from kehrnel.api.bridge.app.core.models import Contribution
from kehrnel.api.domains.openehr.contribution.service import retrieve_contribution
from kehrnel.api.domains.openehr.contribution.api_responses import get_contribution_responses

router = APIRouter(
    prefix="/ehr/{ehr_id}/contribution",
    tags=["Contribution"]
)


@router.get(
    "/{contribution_uid}",
    response_model=Contribution,
    response_model_by_alias=False,
    status_code=status.HTTP_200_OK,
    summary="Get Contribution by ID",
    responses=get_contribution_responses
)
async def get_contribution_endpoint(
    ehr_id: str,
    contribution_uid: str,
    response: Response,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a contribution by its unique identifier for a specific EHR.

    Contributions are the audit entries for every change made to an EHR.
    This endpoint allows clients to inspect the details of a change, such as
    the committer, the time, and what versions were created.
    """
    contribution = await retrieve_contribution(
        ehr_id=ehr_id, contribution_uid=contribution_uid, db=db
    )

    response.headers["Location"] = f"/v1/ehr/{ehr_id}/contribution/{contribution_uid}"
    response.headers["ETag"] = f'"{contribution_uid}"'

    return contribution