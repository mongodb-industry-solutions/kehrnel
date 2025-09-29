from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status

from src.app.core.models import Contribution
from src.api.v1.contribution.repository import find_contribution_by_id


async def retrieve_contribution(ehr_id: str, contribution_uid: str, db: AsyncIOMotorDatabase) -> Contribution:
    """
    Retrieves the a specific Contribution for a given EHR.

    It validates that the contribution exists and that it belongs to the specified EHR

    Args:
        ehr_id: The identifier of the parent EHR.
        contribution_uid: The unique identifier of the contribution
        db: The database session.

    Returns:
        The validated Contribution Pydantic model

    Raises:
        HTTPException: 404 if the contribution is not found or does not belong to the EHR
    """

    contribution_doc = await find_contribution_by_id(contribution_uid, db)

    # If the contribution doesn't exist OR it doesn't belong to the specified EHR,
    # return a 404 to prevent leaking information about the existence of contributions under different EHRs.

    if not contribution_doc or contribution_doc.get("ehr_id") != ehr_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Contribution with id '{contribution_uid}' not found in EHR '{ehr_id}'"
        )
    
    return Contribution.model_validate(contribution_doc)