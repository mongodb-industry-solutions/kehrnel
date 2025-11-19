# src/api/v1/directory/routes.py

from fastapi import APIRouter, Depends, status, Header, HTTPException, Response
from motor.motor_asyncio import AsyncIOMotorDatabase

from src.api.v1.directory.service import create_directory, update_directory
from src.api.v1.directory.models import Folder, FolderCreate
from src.api.v1.directory.api_responses import create_directory_responses, update_directory_responses
from src.app.core.database import get_mongodb_ehr_db

router = APIRouter(
    tags = ["Directory"]
)

@router.post(
    "/ehr/{ehr_id}/directory",
    response_model=Folder,
    status_code=status.HTTP_201_CREATED,
    summary="Create directory",
    responses=create_directory_responses
)
async def create_directory_endpoint(
    ehr_id: str,
    response: Response,
    payload: FolderCreate,
    prefer: str = Header("return=minimal", alias="Prefer"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new directory FOLDER associated with the EHR identified by `ehr_id`.
    An EHR can only have one root directory.

    - `Prefer: return=representation` returns the full resource.
    - `Prefer: return=minimal` returns an empty body with headers.
    """

    if prefer not in ["return=representation", "return=minimal"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail = "Invalid 'Prefer' header value. Must be 'return=representation' or 'return=minimal'."
        )
    
    created_directory = await create_directory(
        ehr_id=ehr_id,
        directory_payload=payload,
        db=db
    )

    version_uid = created_directory.uid.value
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/directory/{version_uid}"
    response.headers["ETag"] = f'"{version_uid}"'

    if prefer == "return=representation":
        return created_directory
    else:
        # FastAPI will not send a response body if the return is a Response object
        # with no content and a 204/205/304 status, but for 201 it will.
        # So we return a Response with an empty body.
        return Response(status_code=status.HTTP_201_CREATED, headers=response.headers)
    
@router.put(
    "/ehr/{ehr_id}/directory",
    status_code=status.HTTP_200_OK,
    summary="Update directory",
    responses=update_directory_responses,
    response_model=Folder, # Set for the 200 case
)
async def update_directory_endpoint(
    ehr_id: str,
    response: Response,
    payload: FolderCreate,
    if_match: str = Header(..., alias="If-Match", description="The last known version_uid of the directory."),
    prefer: str = Header("return=minimal", alias="Prefer"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Updates the directory FOLDER for the specified EHR.

    The `If-Match` header must be provided with the UID of the latest version of the
    directory, which will become the `preceding_version_uid` of the new version.

    - `Prefer: return=representation` returns the full, updated resource.
    - `Prefer: return=minimal` returns an empty body with headers.
    """

    if prefer not in ["return=representation", "return=minimal"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid 'Prefer' header value. Must be 'return=representation' or 'return=minimal'."
        )
    
    # The version UID in If-Match is quoted
    preceding_version_uid = if_match.strip('"')

    updated_directory = await update_directory(
        ehr_id=ehr_id,
        if_match_uid=preceding_version_uid,
        directory_payload=payload,
        db=db
    )

    new_version_uid = updated_directory.uid.value
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/directory/{new_version_uid}"
    response.headers["ETag"] = f'"{new_version_uid}"'

    if prefer == "return=representation":
        return updated_directory
    else:
        # For PUT with minimal return, 204 No Content is more appropriate than 200 OK.
        return Response(status_code=status.HTTP_204_NO_CONTENT, headers=response.headers)