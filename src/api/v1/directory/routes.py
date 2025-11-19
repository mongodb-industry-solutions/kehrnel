# src/api/v1/directory/routes.py

from fastapi import APIRouter, Depends, status, Header, HTTPException, Response, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional

from src.api.v1.directory.service import (
    create_directory, 
    update_directory, 
    retrieve_directory,
    retrieve_directory_by_version_id
)

from src.api.v1.directory.models import Folder, FolderCreate
from src.api.v1.directory.api_responses import (
    create_directory_responses, 
    update_directory_responses,
    get_directory_responses,
    get_directory_by_version_id_responses
)
from src.app.core.database import get_mongodb_ehr_db

router = APIRouter(
    tags = ["Directory"]
)

@router.get(
    "/ehr/{ehr_id}/directory",
    response_model=Folder,
    status_code=status.HTTP_200_OK,
    summary="Get directory version",
    responses=get_directory_responses
)
async def get_directory_endpoint(
    ehr_id: str,
    response: Response,
    version_at_time: Optional[str] = Query(
        None,
        description="A given time in the extended ISO 8601 format. Example: 2015-01-20T19:30:22.765+01:00",
        alias="version_at_time"
    ),
    path: Optional[str] = Query(
        None,
        description="Path to a sub-folder. Example: episodes/a/b/c",
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves the version of the directory FOLDER associated with the EHR.

    - If `version_at_time` is supplied, retrieves the version extant at that time.
    - Otherwise, retrieves the latest (current) directory FOLDER version.
    - If `path` is supplied, retrieves the sub-FOLDER at that path.
    """
    folder, root_version_uid = await retrieve_directory(
        ehr_id=ehr_id,
        version_at_time=version_at_time,
        path=path,
        db=db
    )

    response.headers["ETag"] = f'"{root_version_uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/directory/{root_version_uid}"

    return folder


@router.get(
    "/ehr/{ehr_id}/directory/{version_uid}",
    response_model=Folder,
    status_code=status.HTTP_200_OK,
    summary="Get directory by version ID",
    responses=get_directory_by_version_id_responses
)
async def get_directory_by_version_id_endpoint(
    ehr_id: str,
    version_uid: str,
    response: Response,
    path: Optional[str] = Query(
        None,
        description="Path to a sub-folder. Example: episodes/a/b/c",
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
):
    """
    Retrieves a particular version of the directory FOLDER, identified by `version_uid`.

    - If `path` is supplied, retrieves only the sub-FOLDER at that path. 
    """
    folder = await retrieve_directory_by_version_id(
        ehr_id=ehr_id,
        version_uid=version_uid,
        path=path,
        db=db
    )

    response.headers["ETag"] = f'"{version_uid}"'
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/directory/{version_uid}"

    return folder


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
    
