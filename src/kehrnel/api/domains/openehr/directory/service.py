# src/kehrnel/api/legacy/v1/directory/service.py

import uuid
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from typing import Tuple, Optional
from dateutil.parser import isoparse

from kehrnel.api.domains.openehr.directory.models import Folder, FolderCreate
from kehrnel.api.domains.openehr.directory.repository import (
    update_ehr_and_insert_contribution_for_directory,
    find_folder_in_contribution_at_time,
    find_folder_in_contribution_by_uid,
    delete_directory_and_insert_contribution
)
from kehrnel.api.domains.openehr.ehr.repository import find_ehr_by_id
from kehrnel.api.common.models import ObjectRef, HierObjectID, ObjectVersionID
from kehrnel.api.legacy.app.core.models import Contribution, AuditDetails


async def retrieve_directory_by_version_id(
    ehr_id: str,
    version_uid: str,
    path: Optional[str],
    db: AsyncIOMotorDatabase
) -> Folder:
    """
    Retrieves a specific version of a directory for an EHR, optionally at a sub-path.

    Args:
        ehr_id: The ID of the EHR.
        version_uid: The specific version UID of the directory to retrieve.
        path: Slash-separated path to a sub-folder.
        db: The database session.

    Returns:
        The requested Folder object.

    Raises:
        HTTPException: For various not-found scenarios.
    """
    
    # 1. Validate that the EHR exists.
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found",
        )

    # 2. Find the specific folder version within that EHR.
    folder_dict = await find_folder_in_contribution_by_uid(ehr_id, version_uid, db)

    if not folder_dict:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Directory with version_uid '{version_uid}' not found for EHR '{ehr_id}'.",
        )

    root_folder = Folder.model_validate(folder_dict)

    # 3. If a path is provided, traverse to the sub-folder (reuses existing helper).
    if path:
        return _find_subfolder_by_path(root_folder, path)

    return root_folder

async def create_directory(
    ehr_id: str,
    directory_payload: FolderCreate,
    db: AsyncIOMotorDatabase,
    committer_name: str = "System"
) -> Folder:
    """
    Creates a directory for a given EHR

    Args:
        ehr_id: The ID of the EHR
        directory_payload: The data for the directory to be created
        db: The database session
        committer_name: Name of the committer

    Returns:
        The created Folder object

    Raises:
        HTTPException: 404 if EHR not found, 409 if directory exists, 500 on DB error.
    """
    # 1. Check if EHR exists
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not foudn"
        )
    
    # 2. Check if a directory already exists for this EHR
    if ehr_document.get("directory"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"EHR with id '{ehr_id}' already has a directory."
        )
    
    system_id = ehr_document["system_id"]["value"]
    time_created = datetime.now(timezone.utc)

    # 3. Create the full folder object with a new UID
    folder_uuid = str(uuid.uuid4())
    version_uid = f"{folder_uuid}::{system_id}::1"

    created_directory = Folder(
        **directory_payload.model_dump(by_alias=True),
        uid=ObjectVersionID(value=version_uid)
    )

    # 4. Create the Contribution
    contribution_id = str(uuid.uuid4())
    contribution = Contribution(
        id=contribution_id,
        ehr_id=ehr_id,
        audit=AuditDetails(
            system_id=system_id,
            committer_name=committer_name,
            time_committed=time_created,
            change_type="creation",
            description="Directory created."
        ),
        versions=[created_directory.model_dump(by_alias=True)]
    )

    # 5. Create ObjectRefs to be stored in the EHR document
    directory_ref = ObjectRef(
        id=HierObjectID(value=version_uid),
        namespace="local",
        type="FOLDER"
    )
    
    contribution_ref = ObjectRef(
        id=HierObjectID(value=contribution_id),
        namespace="local",
        type="CONTRIBUTION"
    )

    # 6. Call repository to perform the atomic update
    try:
        await update_ehr_and_insert_contribution_for_directory(
            ehr_id=ehr_id,
            directory_ref=directory_ref.model_dump(by_alias=True),
            contribution_doc=contribution.model_dump(by_alias=True),
            contribution_ref=contribution_ref.model_dump(by_alias=True),
            db=db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not create directory due to a database error: {e}",
        )

    return created_directory


def _parse_and_increment_version_uid(version_uid: str) -> Tuple[str, str]:
    """
    Parses a version_uid, increments its version number, and returns the new UID
    and the base object ID.
    Example: "uuid::host::1" -> ("uuid::host::2", "uuid")
    """
    try:
        parts = version_uid.split("::")
        object_id = parts[0]
        system_id = parts[1]
        version = int(parts[2])
        new_version = version + 1
        new_version_uid = f"{object_id}::{system_id}::{new_version}"
        return new_version_uid, object_id
    except (IndexError, ValueError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid version_uid format: {version_uid}. Error: {e}"
        )
    

async def update_directory(
    ehr_id: str,
    if_match_uid: str,
    directory_payload: FolderCreate,
    db: AsyncIOMotorDatabase,
    committer_name: str = "System",
) -> Folder:
    """
    Updates the directory for a given EHR.

    Args:
        ehr_id: The ID of the EHR.
        if_match_uid: The preceding_version_uid for optimistic locking.
        directory_payload: The new data for the directory.
        db: The database session.
        committer_name: Name of the committer.

    Returns:
        The updated Folder object with its new version UID.

    Raises:
        HTTPException: 404 if EHR/directory not found, 412 if version mismatch, 500 on DB error.
    """

    # 1. Check if EHR exists
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found",
        )
    
    # 2. Check if a directory exists for this EHR
    if "directory" not in ehr_document or ehr_document["directory"] is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' does not have a directory to update.",
        )
    
    # 3. Optimistic Locking: Validate the If-Match header
    current_version_uid = ehr_document["directory"]["id"]["value"]
    if if_match_uid != current_version_uid:
        headers = {
            "Location": f"/v1/ehr/{ehr_id}/directory/{current_version_uid}",
            "ETag": f'"{current_version_uid}"'
        }
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=f"If-Match header '{if_match_uid}' does not match current version '{current_version_uid}'.",
            headers=headers
        )
    
    # 4. Create the new version of the Folder
    new_version_uid, folder_uuid = _parse_and_increment_version_uid(current_version_uid)


    updated_directory = Folder(
        **directory_payload.model_dump(by_alias=True),
        uid=ObjectVersionID(value=new_version_uid)
    )

    # 5. Create the Contribution for the update
    time_committed = datetime.now(timezone.utc)
    system_id = ehr_document["system_id"]["value"]
    contribution_id = str(uuid.uuid4())
    contribution = Contribution(
        id=contribution_id,
        ehr_id=ehr_id,
        audit=AuditDetails(
            system_id=system_id,
            committer_name=committer_name,
            time_committed=time_committed,
            change_type="modification",
            description="Directory updated."
        ),
        versions=[updated_directory.model_dump(by_alias=True)]
    )

    # 6. Create ObjectRefs to update the EHR document
    directory_ref = ObjectRef(
        id=HierObjectID(value=new_version_uid),
        namespace="local",
        type="FOLDER"
    )
    contribution_ref = ObjectRef(
        id=HierObjectID(value=contribution_id),
        namespace="local",
        type="CONTRIBUTION"
    )

    # 7. Call repository to perform the atomic update (reusing the creation function)
    try:
        await update_ehr_and_insert_contribution_for_directory(
            ehr_id=ehr_id,
            directory_ref=directory_ref.model_dump(by_alias=True),
            contribution_doc=contribution.model_dump(by_alias=True),
            contribution_ref=contribution_ref.model_dump(by_alias=True),
            db=db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not update directory due to a database error: {e}",
        )

    return updated_directory


def _find_subfolder_by_path(root_folder: Folder, path: str) -> Folder:
    """
    Traverses a Folder object to find a sub-folder at a given path

    Args:
        root_folder: The starting Folder object
        path: The slash-separated path string

    Returns:
        The located sub-folder

    Raises:
        HTTPException: 404 if the path is invalid or not found.
    """

    # Normalize path by removing leading/trailing slashed and filter empty parts
    path_parts = [part for part in path.strip("/").split("/") if part]

    if not path_parts:
        return root_folder
    
    current_folder = root_folder
    for i, part in enumerate(path_parts):
        found_folder = next(
            (f for f in current_folder.folders if f.name.value == part),
            None
        )

        if found_folder is None:
            # Construc the path that was found so far for a better error message
            found_path = "/".join(path_parts[:i])
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Path path '{part}' not found in folder '{found_path}/'. Full path '{path}' is invalid."
            )
        current_folder = found_folder
    
    return current_folder


async def retrieve_directory(
    ehr_id: str,
    version_at_time: Optional[str],
    path: Optional[str],
    db: AsyncIOMotorDatabase,
) -> Tuple[Folder, str]:
    """
    Retrieves a directory for an EHR, optionally at a specific time and sub-path.

    Args:
        ehr_id: The ID of the EHR
        version_at_time: ISO 8601 timestamp to retrieve a historical version
        path: Slash-separated path to a sub-folder
        db: The database session

    Returns:
        HTTPException: For various errors like not found, bad request, etc.

    Raises:
        HTTPException: For various errors like not found, bad request, etc.
    """

    # 1. Check if EHR exists
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found"
        )
    
    folder_dict = None
    # 2. Decide how to fetch the directory (latest vs at_time)
    if version_at_time:
        try:
            timestamp = isoparse(version_at_time)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid 'version_at_time' format: '{version_at_time}'. Please use ISO 8601 format."
            )
        folder_dict = await find_folder_in_contribution_at_time(ehr_id, timestamp, db)
    # Retrieve the latest version
    else:
        if "directory" not in ehr_document or ehr_document["directory"] is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"EHR with id '{ehr_id}' does not have a directory.",
            )
        latest_version_uid = ehr_document["directory"]["id"]["value"]
        folder_dict = await find_folder_in_contribution_by_uid(ehr_id, latest_version_uid, db)
    
    # 3. Check if a directory version was found
    if not folder_dict:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No directory version found for EHR '{ehr_id}' at the specified time." 
                   if version_at_time else f"Directory for EHR '{ehr_id}' could not be found."
        )

    root_folder = Folder.model_validate(folder_dict)
    
    root_version_uid = root_folder.uid.value

    # 4. If a path is provided, traverse the folder structure
    if path:
        target_folder = _find_subfolder_by_path(root_folder, path)
        return target_folder, root_version_uid

    return root_folder, root_version_uid


async def delete_directory(
    ehr_id: str,
    preceding_version_uid: str,
    db: AsyncIOMotorDatabase,
    committer_name: str = "System"
):
    """
    Performs a logical deletion of an EHR's directory.

    Args:
        ehr_id: The ID of the EHR.
        preceding_version_uid: The UID from the If-Match header for optimistic locking.
        db: The database session.
        committer_name: Name of the committer.

    Raises:
        HTTPException: For not found, precondition failed, or database errors.
    """
    # 1. Check if EHR exists
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found",
        )

    # 2. Check if a directory exists for this EHR
    if "directory" not in ehr_document or ehr_document["directory"] is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' does not have a directory to delete.",
        )

    # 3. Optimistic Locking: Validate the If-Match header
    current_version_uid = ehr_document["directory"]["id"]["value"]
    if preceding_version_uid != current_version_uid:
        headers = {
            "Location": f"/v1/ehr/{ehr_id}/directory/{current_version_uid}",
            "ETag": f'"{current_version_uid}"'
        }
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=f"If-Match header '{preceding_version_uid}' does not match current version '{current_version_uid}'.",
            headers=headers
        )

    # 4. Create the "deletion" Contribution
    time_committed = datetime.now(timezone.utc)
    system_id = ehr_document["system_id"]["value"]
    contribution_id = str(uuid.uuid4())

    contribution = Contribution(
        id=contribution_id,
        ehr_id=ehr_id,
        audit=AuditDetails(
            system_id=system_id,
            committer_name=committer_name,
            time_committed=time_committed,
            change_type="deleted",
            description=f"Directory with uid '{current_version_uid}' logically deleted."
        ),
        versions=[]
    )

    # 5. Create the ObjectRef for the new contribution
    contribution_ref = ObjectRef(
        id=HierObjectID(value=contribution_id),
        namespace="local",
        type="CONTRIBUTION"
    )

    # 6. Call repository to perform the atomic update
    try:
        await delete_directory_and_insert_contribution(
            ehr_id=ehr_id,
            contribution_doc=contribution.model_dump(by_alias=True),
            contribution_ref=contribution_ref.model_dump(by_alias=True),
            db=db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not delete directory due to a database error: {e}",
        )