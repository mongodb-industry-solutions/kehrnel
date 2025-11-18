# src/api/v1/directory/service.py

import uuid
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError

from src.api.v1.directory.models import Folder, FolderCreate
from src.api.v1.directory.repository import update_ehr_and_insert_contribution_for_directory
from src.api.v1.ehr.repository import find_ehr_by_id
from src.api.v1.common.models import ObjectRef, HierObjectID, ObjectVersionID
from src.app.core.models import Contribution, AuditDetails

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
        id=HierObjectID(value=contribution_id),
        namespace="local",
        type="CONTRIBUTION"
    )