
from datetime import datetime, timezone
from dateutil.parser import isoparse
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, Tuple
from fastapi import HTTPException, status

from src.api.v1.ehr_status.models import EHRStatus, VersionedEHRStatus
from src.api.v1.ehr_status.repository import update_ehr_status_in_transaction

from src.api.v1.ehr.models import EHR
from src.api.v1.common.models import (
    ObjectVersionID, HierObjectID, ObjectRef, 
    RevisionHistory, RevisionHistoryItem, OriginalVersionResponse
)

from src.api.v1.ehr.service import retrieve_ehr_by_id
from src.api.v1.ehr.repository import find_ehr_by_id

from src.api.v1.contribution.repository import (
    find_contribution_by_version_uid,
    find_latest_contribution_by_vo_uid,
    find_contributions_for_versioned_object
)
from src.app.core.models import Contribution, AuditDetails


async def retrieve_ehr_status_revision_history(
    ehr_id: str,
    db: AsyncIOMotorDatabase
) -> RevisionHistory:
    """
    Retrieves the revision history for the EHR_STATUS of a given EHR.

    Args:
        ehr_id: The ID of the parent EHR.
        db: The database session.

    Returns:
        A RevisionHistory object containing all audit entries for the EHR_STATUS.

    Raises:
        HTTPException 404 if the EHR is not found.
    """

    # Validate EHR exists and get its status UID
    ehr = await retrieve_ehr_by_id(ehr_id=ehr_id, db=db)

    # Extract the base versioned object UID for the EHR_STATUS
    try:
        ehr_status_uid = ehr.ehr_status.uid.value
        versioned_object_uid = ehr_status_uid.split("::")[0]
    except (IndexError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Malformed EHR_STATUS UID found in the database"
        )
    
    # Fetch all relevant contributions using the generic repository function
    contribution_docs = await find_contributions_for_versioned_object(versioned_object_uid, db)
    if not contribution_docs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No revision history found for EHR_STATUS in EHR '{ehr_id}'"
        )
    
    # Map the contribution data to the RevisionHistoryItem model
    history_items = []
    for contrib_doc in contribution_docs:
        # Find the specific version entry within the contribution that matches the EHR_STATUS
        matching_version = next(
            (v for v in contrib_doc.get("versions", []) 
             if v.get("uid", {}).get("value", "").startswith(versioned_object_uid)),
            None
        )

        if matching_version:
            item = RevisionHistoryItem(
                versionId=ObjectVersionID.model_validate(matching_version["uid"]),
                audit=AuditDetails.model_validate(contrib_doc["audit"])
            )
            history_items.append(item)

    return RevisionHistory(items=history_items)


async def retrieve_versioned_ehr_status(
    ehr_id: str,
    db: AsyncIOMotorDatabase
) -> VersionedEHRStatus:
    """
    Retrieves metadata about the VERSIONED_EHR_STATUS for a given EHR

    This involves validating the EHR's existence, parsing the EHR_STATUS UID to get the base object ID,
    and using the EHR's creation time as the creation time for the versioned status.

    Args:
        ehr_id: The unique identifier of the parent EHR.
        db: The database session

    Returns:
        A versionedEHRStatus Pydantic model instance

    Raises:
        HTTPException: If the EHR with the given ID is not found (status 404)
    """

    # Retrieve the full EHR object. This also validates that the EHR exists.
    ehr = await retrieve_ehr_by_id(ehr_id=ehr_id, db=db)

    # Extract the base object UID from the full version UID
    try:
        versioned_object_uid = ehr.ehr_status.uid.value.split("::")[0]
    except (IndexError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = "Malformed EHR_STATUS UID found in the database"
        )
    
    # Create the response object
    # The `time_created` of the versioned status is the creation time of its first version
    # which is the same as the EHR's creation time.

    versioned_ehr_status_response = VersionedEHRStatus(
        uid=HierObjectID(value=versioned_object_uid),
        owner_id=ObjectRef(
            id=HierObjectID(value=ehr_id),
            type="EHR"
        ),
        time_created=ehr.time_created
    )

    return versioned_ehr_status_response


async def retrieve_ehr_status_by_version_uid(
    ehr_id: str, version_uid: str, db: AsyncIOMotorDatabase
) -> Tuple[EHRStatus, datetime]:
    """
    Retrieves a specific version of the EHR_STATUS for a given EHR.

    Args:
        ehr_id: The unique identifier of the parent EHR.
        version_uid: The unique identifier of the specific EHR_STATUS version
        db: The database session

    Returns:
        A tuple containing the EHRStatus pydantic model and the time it was committed

    Raises:
        HTTPException: If the EHR or the specific version is not found (404)
    """

    # Find the contribution that created this version
    contribution_doc = await find_contribution_by_version_uid(version_uid=version_uid, db=db)

    # Validate that the contribution exists and belongs to the specified EHR
    # This prevents accessing a version from a different EHR
    if not contribution_doc or contribution_doc.get("ehr_id") != ehr_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version '{version_uid}' not found in EHR '{ehr_id}'."
        )
    
    # Find the EHR_STATUS object within the contribution's 'versions' array
    ehr_status_doc = next(
        (v for v in contribution_doc.get("versions", []) if v.get("uid", {}).get("value") == version_uid),
        None,
    )

    if not ehr_status_doc:
        # This case indicates data inconsistency
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inconsistent data: Contribution found for version '{version_uid}', but the version data is missing."
        )
    
    # Parse the document into an EHRStatus model
    ehr_status = EHRStatus.model_validate(ehr_status_doc)
    time_committed = contribution_doc["audit"]["time_committed"]

    return ehr_status, time_committed


async def retrieve_ehr_status_by_ehr_id(
    ehr_id: str, 
    db: AsyncIOMotorDatabase,
    version_at_time: Optional[str] = None
) -> Tuple[EHRStatus, datetime]:
    
    # 1. Retrieve the full EHR document to get the ehr_status UID
    ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not ehr_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found"
        )

    # 2. Extract the base object UID from the full version UID
    try:
        ehr_status_uid = ehr_doc["ehr_status"]["uid"]["value"]
        versioned_object_uid = ehr_status_uid.split("::")[0]
    except (KeyError, IndexError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Malformed EHR_STATUS UID found in the database."
        )

    at_time_datetime: Optional[datetime] = None
    if version_at_time:
        try:
            at_time_datetime = isoparse(version_at_time)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid 'version_at_time' format: {version_at_time}"
            )

    # 3. Use the new generic repository function
    contribution_doc = await find_latest_contribution_by_vo_uid(
        versioned_object_uid=versioned_object_uid, db=db, timestamp=at_time_datetime
    )

    if not contribution_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No EHR_STATUS version found for EHR '{ehr_id}' at the specified time."
        )
    
    # Use the UID for a precise match instead of _type
    ehr_status_doc = next(
        (v for v in contribution_doc.get("versions", []) if v.get("uid", {}).get("value", "").startswith(versioned_object_uid)),
        None
    )

    if not ehr_status_doc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inconsistent data: Contribution found for EHR_STATUS, but the version data is missing."
        )
    
    ehr_status = EHRStatus.model_validate(ehr_status_doc)
    time_committed = contribution_doc["audit"]["time_committed"]

    return ehr_status, time_committed


async def retrieve_ehr_status_version(
    ehr_id: str,
    db: AsyncIOMotorDatabase,
    version_at_time: Optional[str] = None,
) -> OriginalVersionResponse:
    """
    Retrieves a specific version of an EHR_STATUS

    if version_at_time is provided, it finds the version extant at that time
    Otherwise, it finds the latest version

    Args:
        ehr_id: The ID of the parent EHR
        db: The database session
        version_at_time: Optional ISO 8601 timestampt

    Returns:
        An originalVersionResponse object containing the version data and audit

    Raises:
        HTTPException: If the resource or version is not found, or if the timestamp is invalid
    """

    # Validate EHR exists and get its status UID
    ehr = await retrieve_ehr_by_id(ehr_id = ehr_id, db=db)
    try:
        ehr_status_uid = ehr.ehr_status.uid.value
        versioned_object_uid = ehr_status_uid.split("::")[0]
    except (IndexError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Malformed EHR_STATUS UID found in the database"
        )
    
    # Parse timestamp if provided
    at_time_datetime: Optional[datetime] = None
    if version_at_time:
        try:
            at_time_datetime = isoparse(version_at_time)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid 'version_at_time' format: {version_at_time}"
            )
        
    # Find the relevant contribution document
    contribution_doc = await find_latest_contribution_by_vo_uid(
        versioned_object_uid=versioned_object_uid,
        db=db,
        timestamp=at_time_datetime
    )

    if not contribution_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No version of EHR_STATUS for EHR '{ehr_id}' found at the specified time."
        )
    
    # Extract the EHR_STATUS version info form the contribution's 'versions' array
    version_info = next(
        (v for v in contribution_doc.get("versions", [])
         if v.get("uid", {}).get("value", "").startswith(versioned_object_uid)),
        None
    )

    if not version_info:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Inconsistent data: Contribution found but EHR_STATUS version link is missing"
        )
    
    preceding_uid_val = version_info.get("preceding_version_uid")

    # Construct the response. The 'data' is the EHR_STATUS object itself, which is already embedded in the contribution document
    response = OriginalVersionResponse(
        uid=ObjectVersionID.model_validate(version_info["uid"]),
        preceding_version_uid=ObjectVersionID(value=preceding_uid_val) if preceding_uid_val else None,
        data=version_info,
        commit_audit=AuditDetails.model_validate(contribution_doc["audit"]),
        contribution=ObjectRef(
            id=HierObjectID(value=contribution_doc["_id"]),
            type="CONTRIBUTION"
        )
    )

    return response


async def retrieve_ehr_status_version_by_uid(
    ehr_id: str,
    version_uid: str,
    db: AsyncIOMotorDatabase
) -> OriginalVersionResponse:
    """
    Retrieves a specific version of an EHR_STATUS by its full versio UID.

    Args:
        ehr_id: The ID of the parent EHR.
        version_uid: The full, unique version identifier.
        db: The database session.

    Returns:
        An OriginalVersionResponse object containing the version data and audit.

    Raises:
        HTTPException: If the resource or specific version is not found.
    """
    # Find the contribution that created this version.
    contribution_doc = await find_contribution_by_version_uid(version_uid=version_uid, db=db)

    # Validate that the contribution exists and belongs to the specified EHR.
    if not contribution_doc or contribution_doc.get("ehr_id") != ehr_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version '{version_uid}' not found in EHR '{ehr_id}'."
        )

    # Find the EHR_STATUS object within the contribution's 'versions' array.
    version_info = next(
        (v for v in contribution_doc.get("versions", []) if v.get("uid", {}).get("value") == version_uid),
        None,
    )

    if not version_info:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inconsistent data: Contribution found for version '{version_uid}', but the version data is missing."
        )

    preceding_uid_val = version_info.get("preceding_version_uid")

    # Construct and return the response.
    response = OriginalVersionResponse(
        uid=ObjectVersionID.model_validate(version_info["uid"]),
        preceding_version_uid=ObjectVersionID(value=preceding_uid_val) if preceding_uid_val else None,
        data=version_info,
        commit_audit=AuditDetails.model_validate(contribution_doc["audit"]),
        contribution=ObjectRef(
            id=HierObjectID(value=contribution_doc["_id"]),
            type="CONTRIBUTION"
        )
    )

    return response


async def update_ehr_status(
    ehr_id: str,
    status_update_request: EHRStatus,
    if_match: str,
    db: AsyncIOMotorDatabase,
    commiter_name: str = "System"
) -> Tuple[EHRStatus, datetime]:
    """
    Updates the status of an existing EHR.

    Performs a concurrency check using the If-Match header against the current EHR_STATUS version UID. 
    Creates a new Contribution for the change and atomically updates the EHR.

    Args:
        ehr_id: The ID of the EHR to update.
        status_update_request: The new EHR_STATUS data from the client.
        if_match: The ETag value of the current EHR_STATUS, for concurrency control.
        db: The database session.
        commiter_name: The name of the committer for the audit trail.

    Returns:
        A tuple containing the newly created and versioned EHRStatus object and its commit time.
    """

    # 1. Fetch the current EHR
    current_ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not current_ehr_doc:
        raise HTTPException(status_code = status.HTTP_404_NOT_FOUND, detail = f"EHR with id '{ehr_id}' not found")
    
    current_status = EHR.model_validate(current_ehr_doc).ehr_status

    # 2. Concurrency Check (In case two clients are trying to update the same version of the status simultaneously)
    # The ETag format includes quotes, which we must remove for comparison
    expected_uid = if_match.strip('"')
    if current_status.uid.value != expected_uid: # FIX: Access the .value attribute
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=f"The provided version UID ('{expected_uid}') doesn't match the latest version ('{current_status.uid.value}'). Get the latest version and try again"
        )
    
    # 3. Prepare the new versioned objects
    time_committed = datetime.now(timezone.utc)

    # Parse the UID to increment the version: {object_id}::{creating_system_id}::{version_tree_id}
    try:
        object_id, system_id, version = current_status.uid.value.split('::')
        new_version = int(version) + 1
        new_uid_value = f"{object_id}::{system_id}::{new_version}"
    except (ValueError, IndexError):
        raise HTTPException(status_code=500, detail="Couldn't parse the existing version UID")
    
    # Create the new EHRStatus object for storage
    new_ehr_status = EHRStatus(
        uid=ObjectVersionID(value=new_uid_value),
        subject=status_update_request.subject,
        is_modifiable=status_update_request.is_modifiable,
        is_queryable=status_update_request.is_queryable
    )

    # Create the new contribution
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = commiter_name,
            time_committed = time_committed,
            change_type = "modification"
        ),
        versions = [new_ehr_status.model_dump(by_alias = True)]
    )

    # 4. Pass to the repository for atomic update
    await update_ehr_status_in_transaction(
        ehr_id = ehr_id,
        new_status_doc = new_ehr_status.model_dump(by_alias = True),
        contribution_doc = contribution.model_dump(by_alias = True),
        db = db
    )

    return new_ehr_status, time_committed