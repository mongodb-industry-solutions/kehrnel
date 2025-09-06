#ehr/service.py

import uuid
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from src.api.v1.ehr.repository import (
    insert_ehr_and_contribution_in_transaction, 
    find_ehr_by_subject, find_ehr_by_id, 
    update_ehr_status_in_transaction, 
    find_newest_ehrs, 
    insert_composition_contribution_and_update_ehr, 
    find_composition_by_uid,
    add_deletion_contribution_and_update_ehr,
    find_deletion_contribution_for_version
)
from src.api.v1.ehr.models import (
    EHRStatus, PartySelf, EHRCreationResponse, EHR, Composition, CompositionCreate,
    EHRStatusCreate, EhrIdModel, SystemIdModel, ObjectVersionID, DvDateTime,
    ObjectRef, HierObjectID
)
from src.app.core.models import Contribution, AuditDetails


async def retrieve_ehr_by_subject(subject_id: str, subject_namespace: str, db: AsyncIOMotorDatabase) -> EHR:
    """
    Retrieves a single EHR by its subject's ID and namespace.

    Args:
        subject_id: The identifier of the subject.
        subject_namespace: The namespace of the subject's identifier.
        db: The database session.

    Returns:
        The EHR Pydantic model instance.

    Raises:
        HTTPException: If no EHR with the given subject is found (status 404).
    """
    ehr_document = await find_ehr_by_subject(subject_id, subject_namespace, db)
    if not ehr_document:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = f"EHR with subject_id '{subject_id}' and namespace '{subject_namespace}' not found"
        )
    return EHR.model_validate(ehr_document)

async def delete_composition_by_preceding_uid(
    ehr_id: str,
    preceding_version_uid: str,
    if_match: str,
    db: AsyncIOMotorDatabase,
    committer_name: str = "System"
) -> Dict[str, Any]:
    """
    Handles the logical deletion of a composition version.

    This doesn't delete the record. Instead it creates a new "deleted" contribution that points to the version being deleted

    Args:
        ehr_id: The ID of the parent EHR.
        preceding_version_uid: The UID of the composition version to be "deleted".
        if_match: The ETag for optimistic locking, must match preceding_version_uid.
        db: The database session
        committer_name: The name of the committer for the audit trail

    Returns:
        A dictionary containing the UID of the new deletion audit entry and its creation time
    """
    # Cncurrency and consistency check
    expected_uid = if_match.strip('"')
    if expected_uid != preceding_version_uid:
        raise HTTPException(
            status_code = status.HTTP_412_PRECONDITION_FAILED,
            detail = f"The If-Match header ('{expected_uid}') doesn't match the preceding_version_uid in the URL ('{preceding_version_uid}')"
        )
    
    # Fetch the EHR and composition to ensure they exist and are linked
    ehr = await retrieve_ehr_by_id(ehr_id, db)

    # This function already raises 404 if the composition is not found or not linked to the EHR.
    composition_to_delete = await retrieve_composition_by_version_uid(
        ehr_id = ehr_id,
        versioned_object_uid = preceding_version_uid,
        db = db
    )

    # Verify this version hans't already been deleted
    existing_deletion = await find_deletion_contribution_for_version(preceding_version_uid, db)
    if existing_deletion:
        raise HTTPException(
            status_code = status.HTTP_409_CONFLICT,
            detail = f"Version '{preceding_version_uid}' has already been deleted."
        )
    
    # Create the new version UID for the audit entry
    try:
        object_id, system_id, version_str = preceding_version_uid.split('::')
        new_version = int(version_str) + 1
        new_audit_uid = f"{object_id}::{system_id}::{new_version}"
    except (ValueError, IndexError):
        raise HTTPException(
            status_code = 500,
            detail = "Could not parse the existing version UID to create a new version for the deletion audit."
        )
    
    # Prepare the new "deleted" contribution
    time_committed = datetime.now(timezone.utc)

    # The 'versions' field in the contribution audit now records the deletion
    # It points to the version that was deleted
    audit_version_data = {
        "_type": "DELETED",
        "uid": new_audit_uid,
        "preceding_version_uid": preceding_version_uid
    }

    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = committer_name,
            time_committed = time_committed,
            change_type = "deleted"
        ),
        versions = [audit_version_data]
    )

    # Pass to the repository fo atomic update
    try:
        await add_deletion_contribution_and_update_ehr(
            ehr_id = ehr_id,
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not delete Composition due to a database error: {e}"
        )
    
    # Return data needed for response headers
    return {
        "new_audit_uid": new_audit_uid,
        "time_committed": time_committed,
        "versioned_object_locator": f"{object_id}::{system_id}"
    }

async def update_composition(
    ehr_id: str,
    preceding_version_uid: str,
    if_match: str,
    new_composition_data: CompositionCreate,
    db: AsyncIOMotorDatabase,
    committer_name: str = "System"
) -> Composition:
    """
    Updates a composition by creating a new version.

    Args:
        ehr_id: The ID of the parent EHR
        preceding_version_uid: The UID of the composition version to be replaced
        if_match: ETag for optimistic locking, must match preceding_version_uid
        new_composition_data: The new canonical composition data from the request
        db: The database session
        committer_name: The name of the committer for the audit trail

    Returns:
        The newly created composition object (the new version)
    """

    # Concurrency and consistency check
    expected_uid = if_match.strip('"')
    if expected_uid != preceding_version_uid:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = f"The If-Match header ('{expected_uid}') does not match the preceding_version_uid in the URL ('{preceding_version_uid}')."
        )
    
    # Fetch the composition being updated to ensure it exists
    existing_composition = await retrieve_composition_by_version_uid(
        ehr_id = ehr_id,
        versioned_object_uid = preceding_version_uid,
        db = db
    )

    if not existing_composition:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = "EHR with id '{ehr_id}' and composition not '{versioned_object_uid}' found"
        )

    # Create the new version UID
    try:
        object_id, system_id, version_str = preceding_version_uid.split('::')
        new_version = int(version_str) + 1
        new_uid = f"{object_id}::{system_id}::{new_version}"
    except (ValueError, IndexError):
        raise HTTPException(status_code=500, detail="Could not parse the existing version UID to create a new version.")
    
    # Prepare the new versioned objects
    time_commited = datetime.now(timezone.utc)

    # Create the new Composition object for the database
    new_composition_for_db = Composition(
        uid = new_uid,
        time_created = time_commited,
        data = new_composition_data.content
    )

    # Create the Contribution for this modification
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = committer_name,
            time_committed = time_commited,
            change_type = "modification"
        ),
        versions = [{
            "_type": "COMPOSITION",
            "uid": new_uid,
            "template_id": new_composition_data.template_id
        }]
    )

    # Atomically inser the new documents and update the EHR. Reuse the same repository function to do so
    try:
        await insert_composition_contribution_and_update_ehr(
            ehr_id = ehr_id,
            composition_doc = new_composition_for_db.model_dump(by_alias = True),
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not update Composition due to a database error: {e}"
        )
    
    return new_composition_for_db


async def retrieve_composition_by_version_uid(
    ehr_id: str,
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase
) -> Composition:
    """
    Retrieves a specific version of a Composition

    It validates that the EHR exists and that the Composition UID is linked to it, then fetches the full composition data.

    Args:
        ehr_id: The ID of the parent EHR
        versioned_bject_uid: The unique version ID of the composition to retrieve.
        db: The database sessionw

    Returns:
        The validated composition pydantic model

    Raises:
        HTTPException: 404 if the EHR of composition is not found
    """

    # Validate that the EHR exists and contains a composition link
    ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not ehr_doc:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = "EHR with id '{ehr_id}' not found"
        )
    
    # Check if the composition UID is in the EHR's list of compositions
    # Ensure the composition belongs to the specified EHR

    composition_refs = ehr_doc.get("compositions", [])

    if not any(comp["id"]["value"] == versioned_object_uid for comp in composition_refs):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Composition with id '{versioned_object_uid}' not found in EHR '{ehr_id}'"
        )
    
    # Fetch the composition document from the repository
    composition_doc = await find_composition_by_uid(versioned_object_uid, db)
    if not composition_doc:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = "Composition with id '{versioned_object_uid}' not found"
        )
    
    return Composition.model_validate(composition_doc)


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


async def retrieve_ehr_by_id(ehr_id: str, db: AsyncIOMotorDatabase) ->EHR:
    """
    Retrieves an EHR by its ID.

    Args:
        ehr_id: The unique identifier of the EHR.
        db: The database session.

    Returns:
        The EHR Pydantic model instance.

    Raises:
        HTTPException: If the EHR with the given ID is not found (status 404).
    """
    ehr_document = await find_ehr_by_id(ehr_id, db)
    if not ehr_document:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id} not found"
        )
    
    # The document from MongoDB is a dict. We parse it with the EHR model
    # to ensure it's valid and to get a proper Pydantic object back.
    # The aliasing of ehr_id <-> _id is handled automatically by Pydantic.
    return EHR.model_validate(ehr_document)


async def retrieve_ehr_list(db: AsyncIOMotorDatabase, limit: int = 50) -> List[EHR]:
    """
    Retrieves a list of the newest EHRs.

    Args:
        db: The database session.
        limit: The maximum number of EHRs to return.

    Returns:
        A list of EHR Pydantic model instances.
    
    Raises:
        HTTPException: If no EHRs are found (status 404).
    """
    
    ehr_list_documents = await find_newest_ehrs(db, limit)
    
    # Convert the list of dictionaries from the DB into a list of Pydantic models.
    # An empty list is a valid result if the database is empty.
    return [EHR.model_validate(document) for document in ehr_list_documents]


async def create_ehr_with_id(
    ehr_id: str,
    db: AsyncIOMotorDatabase,
    initial_status: Optional[EHRStatusCreate] = None,
    committer_name: str = "System"
) -> EHRCreationResponse:
    """
    Creates an EHR with a client-specified ID
    """
    if await find_ehr_by_id(ehr_id, db):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"EHR with id '{ehr_id}' already exists"
        )
    return await _create_ehr_logic(db, ehr_id, initial_status, committer_name)


async def create_ehr(
    db: AsyncIOMotorDatabase,
    initial_status: Optional[EHRStatusCreate] = None,
    committer_name: str = "System"
) -> EHRCreationResponse:
    """
    Creates an EHR with a server-generated ID.
    """
    ehr_id = str(uuid.uuid4())
    return await _create_ehr_logic(db, ehr_id, initial_status, committer_name)


async def _create_ehr_logic(
    db: AsyncIOMotorDatabase,
    ehr_id: str,
    initial_status: Optional[EHRStatusCreate] = None,
    committer_name: str = "System"
) -> EHRCreationResponse:
    """
    Shared business logic for creating an EHR.
    """
    time_created = datetime.now(timezone.utc)
    system_id = "my-openehr-server"

    if initial_status:
        # Check for conflic by subject ID
        subject_id = initial_status.subject.external_ref["id"]["value"]
        subject_namespace = initial_status.subject.external_ref["namespace"]

        if await find_ehr_by_subject(subject_id, subject_namespace, db):
            raise HTTPException(
                status_code = status.HTTP_409_CONFLICT,
                detail = f"An EHR with subjectId '{subject_id}' already exists"
            )
        subject = initial_status.subject
    else:
        # Create default subject
        subject = PartySelf(
            external_ref={
                "id": {
                    "value": str(uuid.uuid4())
                },
                "namespace": "patients",
                "type": "PERSON"
            }
        )

    # Create the full EHR_status object for storage
    ehr_status_object_id = str(uuid.uuid4())
    ehr_status_uid_val = f"{ehr_status_object_id}::{system_id}::1"

    ehr_status = EHRStatus(
        uid=ObjectVersionID(value=ehr_status_uid_val),
        subject=subject,
        is_modifiable=initial_status.is_modifiable if initial_status else True,
        is_queryable=initial_status.is_queryable if initial_status else True,
    )

    contribution_id = str(uuid.uuid4())
    contribution = Contribution(
        id=contribution_id,
        ehr_id=ehr_id,
        audit=AuditDetails(
            system_id=system_id,
            committer_name=committer_name,
            time_committed=time_created,
            change_type="creation"
        ),
        versions=[ehr_status.model_dump(by_alias=True)]
    )

    ehr_doc = EHR(
        ehr_id=EhrIdModel(value=ehr_id),
        system_id=SystemIdModel(value=system_id),
        time_created=DvDateTime(value=time_created),
        ehr_status=ehr_status,
        ehr_access=ObjectRef(
            id=HierObjectID(value=str(uuid.uuid4())), type="EHR_ACCESS"
        ),
        contributions=[
            ObjectRef(
                id=HierObjectID(value=contribution_id), type="CONTRIBUTION"
            )
        ]
    )

    try:
        await insert_ehr_and_contribution_in_transaction(
            ehr_doc.model_dump(by_alias=True),
            contribution.model_dump(by_alias=True),
            db
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not create EHR due to a database error: {e}"
        )

    return EHRCreationResponse(
        ehr_id=EhrIdModel(value=ehr_id),
        ehr_status=ObjectRef(id=HierObjectID(value=ehr_status.uid.value), type="EHR_STATUS"),
        system_id=SystemIdModel(value=system_id),
        time_created=DvDateTime(value=time_created),
        ehr_access=ehr_doc.ehr_access
    )


async def add_composition(
    ehr_id: str,
    composition_create: CompositionCreate,
    db: AsyncIOMotorDatabase,
    commiter_name: str = "System"
) -> Composition:
    """
    Handles the business logic of adding a new, client-provided canonical
    Composition to an existing EHR.

    It involves:

    1. Validating that the target EHR exists.
    2. Assigning a system-managed version UID to the composition.
    3. Creating a Contribution to audit the change.
    4. Calling the repository to perform an atomic update of all related documents.

    Args:
        ehr_id: The ID of the EHR to which the composition will be added.
        composition_create: The data for the new composition from the request.
        db: The database session
        commiter_name: The name of the committer for the audit trail.

    Returns:
        The newly created and persisted Composition object

    Raises:
        HTTPExceptoin: 404 if EHR not found, 500 on database error
    """

    # Validate that the EHR Exists
    ehr = await find_ehr_by_id(ehr_id, db)
    if not ehr:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = f"EHR with id '{ehr_id}' not found."
        )
    
    # Create versioned objects
    time_created = datetime.now(timezone.utc)

    # Generate a new unique ID for the composition object, and then its first version UID
    composition_object_id = str(uuid.uuid4())
    composition_uid = f"{composition_object_id}::my-openehr-server::1"

    # The composition data is the full dictionary coming from the request
    composition_data = composition_create.content

    # Create the full composition object for storage
    # The _id will be the version UID. The 'data' field holds the full object.
    new_composition_for_db = Composition(
        uid = composition_uid,
        time_created = time_created,
        data = composition_data
    )
    
    # Creates the contribution object for the transaction
    audit_version_data = {
        "_type": "COMPOSITION",
        "uid": composition_uid,
        "template_id": composition_create.template_id
    }
    
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = commiter_name,
            time_committed = time_created,
            change_type = "creation"
        ),
        versions=[audit_version_data]
    )

    # Pass the repository for atomic insertion and update
    try:
        await insert_composition_contribution_and_update_ehr(
            ehr_id = ehr_id,
            composition_doc = new_composition_for_db.model_dump(by_alias = True),
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db
        )
    except PyMongoError as e:
        # The repository re-raises the error, we catch it here to give a user-friendly response
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not create Composition due to a database erorr: {e}"
        )
    
    # Return the created composition object
    return new_composition_for_db