import uuid
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from src.api.v1.ehr.repository import insert_ehr_and_contribution_in_transaction, find_ehr_by_subject, find_ehr_by_id, update_ehr_status_in_transaction, find_newest_ehrs, insert_composition_contribution_and_update_ehr
from src.api.v1.ehr.models import EHRStatus, PartySelf, EHRCreationResponse, EHR, Composition, CompositionCreate
from app.core.models import Contribution, AuditDetails


async def update_ehr_status(
    ehr_id: str,
    status_update_request: EHRStatus,
    if_match: str,
    db: AsyncIOMotorDatabase,
    commiter_name: str = "System"
) -> EHRStatus:
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
        The newly created and versioned EHRStatus object.
    """

    # 1. Fetch the current EHR
    current_ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not current_ehr_doc:
        raise HTTPException(status_cod = status.HTTP_404_NOT_FOUND, detail = f"EHR with id '{ehr_id}' not found")
    
    current_status = EHR.model_validate(current_ehr_doc).ehr_status

    # 2. Concurrency Check (In case two clients are trying to update the same version of the status simultaneously)
    # The ETag format includes quotes, which we must remove for comparison
    expected_uid = if_match.strip('"')
    if current_status.uid != expected_uid:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=f"The provided version UID ('{expected_uid}') doesn't match the latest version ('{current_status.uid}'). Get the latest version and try again"
        )
    
    # 3. Prepare the new versioned objects
    time_committed = datetime.now(timezone.utc)

    # Parse the UID to increment the version: {object_id}::{creating_system_id}::{version_tree_id}
    try:
        object_id, system_id, version = current_status.uid.split('::')
        new_version = int(version) + 1
        new_uid = f"{object_id}::{system_id}::{new_version}"
    except:
        raise HTTPException(status_code=500, detail="Couldn't parse the existing version UID")
    
    # Create the new EHRStatus object for storage
    new_ehr_status = status_update_request.model_copy(
        update = {"uid": new_uid, "type": "EHR_STATUS"}
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

    return new_ehr_status


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


async def retrieve_ehr_list(db: AsyncIOMotorDatabase, limit: int = 50) ->EHR:
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


async def create_ehr(db: AsyncIOMotorDatabase,
                     initial_status: Optional[EHRStatus] = None,
                     commiter_name: str = "System"
) -> EHRCreationResponse:
    
    ehr_id = str(uuid.uuid4())
    time_created = datetime.now(timezone.utc)

    if initial_status:
        # Check if an EHR for this subject already exists
        existing_ehr = await find_ehr_by_subject(
            subject_id = initial_status.subject.id,
            subject_namespace = initial_status.subject.namespace,
            db=db
        )
        if existing_ehr:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"An EHR with subjectId '{initial_status.subject.id}' already exists."
            )
        # Client provided an EHR_STATUS in the request body.
        # We use the status provided by the client.
        ehr_status = initial_status
    else:
        # Client sent an empty request body.
        # We MUST create a default EHR_STATUS.
        # It's good practice to give it a temporary, system-generated subject
        temp_subject_id = f"unassigned.{ehr_id}"
        ehr_status = EHRStatus(
            subject=PartySelf(
                id=temp_subject_id,
                namespace="system.unassigned"
            )
        )

    # The object_id persists across all versions of this EHR_STATUS
    ehr_status_object_id = str(uuid.uuid4())
    ehr_status.uid = f"{ehr_status_object_id}::my-openehr-server::1"

    # Create the Contribution object for this transaction
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = commiter_name,
            time_committed = time_created,
            change_type = "creation"
        ),
        versions=[ehr_status.model_dump(by_alias=True)]
    )

    # Create the main EHR document
    ehr_doc = EHR(
        ehr_id = ehr_id,
        system_id = "my-openehr-server",
        time_created = time_created,
        ehr_status = ehr_status,
        contributions = [contribution.id]
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
        ehr_id=ehr_id,
        ehr_status=ehr_status,
        system_id=ehr_doc.system_id,
        time_created=time_created
    )

async def add_composition(
    ehr_id: str,
    composition_create: CompositionCreate,
    db: AsyncIOMotorDatabase,
    commiter_name: str = "System"
) -> Composition:
    """
    Handles the business logic of adding a new Composition to an existing EHR.

    It involves:

    1. Validating that the target EHR exists
    2. Creating a versioned Composition object
    3. Creating a contribution to audit the change
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
    
    time_created = datetime.now(timezone.utc)

    # Generate a new unique ID for the composition object itself, and then its first version UID
    composition_object_id = str(uuid.uuid4())
    composition_uid = f"{composition_object_id}::my-openehr-server::1"

    # Create the full composition object for storage
    new_composition = Composition(
        **composition_create.model_dump(),
        uid = composition_uid,
        time_created = time_created
    )
    
    # Creates the contribution object for the transaction
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = commiter_name,
            time_committed = time_created,
            change_type = "creation"
        ),
        versions = [new_composition.model_dump(by_alias = True)]
    )

    # Pass the repository for atomic insertion and update
    try:
        await insert_composition_contribution_and_update_ehr(
            ehr_id = ehr_id,
            composition_doc = new_composition.model_dump(by_alias = True),
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
    return new_composition