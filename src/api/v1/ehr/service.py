#ehr/service.py

import uuid
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from src.api.v1.ehr.repository import (
    insert_ehr_and_contribution_in_transaction, 
    find_ehr_by_subject, 
    find_ehr_by_id, 
    find_newest_ehrs
)

from src.api.v1.ehr_status.models import EHRStatusCreate, EHRStatus

from src.api.v1.common.models import(
    PartySelf,
    ObjectVersionID,
    HierObjectID,
    ObjectRef,
    DvDateTime,
    SystemIdModel,
    EhrIdModel
)

from src.api.v1.ehr.models import EHRCreationResponse, EHR
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