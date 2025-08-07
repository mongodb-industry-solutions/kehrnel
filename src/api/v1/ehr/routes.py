from fastapi import APIRouter, Depends, status, Body, Query, HTTPException, Response, Header
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List
from email.utils import formatdate

from datetime import datetime, timezone

from src.api.v1.ehr.service import create_ehr, retrieve_ehr_by_id, update_ehr_status, retrieve_ehr_list, add_composition
from src.api.v1.ehr.models import EHRCreationResponse, EHRStatus, ErrorResponse, EHR, Composition, CompositionCreate
from src.app.core.database import get_mongodb_ehr_db

from src.api.v1.ehr.api_responses import get_ehr_by_id_responses, create_ehr_api_responses, ehr_status_example, update_ehr_status_responses, get_ehr_list_responses, create_composition_responses

router = APIRouter(
    prefix="/ehr",
    tags=["EHR"]
)


# PUT endpoint to update the EHR_STATUS
@router.put(
    "/{ehr_id}/ehr_status",
    response_model = EHRStatus,
    response_model_by_alias = False,
    status_code = status.HTTP_200_OK,
    summary = "Update EHR Status",
    responses = update_ehr_status_responses
)
async def update_ehr_status_endpoint(
    ehr_id: str,
    response: Response,
    status_update: EHRStatus,
    # The If-Match header is mandatory for safe, concurrent updates
    if_match: str = Header(..., alias = "If-Match", description = "The ETag of the concurrent EHR_STATUS version. Required for optimistic locking"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Updates the mutable status of a specific EHR (e.g., to make it no longer modifiable).

    To ensure data consistency, this endpoint **requires** the `If-Match` header to be set
    to the ETag (the version UID) of the current `EHR_STATUS`.

    A new `CONTRIBUTION` is created to audit this change. The new `EHR_STATUS` version
    is returned in the body, and the `ETag` and `Last-Modified` headers are updated.
    """

    new_status = await update_ehr_status(
        ehr_id = ehr_id,
        status_update_request = status_update,
        if_match = if_match,
        db = db
    )

    # Set the response headers with the new version's details
    response.headers["ETag"] = f'"{new_status.uid}"'

    # The Location header points to the EHR itself, as the status is a sub-resource
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/ehr_status" 

    last_modified_gmt = formatdate(datetime.now(timezone.utc).timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return new_status

@router.get(
    "",
    response_model = List[EHR],
    response_model_by_alias = False,
    summary = "Get a list of EHR",
    responses = get_ehr_list_responses
)
async def get_ehr_list(
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a list of the 50 most recently created EHRs.
    The list is sorted by `time_created` in descending order.
    This endpoint does not support pagination.
    """
    ehr_data = await retrieve_ehr_list(db=db, limit=50)
    
    return ehr_data


@router.get(
    "/{ehr_id}",
    response_model = EHR,
    response_model_by_alias = False,
    status_code = status.HTTP_200_OK,
    summary = "Get EHR by ID",
    responses = get_ehr_by_id_responses
)
async def get_ehr_by_id(
    ehr_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a single EHR resource based on its unique identifier (`ehr_id`).

    If the EHR is found, the full EHR document is returned, including its status,
    creation time, and lists of associated contributions and compositions.

    If no EHR with the given `ehr_id` exists, a 404 Not Found error is returned.
    """
    ehr_data = await retrieve_ehr_by_id(ehr_id= ehr_id, db= db)
    return ehr_data


@router.post(
    "/{ehr_id}/composition",
    response_model = Composition,
    response_model_by_alias = False,
    status_code = status.HTTP_201_CREATED,
    summary = "Create Composition",
    responses = create_composition_responses
)
async def create_composition_endpoint(
    ehr_id: str,
    response: Response,
    composition_create: CompositionCreate = Body(
        ...,
        description = "The composition object to be created, structured according to a template"
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new `composition` and adds it to the specified EHR.

    This endpoint creates the first version of a new `composition`.
    - A new `contribution` is created to audit this change.
    - The new `composition` is atomically stored and linked to the EHR.

    Upon successfull creation, the new `composition` object is returned, and the `Location`, `ETag`, and `Last-Modified` headers are set.
    """

    new_composition = await add_composition(
        ehr_id = ehr_id,
        composition_create = composition_create,
        db = db
    )

    # Response Headers
    response.headers["Location"] = f"/v1/ehr/{ehr_id}/composition/{new_composition.uid}"
    response.headers["ETag"] = f'"{new_composition.uid}"'
    last_modified_gmt = formatdate(new_composition.time_created.timestamp(), usegmt = True)
    response.headers["Last-Modified"] = last_modified_gmt

    return new_composition



@router.post(
    "",
    response_model = EHRCreationResponse,
    status_code = status.HTTP_201_CREATED,
    summary = "Create EHR",
    responses = create_ehr_api_responses
)
async def create_ehr_endpoint(
    response: Response,
    ehr_status: Optional[EHRStatus] = Body(
        default = None, 
        description = "An optional EHR_STATUS object. If provided, it will be used as the initial status for the new EHR."),
        examples = ehr_status_example,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)):
    """
    Creates a new Electronic Health Record (EHR).

    This endpoint supports two main use cases:
    - **With a body:** Provide an `EHR_STATUS` object to create an EHR for a specific subject.
    - **Without a body:** Send an empty request to create a subject-less EHR that can be assigned later.

    Upon successful creation, the endpoint returns the unique `ehr_id`, the final `ehr_status` object, the `system_id`, and the creation timestamp.
    """
    
    ehr_response = await create_ehr(
        db=db,
        initial_status=ehr_status
    )

    # Set the response headers using the data from the service layer

    # 1. Set the Location header
    # This points to where the client can retrieve the newly created resource.
    # Note: Ideally, this would use `request.url_for`, but that requires a named "GET" route.
    # For now, we construct it manually.
    response.headers["Location"] = f"/v1/ehr/{ehr_response.ehr_id}"

    # 2. Set the ETag header
    # The ETag is used for caching and conditional requests (e.g., If-None-Match).
    # The HTTP spec requires the value to be in double quotes.
    response.headers["ETag"] = f'"{ehr_response.ehr_status.uid}"'

    # 3. Set the Last-Modified header
    # The datetime object must be formatted into a standard HTTP-date string (RFC 7231).
    # We use a helper from the standard library to do this correctly.
    timestamp = ehr_response.time_created.timestamp()
    last_modified_gmt = formatdate(timestamp, usegmt=True)
    response.headers["Last-Modified"] = last_modified_gmt

    return ehr_response