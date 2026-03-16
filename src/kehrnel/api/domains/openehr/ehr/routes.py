# ehr/routes.py

from fastapi import APIRouter, Depends, status, Body, Query, HTTPException, Response, Header
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, List, Union, Dict, Any
from pydantic import ValidationError

from kehrnel.api.domains.openehr.ehr.service import (
    create_ehr, retrieve_ehr_by_id, 
    retrieve_ehr_list,
    create_ehr_with_id,
    retrieve_ehr_by_subject
)

from kehrnel.api.domains.openehr.ehr_status.models import EHRStatusCreate
from kehrnel.api.domains.openehr.ehr.models import EHRCreationResponse, EHR

from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db
from kehrnel.api.domains.openehr.ehr.api_responses import (
    get_ehr_by_id_responses,
    create_ehr_api_responses,
    ehr_status_example, 
    get_ehr_list_responses,
    get_ehr_by_subject_responses
)

router = APIRouter(
    prefix="/ehr",
    tags=["EHR"]
)

@router.put(
    "/{ehr_id}",
    response_model=EHRCreationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create EHR with specified ID",
    responses=create_ehr_api_responses
)
async def create_ehr_with_id_endpoint(
    ehr_id: str,
    response: Response,
    prefer: str = Header("return=minimal", alias="Prefer"),
    payload: Optional[Dict[str, Any]] = Body(default=None),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new EHR with a client-specified `ehr_id`.
    An optional `EHR_STATUS` can be provided in the body.
    - `Prefer: return=representation` returns the full resource.
    - `Prefer: return=minimal` returns an empty body with headers.
    """
    initial_status: Optional[EHRStatusCreate] = None
    if payload and payload != {}:
        try:
            initial_status = EHRStatusCreate.model_validate(payload)
        except ValidationError as e:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=e.errors())

    ehr_response = await create_ehr_with_id(
        ehr_id=ehr_id, db=db, initial_status=initial_status
    )

    response.headers["Location"] = f"/v1/ehr/{ehr_response.ehr_id.value}"
    response.headers["ETag"] = f'"{ehr_response.ehr_id.value}"'

    if prefer == "return=representation":
        return ehr_response
    else:
        return Response(status_code=status.HTTP_201_CREATED, headers=response.headers)


@router.get(
    "",
    response_model=Union[EHR, List[EHR]],
    response_model_by_alias=True,
    summary="Get EHR by subject ID or list EHRs",
    responses={**get_ehr_by_subject_responses, **get_ehr_list_responses}
)
async def get_ehr_by_subject_or_list(
    subject_id: Optional[str] = Query(None, description="The EHR subject id."),
    subject_namespace: Optional[str] = Query(None, description="The EHR subject id namespace."),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves EHR resources. This endpoint has dual functionality:

    - **Find by Subject:** If `subject_id` and `subject_namespace` query parameters
      are provided, it returns the single EHR matching that subject.
    
    - **List EHRs:** If no query parameters are provided, it retrieves a list
      of the 50 most recently created EHRs.
    """

    # If both query parameters are present, find the specific EHR
    if subject_id and subject_namespace:
        ehr_data = await retrieve_ehr_by_subject(
            subject_id=subject_id,
            subject_namespace=subject_namespace,
            db=db
        )
        return ehr_data
    
    # Otherwise, fall back to the original list functionality
    ehr_list_data = await retrieve_ehr_list(db=db, limit=50)
    return ehr_list_data


@router.get(
    "/{ehr_id}",
    response_model=EHR,
    status_code=status.HTTP_200_OK,
    summary="Get EHR by ID",
    responses=get_ehr_by_id_responses
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
    ehr_data = await retrieve_ehr_by_id(ehr_id=ehr_id, db=db)
    return ehr_data


@router.post(
    "",
    response_model=EHRCreationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create EHR",
    responses=create_ehr_api_responses
)
async def create_ehr_endpoint(
    response: Response,
    prefer: str = Header("return=minimal", alias="Prefer"),
    payload: Optional[Dict[str, Any]] = Body(
        default=None, 
        description="Optional initial EHR_STATUS.",
        examples=ehr_status_example,
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Creates a new EHR with a server-generated `ehr_id`.
    - `Prefer: return=representation` returns the full resource representation.
    - `Prefer: return=minimal` (default) returns an empty body with headers.
    """
    initial_status: Optional[EHRStatusCreate] = None

    if payload and payload != {}:
        try:
            initial_status = EHRStatusCreate.model_validate(payload)
        except ValidationError as e:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=e.errors())

    ehr_response = await create_ehr(
        db=db,
        initial_status=initial_status
    )

    # Set the response headers using the data from the service layer

    # 1. Set the Location header
    # This points to where the client can retrieve the newly created resource.
    # Note: Ideally, this would use `request.url_for`, but that requires a named "GET" route.
    # For now, we construct it manually.
    response.headers["Location"] = f"/v1/ehr/{ehr_response.ehr_id.value}"

    # 2. Set the ETag header
    # The ETag is used for caching and conditional requests (e.g., If-None-Match).
    # The HTTP spec requires the value to be in double quotes.
    response.headers["ETag"] = f'"{ehr_response.ehr_id.value}"'

    if prefer == "return=representation":
        return ehr_response
    else:
        return Response(status_code=status.HTTP_201_CREATED, headers=response.headers)