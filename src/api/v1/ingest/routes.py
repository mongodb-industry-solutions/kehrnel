# src/api/v1/ingest/routes.py

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from src.transform.flattener_g import CompositionFlattener
from src.transform.exceptions_g import FlattenerError
from src.api.v1.ingest.service import IngestionService
from src.api.v1.ingest.repository import IngestionRepository

from src.api.v1.ingest.models import (
    FilePathRequest,
    EhrIdRequest,
    CanonicalCompositionPayload,
    IngestionSuccessResponse
)
from src.api.v1.ingest.api_responses import (
    ingest_from_payload_responses,
    ingest_from_file_responses,
    ingest_from_db_responses,
    ingest_from_body_example
)

router = APIRouter()


def get_ingestion_service(request: Request) -> IngestionService:
    """Dependency to create and provide the IngestionService."""
    db: AsyncIOMotorDatabase = request.app.state.db
    config: dict = request.app.state.config
    flattener: CompositionFlattener = request.app.state.flattener
    
    repository = IngestionRepository(db=db, config=config)
    return IngestionService(flattener=flattener, repository=repository)



@router.post(
    "/body",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from Request Body",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_payload_responses
)
async def ingest_from_payload(
    payload: CanonicalCompositionPayload = Body(..., examples={ "default": ingest_from_body_example }),
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Receives a canonical OpenEHR composition document in the request body, transforms it into the
    flattened format, and stores it in the target database.

    This is a custom ingestion endpoint, not part of the standard OpenEHR REST API.
    """
    try:
        new_comp_id = await service.ingest_from_payload(payload.root)
        return IngestionSuccessResponse(
            message="Composition from payload ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal Server Error: {str(e)}")
    
@router.post(
    "/file",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from a Local File",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_file_responses
)
async def ingest_from_file(
    request_body: FilePathRequest,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Reads a canonical composition from a JSON file on the server's filesystem,
    transforms it, and stores the flattened version.
    """
    try:
        new_comp_id = await service.ingest_from_local_file(request_body.file_path)
        return IngestionSuccessResponse(
            message=f"Composition from file '{request_body.file_path}' ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal Server Error: {str(e)}")
    

@router.post(
    "/database",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from the Source Database",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_db_responses
)
async def ingest_from_db(
    request_body: EhrIdRequest,
    service: IngestionService = Depends(get_ingestion_service),
):
    """
    Finds a canonical composition in the source database collection using an `ehr_id`,
    transforms it, and stores the flattened version.
    """
    try:
        new_comp_id = await service.ingest_from_database(request_body.ehr_id)
        return IngestionSuccessResponse(
            message=f"Composition for ehr_id '{request_body.ehr_id}' ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal Server Error: {str(e)}")