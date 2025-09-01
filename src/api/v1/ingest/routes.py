# src/api/v1/ingest/routes.py

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from src.transform.flattener_g import CompositionFlattener
from src.transform.exceptions_g import FlattenerError
from .service import IngestionService

router = APIRouter()


# --- Dependencies to get shared objects from app.state ---
def get_flattener(request: Request) -> CompositionFlattener:
    return request.app.state.flattener

def get_db(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db

def get_config(request: Request) -> dict:
    return request.app.state.config


@router.post(
    "/composition",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition",
)
async def ingest_composition(
    raw_composition_doc: dict = Body(...),
    flattener: CompositionFlattener = Depends(get_flattener),
    db: AsyncIOMotorDatabase = Depends(get_db),
    config: dict = Depends(get_config),
):
    """
    Receives a canonical OpenEHR composition, transforms it into the
    flattened format, and stores it in the target database.

    This is a custom ingestion endpoint, not part of the standard OpenEHR REST API.
    """
    try:
        if not all(k in raw_composition_doc for k in ["_id", "ehr_id", "canonicalJSON"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Payload must contain '_id', 'ehr_id', and 'canonicalJSON'.",
            )

        new_comp_id = await IngestionService.process_and_store_composition(
            raw_composition_doc=raw_composition_doc,
            flattener=flattener,
            db=db,
            config=config,
        )

        return {
            "status": "success",
            "message": "Composition ingested and stored in flattened format.",
            "flattened_composition_id": new_comp_id,
        }

    except FlattenerError as e:
        raise HTTPException(status_code=422, detail=f"Transformation Error: {e}")
    except Exception as e:
        # In a real app, you would log the full exception traceback here
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")