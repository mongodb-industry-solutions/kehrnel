from fastapi import APIRouter, Depends, status, Body, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from email.utils import formatdate

from src.app.core.database import get_mongodb_ehr_db
from src.api.v1.template.service import create_template, retrieve_template_by_id
from src.api.v1.template.api_responses import create_template_responses, get_template_responses

router = APIRouter(
    prefix = "/template",
    tags = ["Template"]
)

@router.get(
    "/{template_id}",
    summary = "Get template by ID",
    description = "Retrieve a specific OPT template by its unique `template_id`. The response body will be the raw XML content of the template",
    responses = get_template_responses,
    response_class = Response
)
async def get_template_by_id(
    template_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrievesa template by its `template_id`.
    The enpoint returns the raw XML content of the template with the `Content-Type`
    header set to `application/xml`. It also includes `ETag` and `Last-Modified`
    headers for caching purposes
    """
    result = await retrieve_template_by_id(db = db, template_id = template_id)
    
    template = result["template"]
    etag = result["etag"]

    headers = {
        "ETag": f"{etag}",
        "Last-Modified": formatdate(template.created_timestamp.timestamp(), usegmt = True)
    }

    # Return a custom response object with the XML content, status code, headers, and media type
    return Response(
        content = template.content,
        status_code = status.HTTP_200_OK,
        headers = headers,
        media_type = "application/xml"
    )

@router.post(
    "",
    summary = "Upload a new clinical template",
    description = "Upload a new OPERATIONAL_TEMPLATE (OPT). The request body must be the raw XML content of the .opt file.",
)
async def upload_template(
    response: Response,
    template_content: str = Body(..., media_type="application/xml", description="The raw XML content of the OPERATIONAL_TEMPLATE (.opt file)"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Uploads a new clinical template (OPERATIONAL_TEMPLATE). The `template_id` is
    extracted from the XML content. The system checks for conflicts based on this ID.
    """
    result = await create_template(db = db, template_content = template_content)

    new_template = result["template"]
    etag = result["etag"]

    headers = {
        "Location": f"/v1/template/{new_template.template_id}",
        "ETag": f'"{etag}"',
        "Last-Modified": formatdate(new_template.created_timestamp.timestamp(), usegmt=True),
    }

    return Response(status_code=status.HTTP_201_CREATED, headers=headers)