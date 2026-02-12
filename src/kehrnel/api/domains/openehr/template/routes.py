from fastapi import APIRouter, Depends, status, Body, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from email.utils import formatdate
from typing import List

from kehrnel.api.legacy.app.core.database import get_mongodb_ehr_db
from kehrnel.api.domains.openehr.template.service import create_template, retrieve_template_by_id_and_format, list_templates_by_format
from kehrnel.api.domains.openehr.template.api_responses import create_template_responses, get_template_responses, list_templates_responses
from kehrnel.api.domains.openehr.template.models import TemplateFormat, TemplateSummary

router = APIRouter(
    prefix = "/definition/template",
    tags = ["Definition - Template"]
)

@router.get(
    "/{template_format}",
    summary = "List templates by format",
    description = "Lists all available templates for a given format (e.g, adl1.4). Returns a list of template summaries, excluding the full XML content.",
    response_model = List[TemplateSummary],
    responses = list_templates_responses
)
async def list_all_templates_by_format(
    template_format: TemplateFormat,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrieves a list of summaries for all templates of a specific format.
    """
    return await list_templates_by_format(db=db, template_format = template_format)

    

@router.get(
    "/{template_format}/{template_id}",
    summary = "Get template by ID and format",
    description = "Retrieve a specific OPT template by its unique `template_id`. The response body will be the raw XML content of the template",
    responses = get_template_responses,
    response_class = Response
)
async def get_template_by_id(
    template_id: str,
    template_format: TemplateFormat,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Retrievesa template by its `template_id` and `template_format`.
    The enpoint returns the raw XML content of the template with the `Content-Type`
    header set to `application/xml`. It also includes `ETag` and `Last-Modified`
    headers for caching purposes
    """
    result = await retrieve_template_by_id_and_format(
        db = db, 
        template_format = template_format, 
        template_id = template_id
    )
    
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
    "/{template_format}",
    summary = "Upload a new clinical template",
    description = "Upload a new OPERATIONAL_TEMPLATE (OPT). The `template_format` must be specified in the URL (e.g., 'adl1.4' or 'adl2'). The request body must be the raw XML content of the .opt file.",
    responses = create_template_responses,
    status_code = status.HTTP_201_CREATED
)
async def upload_template(
    template_format: TemplateFormat,
    response: Response,
    template_content: str = Body(..., media_type="application/xml", description="The raw XML content of the OPERATIONAL_TEMPLATE (.opt file)"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Uploads a new clinical template (OPERATIONAL_TEMPLATE). The `template_id` is
    extracted from the XML content. The system checks for conflicts based on this ID.
    The template format (e.g., 'adl1.4') must be provided in the URL.
    """
    result = await create_template(
        db = db, 
        template_content = template_content,
        template_format = template_format
    )

    new_template = result["template"]
    etag = result["etag"]

    location_path = f"/v1/definition/template/{new_template.template_format.value}/{new_template.template_id}"

    headers = {
        "Location": location_path,
        "ETag": f'"{etag}"',
        "Last-Modified": formatdate(new_template.created_timestamp.timestamp(), usegmt=True),
    }

    return Response(status_code=status.HTTP_201_CREATED, headers=headers)