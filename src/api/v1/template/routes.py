from fastapi import APIRouter, Depends, status, Body, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from email.utils import formatdate

from src.app.core.database import get_mongodb_ehr_db
from src.api.v1.template.service import create_template
from src.api.v1.template.api_responses import create_template_responses

router = APIRouter(
    prefix = "/template",
    tags = ["Template"]
)

@router.post(
    "/",
    status_code = status.HTTP_201_CREATED,
    summary = "Upload a new clinical template",
    description = "Upload a new OPERATIONAL_TEMPLATE (OPT). The request body must be the raw XML content of the .opt file.",
    responses = create_template_responses,
    response_class= Response
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

    response.headers["Location"] = f"/v1/template/{new_template.template_id}"
    response.headers["Etag"] = f'"{etag}"'

    last_modified_gmt = formatdate(new_template.created_timestamp.timestamp(), usegmt=True)
    response.headers["Last-Modified"] = last_modified_gmt

    return response