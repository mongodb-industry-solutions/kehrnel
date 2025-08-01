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