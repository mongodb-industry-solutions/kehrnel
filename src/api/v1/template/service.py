import xml.etree.ElementTree as ET
import hashlib
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError

from src.api.v1.template.repository import find_template_by_id, insert_template
from src.api.v1.template.models import Template, TemplateFormat

# Define the namespace map
NAMESPACES = {'openEHR': 'http://schemas.openehr.org/v1'}

def get_template_id_from_opt(xml_content: str) -> str:
    """
    Parses the template_id from the raw XML content of an OPT file.
    """
    try:
        root = ET.fromstring(xml_content)
        # Use the namespace map for a cleaner find() call
        template_id_element = root.find('.//openEHR:template_id/openEHR:value', NAMESPACES)

        if template_id_element is None or not template_id_element.text:
            raise ValueError("Template ID not found in the XML content.")
        return template_id_element.text
    except ET.ParseError as e:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = "Invalid XML content provided for template parsing."
        )
    except ValueError as e:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = str(e)
        )
    

def generate_template_etag(content: str) -> str:
    """
    Generates an ETag for the template content using SHA1 hashing.
    """
    return hashlib.sha1(content.encode('utf-8')).hexdigest()


async def create_template(db: AsyncIOMotorDatabase, template_content: str) -> dict:
    """
    Handles the business logic of creating and storing a new clinical template.

    Args:
        db: The database session.
        template_content: The raw XML string of the OPERATIONAL_TEMPLATE.

    Returns:
        A dictionary containing the created template object and its ETag.
    """

    # Parse the template ID from the XML content
    template_id = get_template_id_from_opt(template_content)

    # Check if a template with this ID already exists
    if await find_template_by_id(template_id, db):
        raise HTTPException(
            status_code = status.HTTP_409_CONFLICT,
            detail = f"Template with ID {template_id} already exists."
        )
    
    # Create the template model instance
    new_template = Template(
        template_id = template_id,
        content = template_content,
    )

    # Convert to a dictionary for database insertion, respecting aliases
    template_doc = new_template.model_dump(by_alias=True)

    # Insert the template into the database
    try:
        await insert_template(template_doc, db)
    except PyMongoError as e:
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not create template due to a database error: {e}"
        )
    
    # Generate the ETag for the response header
    etag = generate_template_etag(new_template.content)

    # Return the created template and its ETag
    return {
        "template": new_template,
        "etag": etag
    }

async def retrieve_template_by_id(db: AsyncIOMotorDatabase, template_id: str) -> dict:
    """
    Handles the business logic of retrieving a clinical template by its ID.

    Args:
        db: The database session.
        template_id: The unique ID of the template to retrieve

    Returns:
        A dictionary containing the retrieved template object and its ETag

    Raises:
        HTTPException: If the template is not found (404)
    """

    # Fetch the template document from the repository
    template_doc = await find_template_by_id(template_id, db)

    # Handle the "not found" case
    if not template_doc:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = f"Template with ID '{template_id}' not found"
        )
    
    # Validate the database data against the Pydantic model
    template_model = Template.model_validate(template_doc)
    
    # Generate the ETag from the template's content
    etag = generate_template_etag(template_model.content)

    # Return the model and ETag for the route to use
    return {
        "template": template_model,
        "etag": etag
    }