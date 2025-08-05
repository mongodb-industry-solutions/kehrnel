from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging

# Create a logger instace
logger = logging.getLogger(__name__)

TEMPLATE_COLL_NAME = "templates"

async def find_template_by_id_and_format(template_id: str, template_format: str, db: AsyncIOMotorDatabase):
    """
    Retrieves a single template document fron the database by its template_id and format.
    The `_id` in the database is the template_id.
    """
    try:
        query_criteria = {
            "_id": template_id,
            "template_format": template_format
        }

        template = await db[TEMPLATE_COLL_NAME].find_one(query_criteria)
        if template:
            return template
        else:
            logger.warning(f"Template with id '{template_id}' and format '{template_format}' not found.")
            return None
    except PyMongoError as e:
        logger.error(f"Error retrieving template with id {template_id}: {e}")
        raise e
    

async def insert_template(template_doc: dict, db: AsyncIOMotorDatabase):
    """
    Inserts a new template document into the database.
    Since this is a single operation, a transaction is not strictly necessary
    but could be added if more operations were grouped with it.
    """
    
    try:        
        result = await db[TEMPLATE_COLL_NAME].insert_one(template_doc)
        logger.info(f"Template with id: {result.inserted_id} inserted successfully.")
    except PyMongoError as e:
        logger.error(f"Template insertion failed: {e}")
        # Re-raise the exception for the service layer to handle
        raise
