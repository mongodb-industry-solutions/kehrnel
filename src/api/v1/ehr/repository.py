# repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Optional
from datetime import datetime

# Create a logger instance
logger = logging.getLogger(__name__)

# TODO: Remove the COLL variables from the ehr/repository and take them to the .env or whatever the best approach is
EHR_COLL_NAME = "ehr"
EHR_CONTRIBUTIONS_COLL = "contributions"
COMPOSITIONS_COLL_NAME = "compositions"


async def find_ehr_by_subject(subject_id: str, subject_namespace: str, db: AsyncIOMotorDatabase):
    """
    Finds an EHR by its subject's external reference ID and namespace.
    The query path is updated to match the new nested structure.
    """
    return await db[EHR_COLL_NAME].find_one(
        {
            "ehr_status.subject.external_ref.id.value": subject_id, 
            "ehr_status.subject.external_ref.namespace": subject_namespace
        }
    )


async def insert_ehr_and_contribution_in_transaction(ehr_doc: dict, contribution_doc: dict, db: AsyncIOMotorDatabase):
    """
    Inserts the EHR and its initial Contribution document within a single atomic transaction.
    """
    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                await db[EHR_COLL_NAME].insert_one(ehr_doc, session=session)
                await db[EHR_CONTRIBUTIONS_COLL].insert_one(contribution_doc, session=session)
            except PyMongoError as e:
                logger.error(f"Database transaction failed: {e}")
                # The transaction will be automatically aborted by the context manager
                # We re-raise the exception so the service layer can handle it on the try except block
                raise


async def find_ehr_by_id(ehr_id: str, db: AsyncIOMotorDatabase):
    """
    Retrieves a single EHR document from the database by its ehr_id.
    """
    find_ehr_result = await db[EHR_COLL_NAME].find_one({"_id.value": ehr_id})
    return find_ehr_result


async def find_newest_ehrs(db: AsyncIOMotorDatabase, limit: int = 50):
    """
    Retrieves a list of the most recently created EHR documents from the database.
    """
    
    # The query finds all documents ({}), sorts them by time_created in
    # descending order (-1), and limits the result set.

    cursor_ehr_result = db[EHR_COLL_NAME].find().sort("time_created.value", -1).limit(limit)
    if cursor_ehr_result is None:
        logger.warning("No EHRs found in the database.")
        return []
    
    return await cursor_ehr_result.to_list(length=limit)


async def update_ehr_status_in_transaction(ehr_id: str, new_status_doc: dict, contribution_doc: dict, db: AsyncIOMotorDatabase):
    """
    Atomically updates the EHR document's status and pushes the new contribution ID.
    """
    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                ehr_update_criteria = {
                    "_id.value": ehr_id
                }

                ehr_update_doc = {
                    "$set": {
                        "ehr_status": new_status_doc
                    },
                    "$push": {
                        "contributions": { # Push an ObjectRef
                            "id": {"value": contribution_doc["_id"]},
                            "namespace": "local",
                            "type": "CONTRIBUTION"
                        }
                    }
                }

                # 1. Insert the new contribution document
                await db[EHR_CONTRIBUTIONS_COLL].insert_one(contribution_doc, session=session)

                # 2. Update the main EHR document
                await db[EHR_COLL_NAME].update_one(
                    ehr_update_criteria,
                    ehr_update_doc,
                    session=session
                )
            except PyMongoError as e:
                logger.error(f"EHR status update transaction failed: {e}")
                raise