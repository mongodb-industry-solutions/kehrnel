# src/api/v1/directory/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging

logger = logging.getLogger(__name__)

EHR_COLL_NAME = "ehr"
EHR_CONTRIBUTIONS_COLL = "contributions"

async def update_ehr_and_insert_contribution_for_directory(
    ehr_id: str,
    directory_ref: dict,
    contribution_doc: dict,
    contribution_ref: dict,
    db: AsyncIOMotorDatabase
):
    
    """Updates the EHR with the directory and contribution references, and inserts
    the contribution document in a single atomic transaction

    Args:
        ehr_id: The ID of the EHR to update
        directory_ref: The ObjectRef dictionary for the new directory
        contribution_doc: The full Contribution document to insert
        contribution_ref: The ObjectRef dictionary for the new contribution
        db: The database session
    """

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                # 1. Insert the new contribution document
                await db[EHR_CONTRIBUTIONS_COLL].insert_one(
                    contribution_doc, session=session
                )

                # 2. Update the EHR document
                result = await db[EHR_COLL_NAME].update_one(
                    {
                        "_id.value": ehr_id
                    },
                    {
                        "$set": {
                            "directory": directory_ref
                        },
                        "$push": {
                            "contributions": contribution_ref
                        }
                    },
                    session=session,
                )

                if result.matched_count == 0:
                    await session.abort_transaction()
                    raise PyMongoError(f"Failed to find EHR with id {ehr_id} during transaction.")
            
            except PyMongoError as e:
                logger.error(f"Directory creation transaction failed: {e}")
                # The transaction is automatically aborted on exception
                raise
