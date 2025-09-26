from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging

COMPOSITIONS_COLL_NAME = "compositions"
EHR_CONTRIBUTIONS_COLL = "contributions"
EHR_COLL_NAME = "ehr"

logger = logging.getLogger(__name__)

async def find_composition_by_uid(uid: str, db: AsyncIOMotorDatabase):
    return await db[COMPOSITIONS_COLL_NAME].find_one({"_id": uid})


async def find_latest_composition_by_object_id(object_id: str, db: AsyncIOMotorDatabase):
    filter_criteria = {"_id": {"$regex": f"^{object_id}::"}}
    cursor = db[COMPOSITIONS_COLL_NAME].find(filter_criteria).sort("time_created", -1).limit(1)
    documents = await cursor.to_list(length=1)
    return documents[0] if documents else None


async def find_first_composition_by_object_id(object_id: str, db: AsyncIOMotorDatabase):
    filter_criteria = {"_id": {"$regex": f"^{object_id}::"}}
    cursor = db[COMPOSITIONS_COLL_NAME].find(filter_criteria).sort("time_created", 1).limit(1)
    documents = await cursor.to_list(length=1)
    return documents[0] if documents else None


async def insert_composition_contribution_and_update_ehr(
    ehr_id: str,
    composition_doc: dict,
    contribution_doc: dict,
    db: AsyncIOMotorDatabase      
):
    """
    Inserts a new Composition and its associated Contribution, and updates the parent EHR document to link them.
    All operations are performed within a single atomic transaction
    """

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                composition_doc["ehr_id"] = ehr_id
                # Insert the new Contribution document
                await db[EHR_CONTRIBUTIONS_COLL].insert_one(contribution_doc, session = session)

                # Insert the new Composition document
                await db[COMPOSITIONS_COLL_NAME].insert_one(composition_doc, session = session)

                # Update the EHR document by pushin the new IDs to ther respective lists
                update_criteria = {
                    "$push": {
                        "contributions": {
                            "id": {"value": contribution_doc["_id"]},
                            "namespace": "local",
                            "type": "CONTRIBUTION"
                        },
                        "compositions": {
                            "id": {"value": composition_doc["_id"]},
                            "namespace": "local",
                            "type": "COMPOSITION"
                        }
                    }
                }

                update_result = await db[EHR_COLL_NAME].update_one(
                    {"_id.value": ehr_id},
                    update_criteria,
                    session = session
                )

                # Ensure the EHR document was actually found and updated
                if update_result.matched_count == 0:
                    # Cause the transaction to abort
                    raise PyMongoError(f"Failed to find and update the EHR with id '{ehr_id}' during transaction.")
            except PyMongoError as e:
                logger.error(f"Composition creating transaction failed: {e}")
                # Transaction is automatically aborted if there is an exception
                # Re-raise it for the service layer to handle
                raise


async def add_deletion_contribution_and_update_ehr(
    ehr_id: str,
    contribution_doc: dict,
    db: AsyncIOMotorDatabase
):
    """
    Atomicalloy adds a 'deleted' contribution and updates the parent EHR to link it
    This is used for the logical deletion of a composition version.
    """
    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                # Inser the new contribution document marking the deletion
                await db[EHR_CONTRIBUTIONS_COLL].insert_one(contribution_doc, session = session)

                update_set_criteria = {
                    "$push": {
                        "contributions": {
                            "id": {"value": contribution_doc["_id"]},
                            "namespace": "local",
                            "type": "CONTRIBUTION"
                        }
                    }
                }

                # Update the parent EHR document by pushin the new contribution ID
                update_result = await db[EHR_COLL_NAME].update_one(
                    {"_id.value": ehr_id}, 
                    update_set_criteria,
                    session = session
                )

                # Ensure the EHR was found and updated
                if update_result.matched_count == 0:
                    raise PyMongoError(f"Failed to find EHR with id '{ehr_id}' during deletion transaction")
            except PyMongoError as e:
                logger.error(f"Composition deletion transaction failed: {e}")