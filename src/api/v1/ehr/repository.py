from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging

# Create a logger instance
logger = logging.getLogger(__name__)

# TODO: Remove the COLL variables from the ehr/repository and take them to the .env or whatever the best approach is
EHR_COLL_NAME = "ehr"
EHR_CONTRIBUTIONS_COLL = "contributions"
COMPOSITIONS_COLL_NAME = "compositions"

async def find_deletion_contribution_for_version(
        preceding_version_uid: str,
        db: AsyncIOMotorDatabase
):
    """
    Checks if a 'deleted' contribution already exists for a specific version UID
    """
    return await db[EHR_CONTRIBUTIONS_COLL].find_one(
        {
            "audit.change_type": "deleted",
            "versions.preceding_version_uid": preceding_version_uid
        }
    )

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
                        "contributions": contribution_doc["_id"]
                    }
                }

                # Update the parent EHR document by pushin the new contribution ID
                update_result = await db[EHR_COLL_NAME].update_one(
                    {"_id.value": ehr_id}, # Update query to match new model
                    update_set_criteria,
                    session = session
                )

                # Ensure the EHR was found and updated
                if update_result.matched_count == 0:
                    raise PyMongoError(f"Failed to find EHR with id '{ehr_id}' during deletion transaction")
            except PyMongoError as e:
                logger.error(f"Composition deletion transaction failed: {e}")

async def find_composition_by_uid(uid: str, db: AsyncIOMotorDatabase):
    """
    Retrieves a single COMPOSITION document from the database by its versioned UID
    The `_id` in the database is the composition's versioned UID
    """
    return await db[COMPOSITIONS_COLL_NAME].find_one({"_id": uid})


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

# Transactional function to create a Composition
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
