from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Optional

from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames

logger = logging.getLogger(__name__)

async def find_composition_by_uid(uid: str, db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    """
    Retrieves a single COMPOSITION document from the database by its versioned UID
    The `_id` in the database is the composition's versioned UID
    """
    result = await db[config.compositions].find_one({"_id": uid})
    return result


async def find_flattened_composition_by_uid(uid: str, db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    """
    Retrieves a single flattened COMPOSITION document from the flatten compositions
    collection by its versioned UID.
    """
    return await db[config.flatten_compositions].find_one({"_id": uid})


async def find_latest_composition_by_object_id(object_id: str, db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    """
    Finds the latest version of a composition by its base object ID.

    It queries for all versions matching the base object ID and sorts them by creation time to return the most recent one.

    Args:
        object_id: The base ID of the composition (without the ::version part).
        db: The database session.
        config: Configuration containing collection names.

    Returns:
        The latest composition document, or None if not found.
    """

    # Regex to find all versions of a given composition object
    filter_criteria = {
        "_id": {
            "$regex": f"^{object_id}::"
        }
    }

    # Find all matching documents, sort by time_created descending and get the first one
    cursor = db[config.compositions].find(filter_criteria).sort("time_created", -1).limit(1)
    documents = await cursor.to_list(length=1)

    if documents:
        return documents[0]
    return None


async def find_first_composition_by_object_id(object_id: str, db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    """
    Finds the first version of a composition by its base object ID

    It queries for all versiones matching the base object ID and sorts them by creation time 
    in ascending order to return the very first one

    Args:
        object_id: The base ID of the composition (without the ::version part).
        db: The database session
        config: Configuration containing collection names.

    Returns:
        The first composition document, or None if not found
    """

    # Regex to find all versions of a given composition object
    filter_criteria = {
        "_id": {
            "$regex": f"^{object_id}::"
        }
    }

    # Find all matching documents, sort by time_created ascending (1), and get the first one
    cursor = db[config.compositions].find(filter_criteria).sort("time_created", 1).limit(1)
    documents = await cursor.to_list(length=1)

    if documents:
        return documents[0]
    return None


async def insert_composition_contribution_and_update_ehr(
    ehr_id: str,
    composition_doc: dict,
    contribution_doc: dict,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    flattened_base_doc: Optional[Dict] = None,
    flattened_search_doc: Optional[Dict] = None,
    merge_search_docs: bool = False
):
    """
    Inserts a new Composition, its Contribution, its flattened versions,
    and updates the parent EHR, all within a single atomic transaction.
    """

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                composition_doc["ehr_id"] = ehr_id
                composition_uid = composition_doc["_id"]

                # Insert the new Contribution document
                await db[config.contributions].insert_one(contribution_doc, session = session)

                # Insert the new Composition document
                await db[config.compositions].insert_one(composition_doc, session = session)

                # Insert the flattened versions if they exist
                if flattened_base_doc and flattened_search_doc:
                    # Use the canonical version UID as the _id for flattened docs for consistency
                    flattened_base_doc["_id"] = composition_uid
                    result_flatten_composition = await db[config.flatten_compositions].insert_one(flattened_base_doc, session=session)
                    logger.debug("Inserted flattened composition _id=%s", result_flatten_composition.inserted_id)

                    # Use dynamic field name for search nodes
                    search_field = config.search_fields.nodes
                    has_search_data = flattened_search_doc and flattened_search_doc.get(search_field)
                    
                    # Debug logging
                    logger.info(f"Search field name: {search_field}")
                    logger.info(f"Search doc keys: {list(flattened_search_doc.keys()) if flattened_search_doc else 'None'}")
                    logger.info(f"Has search data: {has_search_data}")

                    if has_search_data:
                        if not merge_search_docs:
                            flattened_search_doc["_id"] = composition_uid
                            inserted_search_document = await db[config.search_compositions].insert_one(flattened_search_doc, session=session)
                            logger.debug("Inserted flattened search document _id=%s", inserted_search_document.inserted_id)
                        else:
                            search_sub_doc = {
                                "comp_id": composition_uid,
                                "tid": flattened_search_doc.get(config.search_fields.template_id),
                                config.search_fields.nodes: flattened_search_doc.get(search_field, [])
                            }
                            
                            filter_query = {"_id": ehr_id}
                            update_operation = {
                                "$push": {"comps": search_sub_doc},
                                "$setOnInsert": {"ehr_id": ehr_id}
                            }

                            await db[config.search_compositions].update_one(
                                filter_query,
                                update_operation,
                                upsert=True,
                                session=session
                            )

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

                update_result = await db[config.ehr].update_one(
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
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames
):
    """
    Atomically adds a 'deleted' contribution and updates the parent EHR to link it
    This is used for the logical deletion of a composition version.
    """
    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                # Insert the new contribution document marking the deletion
                await db[config.contributions].insert_one(contribution_doc, session = session)

                update_set_criteria = {
                    "$push": {
                        "contributions": {
                            "id": {"value": contribution_doc["_id"]},
                            "namespace": "local",
                            "type": "CONTRIBUTION"
                        }
                    }
                }

                # Update the parent EHR document by pushing the new contribution ID
                update_result = await db[config.ehr].update_one(
                    {"_id.value": ehr_id}, 
                    update_set_criteria,
                    session = session
                )

                # Ensure the EHR was found and updated
                if update_result.matched_count == 0:
                    raise PyMongoError(f"Failed to find EHR with id '{ehr_id}' during deletion transaction")
            except PyMongoError as e:
                logger.error(f"Composition deletion transaction failed: {e}")
