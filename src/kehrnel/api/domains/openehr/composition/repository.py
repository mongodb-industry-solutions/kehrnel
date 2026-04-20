from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from copy import deepcopy
from typing import Dict, Optional, List

from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames

logger = logging.getLogger(__name__)

async def find_composition_by_uid(uid: str, db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    """
    Retrieves a single COMPOSITION document from the database by its versioned UID
    The `_id` in the database is the composition's versioned UID
    """
    result = await db[config.compositions].find_one({"_id": uid})
    return result


async def find_compositions_by_uids(uids: List[str], db: AsyncIOMotorDatabase, config: CompositionCollectionNames):
    if not uids:
        return []
    cursor = db[config.compositions].find({"_id": {"$in": list(uids)}})
    return await cursor.to_list(length=len(uids))


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

                # Insert the flattened base document whenever it exists. Search projections are optional,
                # but the base flattened collection is required for EHR-scoped retrieval and AQL queries.
                if flattened_base_doc:
                    flattened_base_doc["_id"] = composition_uid
                    result_flatten_composition = await db[config.flatten_compositions].insert_one(
                        flattened_base_doc,
                        session=session,
                    )
                    logger.debug("Inserted flattened composition _id=%s", result_flatten_composition.inserted_id)

                if flattened_search_doc:
                    search_field = config.search_fields.nodes
                    has_search_data = bool(flattened_search_doc.get(search_field))

                    logger.info(f"Search field name: {search_field}")
                    logger.info(
                        f"Search doc keys: {list(flattened_search_doc.keys()) if flattened_search_doc else 'None'}"
                    )
                    logger.info(f"Has search data: {has_search_data}")

                    if has_search_data:
                        if not merge_search_docs:
                            flattened_search_doc["_id"] = composition_uid
                            inserted_search_document = await db[config.search_compositions].insert_one(
                                flattened_search_doc,
                                session=session,
                            )
                            logger.debug(
                                "Inserted flattened search document _id=%s",
                                inserted_search_document.inserted_id,
                            )
                        else:
                            search_sub_doc = {
                                "comp_id": composition_uid,
                                "tid": flattened_search_doc.get(config.search_fields.template_id),
                                config.search_fields.nodes: flattened_search_doc.get(search_field, []),
                            }

                            filter_query = {"_id": ehr_id}
                            update_operation = {
                                "$push": {"comps": search_sub_doc},
                                "$setOnInsert": {"ehr_id": ehr_id},
                            }

                            await db[config.search_compositions].update_one(
                                filter_query,
                                update_operation,
                                upsert=True,
                                session=session,
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


async def insert_compositions_contributions_and_update_ehr(
    ehr_id: str,
    composition_docs: List[dict],
    contribution_docs: List[dict],
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    flattened_base_docs: Optional[List[Dict]] = None,
    flattened_search_docs: Optional[List[Dict]] = None,
    merge_search_docs: bool = False
):
    """
    Inserts multiple compositions and their contributions in one transaction,
    then updates the parent EHR once.
    """
    comp_docs = [deepcopy(doc) for doc in (composition_docs or []) if isinstance(doc, dict) and doc.get("_id")]
    contrib_docs = [deepcopy(doc) for doc in (contribution_docs or []) if isinstance(doc, dict) and doc.get("_id")]
    base_docs = [deepcopy(doc) for doc in (flattened_base_docs or []) if isinstance(doc, dict)]
    search_docs = [deepcopy(doc) for doc in (flattened_search_docs or []) if isinstance(doc, dict)]

    if not comp_docs or not contrib_docs:
        return

    for doc in comp_docs:
        doc["ehr_id"] = ehr_id

    composition_uids = [doc["_id"] for doc in comp_docs]

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                await db[config.contributions].insert_many(contrib_docs, ordered=True, session=session)
                await db[config.compositions].insert_many(comp_docs, ordered=True, session=session)

                if base_docs:
                    docs_to_insert = []
                    for idx, doc in enumerate(base_docs):
                        composition_uid = composition_uids[idx] if idx < len(composition_uids) else None
                        if not composition_uid:
                            continue
                        doc["_id"] = composition_uid
                        docs_to_insert.append(doc)
                    if docs_to_insert:
                        await db[config.flatten_compositions].insert_many(docs_to_insert, ordered=True, session=session)

                if search_docs:
                    search_field = config.search_fields.nodes
                    if not merge_search_docs:
                        docs_to_insert = []
                        for idx, doc in enumerate(search_docs):
                            composition_uid = composition_uids[idx] if idx < len(composition_uids) else None
                            if not composition_uid:
                                continue
                            has_search_data = bool(doc.get(search_field))
                            if not has_search_data:
                                continue
                            doc["_id"] = composition_uid
                            docs_to_insert.append(doc)
                        if docs_to_insert:
                            await db[config.search_compositions].insert_many(docs_to_insert, ordered=True, session=session)
                    else:
                        search_sub_docs = []
                        for idx, doc in enumerate(search_docs):
                            composition_uid = composition_uids[idx] if idx < len(composition_uids) else None
                            if not composition_uid:
                                continue
                            has_search_data = bool(doc.get(search_field))
                            if not has_search_data:
                                continue
                            search_sub_docs.append({
                                "comp_id": composition_uid,
                                "tid": doc.get(config.search_fields.template_id),
                                config.search_fields.nodes: doc.get(search_field, []),
                            })
                        if search_sub_docs:
                            await db[config.search_compositions].update_one(
                                {"_id": ehr_id},
                                {
                                    "$push": {"comps": {"$each": search_sub_docs}},
                                    "$setOnInsert": {"ehr_id": ehr_id},
                                },
                                upsert=True,
                                session=session,
                            )

                update_criteria = {
                    "$push": {
                        "contributions": {
                            "$each": [
                                {
                                    "id": {"value": doc["_id"]},
                                    "namespace": "local",
                                    "type": "CONTRIBUTION"
                                }
                                for doc in contrib_docs
                            ]
                        },
                        "compositions": {
                            "$each": [
                                {
                                    "id": {"value": doc["_id"]},
                                    "namespace": "local",
                                    "type": "COMPOSITION"
                                }
                                for doc in comp_docs
                            ]
                        }
                    }
                }

                update_result = await db[config.ehr].update_one(
                    {"_id.value": ehr_id},
                    update_criteria,
                    session=session
                )

                if update_result.matched_count == 0:
                    raise PyMongoError(f"Failed to find and update the EHR with id '{ehr_id}' during batch create transaction.")
            except PyMongoError as e:
                logger.error(f"Batch composition creating transaction failed: {e}")
                raise


async def add_deletion_contribution_and_update_ehr(
    ehr_id: str,
    preceding_version_uid: str,
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
                    },
                    "$pull": {
                        "compositions": {
                            "id.value": preceding_version_uid
                        }
                    }
                }

                await db[config.flatten_compositions].delete_one(
                    {"_id": preceding_version_uid},
                    session=session,
                )

                if config.search_compositions:
                    if getattr(config, "merge_search_docs", False):
                        await db[config.search_compositions].update_one(
                            {"_id": ehr_id},
                            {"$pull": {"comps": {"comp_id": preceding_version_uid}}},
                            session=session,
                        )
                    else:
                        await db[config.search_compositions].delete_one(
                            {"_id": preceding_version_uid},
                            session=session,
                        )

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
                raise


async def add_bulk_deletion_contributions_and_update_ehr(
    ehr_id: str,
    preceding_version_uids: List[str],
    contribution_docs: List[dict],
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames
):
    """
    Atomically records multiple logical composition deletions and updates the
    parent EHR once.
    """
    version_uids = [uid for uid in preceding_version_uids if isinstance(uid, str) and uid]
    docs = [doc for doc in contribution_docs if isinstance(doc, dict) and doc.get("_id")]

    if not version_uids or not docs:
        return

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                await db[config.contributions].insert_many(docs, ordered=True, session=session)

                update_set_criteria = {
                    "$push": {
                        "contributions": {
                            "$each": [
                                {
                                    "id": {"value": doc["_id"]},
                                    "namespace": "local",
                                    "type": "CONTRIBUTION"
                                }
                                for doc in docs
                            ]
                        }
                    },
                    "$pull": {
                        "compositions": {
                            "id.value": {"$in": version_uids}
                        }
                    }
                }

                await db[config.flatten_compositions].delete_many(
                    {"_id": {"$in": version_uids}},
                    session=session,
                )

                if config.search_compositions:
                    if getattr(config, "merge_search_docs", False):
                        await db[config.search_compositions].update_one(
                            {"_id": ehr_id},
                            {"$pull": {"comps": {"comp_id": {"$in": version_uids}}}},
                            session=session,
                        )
                    else:
                        await db[config.search_compositions].delete_many(
                            {"_id": {"$in": version_uids}},
                            session=session,
                        )

                update_result = await db[config.ehr].update_one(
                    {"_id.value": ehr_id},
                    update_set_criteria,
                    session=session
                )

                if update_result.matched_count == 0:
                    raise PyMongoError(f"Failed to find EHR with id '{ehr_id}' during bulk deletion transaction")
            except PyMongoError as e:
                logger.error(f"Bulk composition deletion transaction failed: {e}")
                raise
