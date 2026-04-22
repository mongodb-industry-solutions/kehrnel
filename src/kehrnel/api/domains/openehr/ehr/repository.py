# repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Optional, Sequence
from datetime import datetime
from kehrnel.api.bridge.app.core.config import settings

# Create a logger instance
logger = logging.getLogger(__name__)

def _ehr_coll() -> str:
    return settings.EHR_COLL_NAME


def _contrib_coll() -> str:
    return settings.EHR_CONTRIBUTIONS_COLL


def _composition_coll() -> str:
    return settings.COMPOSITIONS_COLL_NAME


def _flatten_coll() -> str:
    return settings.FLAT_COMPOSITIONS_COLL_NAME


def _search_coll() -> str:
    return settings.SEARCH_COMPOSITIONS_COLL_NAME


async def find_ehr_by_subject(subject_id: str, subject_namespace: str, db: AsyncIOMotorDatabase):
    """
    Finds an EHR by its subject's external reference ID and namespace.
    The query path is updated to match the new nested structure.
    """
    return await db[_ehr_coll()].find_one(
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
                await db[_ehr_coll()].insert_one(ehr_doc, session=session)
                await db[_contrib_coll()].insert_one(contribution_doc, session=session)
            except PyMongoError as e:
                logger.error(f"Database transaction failed: {e}")
                # The transaction will be automatically aborted by the context manager
                # We re-raise the exception so the service layer can handle it on the try except block
                raise


async def find_ehr_by_id(ehr_id: str, db: AsyncIOMotorDatabase):
    """
    Retrieves a single EHR document from the database by its ehr_id.
    """
    find_ehr_result = await db[_ehr_coll()].find_one({"_id.value": ehr_id})
    return find_ehr_result


async def find_newest_ehrs(db: AsyncIOMotorDatabase, limit: int = 50):
    """
    Retrieves a list of the most recently created EHR documents from the database.
    """
    
    # The query finds all documents ({}), sorts them by time_created in
    # descending order (-1), and limits the result set.

    cursor_ehr_result = db[_ehr_coll()].find().sort("time_created.value", -1).limit(limit)
    if cursor_ehr_result is None:
        logger.warning("No EHRs found in the database.")
        return []
    
    return await cursor_ehr_result.to_list(length=limit)


async def delete_ehr_and_related_documents(
    ehr_id: str,
    composition_ids: Sequence[str],
    contribution_ids: Sequence[str],
    db: AsyncIOMotorDatabase,
):
    """
    Deletes an EHR and its related sandbox data in one transaction.

    This performs a hard delete for local sandbox workflows:
    - the EHR document
    - canonical compositions
    - flattened/search composition records
    - contributions (including EHR_STATUS/directory history)
    """
    comp_ids = [value for value in composition_ids if isinstance(value, str) and value]
    contrib_ids = [value for value in contribution_ids if isinstance(value, str) and value]

    async with await db.client.start_session() as session:
        async with session.start_transaction():
            try:
                composition_filters = [{"ehr_id": ehr_id}]
                if comp_ids:
                    composition_filters.append({"_id": {"$in": comp_ids}})

                await db[_composition_coll()].delete_many(
                    {"$or": composition_filters},
                    session=session,
                )
                await db[_flatten_coll()].delete_many(
                    {"$or": composition_filters},
                    session=session,
                )

                search_filters = [{"ehr_id": ehr_id}, {"_id": ehr_id}]
                if comp_ids:
                    search_filters.extend(
                        [
                            {"_id": {"$in": comp_ids}},
                            {"comp_id": {"$in": comp_ids}},
                            {"comps.comp_id": {"$in": comp_ids}},
                        ]
                    )

                await db[_search_coll()].delete_many(
                    {"$or": search_filters},
                    session=session,
                )

                contribution_filters = [{"ehr_id": ehr_id}]
                if contrib_ids:
                    contribution_filters.append({"_id": {"$in": contrib_ids}})

                await db[_contrib_coll()].delete_many(
                    {"$or": contribution_filters},
                    session=session,
                )

                delete_result = await db[_ehr_coll()].delete_one(
                    {"_id.value": ehr_id},
                    session=session,
                )

                if delete_result.deleted_count == 0:
                    raise PyMongoError(f"Failed to delete EHR with id '{ehr_id}' during transaction.")
            except PyMongoError as e:
                logger.error(f"EHR deletion transaction failed: {e}")
                raise
