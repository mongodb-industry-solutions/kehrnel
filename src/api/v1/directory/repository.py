# src/api/v1/directory/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from datetime import datetime
from typing import Optional, Dict, Any

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


async def find_folder_in_contribution_by_uid(version_uid: str, db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    """
    Finds the contribution containing a specific FOLDER version and returns the FOLDER.

    Args: 
        version_uid: The full version UID of the FOLDER
        db: The database session

    Returns:
        The dictionary of the FOLDER object if found, otherwise None
    """
    contribution = await db[EHR_CONTRIBUTIONS_COLL].find_one(
        {"versions.uid.value": version_uid}
    )

    if not contribution:
        return None
    
    # Iterate through the versions in the contribution to find the matching FOLDER
    for version in contribution.get("versions", []):
        if version.get("_type") == "FOLDER" and version.get("uid", {}).get("value") == version_uid:
            return version
        
    return None


async def find_folder_in_contribution_at_time(ehr_id: str, timestamp: datetime, db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    """
    Finds the latest FOLDER version for an EHR at or before a specific time

    Args:
        ehr_id: The ID of the EHR
        timestamp: The time to find the version at.
        db: The database session.

    Returns:
        The dictionary of the FOLDER object if found, otherwise None
    """
    pipeline = [
        # 1. Find relevant contributions
        {
            "$match": {
                "ehr_id": ehr_id,
                "audit.time_committed": {
                    "$lte": timestamp
                },
                "version._type": "FOLDER"
            }
        },
        # 2. Sort by most recent
        {
            "$sort": {
                "audit.time_committed": -1
            }
        },
        # 3. Take only the latest one
        {
            "$limit": 1
        },
        # 4. Deconstruct the versions array
        {
            "$unwind": "$versions"
        },
        # 5. Filter to only the FOLDER object in the versions array
        {
            "$match": {
                "versions._type": "FOLDER"
            }
        },
        # 6. Make the FOLDER object the root of the returned document
        {
            "$replaceRoot": {
                "newRoot": "$versions"
            }
        }
    ]

    cursor = db[EHR_CONTRIBUTIONS_COLL].aggregate(pipeline)
    result = await cursor.to_list(length=1)

    return result[0] if result else None
