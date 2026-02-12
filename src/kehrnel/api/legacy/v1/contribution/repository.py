from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
from datetime import datetime
from kehrnel.api.legacy.app.core.config import settings


def _contrib_coll() -> str:
    return settings.EHR_CONTRIBUTIONS_COLL

async def find_contribution_by_id(contribution_uid: str, db: AsyncIOMotorDatabase):
    """
    Retrieved a single CONTRIBUTION document from the database by its _id
    """
    return await db[_contrib_coll()].find_one({"_id": contribution_uid})


async def find_contribution_by_version_uid(version_uid: str, db: AsyncIOMotorDatabase):
    """
    Finds a contribution document by the UID of a version it contains.
    This is used to find the commit time for a specific version of an object
    """
    return await db[_contrib_coll()].find_one(
        {"versions.uid.value": version_uid}
    )


async def find_deletion_contribution_for_version(
        preceding_version_uid: str,
        db: AsyncIOMotorDatabase
):
    """
    Checks if a 'deleted' contribution already exists for a specific version UID
    """
    return await db[_contrib_coll()].find_one(
        {
            "audit.change_type": "deleted",
            "versions.preceding_version_uid": preceding_version_uid
        }
    )


async def find_contributions_for_versioned_object(versioned_object_uid: str, db: AsyncIOMotorDatabase):
    """
    Finds all the contributions documents related to a specific versioned object (e.g., a COMPOSITION or EHR_STATUS).

    It searches for contributions where at least one version inside it has a UID
    that starts with the given versioned_object_uid.

    Args:
        versioned_object_uid: The base ID of the versioned object.
        db: The database session.

    Returns:
        A list of matching contribution documents, sorted by commit time.
    """

    filter_criteria = {
        # Looks inside the 'versions' array for an element where the 'uid.value' starts with the base ID
        "versions": {
            "$elemMatch": {
                "uid.value": {
                    "$regex": f"^{versioned_object_uid}::"
                }
            }
        }
    }

    # Sort by the audit's time_committed to get a chronological history
    cursor = db[_contrib_coll()].find(filter_criteria).sort("audit.time_committed", 1)
    return await cursor.to_list(length=None)


async def find_latest_contribution_by_vo_uid(
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase,
    timestamp: Optional[datetime] = None
):
    """
    Finds the most recent contribution for a specific versioned object, 
    optionally at or before a given time. Works for Compositions and EHR_STATUS.
    """
    filter_criteria = {
        "versions": {
            "$elemMatch": {
                "uid.value": {
                    "$regex": f"^{versioned_object_uid}::"
                }
            }
        }
    }

    if timestamp:
        filter_criteria["audit.time_committed"] = {"$lte": timestamp}
    
    cursor = db[_contrib_coll()].find(filter_criteria).sort("audit.time_committed", -1).limit(1)
    documents = await cursor.to_list(length=1)

    if documents:
        return documents[0]
    return None
