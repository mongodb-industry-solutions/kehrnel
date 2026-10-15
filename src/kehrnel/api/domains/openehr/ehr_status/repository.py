from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from kehrnel.api.bridge.app.core.config import settings
from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames

logger = logging.getLogger(__name__)

def _ehr_coll(config: CompositionCollectionNames | None = None) -> str:
    return config.ehr if config is not None else settings.EHR_COLL_NAME


def _contrib_coll(config: CompositionCollectionNames | None = None) -> str:
    return config.contributions if config is not None else settings.EHR_CONTRIBUTIONS_COLL


async def update_ehr_status_in_transaction(
    ehr_id: str,
    new_status_doc: dict,
    contribution_doc: dict,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames | None = None,
):
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
                await db[_contrib_coll(config)].insert_one(contribution_doc, session=session)

                # 2. Update the main EHR document
                await db[_ehr_coll(config)].update_one(
                    ehr_update_criteria,
                    ehr_update_doc,
                    session=session
                )
            except PyMongoError as e:
                logger.error(f"EHR status update transaction failed: {e}")
                raise
