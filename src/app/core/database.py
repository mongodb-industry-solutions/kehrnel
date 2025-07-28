import sys
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure
from app.core.config import settings
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    client: AsyncIOMotorClient = None

db = Database()

async def get_mongodb_ehr_db() -> AsyncIOMotorDatabase:
    return db.client[settings.MONGODB_DB]

async def connect_to_mongo():
    logger.info("Connecting to MongoDB")
    try:

        db.client = AsyncIOMotorClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS = 5000
        )

        # It sends an ismaster command which let us know if the connections was established correctly
        await db.client.admin.command('ismaster')
        logger.info("Successfully connected to MongoDB!")
    except ConnectionFailure as e:
        logger.critical(f"Fatal error: Could not connect to MongoDB. Application will shut down. Error: {e}")

async def close_mongo_connection():
    if db.client:
        logger.info("Closing MongoDB connection")
        db.client.close()
        logger.info("MongoDB connection closed")