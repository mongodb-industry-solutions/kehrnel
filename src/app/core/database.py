import sys
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure
from src.app.core.config import settings
from src.app.core.config_manager import initialize_config_manager, close_config_manager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    client: AsyncIOMotorClient = None

db = Database()

async def get_mongodb_ehr_db() -> AsyncIOMotorDatabase:
    return db.client[settings.MONGODB_DB]

async def get_mongodb_database(database_name: str) -> AsyncIOMotorDatabase:
    """Get a specific database by name"""
    return db.client[database_name]

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
        
        # Initialize configuration manager if dynamic config is enabled
        if settings.USE_DYNAMIC_CONFIG:
            logger.info("Initializing configuration manager...")
            await initialize_config_manager(
                config_db_uri=settings.config_db_uri,
                config_db_name=settings.CONFIG_DB_NAME,
                config_collection_name=settings.CONFIG_COLLECTION_NAME,
                cache_ttl_minutes=settings.CONFIG_CACHE_TTL_MINUTES
            )
            logger.info("Configuration manager initialized successfully")
        else:
            logger.info("Dynamic configuration disabled, using static configuration")
            
    except ConnectionFailure as e:
        logger.critical(f"Fatal error: Could not connect to MongoDB. Application will shut down. Error: {e}")

async def close_mongo_connection():
    if db.client:
        logger.info("Closing MongoDB connection")
        db.client.close()
        logger.info("MongoDB connection closed")
    
    # Close configuration manager
    if settings.USE_DYNAMIC_CONFIG:
        await close_config_manager()
        logger.info("Configuration manager connection closed")