import logging
from typing import Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta
import asyncio

from src.app.core.config_models import ConfigurationDocument, CompositionCollectionNames

logger = logging.getLogger(__name__)

class ConfigurationManager:
    """
    Manages dynamic configurations fetched from MongoDB.
    Provides caching and automatic refresh capabilities.
    """
    
    def __init__(
        self,
        config_db_uri: str,
        config_db_name: str = "configuration",
        config_collection_name: str = "node_configurations",
        cache_ttl_minutes: int = 30
    ):
        self.config_db_uri = config_db_uri
        self.config_db_name = config_db_name
        self.config_collection_name = config_collection_name
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        
        # Cache storage
        self._config_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        
        # Database connection
        self._config_client: Optional[AsyncIOMotorClient] = None
        self._config_db: Optional[AsyncIOMotorDatabase] = None
        
        # Default configuration fallback
        self._default_composition_config = CompositionCollectionNames()
    
    async def initialize(self):
        """Initialize the configuration manager and database connection"""
        try:
            logger.info(f"Initializing ConfigurationManager with URI: {self.config_db_uri}")
            self._config_client = AsyncIOMotorClient(
                self.config_db_uri,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            await self._config_client.admin.command('ismaster')
            self._config_db = self._config_client[self.config_db_name]
            logger.info("Successfully connected to configuration database")
            
        except Exception as e:
            logger.error(f"Failed to initialize configuration database connection: {e}")
            raise
    
    async def close(self):
        """Close the configuration database connection"""
        if self._config_client:
            self._config_client.close()
            logger.info("Configuration database connection closed")
    
    async def get_configuration_by_name(self, config_name: str) -> Optional[ConfigurationDocument]:
        """
        Retrieve a configuration document by name from the database
        
        Args:
            config_name: The name of the configuration to retrieve
            
        Returns:
            ConfigurationDocument or None if not found
        """
        if self._config_db is None:
            raise RuntimeError("Configuration manager not initialized. Call initialize() first.")
        
        try:
            logger.info(f"🔍 SEARCHING for configuration with name: '{config_name}' in database: {self.config_db_name}, collection: {self.config_collection_name}")
            
            # Debug database connection details
            logger.info(f"🔧 Database object: {self._config_db}")
            logger.info(f"🔧 Database name: {self._config_db.name}")
            logger.info(f"🔧 Collection name: {self.config_collection_name}")
            logger.info(f"🔧 Full connection URI: {self.config_db_uri}")
            
            # First, let's see what documents exist in the collection
            try:
                logger.info(f"🔍 Listing all documents in {self._config_db.name}.{self.config_collection_name}")
                all_docs = await self._config_db[self.config_collection_name].find({}).to_list(length=10)
                logger.info(f"📋 Total documents found: {len(all_docs)}")
                logger.info(f"📋 Available documents in collection: {[doc.get('name', 'unnamed') for doc in all_docs]}")
                
                # Also try to get document count
                doc_count = await self._config_db[self.config_collection_name].count_documents({})
                logger.info(f"📊 Total document count in collection: {doc_count}")
                
                # List all collections in the database
                collections = await self._config_db.list_collection_names()
                logger.info(f"📁 Available collections in database: {collections}")
                
            except Exception as list_error:
                logger.error(f"❌ Error listing documents: {list_error}")
                logger.error(f"🐛 List error type: {type(list_error)}")
                import traceback
                logger.error(f"📍 Full traceback: {traceback.format_exc()}")
            
            try:
                logger.info(f"🔍 Executing find_one query for name: '{config_name}'...")
                doc = await self._config_db[self.config_collection_name].find_one({"name": config_name})
                logger.info(f"✅ find_one query completed, doc found: {doc is not None}")
                
                # Also try a broader search to see if there are any documents at all
                any_doc = await self._config_db[self.config_collection_name].find_one({})
                logger.info(f"🔍 Any document in collection: {any_doc is not None}")
                if any_doc:
                    logger.info(f"🔍 Sample document keys: {list(any_doc.keys())}")
                    logger.info(f"🔍 Sample document name field: {any_doc.get('name', 'NO NAME FIELD')}")
                
            except Exception as query_error:
                logger.error(f"❌ Error in find_one query: {query_error}")
                logger.error(f"🐛 Query error type: {type(query_error)}")
                import traceback
                logger.error(f"📍 Full traceback: {traceback.format_exc()}")
                raise
            if doc:
                logger.info(f"✅ Configuration document found with ID: {doc.get('_id')}")
                logger.info(f"📄 Document keys: {list(doc.keys())}")
                logger.info(f"📝 Document name: {doc.get('name')}")
                logger.info(f"📝 Document description: {doc.get('description', 'N/A')}")
                
                # Check if config field exists
                if "config" not in doc:
                    logger.error(f"❌ Configuration document '{config_name}' missing 'config' field")
                    logger.error(f"📄 Full document structure: {doc}")
                    return None
                
                # Log config structure
                config = doc["config"]
                logger.info(f"⚙️ Config keys: {list(config.keys())}")
                logger.info(f"🗃️ Database: {config.get('database', 'N/A')}")
                if "collections" in config:
                    logger.info(f"📁 Collections: {list(config['collections'].keys())}")
                    logger.info(f"📁 Collections content: {config['collections']}")
                if "fields" in config:
                    logger.info(f"🏷️ Field mappings: {list(config['fields'].keys())}")
                    logger.info(f"🏷️ Field mappings content: {config['fields']}")
                
                logger.info(f"🔧 About to create ConfigurationDocument from raw doc...")
                try:
                    config_doc = ConfigurationDocument(**doc)
                    logger.info(f"✅ ConfigurationDocument created successfully")
                    return config_doc
                except Exception as pydantic_error:
                    logger.error(f"❌ Error creating ConfigurationDocument: {pydantic_error}")
                    logger.error(f"🐛 Pydantic error type: {type(pydantic_error)}")
                    logger.error(f"📄 Raw document that caused error: {doc}")
                    raise
            else:
                logger.warning(f"❌ No configuration found with name: '{config_name}'")
                return None
                
        except Exception as e:
            logger.error(f"Failed to retrieve configuration '{config_name}': {e}")
            logger.error(f"Database: {self.config_db_name}, Collection: {self.config_collection_name}")
            return None
    
    async def get_configuration_by_id(self, config_id: str) -> Optional[ConfigurationDocument]:
        """
        Retrieve a configuration document by ID from the database
        
        Args:
            config_id: The ObjectId of the configuration to retrieve
            
        Returns:
            ConfigurationDocument or None if not found
        """
        if self._config_db is None:
            raise RuntimeError("Configuration manager not initialized. Call initialize() first.")
        
        try:
            from bson import ObjectId
            doc = await self._config_db[self.config_collection_name].find_one({"_id": ObjectId(config_id)})
            if doc:
                return ConfigurationDocument(**doc)
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve configuration with ID '{config_id}': {e}")
            return None
    
    async def list_configurations(self) -> list[ConfigurationDocument]:
        """
        List all available configurations
        
        Returns:
            List of ConfigurationDocument objects
        """
        if self._config_db is None:
            raise RuntimeError("Configuration manager not initialized. Call initialize() first.")
        
        try:
            cursor = self._config_db[self.config_collection_name].find({})
            docs = await cursor.to_list(length=None)
            return [ConfigurationDocument(**doc) for doc in docs]
        except Exception as e:
            logger.error(f"Failed to list configurations: {e}")
            return []
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached configuration is still valid"""
        if cache_key not in self._cache_timestamps:
            return False
        
        cache_time = self._cache_timestamps[cache_key]
        return datetime.now() - cache_time < self.cache_ttl
    
    async def get_composition_config_cached(
        self, 
        config_identifier: str, 
        use_name: bool = True
    ) -> CompositionCollectionNames:
        """
        Get composition configuration with caching support
        
        Args:
            config_identifier: Configuration name or ID
            use_name: If True, treat identifier as name; if False, treat as ID
            
        Returns:
            CompositionCollectionNames configuration
        """
        cache_key = f"{'name' if use_name else 'id'}:{config_identifier}"
        
        try:
            logger.info(f"🚀 STARTING get_composition_config_cached for: {config_identifier}")
            
            # Check cache first
            if self._is_cache_valid(cache_key):
                logger.info(f"💾 Using cached configuration for {cache_key}")
                cached_data = self._config_cache[cache_key]
                return CompositionCollectionNames(**cached_data)
            
            logger.info(f"🔄 Cache miss for {cache_key}, fetching from database")
            
            # Fetch from database
            if use_name:
                logger.info(f"📥 Fetching configuration by name: {config_identifier}")
                config_doc = await self.get_configuration_by_name(config_identifier)
            else:
                logger.info(f"📥 Fetching configuration by ID: {config_identifier}")
                config_doc = await self.get_configuration_by_id(config_identifier)
            
            if config_doc:
                logger.info(f"✅ Configuration document found: {config_doc.name}")
                logger.info(f"📊 Config structure: database={config_doc.config.database}, "
                           f"collections={list(config_doc.config.collections.keys())}, "
                           f"fields={list(config_doc.config.fields.keys())}")
                
                # Parse configuration document
                logger.info(f"🔧 About to call from_config_document...")
                composition_config = CompositionCollectionNames.from_config_document(config_doc)
                logger.info(f"✅ Successfully parsed configuration document")
                
                # Update cache
                self._config_cache[cache_key] = composition_config.dict()
                self._cache_timestamps[cache_key] = datetime.now()
                
                logger.info(f"Successfully loaded and cached configuration: {cache_key}")
                logger.info(f"Configuration details: database={composition_config.database}, "
                           f"collections=[{composition_config.compositions}, {composition_config.search_compositions}]")
                return composition_config
            else:
                logger.warning(f"Configuration not found in database: {config_identifier}")
                logger.warning(f"Available configurations: {await self._list_available_configs()}")
                logger.warning("Using default configuration as fallback")
                return self._default_composition_config
                
        except ValueError as e:
            logger.error(f"Configuration parsing error for {config_identifier}: {e}")
            logger.warning("Using default configuration due to parsing error")
            return self._default_composition_config
        except Exception as e:
            logger.error(f"Unexpected error loading configuration {config_identifier}: {e}")
            logger.warning("Using default configuration due to unexpected error")
            return self._default_composition_config
    
    async def get_composition_config(
        self, 
        config_identifier: Optional[str] = None, 
        use_name: bool = True
    ) -> CompositionCollectionNames:
        """
        Get composition configuration without caching (always fresh from DB)
        
        Args:
            config_identifier: Configuration name or ID. If None, returns default config
            use_name: If True, treat identifier as name; if False, treat as ID
            
        Returns:
            CompositionCollectionNames configuration
        """
        if config_identifier is None:
            return self._default_composition_config
            
        # Fetch fresh from database
        if use_name:
            config_doc = await self.get_configuration_by_name(config_identifier)
        else:
            config_doc = await self.get_configuration_by_id(config_identifier)
        
        if config_doc:
            return CompositionCollectionNames.from_config_document(config_doc)
        else:
            logger.warning(f"Configuration not found: {config_identifier}. Using default configuration.")
            return self._default_composition_config
    
    async def _list_available_configs(self) -> str:
        """
        Helper method to list available configuration names for debugging
        
        Returns:
            Comma-separated string of available configuration names
        """
        try:
            if self._config_db is None:
                return "database not initialized"
            
            cursor = self._config_db[self.config_collection_name].find({}, {"name": 1})
            docs = await cursor.to_list(length=10)  # Limit to 10 for debugging
            names = [doc.get("name", "unnamed") for doc in docs]
            return ", ".join(names) if names else "no configurations found"
        except Exception as e:
            return f"error listing configs: {e}"
    
    def clear_cache(self, config_identifier: Optional[str] = None):
        """
        Clear configuration cache
        
        Args:
            config_identifier: Specific config to clear, or None to clear all
        """
        if config_identifier:
            # Clear specific configuration
            keys_to_remove = [key for key in self._config_cache.keys() 
                             if key.endswith(f":{config_identifier}")]
            for key in keys_to_remove:
                self._config_cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
            logger.info(f"Cleared cache for configuration: {config_identifier}")
        else:
            # Clear all cache
            self._config_cache.clear()
            self._cache_timestamps.clear()
            logger.info("Cleared all configuration cache")
    
    async def refresh_configuration(self, config_identifier: str, use_name: bool = True):
        """
        Force refresh a specific configuration from database
        
        Args:
            config_identifier: Configuration name or ID to refresh
            use_name: If True, treat identifier as name; if False, treat as ID
        """
        cache_key = f"{'name' if use_name else 'id'}:{config_identifier}"
        
        # Remove from cache to force fresh fetch
        self._config_cache.pop(cache_key, None)
        self._cache_timestamps.pop(cache_key, None)
        
        # Fetch fresh configuration
        await self.get_composition_config_cached(config_identifier, use_name)
        logger.info(f"Refreshed configuration: {config_identifier}")


# Global configuration manager instance
config_manager: Optional[ConfigurationManager] = None

async def get_config_manager() -> ConfigurationManager:
    """Get the global configuration manager instance"""
    global config_manager
    if config_manager is None:
        raise RuntimeError("Configuration manager not initialized")
    return config_manager

async def initialize_config_manager(
    config_db_uri: str,
    config_db_name: str = "configuration",
    config_collection_name: str = "node_configurations",
    cache_ttl_minutes: int = 30
) -> ConfigurationManager:
    """Initialize the global configuration manager"""
    global config_manager
    config_manager = ConfigurationManager(
        config_db_uri=config_db_uri,
        config_db_name=config_db_name,
        config_collection_name=config_collection_name,
        cache_ttl_minutes=cache_ttl_minutes
    )
    await config_manager.initialize()
    return config_manager

async def close_config_manager():
    """Close the global configuration manager"""
    global config_manager
    if config_manager:
        await config_manager.close()
        config_manager = None