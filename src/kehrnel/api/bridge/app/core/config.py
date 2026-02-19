import json
import logging
import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

class SearchConfig(BaseSettings):
    """Configuration for Atlas Search dual-query strategy"""
    # Collection names
    search_collection: str = "sm_search3"
    flatten_collection: str = "flatten_compositions"
    codes_collection: str = "_codes"

    # Search index configuration
    search_index_name: str = "search_compositions_index"
    search_compositions_merge: bool = False

    # Atlas Search compatibility settings
    use_string_date_queries: bool = False  # Fallback for date query issues

    # Query strategy thresholds
    enable_dual_strategy: bool = True
    force_search_strategy: bool = False  # For testing

    model_config = SettingsConfigDict(
        env_prefix="SEARCH_",
        extra="ignore",
    )

class Settings(BaseSettings):
    # Main CDR Database
    MONGODB_URI: str = Field("mongodb://localhost:27017", alias="MONGODB_URI")
    MONGODB_DB: str = Field("openehr_playground", alias="MONGODB_DB")

    # Configuration Database
    CONFIG_DB_URI: Optional[str] = Field(None, alias="CONFIG_DB_URI")
    CONFIG_DB_NAME: str = Field("configuration", alias="CONFIG_DB_NAME")
    CONFIG_COLLECTION_NAME: str = Field("node_configurations", alias="CONFIG_COLLECTION_NAME")

    # Dynamic Configuration Settings
    USE_DYNAMIC_CONFIG: bool = Field(False, alias="USE_DYNAMIC_CONFIG")
    DEFAULT_CONFIG_NAME: Optional[str] = Field(None, alias="DEFAULT_CONFIG_NAME")
    CONFIG_CACHE_TTL_MINUTES: int = Field(30, alias="CONFIG_CACHE_TTL_MINUTES")

    # Compatibility collection names (for fallback when dynamic config fails)
    COMPOSITIONS_COLL_NAME: str = Field("compositions", alias="COMPOSITIONS_COLL_NAME")
    EHR_CONTRIBUTIONS_COLL: str = Field("contributions", alias="EHR_CONTRIBUTIONS_COLL")
    EHR_COLL_NAME: str = Field("ehr", alias="EHR_COLL_NAME")
    FLAT_COMPOSITIONS_COLL_NAME: str = Field("flatten_compositions", alias="FLAT_COMPOSITIONS_COLL_NAME")
    SEARCH_COMPOSITIONS_COLL_NAME: str = Field("sm_search3", alias="SEARCH_COMPOSITIONS_COLL_NAME")

    # Search configuration (compatibility - will be replaced by dynamic config)
    search_config: SearchConfig = SearchConfig()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def config_db_uri(self) -> str:
        """Get the configuration database URI, fallback to main URI if not specified"""
        return self.CONFIG_DB_URI or self.MONGODB_URI

    def load_from_config_file(self, config_path: str = "config.json") -> None:
        """Load search configuration from config.json if it exists"""
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config_data = json.load(f)

                # Extract target configuration
                target_config = config_data.get("target", {})
                if target_config:
                    # Update search config from target section
                    self.search_config.search_collection = target_config.get("search_collection", "sm_search3")
                    self.search_config.flatten_collection = target_config.get("compositions_collection", "flatten_compositions")
                    self.search_config.codes_collection = target_config.get("codes_collection", "_codes")
                    self.search_config.search_compositions_merge = target_config.get("search_compositions_merge", False)

            except Exception as e:
                logger.warning("Could not load config from %s: %s", config_path, e)

settings = Settings()

# Try to load from config.json on import
settings.load_from_config_file()
