from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any
import json
import os

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
    
    class Config:
        env_prefix = "SEARCH_"
        extra = "ignore"

class Settings(BaseSettings):
    MONGODB_URI: str = Field(..., alias="MONGODB_URI")
    MONGODB_DB: str = Field(..., alias="MONGODB_DB")
    
    # Search configuration 
    search_config: SearchConfig = SearchConfig()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
    
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
                print(f"Warning: Could not load config from {config_path}: {e}")

settings = Settings()

# Try to load from config.json on import
settings.load_from_config_file()