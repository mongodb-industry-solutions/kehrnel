from pydantic import BaseModel, Field, field_validator
from typing import Dict, Optional, List, Any
from datetime import datetime
from bson import ObjectId

class CollectionConfig(BaseModel):
    """Configuration for a MongoDB collection"""
    name: str
    enabled: bool = True
    atlas_index_name: Optional[str] = None

class DictionaryConfig(BaseModel):
    """Configuration for dictionaries"""
    enabled: bool = True
    collection: str
    doc_id: str

class CodingArchetypeConfig(BaseModel):
    """Configuration for archetype ID coding"""
    store: str = "int"
    dictionary: str = "arcodes"
    sequential: bool = True

class CodingAtcodesConfig(BaseModel):
    """Configuration for AT codes"""
    strategy: str = "alpha_compact"
    zero_prefix_map: Dict[str, str] = Field(default_factory=dict)
    dotted_variants: Dict[str, Any] = Field(default_factory=dict)
    case: str = "lower"
    store_original: bool = True

class CodingConfig(BaseModel):
    """Configuration for coding strategies"""
    archetype_ids: CodingArchetypeConfig = Field(default_factory=CodingArchetypeConfig)
    atcodes: CodingAtcodesConfig = Field(default_factory=CodingAtcodesConfig)

class FieldMappingConfig(BaseModel):
    """Configuration for field mappings"""
    nodes: str = "cn"
    data: str = "data"
    path: str = "p"
    archetype_path: str = "ap"
    ancestors: str = "anc"
    ehr_id: str = "ehr_id"
    comp_id: Optional[str] = "comp_id"
    template_id: str = "tid"
    version: Optional[str] = "v"
    score: Optional[str] = None

class NodeRepresentationConfig(BaseModel):
    """Configuration for node representation"""
    path: Dict[str, str] = Field(default_factory=lambda: {"direction": "leaf_to_root", "token_joiner": "."})
    archetype_path: Dict[str, Any] = Field(default_factory=lambda: {"format": "aql_like", "use_raw_atcodes": True})
    ancestors: Dict[str, Any] = Field(default_factory=lambda: {"content": "archetype_ids", "enabled": True})

class EnrichmentConfig(BaseModel):
    """Configuration for data enrichment"""
    vectors: Dict[str, bool] = Field(default_factory=lambda: {"enabled": False})

class DatabaseConfig(BaseModel):
    """Main database configuration model"""
    database: str
    collections: Dict[str, CollectionConfig]
    dictionaries: Dict[str, DictionaryConfig] = Field(default_factory=dict)
    coding: CodingConfig = Field(default_factory=CodingConfig)
    fields: Dict[str, FieldMappingConfig]
    node_representation: NodeRepresentationConfig = Field(default_factory=NodeRepresentationConfig)
    enrichment: EnrichmentConfig = Field(default_factory=EnrichmentConfig)

class ConfigurationDocument(BaseModel):
    """Complete configuration document model"""
    id: Optional[str] = Field(None, alias="_id")
    name: str
    description: str
    tags: List[str] = Field(default_factory=list)
    config: DatabaseConfig
    
    # Optional metadata fields
    owner_type: Optional[str] = Field(None, alias="ownerType")
    owner_id: Optional[str] = Field(None, alias="ownerId") 
    visibility: Optional[str] = None
    created_at: Optional[datetime] = Field(None, alias="createdAt")
    updated_at: Optional[datetime] = Field(None, alias="updatedAt")
    created_by: Optional[Dict[str, Any]] = Field(None, alias="createdBy")
    updated_by: Optional[Dict[str, Any]] = Field(None, alias="updatedBy")
    
    # Optional complex metadata (stored as generic dicts)
    meta: Optional[Dict[str, Any]] = None
    tabs: Optional[Dict[str, Any]] = None
    blueprint: Optional[Dict[str, Any]] = None
    
    @field_validator('id', mode='before')
    @classmethod
    def convert_objectid_to_string(cls, v):
        """Convert MongoDB ObjectId to string"""
        if isinstance(v, ObjectId):
            return str(v)
        return v
    
    class Config:
        extra = "ignore"  # Allow extra fields to be ignored
        arbitrary_types_allowed = True  # Allow ObjectId type

class CompositionCollectionNames(BaseModel):
    """Simplified model for composition-specific collection names"""
    compositions: str = "compositions"
    flatten_compositions: str = "flatten_compositions"
    search_compositions: str = "sm_search3"
    contributions: str = "contributions" 
    ehr: str = "ehr"
    dictionaries: str = "dictionaries"
    database: str = "openehr_cdr"
    
    # Search-specific configurations
    atlas_index_name: Optional[str] = None
    merge_search_docs: bool = False
    
    # Field mappings for compositions
    composition_fields: FieldMappingConfig = Field(default_factory=FieldMappingConfig)
    search_fields: FieldMappingConfig = Field(default_factory=lambda: FieldMappingConfig(nodes="sn", score="score"))

    @classmethod
    def from_config_document(cls, config_doc: ConfigurationDocument) -> "CompositionCollectionNames":
        """Create CompositionCollectionNames from a ConfigurationDocument"""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"🔧 PARSING configuration document: {config_doc.name}")
            config = config_doc.config
            logger.info(f"📊 Config object type: {type(config)}")
            logger.info(f"📊 Config object: {config}")
            
            # Extract collection configurations with proper None checks
            logger.info(f"🔍 Extracting collections from config.collections: {config.collections}")
            compositions_coll = config.collections.get("compositions")
            flatten_compositions_coll = config.collections.get("flatten_compositions")
            search_coll = config.collections.get("search")
            dictionaries_coll = config.collections.get("dictionaries")
            
            logger.info(f"📁 Collections extracted:")
            logger.info(f"  - compositions: {compositions_coll} (type: {type(compositions_coll)})")
            logger.info(f"  - flatten_compositions: {flatten_compositions_coll} (type: {type(flatten_compositions_coll)})")
            logger.info(f"  - search: {search_coll} (type: {type(search_coll)})")
            logger.info(f"  - dictionaries: {dictionaries_coll} (type: {type(dictionaries_coll)})")
            
            # Extract field mapping configurations with proper None checks
            composition_fields_config = config.fields.get("composition")
            search_fields_config = config.fields.get("search")
            
            # Create field mappings with defaults if None
            if composition_fields_config is not None:
                composition_fields = composition_fields_config
            else:
                logger.debug("No composition fields config found, using defaults")
                composition_fields = FieldMappingConfig()
            
            if search_fields_config is not None:
                search_fields = search_fields_config
                # Ensure score field is set for search fields
                if search_fields.score is None:
                    search_fields.score = "score"
            else:
                logger.debug("No search fields config found, using defaults")
                search_fields = FieldMappingConfig(nodes="sn", score="score")
            
            # Build collection names with safe None checks
            
            compositions_name = (
                compositions_coll.name 
                if compositions_coll is not None 
                else "compositions"
            )

            flatten_compositions_name = (
                flatten_compositions_coll.name
                if flatten_compositions_coll is not None
                else "flatten_compositions"
            )

            search_name = (
                search_coll.name 
                if search_coll is not None 
                else "sm_search3"
            )

            dictionaries_name = (
                dictionaries_coll.name 
                if dictionaries_coll is not None 
                else "dictionaries"
            )

            atlas_index = (
                search_coll.atlas_index_name 
                if search_coll is not None 
                else None
            )
            
            logger.debug(f"Final collection names - database: {config.database}, "
                        f"compositions: {compositions_name}, search: {search_name}, "
                        f"dictionaries: {dictionaries_name}, atlas_index: {atlas_index}")
            
            return cls(
                database=config.database,
                compositions=compositions_name,
                flatten_compositions=flatten_compositions_name,
                search_compositions=search_name,
                dictionaries=dictionaries_name,
                atlas_index_name=atlas_index,
                merge_search_docs=False,
                composition_fields=composition_fields,
                search_fields=search_fields
            )
            
        except AttributeError as e:
            logger.error(f"❌ Missing required configuration attribute: {e}")
            logger.error(f"📄 Configuration document structure: {config_doc.dict() if hasattr(config_doc, 'dict') else str(config_doc)}")
            raise ValueError(f"Invalid configuration document structure: {e}")
        except Exception as e:
            logger.error(f"💥 UNEXPECTED ERROR parsing configuration document: {e}")
            logger.error(f"🐛 Error type: {type(e)}")
            logger.error(f"📄 Configuration document: {config_doc.dict() if hasattr(config_doc, 'dict') else str(config_doc)}")
            logger.error(f"📊 Config object if available: {getattr(config_doc, 'config', 'N/A')}")
            import traceback
            logger.error(f"📍 Full traceback: {traceback.format_exc()}")
            raise