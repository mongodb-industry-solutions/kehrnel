# src/kehrnel/api/legacy/v1/composition/dependencies.py

from kehrnel.api.legacy.app.core.config import settings
from kehrnel.api.legacy.app.core.config_models import CompositionCollectionNames
from kehrnel.api.legacy.app.core.config_manager import get_config_manager
import logging

logger = logging.getLogger(__name__)

async def get_composition_config() -> CompositionCollectionNames:
    """
    Dependency to get composition configuration.
    Uses dynamic configuration if enabled, otherwise falls back to static configuration.
    """
    if settings.USE_DYNAMIC_CONFIG:
        try:
            logger.info(f"🎯 DYNAMIC CONFIG ENABLED - Loading config: {settings.DEFAULT_CONFIG_NAME}")
            config_manager = await get_config_manager()
            logger.info(f"📋 Config manager obtained, calling get_composition_config_cached...")
            return await config_manager.get_composition_config_cached(
                settings.DEFAULT_CONFIG_NAME,
                use_name=True
            )
        except Exception as e:
            logger.error(f"💥 FULL ERROR in get_composition_config: {e}")
            logger.error(f"🐛 Error type: {type(e)}")
            import traceback
            logger.error(f"📍 Full traceback: {traceback.format_exc()}")
            logger.warning(f"Failed to load dynamic configuration, using default: {e}")
            # Use legacy environment variables as fallback
            return CompositionCollectionNames(
                database=settings.MONGODB_DB,
                compositions=settings.COMPOSITIONS_COLL_NAME,
                flatten_compositions=settings.FLAT_COMPOSITIONS_COLL_NAME,
                search_compositions=settings.SEARCH_COMPOSITIONS_COLL_NAME,
                contributions=settings.EHR_CONTRIBUTIONS_COLL,
                ehr=settings.EHR_COLL_NAME,
                dictionaries="dictionaries",
                merge_search_docs=False  # Keep search insertion enabled
            )
    else:
        # Use static configuration from settings - legacy mode
        return CompositionCollectionNames(
            database=settings.MONGODB_DB,
            compositions=settings.COMPOSITIONS_COLL_NAME,
            flatten_compositions=settings.FLAT_COMPOSITIONS_COLL_NAME,
            search_compositions=settings.SEARCH_COMPOSITIONS_COLL_NAME,
            contributions=settings.EHR_CONTRIBUTIONS_COLL,
            ehr=settings.EHR_COLL_NAME,
            dictionaries="dictionaries",
            merge_search_docs=False  # Keep search insertion enabled
        )