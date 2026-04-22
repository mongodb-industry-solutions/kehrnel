# src/kehrnel/api/compatibility/v1/composition/dependencies.py

from fastapi import Request

from kehrnel.api.bridge.app.core.config import settings
from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames, FieldMappingConfig
from kehrnel.api.bridge.app.core.config_manager import get_config_manager
from kehrnel.api.bridge.app.core.database import resolve_active_openehr_context
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config
import logging

logger = logging.getLogger(__name__)

def _config_from_settings() -> CompositionCollectionNames:
    return CompositionCollectionNames(
        database=settings.MONGODB_DB,
        compositions=settings.COMPOSITIONS_COLL_NAME,
        flatten_compositions=settings.FLAT_COMPOSITIONS_COLL_NAME,
        search_compositions=settings.SEARCH_COMPOSITIONS_COLL_NAME,
        contributions=settings.EHR_CONTRIBUTIONS_COLL,
        ehr=settings.EHR_COLL_NAME,
        dictionaries=settings.search_config.codes_collection or "dictionaries",
        merge_search_docs=bool(settings.search_config.search_compositions_merge),
    )


async def get_composition_config(request: Request = None) -> CompositionCollectionNames:
    """
    Dependency to get composition configuration.
    Uses dynamic configuration if enabled, otherwise falls back to static configuration.
    """
    if request is not None:
        try:
            context = await resolve_active_openehr_context(request, ensure_ingestion=False)
            strategy_cfg = normalize_config(getattr(context.get("activation"), "config", {}) or {})
            return CompositionCollectionNames(
                database=context.get("database_name") or settings.MONGODB_DB,
                compositions=settings.COMPOSITIONS_COLL_NAME,
                flatten_compositions=settings.FLAT_COMPOSITIONS_COLL_NAME,
                search_compositions=settings.SEARCH_COMPOSITIONS_COLL_NAME,
                contributions=settings.EHR_CONTRIBUTIONS_COLL,
                ehr=settings.EHR_COLL_NAME,
                dictionaries=strategy_cfg.collections.codes.name or settings.search_config.codes_collection or "dictionaries",
                merge_search_docs=bool(settings.search_config.search_compositions_merge),
                composition_fields=FieldMappingConfig(
                    nodes=strategy_cfg.fields.document.cn,
                    data=strategy_cfg.fields.node.data,
                    path=strategy_cfg.fields.node.p,
                    ehr_id=strategy_cfg.fields.document.ehr_id,
                    comp_id=strategy_cfg.fields.document.comp_id,
                    template_id=strategy_cfg.fields.document.tid,
                    version=strategy_cfg.fields.document.v,
                ),
                search_fields=FieldMappingConfig(
                    nodes=strategy_cfg.fields.document.sn,
                    data=strategy_cfg.fields.node.data,
                    path=strategy_cfg.fields.node.p,
                    ehr_id=strategy_cfg.fields.document.ehr_id,
                    comp_id=strategy_cfg.fields.document.comp_id,
                    template_id=strategy_cfg.fields.document.tid,
                    score="score",
                ),
            )
        except Exception as e:
            logger.warning(f"Failed to resolve request-scoped composition configuration, using settings fallback: {e}")

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
            return _config_from_settings()
    else:
        return _config_from_settings()
