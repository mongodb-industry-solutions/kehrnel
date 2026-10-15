# src/kehrnel/api/compatibility/v1/composition/dependencies.py

from fastapi import HTTPException, Request

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
        # Request paths fail closed: falling back to process-wide settings after
        # an activation/binding error could silently route one tenant to another
        # tenant's collections.
        context = await resolve_active_openehr_context(request, ensure_ingestion=False)
        strategy_cfg = normalize_config(getattr(context.get("activation"), "config", {}) or {})
        synced = context.get("synced") or {}
        request_values = {
            "database": context.get("database_name"),
            "compositions": synced.get("canonical_collection"),
            "flatten_compositions": synced.get("flat_collection"),
            "search_compositions": synced.get("search_collection"),
            "contributions": synced.get("contributions_collection"),
            "ehr": synced.get("ehr_collection"),
            "dictionaries": synced.get("codes_collection"),
        }
        missing = [
            name
            for name, value in request_values.items()
            if not isinstance(value, str) or not value.strip()
        ]
        if missing:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Resolved openEHR request context is incomplete; refusing process-global "
                    f"collection fallback for: {', '.join(sorted(missing))}."
                ),
            )
        return CompositionCollectionNames(
            **request_values,
            atlas_index_name=synced.get("search_index_name"),
            merge_search_docs=bool(synced.get("search_compositions_merge", False)),
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
