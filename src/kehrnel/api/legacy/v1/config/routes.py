# src/kehrnel/api/legacy/v1/config/routes.py

from typing import Any, Dict, Optional, Union, List
import os
from pathlib import Path
import json

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from kehrnel.api.legacy.app.utils.config_runtime import apply_ingestion_config, DEFAULT_MAPPINGS_PATH
from kehrnel.api.legacy.app.core.config import settings
from kehrnel.persistence import load_strategy_from_file, load_strategy_from_json, PersistenceStrategy

router = APIRouter()


def _allow_local_file_inputs() -> bool:
    return os.getenv("KEHRNEL_ALLOW_LOCAL_FILE_INPUTS", "false").lower() in ("1", "true", "yes")


def _safe_local_json_path(raw_path: str) -> Path:
    p = Path(raw_path)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    if not p.exists() or not p.is_file():
        raise ValueError("Provided path does not exist or is not a file")
    if p.suffix.lower() not in (".json", ".jsonc"):
        raise ValueError("Only .json/.jsonc files are allowed")
    return p


# --- Payload Schemas ---
class CollectionsCompositions(BaseModel):
    store_canonical: bool = Field(False, description="Persist canonical doc alongside flattened output.")
    name: str = Field(..., description="Collection for canonical compositions (and source for /database).")


class CollectionsSemiFlattened(BaseModel):
    name: str = Field(..., description="Collection for semi-flattened base documents.")


class CollectionsSearch(BaseModel):
    name: str = Field(..., description="Collection for search documents.")
    enabled: bool = Field(True, description="If false, skip writing search docs.")
    atlas_index_name: Optional[str] = Field(None, description="Atlas Search index name (metadata).")


class CollectionsConfig(BaseModel):
    compositions: CollectionsCompositions
    semiflattened_compositions: CollectionsSemiFlattened
    search: CollectionsSearch


class AtCodesConfig(BaseModel):
    enabled: bool = True
    strategy: str = "negative_int"
    store_original: bool = False


class ArchetypeIdsConfig(BaseModel):
    enabled: bool = True
    dictionary: str = "ar_code"
    sequential: bool = True
    name: str = "_codes"
    atcodes: AtCodesConfig = Field(default_factory=AtCodesConfig)


class DataShortcutsConfig(BaseModel):
    enabled: bool = True
    name: str = "_shortcuts"


class CodingConfig(BaseModel):
    archetype_ids: ArchetypeIdsConfig = Field(default_factory=ArchetypeIdsConfig)
    dataShortcuts: DataShortcutsConfig = Field(default_factory=DataShortcutsConfig)


class FieldsCompositionsConfig(BaseModel):
    data: str = "data"
    composition_nodes: str = "cn"
    path: str = "p"
    keyPath: str = "kp"
    lineIndex: str = "li"
    ehr_id: str = "ehr_id"
    composition_id: str = "comp_id"
    template_id: str = "tid"
    version: str = "v"


class FieldsSearchConfig(BaseModel):
    data: str = "data"
    search_nodes: str = "sn"
    path: str = "p"
    ehr_id: str = "ehr_id"
    composition_id: str = "comp_id"
    template_id: str = "tid"


class FieldsConfig(BaseModel):
    compositions: FieldsCompositionsConfig = Field(default_factory=FieldsCompositionsConfig)
    search: FieldsSearchConfig = Field(default_factory=FieldsSearchConfig)


class NodePathConfig(BaseModel):
    mode: str = Field("reversed", description="Informational; paths are emitted reversed.")
    token_joiner: str = "."


class NodeRepresentationConfig(BaseModel):
    path: NodePathConfig = Field(default_factory=NodePathConfig)


class MappingsConfig(BaseModel):
    inline: Optional[Union[Dict[str, Any], str]] = Field(
        default=None,
        description="Inline mapping rules (dict or JSON string). Overrides file-based mappings.",
    )
    use_file: bool = Field(
        default=True,
        description="When false and no inline mappings are provided, use an empty ruleset.",
    )
    path: Optional[str] = Field(
        default=str(DEFAULT_MAPPINGS_PATH),
        description="Path to the mappings file when use_file is true and inline is not provided.",
    )


class IngestionConfigPayload(BaseModel):
    collections: CollectionsConfig
    coding: CodingConfig = Field(default_factory=CodingConfig)
    fields: FieldsConfig = Field(default_factory=FieldsConfig)
    node_representation: NodeRepresentationConfig = Field(default_factory=NodeRepresentationConfig)
    mappings: MappingsConfig = Field(default_factory=MappingsConfig)
    strategy_path: Optional[str] = Field(
        default=None, description="Path to a strategy JSON file (takes precedence if provided)."
    )
    strategy_inline: Optional[Union[Dict[str, Any], str]] = Field(
        default=None, description="Inline strategy JSON (used if path not provided)."
    )
    analytics_path: Optional[str] = Field(
        default=None, description="Path to analytics template mappings (simple fields format)."
    )
    analytics_inline: Optional[Union[Dict[str, Any], List[Dict[str, Any]], str]] = Field(
        default=None, description="Inline analytics template mappings (simple fields format)."
    )


# --- Route ---
@router.post(
    "",
    status_code=status.HTTP_200_OK,
    summary="Set ingestion configuration at runtime",
    description="Initializes or reinitializes the flattener/transformer using the provided configuration. "
    "Mappings can be inline or file-based.",
)
async def set_ingestion_config(payload: IngestionConfigPayload, request: Request):
    try:
        # Load strategy if provided
        strategy: Optional[PersistenceStrategy] = None
        strategy_raw: Dict[str, Any] = {}
        if payload.strategy_path:
            if not _allow_local_file_inputs():
                raise ValueError("Local file paths are disabled. Use strategy_inline instead.")
            safe_path = _safe_local_json_path(payload.strategy_path)
            with safe_path.open("r", encoding="utf-8") as f:
                strategy_raw = json.load(f)
            strategy = load_strategy_from_json(strategy_raw)
        elif payload.strategy_inline is not None:
            strategy_raw = payload.strategy_inline if isinstance(payload.strategy_inline, dict) else json.loads(payload.strategy_inline)
            strategy = load_strategy_from_json(strategy_raw)

        # Derive DB names from strategy if present
        source_db_name = target_db_name = None
        if strategy and strategy.database:
            source_db_name = strategy.database
            target_db_name = strategy.database

        # Decide mappings source: analytics path/inline > strategy templates > provided mappings
        mappings_inline = payload.mappings.inline
        if payload.analytics_path:
            if not _allow_local_file_inputs():
                raise ValueError("Local file paths are disabled. Use analytics_inline instead.")
            safe_path = _safe_local_json_path(payload.analytics_path)
            with safe_path.open("r", encoding="utf-8") as f:
                mappings_inline = json.load(f)
        elif payload.analytics_inline is not None:
            mappings_inline = payload.analytics_inline
        elif strategy_raw.get("templates"):
            mappings_inline = strategy_raw.get("templates")
        elif "templateId" in strategy_raw:
            mappings_inline = strategy_raw

        # Build internal config for flattener/repository
        config_internal = {
            "apply_shortcuts": payload.coding.dataShortcuts.enabled,
            "source": {
                "canonical_compositions_collection": payload.collections.compositions.name,
                "database_name": source_db_name,
            },
            "target": {
                "compositions_collection": payload.collections.semiflattened_compositions.name,
                "search_collection": payload.collections.search.name,
                "codes_collection": payload.coding.archetype_ids.name,
                "shortcuts_collection": payload.coding.dataShortcuts.name,
                "rebuilt_collection": payload.collections.semiflattened_compositions.name,
                "database_name": target_db_name,
            },
        }

        field_map = {
            "compositions": {
                "ehr_id": payload.fields.compositions.ehr_id,
                "composition_id": payload.fields.compositions.composition_id,
                "template_id": payload.fields.compositions.template_id,
                "version": payload.fields.compositions.version,
                "composition_nodes": payload.fields.compositions.composition_nodes,
            },
            "composition_nodes": {
                "path": payload.fields.compositions.path,
                "keyPath": payload.fields.compositions.keyPath,
                "lineIndex": payload.fields.compositions.lineIndex,
                "data": payload.fields.compositions.data,
            },
            "search": {
                "ehr_id": payload.fields.search.ehr_id,
                "composition_id": payload.fields.search.composition_id,
                "template_id": payload.fields.search.template_id,
                "search_nodes": payload.fields.search.search_nodes,
            },
            "search_nodes": {
                "path": payload.fields.search.path,
                "data": payload.fields.search.data,
            },
        }

        coding_opts = {
            "dictionary": payload.coding.archetype_ids.dictionary,
            "atcodes": payload.coding.archetype_ids.atcodes.model_dump(),
        }

        ingest_options = {
            "store_canonical": payload.collections.compositions.store_canonical,
            "canonical_collection": payload.collections.compositions.name,
            "search_enabled": payload.collections.search.enabled,
            "atlas_index_name": payload.collections.search.atlas_index_name,
            "field_map": field_map,
            "coding": coding_opts,
        }

        # Update shared search settings for downstream AQL/search usage
        settings.search_config.search_collection = payload.collections.search.name
        settings.search_config.flatten_collection = payload.collections.semiflattened_compositions.name
        if payload.collections.search.atlas_index_name:
            settings.search_config.search_index_name = payload.collections.search.atlas_index_name

        # Reinitialize ingestion components with the provided config
        result = await apply_ingestion_config(
            app=request.app,
            config=config_internal,
            mappings_inline=mappings_inline,
            use_mappings_file=payload.mappings.use_file,
            mappings_path=payload.mappings.path,
            field_map=field_map,
            coding_opts=coding_opts,
            ingest_options=ingest_options,
            strategy_raw=strategy_raw,
        )
        return {
            "status": "success",
            "message": "Ingestion configuration applied.",
            "mappings_path": result.get("mappings_path"),
            "strategy": strategy.model_dump() if strategy else None,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to apply configuration: {exc}",
        )
