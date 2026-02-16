from __future__ import annotations

from copy import deepcopy
from typing import Optional, Dict, Any, Union, List
from pydantic import BaseModel, Field


# --- Collection Configuration ---

class AtlasIndexCfg(BaseModel):
    name: str = "search_nodes_index"
    definition: Union[str, Dict[str, Any]] = Field(default_factory=dict)


class CompositionsCollectionCfg(BaseModel):
    name: str = "compositions_rps"
    encodingProfile: str = "profile.codedpath"


class SearchCollectionCfg(BaseModel):
    name: str = "compositions_search"
    encodingProfile: str = "profile.search_shortcuts"
    enabled: bool = True
    atlasIndex: Optional[AtlasIndexCfg] = None


class CodesCollectionCfg(BaseModel):
    name: str = "_codes"
    seed: Optional[Union[str, Dict[str, Any]]] = None


class ShortcutsCollectionCfg(BaseModel):
    name: str = "_shortcuts"
    seed: Optional[Union[str, Dict[str, Any]]] = None


class CollectionsCfg(BaseModel):
    compositions: CompositionsCollectionCfg = Field(default_factory=CompositionsCollectionCfg)
    search: SearchCollectionCfg = Field(default_factory=SearchCollectionCfg)
    codes: CodesCollectionCfg = Field(default_factory=CodesCollectionCfg)
    shortcuts: ShortcutsCollectionCfg = Field(default_factory=ShortcutsCollectionCfg)


# --- ID Encoding ---

class IdsCfg(BaseModel):
    ehr_id: str = "string"
    composition_id: str = "objectid"


# --- Paths ---

class PathsCfg(BaseModel):
    separator: str = "."


# --- Fields Configuration ---

class DocumentFieldsCfg(BaseModel):
    ehr_id: str = "ehr_id"
    comp_id: str = "comp_id"
    tid: str = "tid"
    v: str = "v"
    cn: str = "cn"
    sn: str = "sn"


class NodeFieldsCfg(BaseModel):
    p: str = "p"
    pi: str = "pi"
    data: str = "data"


class FieldsCfg(BaseModel):
    document: DocumentFieldsCfg = Field(default_factory=DocumentFieldsCfg)
    node: NodeFieldsCfg = Field(default_factory=NodeFieldsCfg)


# --- Transform / Coding ---

class ArcodesCfg(BaseModel):
    strategy: str = "sequential"
    prefix_replace: Optional[List[Dict[str, str]]] = None


class AtcodesCfg(BaseModel):
    strategy: str = "negative_int"
    store_original: bool = False


class CodingCfg(BaseModel):
    arcodes: ArcodesCfg = Field(default_factory=ArcodesCfg)
    atcodes: AtcodesCfg = Field(default_factory=AtcodesCfg)


class TransformCfg(BaseModel):
    mappings: Optional[Union[str, Dict[str, Any]]] = None
    apply_shortcuts: bool = True
    coding: CodingCfg = Field(default_factory=CodingCfg)


# --- Main Strategy Config ---

class RPSDualConfig(BaseModel):
    """Main strategy configuration model (portal-visible)."""
    collections: CollectionsCfg = Field(default_factory=CollectionsCfg)
    ids: IdsCfg = Field(default_factory=IdsCfg)
    paths: PathsCfg = Field(default_factory=PathsCfg)
    fields: FieldsCfg = Field(default_factory=FieldsCfg)
    transform: TransformCfg = Field(default_factory=TransformCfg)


# --- Bulk Config (operational, not portal-visible) ---

class SourceCfg(BaseModel):
    connection_string: str = "mongodb://localhost:27017"
    database_name: str = "source_openEHR"
    collection_name: str = "samples"


class TargetCfg(BaseModel):
    connection_string: str = "mongodb://localhost:27017"
    database_name: str = "openEHR"


class BulkConfig(BaseModel):
    """Bulk operation configuration (CLI/API only)."""
    role: str = "primary"
    source: SourceCfg = Field(default_factory=SourceCfg)
    target: TargetCfg = Field(default_factory=TargetCfg)
    batch_size: int = 100
    patient_limit: Optional[int] = None
    clean_collections: bool = False
    reset_used_flags: bool = False
    codes_refresh_interval: int = 60
    replication_factor: int = 1
    parallel_workers: int = 4
    dry_run: bool = False
    resume_from: Optional[str] = None


# --- Normalization Functions ---

def normalize_config(raw: Dict[str, Any]) -> RPSDualConfig:
    """Normalize raw config dict into RPSDualConfig model."""
    if not raw:
        return RPSDualConfig()
    # Backward-compatible aliases:
    # - older clients used collections.search.atlas_index_name (string)
    # - current config uses collections.search.atlasIndex.name
    data = deepcopy(raw)
    collections = data.get("collections") if isinstance(data, dict) else None
    if isinstance(collections, dict):
        search = collections.get("search")
        if isinstance(search, dict):
            atlas_index_name = search.get("atlas_index_name")
            if isinstance(atlas_index_name, str) and atlas_index_name.strip():
                atlas_name = atlas_index_name.strip()
                atlas_idx = search.get("atlasIndex")
                if not isinstance(atlas_idx, dict):
                    search["atlasIndex"] = {"name": atlas_name}
                else:
                    atlas_idx["name"] = atlas_name
    return RPSDualConfig(**data)


def normalize_bulk_config(raw: Dict[str, Any]) -> BulkConfig:
    """Normalize raw bulk config dict into BulkConfig model."""
    if not raw:
        return BulkConfig()
    return BulkConfig(**raw)


def build_schema_config(cfg: RPSDualConfig) -> Dict[str, Dict[str, Any]]:
    """Build schema config for query compilation."""
    comp_coll = cfg.collections.compositions
    search_coll = cfg.collections.search
    fields = cfg.fields

    composition_schema = {
        "composition_array": fields.document.cn,
        "path_field": fields.node.p,
        "data_field": fields.node.data,
        "ehr_id": fields.document.ehr_id,
        "comp_id": fields.document.comp_id,
        "collection": comp_coll.name,
    }

    search_schema = {
        "composition_array": fields.document.sn,
        "path_field": fields.node.p,
        "data_field": fields.node.data,
        "ehr_id": fields.document.ehr_id,
        "comp_id": fields.document.comp_id,
        "index_name": search_coll.atlasIndex.name if search_coll.atlasIndex else None,
        "lookup_from": comp_coll.name,
        "lookup_as": "full_composition",
        "collection": search_coll.name,
    }

    return {"composition": composition_schema, "search": search_schema}


def build_flattener_config(
    strategy_cfg: RPSDualConfig,
    bulk_cfg: Optional[BulkConfig] = None,
) -> Dict[str, Any]:
    """Build config dict for CompositionFlattener."""
    bulk = bulk_cfg or BulkConfig()

    return {
        "role": bulk.role,
        "apply_shortcuts": strategy_cfg.transform.apply_shortcuts,
        "paths": {"separator": strategy_cfg.paths.separator},
        "ids": {
            "ehr_id": strategy_cfg.ids.ehr_id,
            "composition_id": strategy_cfg.ids.composition_id,
        },
        "collections": {
            "compositions": {
                "name": strategy_cfg.collections.compositions.name,
                "encodingProfile": strategy_cfg.collections.compositions.encodingProfile,
            },
            "search": {
                "name": strategy_cfg.collections.search.name,
                "encodingProfile": strategy_cfg.collections.search.encodingProfile,
            },
        },
        "composition_fields": {
            "nodes": strategy_cfg.fields.document.cn,
            "path": strategy_cfg.fields.node.p,
            "path_instance": strategy_cfg.fields.node.pi,
            "data": strategy_cfg.fields.node.data,
            "ehr_id": strategy_cfg.fields.document.ehr_id,
            "comp_id": strategy_cfg.fields.document.comp_id,
            "template_id": strategy_cfg.fields.document.tid,
            "version": strategy_cfg.fields.document.v,
        },
        "search_fields": {
            "nodes": strategy_cfg.fields.document.sn,
            "path": strategy_cfg.fields.node.p,
            "data": strategy_cfg.fields.node.data,
            "ehr_id": strategy_cfg.fields.document.ehr_id,
            "template_id": strategy_cfg.fields.document.tid,
        },
        "target": {
            "codes_collection": strategy_cfg.collections.codes.name,
            "shortcuts_collection": strategy_cfg.collections.shortcuts.name,
        },
    }


def build_coding_opts(cfg: RPSDualConfig) -> Dict[str, Any]:
    """Build coding options dict for flattener."""
    coding = cfg.transform.coding
    return {
        "arcodes": {
            "strategy": coding.arcodes.strategy,
            "prefix_replace": coding.arcodes.prefix_replace or [],
        },
        "atcodes": {
            "strategy": coding.atcodes.strategy,
            "store_original": coding.atcodes.store_original,
        },
    }
