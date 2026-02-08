import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Union

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from kehrnel.api.legacy.app.core.database import get_mongodb_ehr_db
from kehrnel.api.legacy.app.core.config import settings
from kehrnel.legacy.transform.flattener_g import CompositionFlattener
from kehrnel.legacy.transform.core import Transformer, load_default_cfg

# Default mappings path used when no override is provided
DEFAULT_MAPPINGS_PATH = (
    Path(__file__).resolve().parents[2]
    / "kehrnel"
    / "strategies"
    / "openehr"
    / "rps_dual"
    / "ingest"
    / "config"
    / "flattener_mappings_f.jsonc"
)


def _write_temp_mappings(content: Union[str, Dict[str, Any]]) -> str:
    """
    Persist inline mapping content into a temporary file so components that expect
    a file path (e.g., Transformer) can load it.
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix="kehrnel_mappings_"))
    tmp_file = tmp_dir / "mappings.jsonc"
    with tmp_file.open("w", encoding="utf-8") as f:
        if isinstance(content, str):
            f.write(content)
        else:
            json.dump(content, f)
    return str(tmp_file)


async def apply_ingestion_config(
    app: FastAPI,
    config: Dict[str, Any],
    mappings_inline: Optional[Union[str, Dict[str, Any]]] = None,
    use_mappings_file: bool = True,
    mappings_path: Optional[str] = None,
    field_map: Optional[Dict[str, Dict[str, str]]] = None,
    coding_opts: Optional[Dict[str, Any]] = None,
    ingest_options: Optional[Dict[str, Any]] = None,
    strategy_raw: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Initialize or reinitialize the ingestion components using a runtime configuration.

    - mappings_inline: JSON string or dict for inline mapping rules; takes precedence.
    - use_mappings_file: when False and no inline mappings are provided, fall back to an empty ruleset.
    - mappings_path: optional file path to the mappings file when using file-based rules.
    """
    # Resolve source/target DBs (can be the same). Fallback to existing app.state.db if present.
    src_conn = config.get("source", {}).get("connection_string")
    src_db_name = config.get("source", {}).get("database_name")
    tgt_conn = config.get("target", {}).get("connection_string")
    tgt_db_name = config.get("target", {}).get("database_name")

    # Build target DB
    if tgt_conn and tgt_db_name:
        target_client = AsyncIOMotorClient(tgt_conn)
        target_db = target_client[tgt_db_name]
    elif tgt_db_name:
        # Use env connection string but override DB name
        target_client = AsyncIOMotorClient(settings.MONGODB_URI)
        target_db = target_client[tgt_db_name]
    else:
        target_db = await get_mongodb_ehr_db()

    # Build source DB
    if src_conn and src_db_name:
        source_client = AsyncIOMotorClient(src_conn)
        source_db = source_client[src_db_name]
    elif src_db_name:
        source_client = AsyncIOMotorClient(settings.MONGODB_URI)
        source_db = source_client[src_db_name]
    else:
        # fall back to target_db for source
        source_db = target_db

    # Resolve mapping content/path for the flattener
    flattener_mappings_path = mappings_path or str(DEFAULT_MAPPINGS_PATH)
    mapping_content_for_flattener = mappings_inline
    if not use_mappings_file and mappings_inline is None:
        mapping_content_for_flattener = {"templates": {}}

    # Transformer needs a path; create a temp file if using inline/empty content
    transformer_mappings_path = flattener_mappings_path
    if mapping_content_for_flattener is not None or not use_mappings_file:
        content = mapping_content_for_flattener or {"templates": {}}
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Inline mappings must be valid JSON: {exc}") from exc
            content = parsed
        transformer_mappings_path = _write_temp_mappings(content)

    # Initialize flattener
    app.state.flattener = await CompositionFlattener.create(
        db=target_db,
        config=config,
        mappings_path=flattener_mappings_path,
        mappings_content=mapping_content_for_flattener,
        field_map=field_map,
        coding_opts=coding_opts,
    )

    # Initialize transformer (used by other endpoints)
    transformer_config = load_default_cfg(None)
    transformer_config["mappings"] = transformer_mappings_path
    app.state.transformer = Transformer(cfg=transformer_config, role="primary")

    # Store runtime state
    app.state.db = target_db
    app.state.target_db = target_db
    app.state.source_db = source_db
    app.state.config = config
    app.state.mappings_path = transformer_mappings_path
    app.state.ingest_options = ingest_options or {}
    app.state.strategy_raw = strategy_raw or {}

    return {"mappings_path": transformer_mappings_path}
