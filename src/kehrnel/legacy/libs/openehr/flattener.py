from __future__ import annotations

from typing import Any, Dict, Optional, Union
from motor.motor_asyncio import AsyncIOMotorDatabase

# Re-export the existing CompositionFlattener until full extraction happens.
from kehrnel.legacy.transform.flattener_g import CompositionFlattener
from kehrnel.legacy.transform.core import load_default_cfg, Transformer
from kehrnel.legacy.transform.shortcuts import ShortcutApplier
from kehrnel.legacy.transform.rules_engine import RulesEngine
from kehrnel.legacy.transform.at_code_codec import (
    AtCodeCodec,
    load_codes_from_db as _load_codes_from_db,
    set_shared_role,
)


async def get_flattener(
    db: AsyncIOMotorDatabase,
    config: Dict[str, Any],
    mappings_path: str,
    mappings_content: Optional[Union[str, Dict[str, Any]]] = None,
    field_map: Optional[Dict[str, Dict[str, str]]] = None,
    coding_opts: Optional[Dict[str, Any]] = None,
) -> CompositionFlattener:
    """
    Factory wrapper to keep strategy plugins decoupled from the concrete flattener location.
    """
    return await CompositionFlattener.create(
        db=db,
        config=config,
        mappings_path=mappings_path,
        mappings_content=mappings_content,
        field_map=field_map,
        coding_opts=coding_opts,
    )


def get_transformer_with_rules(mappings_path: str, role: str = "primary") -> Transformer:
    cfg = load_default_cfg(None)
    cfg["mappings"] = mappings_path
    set_shared_role(role)
    return Transformer(cfg=cfg, role=role)


def build_shortcuts(path: str | None = None) -> ShortcutApplier:
    return ShortcutApplier(path or "transform/config/shortcuts.json")


def build_rules_engine(mappings_path: str, codec: AtCodeCodec) -> RulesEngine:
    return RulesEngine(mappings_path, codec)


async def load_codes(codec: AtCodeCodec, db, config: Dict[str, Any]):
    await _load_codes_from_db(db, config)
