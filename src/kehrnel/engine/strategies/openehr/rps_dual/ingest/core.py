"""Legacy transformer adapter retained for CLI/smoke compatibility.

This adapter wraps ``CompositionFlattener`` with local-only defaults:
- no DB dependency
- no dictionary/shortcut dependency
- stable dotted path separator
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from kehrnel.engine.strategies.openehr.rps_dual import ingest as _ingest_pkg
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener


class Transformer:
    def __init__(self, cfg: Optional[Dict[str, Any]] = None, role: str = "primary"):
        self.cfg = cfg or {}
        self.role = role or "primary"
        self._default_mappings_path = (
            Path(_ingest_pkg.__file__).resolve().parent / "config" / "flattener_mappings_f.jsonc"
        )
        self._mappings_content = self._load_mappings_content(self.cfg.get("mappings"))

        self._flattener = CompositionFlattener(
            db=None,
            config={
                "role": self.role,
                "apply_shortcuts": False,
                "paths": {"separator": ":"},
                "collections": {},
            },
            mappings_path=str(self._default_mappings_path),
            mappings_content=self._mappings_content,
            coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
        )

    @staticmethod
    def _load_mappings_content(path: Any) -> Optional[Dict[str, Any]]:
        if not path:
            return None
        try:
            file_path = Path(str(path))
            raw = file_path.read_text(encoding="utf-8")
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = yaml.safe_load(raw)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def flatten(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        composition = {
            "_id": raw.get("_id", "comp-1"),
            "ehr_id": raw.get("ehr_id", "ehr-1"),
            "composition_version": raw.get("composition_version"),
            "canonicalJSON": raw.get("canonicalJSON") or raw,
        }
        base, search = self._flattener.transform_composition(composition)
        output = {"base": base}
        if search:
            output["search"] = search
        return output


__all__ = ["Transformer"]
