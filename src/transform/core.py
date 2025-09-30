# transform/core.py
from __future__ import annotations
import re
import json, copy, threading
import json5  
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from .at_code_codec  import AtCodeCodec
from .rules_engine   import RulesEngine 
from .shortcuts      import ShortcutApplier
from ._bulk_flatten  import walk, dpath_get          
from .unflattener import rebuild_composition

__all__ = ["Transformer", "load_default_cfg"]

DEFAULT_CFG = Path(__file__).parent / "config" / "default_config.jsonc"

def _strip_jsonc_comments(text: str) -> str:
    text = re.sub(r"//.*?$",   "", text, flags=re.M)
    text = re.sub(r"/\*.*?\*/","", text, flags=re.S)
    text = (text.replace("\u2010","-").replace("\u2011","-")
                 .replace("\u2012","-").replace("\u2013","-")
                 .replace("\u2014","-").replace("\u2015","-")
                 .replace("\u00a0"," "))
    text = re.sub(r",\s*(?=[}\]])", "", text)

    return text

def load_default_cfg(path: Path | None) -> dict:
    cfg_path = path if path else DEFAULT_CFG
    text = cfg_path.read_text(encoding="utf-8")
    return json5.loads(text)

class Transformer:
    """
    Stateless helper that can **flatten** or **reverse** a composition.

    Parameters
    ----------
    cfg : dict
        Parsed `default_config.json` + user overrides.
    role : {"primary","secondary"}
        • *primary* may allocate new codes  
        • *secondary* never allocates – it will skip docs with unknown codes
    """
    def __init__(self, cfg: dict, *, role: str = "primary"):
        self.cfg      = cfg
        self.codec   = AtCodeCodec(role)
        from . import at_code_codec as _acc
        _acc.set_shared_role(role)
        self.shortcuts = ShortcutApplier(
            cfg.get("shortcuts", "transform/config/shortcuts.json")
        )
        self.rules    = RulesEngine(cfg["mappings"], self.codec)
        self.role     = role
        self._local   = threading.local()

    # ──────────────────────────────────────────────────────────────
    # flatten one composition
    # ──────────────────────────────────────────────────────────────
    def flatten(self, raw: dict) -> Dict[str, dict]:
        comp      = raw["canonicalJSON"]
        root_sid  = comp.get("archetype_node_id") or "unknown"
        template  = self.codec.alloc("ar_code", root_sid)

        cn: List[dict] = []
        walk(comp, (), cn, kp_chain=[], list_index=None,
             codec=self.codec, shortcuts=self.shortcuts,
             local=self._local)

        # skip if secondary & unknown code encountered
        if getattr(self._local, "bad_code", False) and self.role!="primary":
            raise ValueError("unknown archetype / at-code in secondary mode")

        # rules → sn
        sn: List[dict] = []
        for node in cn:
            if self.rules.match(node, template, comp_sid=root_sid):
                sn.append(self.rules.copy_block(node))

        for n in cn: n.pop("_anc", None)          # drop ancestry

        base = {
            "_id":     raw["_id"],
            "ehr_id":  raw["ehr_id"],
            "comp_id": raw["_id"],
            "v":       raw.get("composition_version"),
            "tid":     template,
            "cn":      cn,
        }
        docs = {"base": self.shortcuts.apply(base)}
        if sn:
            docs["search"] = self.shortcuts.apply({
                "_id":  raw["_id"],
                "ehr_id": raw["ehr_id"],
                "tid":  template,
                "sn":   sn,
            })
        return docs

    # ──────────────────────────────────────────────────────────────
    # reverse one flattened doc
    # ──────────────────────────────────────────────────────────────
    async def load_codes_from_db(self, db, config: dict) -> None:
        """Load codes from the database into the global CODE_BOOK for this transformer."""
        from . import at_code_codec as _acc
        await _acc.load_codes_from_db(db, config)

    def reverse(self, flat: dict) -> dict:
        ar_map = self.codec._book("ar_code")   # maps within codec
        at_map = self.codec._book("at")
        comp   = rebuild_composition(
            flat, ar_map, at_map,
            self.shortcuts.keys, self.shortcuts.vals
        )
        return comp