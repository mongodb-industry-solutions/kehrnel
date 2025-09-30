import json5 
import threading
from typing import Any, Dict, List, Tuple
from .at_code_codec import AtCodeCodec

_thread_loc = threading.local()

class RulesEngine:
    def __init__(self, mappings_file: str, codec: AtCodeCodec):
        self.codec = codec
        with open(mappings_file, 'r', encoding='utf-8') as f:
            cfg = json5.load(f) 
        
        
        self.raw_templates = cfg.get("templates", {})
        self._cache: Dict[str, List[Dict]] = {}

    def _seg_to_int(self, seg: Any) -> int:
        if isinstance(seg, int):
            return seg
        s = str(seg)
        if s.lower().startswith("at"):
            n = self.codec.at_code_to_int(s)
            if n is None:
                raise ValueError(f"bad at-code {s}")
            return n
        return self.codec.alloc("ar_code", s) or 0

    def get(self, template_sid: str) -> List[Dict]:
        if template_sid in self._cache:
            return self._cache[template_sid]
        rules = []
        block = self.raw_templates.get(template_sid, {}).get("rules", [])
        for r in block:
            when = r["when"]
            pathChain = tuple(self._seg_to_int(x) for x in when.get("path_chain", []))
            contains  = tuple(self._seg_to_int(x) for x in when.get("contains", []))
            extra = [(k, v) for k,v in when.items() if k not in ("path_chain","contains")]
            rules.append({
                "_path": pathChain,
                "_cont": contains,
                "_extra": extra,
                "copy": r["copy"]
            })
        self._cache[template_sid] = rules
        return rules

_ENGINE: RulesEngine | None = None

def get_rules_for(template_sid: str,
                  *,
                  mappings_yaml: str = "transform/config/mappings.yaml",
                  codec: AtCodeCodec | None = None) -> List[Dict]:
    """
    Public helper used by `_bulk_flatten.py`.
    Lazily initialises a **single** RulesEngine instance.
    """
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = RulesEngine(mappings_yaml, codec or AtCodeCodec())
    return _ENGINE.get(template_sid)