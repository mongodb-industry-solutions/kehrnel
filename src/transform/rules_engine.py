# kehrnel/transform/rules_engine.py

import yaml, threading
from typing import Any, Dict, List, Tuple
from .at_code_codec import AtCodeCodec

_thread_loc = threading.local()

class RulesEngine:
    def __init__(self, mappings_yaml: str, codec: AtCodeCodec):
        self.codec = codec
        with open(mappings_yaml, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        self.raw_templates = cfg.get("rules", []) or cfg.get("rules", {})
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