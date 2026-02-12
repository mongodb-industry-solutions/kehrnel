# kehrnel/transform/shortcuts.py

import json, threading, re
from typing import Any, Dict

_cache = threading.local()

class ShortcutApplier:
    def __init__(self, path: str):
        with open(path, encoding='utf-8') as f:
            doc = json.loads(re.sub(r"//.*?$|/\*.*?\*/", "", f.read(), flags=re.M|re.S))
        self.keys = doc.get("keys", {})
        self.vals = doc.get("values", {})

    def apply(self, o: Any) -> Any:
        if isinstance(o, dict):
            return { self.keys.get(k,k): self.apply(v) for k,v in o.items() }
        if isinstance(o, list):
            return [ self.apply(x) for x in o ]
        if isinstance(o, str) and o in self.vals:
            return self.vals[o]
        return o

    @property
    def expander(self):
        # for reverse: swap keys<->long, vals<->long
        invk = {v:k for k,v in self.keys.items()}
        invv = {v:k for k,v in self.vals.items()}
        def expand(o):
            if isinstance(o, dict):
                return { invk.get(k,k): expand(v) for k,v in o.items() }
            if isinstance(o, list):
                return [ expand(x) for x in o ]
            if isinstance(o, str):
                return invv.get(o,o)
            return o
        return expand