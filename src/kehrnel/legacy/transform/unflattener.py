# kehrnel/transform/unflattener.py

import copy
from typing import Any, Dict, List, Optional

class ShortcutExpander:
    def __init__(self, keys, vals):
        self.kmap = {v:k for k,v in keys.items()}
        self.vmap = {v:k for k,v in vals.items()}
    def key_long(self, s): return self.kmap.get(s,s)
    def __call__(self, obj: Any, _inside=False):
        if isinstance(obj, dict):
            return { self.key_long(k): self(v, _inside=(k=="T")) for k,v in obj.items() }
        if isinstance(obj, list):
            return [ self(x, _inside=_inside) for x in obj ]
        if isinstance(obj, str) and _inside:
            return self.vmap.get(obj,obj)
        return obj

class CodeBook:
    def __init__(self, ar_map, at_map):
        self.ar = {v:k for k,v in ar_map.items()}
        self.at = at_map
    
    def decode(self, code:int)->str:
        if code>=0: 
            # Look up the archetype identifier from the reverse mapping
            archetype_id = self.ar.get(code)
            if archetype_id:
                return archetype_id
            # Fallback to string representation if not found
            return str(code)
        raw = self.at.get(code)
        if raw and raw.lower().startswith("at"):
            return f"at{int(raw[2:]):04d}"
        return f"at{-code:04d}"

def _insert_at(parent, kp, value, li, exp):
    cur = parent
    for i,token in enumerate(kp):
        key = exp.key_long(token)
        last = (i==len(kp)-1)
        if last:
            if li is None:
                cur[key] = value
            else:
                arr = cur.setdefault(key, [])
                while len(arr)<=li: arr.append(None)
                if arr[li] is None: arr[li] = value
                else: arr.append(value)
        else:
            cur = cur.setdefault(key, {})

def rebuild_composition(flat, ar_map, at_map, keys, vals):
    """
    Reconstructs a canonical openEHR Composition from its flattened representation.
    """
    from .at_code_codec import CODE_BOOK, CACHE_LOCK
    
    exp = ShortcutExpander(keys, vals)
    
    # Get the forward mappings from the loaded codes (CodeBook.__init__ will reverse them)
    with CACHE_LOCK:
        ar_forward = CODE_BOOK["ar_code"].copy()  # {string: int}
        at_forward = CODE_BOOK["at"].copy()       # {string: int}
    
    # Use the loaded forward mappings (CodeBook will reverse them internally)
    cb = CodeBook(ar_forward, at_forward)
    path2obj = {}
    root = None
    
    def decode_archetype_ids(obj):
        """Recursively decode numeric archetype_id fields back to strings."""
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                if key == "archetype_id" and isinstance(value, int):
                    # Decode numeric archetype_id back to proper object structure
                    decoded_value = cb.decode(value)
                    result[key] = {"value": decoded_value}
                elif isinstance(value, (dict, list)):
                    result[key] = decode_archetype_ids(value)
                else:
                    result[key] = value
            return result
        elif isinstance(obj, list):
            return [decode_archetype_ids(item) for item in obj]
        else:
            return obj
    
    for node in flat["cn"]:
        d = copy.deepcopy(node["data"])
        ani = d.pop("archetype_node_id")
        kp  = node.get("kp", []) or node.get("ak", [])
        li  = node.get("li")
        d = exp(d)
        d["archetype_node_id"] = cb.decode(ani)
        p  = node["p"]
        parent_p = p.split(".",1)[1] if "." in p else None
        if parent_p is None:
            root = d
        else:
            parent = path2obj[parent_p]
            _insert_at(parent, kp, d, li, exp)
        path2obj[p] = d
    
    # Decode any remaining numeric archetype_id fields in the final result
    if root:
        root = decode_archetype_ids(root)
    
    return root