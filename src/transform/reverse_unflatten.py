# kehrnel/transform/reverse_unflatten.py

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
        if code>=0: return self.ar.get(code,str(code))
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
    exp = ShortcutExpander(keys, vals)
    cb  = CodeBook(ar_map, at_map)
    path2obj = {}
    root = None
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
    return root