# src/transform/flattener_g.py

import json
import re
import copy
from datetime import timezone
from typing import Any, Dict, List, Tuple, Optional

from dateutil import parser
from motor.motor_asyncio import AsyncIOMotorDatabase

# Constants from original ingestionOptimized.py
LOCATABLE = {
    "COMPOSITION","SECTION","ADMIN_ENTRY","OBSERVATION","EVALUATION",
    "INSTRUCTION","ACTION","CLUSTER","ITEM_TREE","ITEM_LIST","ITEM_SINGLE",
    "ITEM_TABLE","ELEMENT","HISTORY","EVENT","POINT_EVENT","INTERVAL_EVENT",
    "ACTIVITY","ISM_TRANSITION","INSTRUCTION_DETAILS","CARE_ENTRY",
    "PARTY_PROXY","EVENT_CONTEXT"
}

NON_ARCHETYPED_RM = {
    "HISTORY","ISM_TRANSITION", "EVENT_CONTEXT"
}

LOC_HINT = {"archetype_node_id", "archetype_details"}
SKIP_ATTRS = {}

_START_DOTTED = -10_000
_AT_ROOT = re.compile(r"^at0*([1-9][0-9]*)$", re.I)
_AT_DOTTED = re.compile(r"^at[0-9]+(?:\.[0-9]{1,4})+$", re.I)

# Use a relative import for your new exceptions file
from .exceptions_g import UnknownCodeError

# Constants can remain at the module level
LOCATABLE = {
    "COMPOSITION", "SECTION", "ADMIN_ENTRY", "OBSERVATION", "EVALUATION",
    "INSTRUCTION", "ACTION", "CLUSTER", "ITEM_TREE", "ITEM_LIST", "ITEM_SINGLE",
    "ITEM_TABLE", "ELEMENT", "HISTORY", "EVENT", "POINT_EVENT", "INTERVAL_EVENT",
    "ACTIVITY", "ISM_TRANSITION", "INSTRUCTION_DETAILS", "CARE_ENTRY",
    "PARTY_PROXY", "EVENT_CONTEXT"
}
NON_ARCHETYPED_RM = {"HISTORY", "ISM_TRANSITION", "EVENT_CONTEXT"}
LOC_HINT = {"archetype_node_id", "archetype_details"}
SKIP_ATTRS = {}
_AT_ROOT = re.compile(r"^at0*([1-9][0-9]*)$", re.I)
_AT_DOTTED = re.compile(r"^at[0-9]+(?:\.[0-9]{1,4})+$", re.I)
_START_DOTTED = -10_000

class CompositionFlattener:
    """
    Encapsulates the logic for transforming and flattening OpenEHR compositions.
    """
    def __init__(self, db: AsyncIOMotorDatabase, config: dict, mappings_path: str):
        self.db = db
        self.config = config
        self.role = config.get("role", "primary").lower()
        self.apply_shortcuts = config.get("apply_shortcuts", True)

        # Encapsulate state previously stored in globals
        self.code_book: Dict[str, Dict[str, int]] = {"ar_code": {}, "at": {}}
        self.seq: Dict[str, int] = {"ar_code": 0, "at": -1}
        self.active_rules: Dict[int, List[dict]] = {}
        self.raw_rules: Dict[str, Any] = {}
        self.shortcut_keys: Dict[str, str] = {}
        self.shortcut_vals: Dict[str, str] = {}

        # Load mappings synchronously from file
        self._load_mappings(mappings_path)
    
    @classmethod
    async def create(cls, db: AsyncIOMotorDatabase, config: dict, mappings_path: str):
        """Asynchronous factory to create and initialize an instance."""
        instance = cls(db, config, mappings_path)
        await instance._load_codes_from_db()
        if instance.apply_shortcuts:
            await instance._load_shortcuts_from_db()
        return instance

    # --- Public API Method (Remains Synchronous) ---
    def transform_composition(self, raw_doc: dict) -> tuple[dict, dict]:
        """
        Takes a raw composition document from the source collection and returns
        the flattened base document and the search document.
        """
        comp = raw_doc["canonicalJSON"]
        root_aid = self._archetype_id(comp) or "unknown"
        template_id = self._alloc_code("ar_code", root_aid)

        # STEP 1: Complete flattening regardless of mapping rules
        cn: List[dict] = []
        self._walk(comp, ancestors=(), cn=cn, kp_chain=[], list_index=None)

        # STEP 2: Apply mapping rules only for search document generation
        rules = self._get_rules_for(template_id, root_aid)
        sn: List[dict] = []
        for node in cn:
            if "p" not in node:  # Skip nodes without path
                continue
            parts = node["p"].split(".")
            pc_set = {".".join(parts[:i]) for i in range(1, len(parts) + 1)}
            dblock = node["data"]

            for rule in rules:
                if self._rule_matches(rule, pc_set, parts, dblock):
                    slim = self._apply_rule(rule, node, dblock)
                    if slim:
                        sn.append(slim)

        # STEP 3: Clean up ancestry markers (used only during processing)
        for node in cn:
            node.pop("_anc", None)

        # STEP 4: Build final documents
        base_doc = {
            "ehr_id": raw_doc["ehr_id"],
            "comp_id": raw_doc["_id"],
            "v": raw_doc.get("composition_version"),
            "tid": template_id,
            "cn": cn,
        }
        search_doc = {
            "_id": raw_doc["_id"],
            "ehr_id": raw_doc["ehr_id"],
            "tid": template_id,
            "sn": sn,
        }

        # STEP 5: Apply shortcuts if enabled
        if self.apply_shortcuts:
            base_doc = self._apply_sc_deep(base_doc)
            search_doc = self._apply_sc_deep(search_doc)

        return base_doc, search_doc

    # --- Internal Helper Methods (Refactored from original script) ---
    async def _load_codes_from_db(self):
        codes_col = self.db[self.config["target"]["codes_collection"]]
        doc = await codes_col.find_one({"_id": "ar_code"}) or {}

        ar_book: dict[str, int] = {}
        for rm, subtree in doc.items():
            if rm in ("_id", "_max", "_min", "unknown", "at"): continue
            if isinstance(subtree, dict):
                for name, vers in subtree.items():
                    if isinstance(vers, dict):
                        for ver, code in vers.items():
                            ar_book[f"{rm}.{name}.{ver}"] = code
        
        at_book = {k: int(v) for k, v in (doc.get("at") or {}).items() if isinstance(v, int)}
        
        self.code_book["ar_code"] = ar_book
        self.code_book["at"] = at_book
        self.seq["ar_code"] = doc.get("_max", self.seq["ar_code"])
        self.seq["at"] = doc.get("_min", self.seq.get("at", -1))
        print(f"Loaded {len(ar_book)} ar_codes and {len(at_book)} at_codes from DB.")

    async def flush_codes_to_db(self):
        """Persists the current in-memory codebook back to the database."""
        codes_col = self.db[self.config["target"]["codes_collection"]]
        nested: Dict[str, Any] = {
            "at": self.code_book.get("at", {}),
            "_min": self.seq.get("at", -1),
        }
        book = self.code_book.get("ar_code", {})
        max_code = max((code for code in book.values() if isinstance(code, int)), default=0)

        for sid, code in book.items():
            parts = sid.split(".")
            rm, name, ver = parts[0], parts[1] if len(parts) > 1 else "", ".".join(parts[2:])
            nested.setdefault(rm, {}).setdefault(name, {})[ver] = code
        
        nested["_max"] = max(max_code, self.seq.get("ar_code", 0))
        
        await codes_col.replace_one(
            {"_id": "ar_code"},
            {"_id": "ar_code", **nested},
            upsert=True,
        )
        print("Flushed codes to DB.")

    async def _load_shortcuts_from_db(self):
        sc_col = self.db[self.config["target"]["shortcuts_collection"]]
        sc_doc = await sc_col.find_one({"_id": "shortcuts"}) or {}
        self.shortcut_keys.update(sc_doc.get("keys", {}))
        self.shortcut_vals.update(sc_doc.get("values", {}))

    def _load_mappings(self, path: str):
        with open(path, encoding="utf-8") as f:
            txt = f.read()
        txt = re.sub(r"//.*?$|/\*.*?\*/", "", txt, flags=re.M | re.S)
        self.raw_rules = json.loads(txt).get("templates", {})

    def _at_code_to_int(self, at: str) -> int:
        s = at.lower()
        if s in self.code_book["at"]:
            return self.code_book["at"][s]
        if self.role != "primary":
            raise UnknownCodeError("at", s)

        m_root = _AT_ROOT.match(s)
        if m_root:
            code = -int(m_root.group(1))
            self.code_book["at"][s] = code
            self.seq["at"] = min(self.seq["at"], code)
            return code
        if _AT_DOTTED.match(s):
            self.seq["at"] = self.seq["at"] - 1 if self.seq["at"] <= _START_DOTTED else _START_DOTTED
            code = self.seq["at"]
            self.code_book["at"][s] = code
            return code
        raise ValueError(f"Could not parse at-code: {at}")

    def _alloc_code(self, key: str, sid: str) -> int:
        sid_lower = sid.lower()
        if key == "ar_code" and sid_lower.startswith("at"):
            return self._at_code_to_int(sid)
        
        book = self.code_book.setdefault(key, {})
        if sid in book:
            return book[sid]

        if self.role != "primary":
            raise UnknownCodeError(key, sid)
        
        self.seq.setdefault(key, 0)
        self.seq[key] += 1
        code = self.seq[key]
        book[sid] = code
        return code

    def _walk(self, node: dict, ancestors: Tuple[int, ...], cn: List[dict], *, kp_chain: List[str], list_index: Optional[int]):
        """Flatten *node* into the cn[ ] array. Direct adaptation from proven ingestionOptimized.py"""

        # ── 0.  numeric code of *this* node ─────────────────────────────
        aid = self._archetype_id(node)
        if aid:
            code = self._at_code_to_int(aid) if aid.lower().startswith("at") \
                   else self._alloc_code("ar_code", aid)
        else:
            code = 0                    # non-archetyped RM object (HISTORY, EC …)

        is_root = not ancestors
        emit    = (list_index is not None or is_root or self._split_me_as_a_new_node(node)) \
                  and code is not None

        # ── 1.  collect scalar members  ─────────────────────────────────
        scalars: Dict[str, Any] = {}
        for k, v in node.items():

            # 1a – drop attributes explicitly skipped
            if k in SKIP_ATTRS:
                continue

            # 1b – child is its own locatable node → don't duplicate here
            if isinstance(v, dict) and self._is_locatable(v) and self._split_me_as_a_new_node(v):
                continue

            # 1c – EVENT_CONTEXT, HISTORY … keep only **their** scalars
            if isinstance(v, dict) and self._is_locatable(v) \
               and v.get("_type") in NON_ARCHETYPED_RM:
                scalars[k] = self._strip_locatables(v)
                continue

            # 1d – list that contains any splittable locatable → skip whole list
            if isinstance(v, list) and any(self._is_locatable(x) and
                                           self._split_me_as_a_new_node(x) for x in v):
                continue

            # 1e – ordinary scalar
            scalars[k] = v

        # ── 2.  date coercion + archetype_node_id ──────────────────────────────
        scalars = self._to_bson_dates(scalars)   
        scalars["archetype_node_id"] = code     
        
        # ── 2b. shrink archetype_details.archetype_id / ai -----------------
        ad = scalars.get("archetype_details") or scalars.get("ad")
        if isinstance(ad, dict):
            # depending on whether shortcuts have already run
            ai_obj = ad.get("archetype_id") or ad.get("ai")
            if isinstance(ai_obj, dict):
                # full form: {"value": "..."}      | short form: {"v": "..."}
                sid = ai_obj.get("value") or ai_obj.get("v")
                if isinstance(sid, str):
                    num = self._alloc_code("ar_code", sid)      # look-up / allocate
                    if num is not None:
                        # always store the int under the canonical long key;
                        # later apply_sc will rename 'archetype_id' → 'ai'
                        ad["archetype_id"] = num

        payload = scalars                            

        # ── 3.  emit the node (if required) ────────────────────────────
        if emit:
            leaf_path = ".".join([str(code)] + [str(a) for a in reversed(ancestors)])

            cn_node: Dict[str, Any] = {
                "data": payload,
                "p": leaf_path,
                "_anc": ancestors,           # kept only until reverse stage
            }
            if kp_chain:                     # store only when non-empty
                cn_node["kp"] = kp_chain[:]
            if list_index is not None:       # add li only when parent is a list
                cn_node["li"] = list_index

            cn.append(cn_node)

        # ── 4.  recurse into children  ─────────────────────────────────
        new_anc = ancestors + ((code,) if emit else ())
        base_kp = [] if emit else kp_chain          # <== key trick

        for k, v in node.items():
            k_long = self._sc_key(k)  # Use shortcut keys if available

            if isinstance(v, dict) and self._is_locatable(v):
                self._walk(v, new_anc, cn,
                     kp_chain=base_kp + [k_long],
                     list_index=None)

            elif isinstance(v, list):
                for idx, itm in enumerate(v):
                    if self._is_locatable(itm):
                        self._walk(itm, new_anc, cn,
                             kp_chain=base_kp + [k_long],
                             list_index=idx)

    # --- Utility functions converted to private methods ---
    
    @staticmethod
    def _archetype_id(obj: dict) -> str | None:
        ad = obj.get("archetype_details", {})
        ai = ad.get("archetype_id")
        if isinstance(ai, dict) and ai.get("value"): return ai["value"].strip()
        if isinstance(ai, str): return ai.strip()
        ani = obj.get("archetype_node_id")
        if isinstance(ani, str): return ani.strip()
        return None

    @staticmethod
    def _is_locatable(obj: Any) -> bool:
        if not isinstance(obj, dict): return False
        t = obj.get("_type")
        if isinstance(t, str) and t in LOCATABLE: return True
        return any(k in obj for k in LOC_HINT)

    def _split_me_as_a_new_node(self, obj: dict) -> bool:
        """
        Keep the current flattening granularity: emit a separate cn[ ] entry
        **only** for dictionaries that already carry an 'archetype_node_id'.
        Everything else stays embedded in its parent.
        """
        return isinstance(obj, dict) and "archetype_node_id" in obj
        
    def _sc_key(self, key: str) -> str:
        """Return shortcut key if available, otherwise return original key."""
        return self.shortcut_keys.get(key, key)

    def _strip_locatables(self, d: dict) -> dict:
        return {k: v for k, v in d.items() if not (
            (isinstance(v, dict) and self._is_locatable(v)) or
            (isinstance(v, list) and any(self._is_locatable(x) for x in v))
        )}

    def _get_rules_for(self, template: int, original_num: str) -> list[dict]:
        if template in self.active_rules:
            return self.active_rules[template]
        
        # Debug: log the template lookup
        print(f"DEBUG: Looking for template '{original_num}' in mappings")
        print(f"DEBUG: Available templates: {list(self.raw_rules.keys())}")
        
        raw_block = self.raw_rules.get(original_num, {})
        if not raw_block:
            # Try version-flexible matching as fallback
            base_name = original_num.rsplit('.v', 1)[0] if '.v' in original_num else original_num
            for rule_key in self.raw_rules.keys():
                if rule_key.startswith(base_name + '.v'):
                    print(f"DEBUG: Found version-flexible match: {rule_key}")
                    raw_block = self.raw_rules[rule_key]
                    break
        
        compiled: list[dict] = []
        for r in raw_block.get("rules", []):
            w = r["when"]
            p_raw = w.get("pathChain", [])
            c_raw = w.get("contains", [])
            extra = [(k, v) for k, v in w.items() if k not in ("pathChain", "contains")]
            
            try:
                compiled.append({
                    "_path": tuple(self._seg_to_int(x) for x in p_raw),
                    "_cont": tuple(self._seg_to_int(x) for x in reversed(c_raw)),
                    "_extra": extra, "copy": r["copy"],
                })
            except (ValueError, UnknownCodeError) as e:
                print(f"Warning: Skipping rule due to code error: {e}")

        print(f"DEBUG: Compiled {len(compiled)} rules for template")
        self.active_rules[template] = compiled
        return compiled
        
    def _seg_to_int(self, seg: str | int) -> int:
        if isinstance(seg, int): return seg
        if isinstance(seg, str):
            if seg.lower().startswith("at"): return self._at_code_to_int(seg)
            return self._alloc_code("ar_code", seg)
        raise TypeError("Segment must be str or int")

    def _apply_sc_deep(self, o):
        if isinstance(o, dict):
            return {self.shortcut_keys.get(k, k): self._apply_sc_deep(v) for k, v in o.items()}
        if isinstance(o, list):
            return [self._apply_sc_deep(x) for x in o]
        if isinstance(o, str) and o in self.shortcut_vals:
            return self.shortcut_vals[o]
        return o

    @staticmethod
    def _dpath_get(obj: Any, path: str) -> Any | None:
        parts = path.split(".")
        def walk(cur, idx):
            if idx == len(parts): return cur
            part = parts[idx]
            if isinstance(cur, dict): return walk(cur.get(part), idx + 1)
            if isinstance(cur, list):
                if part.isdigit() and 0 <= int(part) < len(cur): return walk(cur[int(part)], idx + 1)
                if len(cur) == 1: return walk(cur[0], idx)
                coll = [v for v in [walk(item, idx) for item in cur] if v is not None]
                if not coll: return None
                return coll[0] if len(coll) == 1 else coll
            return None
        return walk(obj, 0)
        
    def _rule_matches(self, rule, pc_set, parts, dblock) -> bool:
        if rule["_path"] and ".".join(str(x) for x in rule["_path"]) not in pc_set:
            return False
        if rule["_cont"]:
            j = 0
            for code in reversed(parts[1:]):
                if code == str(rule["_cont"][j]):
                    j += 1
                    if j == len(rule["_cont"]): break
            if j != len(rule["_cont"]): return False
        if any(self._dpath_get({"data": dblock}, path) != val_req for path, val_req in rule["_extra"]):
            return False
        return True

    def _apply_rule(self, rule, node, dblock) -> dict:
        slim: dict = {}
        for expr in rule["copy"]:
            if expr.startswith("data."):
                sub = expr[5:]
                val = self._dpath_get(dblock, sub)
                if val is None: continue
                cur = slim.setdefault("data", {})
                path_parts = sub.split(".")
                for part in path_parts[:-1]:
                    cur = cur.setdefault(part, {})
                cur[path_parts[-1]] = val
            elif expr == "p":
                slim["p"] = node["p"]
        return slim
        
    def _to_bson_dates(self, obj):
        if isinstance(obj, dict):
            t0 = obj.get("_type")
            if t0 in ("DV_DATE_TIME", "DV_DATE") and isinstance(obj.get("value"), str):
                try:
                    dt = parser.isoparse(obj["value"])
                    if dt.tzinfo is not None: dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    new = obj.copy(); new["value"] = dt; return new
                except Exception: return obj
            return {k: self._to_bson_dates(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._to_bson_dates(item) for item in obj]
        return obj