# src/transform/flattener_g.py

import json
import re
from pathlib import Path
from datetime import timezone
from typing import Any, Dict, List, Tuple, Optional, Union

from dateutil import parser
from motor.motor_asyncio import AsyncIOMotorDatabase
from .exceptions_g import UnknownCodeError
from .encoding import PathCodec
import jsonschema
from bson import ObjectId
import uuid

# Constants from original ingestionOptimized.py
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
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        config: dict,
        mappings_path: str,
        mappings_content: Optional[Union[str, Dict[str, Any]]] = None,
        field_map: Optional[Dict[str, Dict[str, str]]] = None,
        coding_opts: Optional[Dict[str, Any]] = None,
    ):
        self.db = db
        self.config = config
        self.role = config.get("role", "primary").lower()
        self.apply_shortcuts = config.get("apply_shortcuts", True)
        self.field_map = field_map or self._default_field_map()
        self.coding_opts = coding_opts or {}
        self.codes_document_id = (
            self.coding_opts.get("dictionary")
            or self.config.get("coding_dictionary")
            or "ar_code"
        )
        self.at_strategy = (self.coding_opts.get("atcodes") or {}).get("strategy", "negative_int")
        self.ar_strategy = (self.coding_opts.get("arcodes") or {}).get("strategy", "sequential")
        pr = (self.coding_opts.get("arcodes") or {}).get("prefix_replace")
        if isinstance(pr, dict):
            self.ar_prefix_replace: list[dict] = [pr]
        elif isinstance(pr, list):
            self.ar_prefix_replace = [r for r in pr if isinstance(r, dict)]
        else:
            self.ar_prefix_replace = []
        self.store_original_at = (self.coding_opts.get("atcodes") or {}).get("store_original", False)
        self.use_codes_db = not (self.ar_strategy == "literal" and self.at_strategy == "literal")

        # 1. Composition Fields (Source / Intermediate)
        # UPDATED: Matches the key used in main.py ("composition_fields")
        c_fields = config.get("composition_fields", {})

        self.cf_nodes = c_fields.get("nodes", "cn")
        self.cf_data  = c_fields.get("data", "data")
        self.cf_path  = c_fields.get("path", "p")
        self.cf_ap    = c_fields.get("archetype_path", "ap")
        self.cf_anc   = c_fields.get("ancestors", "anc")
        self.cf_ehr   = c_fields.get("ehr_id", "ehr_id")
        self.cf_tmpl  = c_fields.get("template_id", "tid")
        self.cf_ver   = c_fields.get("version", "v")
        self.cf_cid   = c_fields.get("comp_id", "comp_id")

        # 2. Search Fields (Target)
        s_fields = config.get("search_fields", {})

        self.sf_nodes = s_fields.get("nodes", "sn")
        self.sf_data  = s_fields.get("data", "data")
        self.sf_path  = s_fields.get("path", "p")
        self.sf_ap    = s_fields.get("archetype_path", "ap")
        self.sf_anc   = s_fields.get("ancestors", "anc")
        self.sf_ehr   = s_fields.get("ehr_id", "ehr_id")
        self.sf_tmpl  = s_fields.get("template_id", "tid")
        self.sf_score = s_fields.get("score", "score")

        # Encapsulate state previously stored in globals
        self.code_book: Dict[str, Dict[str, int]] = {"ar_code": {}, "at": {}}
        self.seq: Dict[str, int] = {"ar_code": 0, "at": -1}
        self.active_rules: Dict[int, List[dict]] = {}
        self.raw_rules: Dict[str, Any] = {}
        self.shortcut_keys: Dict[str, str] = {}
        self.shortcut_vals: Dict[str, str] = {}
        self.simple_fields: Dict[str, List[dict]] = {}
        self.path_separator = (config.get("paths") or {}).get("separator", ".")
        collections_cfg = config.get("collections", {}) or {}
        self.search_encoding_profile = collections_cfg.get("search", {}).get("encodingProfile")
        self.composition_encoding_profile = collections_cfg.get("compositions", {}).get("encodingProfile")
        self.id_encoding = (config.get("ids") or {}) if isinstance(config.get("ids"), dict) else {}
        self.path_codec = PathCodec(separator=self.path_separator)
        self.template_fields: Dict[str, List[dict]] = {}
        self.compiled_template_rules: Dict[str, List[dict]] = {}

        # Load mappings synchronously from file or inline content
        self._load_mappings(mappings_path, mappings_content)
    
    @classmethod
    async def create(
        cls,
        db: AsyncIOMotorDatabase,
        config: dict,
        mappings_path: str,
        mappings_content: Optional[Union[str, Dict[str, Any]]] = None,
        field_map: Optional[Dict[str, Dict[str, str]]] = None,
        coding_opts: Optional[Dict[str, Any]] = None,
    ):
        """Asynchronous factory to create and initialize an instance."""
        instance = cls(
            db,
            config,
            mappings_path,
            mappings_content,
            field_map=field_map,
            coding_opts=coding_opts,
        )
        if instance.use_codes_db:
            await instance._load_codes_from_db()
        if instance.apply_shortcuts:
            await instance._load_shortcuts_from_db()
        instance._refresh_codec()
        return instance

    # --- Public API Method (Remains Synchronous) ---
    def transform_composition(self, raw_doc: dict) -> tuple[dict, dict]:
        """
        Takes a raw composition document from the source collection and returns
        the flattened base document and the search document using dynamic keys.
        """
        comp = raw_doc["canonicalJSON"]
        root_aid = self._archetype_id(comp) or "unknown"
        template_name = self._template_id(comp) or root_aid
        template_id = self._alloc_code("ar_code", root_aid)

        # STEP 1: Complete flattening regardless of mapping rules
        cn: List[dict] = []
        self._walk(comp, ancestors=(), cn=cn, kp_chain=[], list_index=None)

        # STEP 2: Apply mapping rules only for search document generation
        sn: List[dict] = []
        rules = self._compiled_rules_for_template(template_name)
        for node in cn:
            path_val = node.get(self.cf_path)
            dblock = node.get(self.cf_data, {})
            if not path_val:
                continue
            parts = str(path_val).split(self.path_separator)
            pc_set = {self.path_separator.join(parts[:i]) for i in range(1, len(parts) + 1)}

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
            self.cf_ehr: self._encode_id(raw_doc["ehr_id"], "ehr_id"),
            self.cf_cid: self._encode_id(raw_doc["_id"], "composition_id"),
            self.cf_ver: raw_doc.get("composition_version"),
            self.cf_tmpl: template_id,
            self.cf_nodes: cn,
        }

        search_doc = {
            "_id": self._encode_id(raw_doc["_id"], "composition_id"),
            self.sf_ehr: self._encode_id(raw_doc["ehr_id"], "ehr_id"),
            self.sf_tmpl: template_id,
            self.sf_nodes: sn,
        }

        # STEP 5: Apply shortcuts if enabled
        if self.apply_shortcuts:
            base_doc = self._apply_sc_deep(base_doc)
            search_doc = self._apply_sc_deep(search_doc)

        # STEP 6: Apply field renaming map for output
        base_doc = self._apply_field_map_to_doc(
            base_doc,
            root_map=self.field_map.get("compositions", {}),
            node_map=self.field_map.get("composition_nodes", {}),
        )
        search_doc = self._apply_field_map_to_doc(
            search_doc,
            root_map=self.field_map.get("search", {}),
            node_map=self.field_map.get("search_nodes", {}),
        )

        return base_doc, search_doc


    async def _load_codes_from_db(self):
        if not self.use_codes_db:
            return
        # Get codes collection name with fallback
        codes_collection = (
            self.config.get("target", {}).get("codes_collection")
            or self.config.get("codes_collection")
            or "dictionaries"
        )
        codes_col = self.db[codes_collection]
        doc = await codes_col.find_one({"_id": self.codes_document_id}) or {}

        ar_book: dict[str, Any] = {}
        for rm, subtree in doc.items():
            if rm in ("_id", "_max", "_min", "unknown", "at"): continue
            if isinstance(subtree, dict):
                for name, vers in subtree.items():
                    if isinstance(vers, dict):
                        for ver, code in vers.items():
                            ar_book[f"{rm}.{name}.{ver}"] = code
        
        at_book = {}
        for k, v in (doc.get("at") or {}).items():
            at_book[k] = v
        
        self.code_book["ar_code"] = ar_book
        self.code_book["at"] = at_book
        if isinstance(doc.get("_max"), int):
            self.seq["ar_code"] = doc.get("_max", self.seq["ar_code"])
        if isinstance(doc.get("_min"), int):
            self.seq["at"] = doc.get("_min", self.seq.get("at", -1))
        print(f"Loaded {len(ar_book)} ar_codes and {len(at_book)} at_codes from DB.")
        self._refresh_codec()

    async def flush_codes_to_db(self):
        """Persists the current in-memory codebook back to the database."""
        if not self.use_codes_db:
            return
        codes_collection = (
            self.config.get("target", {}).get("codes_collection")
            or self.config.get("codes_collection")
            or "dictionaries"
        )
        codes_col = self.db[codes_collection]
        nested: Dict[str, Any] = {
            "at": self.code_book.get("at", {}),
        }
        if isinstance(self.seq.get("at"), int):
            nested["_min"] = self.seq.get("at", -1)
        book = self.code_book.get("ar_code", {})
        max_code = max((code for code in book.values() if isinstance(code, int)), default=0)

        for sid, code in book.items():
            parts = sid.split(".")
            rm, name, ver = parts[0], parts[1] if len(parts) > 1 else "", ".".join(parts[2:])
            nested.setdefault(rm, {}).setdefault(name, {})[ver] = code
        
        if isinstance(self.seq.get("ar_code"), int):
            nested["_max"] = max(max_code, self.seq.get("ar_code", 0))
        
        await codes_col.replace_one(
            {"_id": self.codes_document_id},
            {"_id": self.codes_document_id, **nested},
            upsert=True,
        )
        print("Flushed codes to DB.")

    async def _load_shortcuts_from_db(self):
        sc_col = self.db[self.config["target"]["shortcuts_collection"]]
        sc_doc = await sc_col.find_one({"_id": "shortcuts"}) or {}
        self.shortcut_keys.update(sc_doc.get("keys", {}))
        self.shortcut_vals.update(sc_doc.get("values", {}))
        self._refresh_codec()

    def _load_mappings(
        self,
        path: str,
        content: Optional[Union[str, Dict[str, Any]]] = None,
    ):
        """
        Load simplified mapping rules either from inline content (preferred) or from a file path.
        Expected shape:
        {
          "templates": [
            { "templateId": "...", "fields": [ { "path": "..." }, ... ] }
          ]
        }
        """
        if content is not None:
            data = content if isinstance(content, dict) else json.loads(content)
        else:
            with open(path, encoding="utf-8") as f:
                txt = f.read()
            txt = re.sub(r"//.*?$|/\*.*?\*/", "", txt, flags=re.M | re.S)
            data = json.loads(txt)

        self.raw_rules = {}
        self.simple_fields = {}
        self.template_fields = {}
        self.compiled_template_rules = {}
        if not isinstance(data, dict):
            return

        templates = data.get("templates")
        if not isinstance(templates, list):
            return

        for tmpl in templates:
            if not isinstance(tmpl, dict):
                continue
            tid = tmpl.get("templateId")
            fields = [fld for fld in (tmpl.get("fields") or []) if isinstance(fld, dict) and fld.get("path")]
            if tid and fields:
                self.template_fields[tid] = fields
                self.simple_fields[tid] = fields  # reuse existing simple field plumbing

    def _validate_mapping_schema(self, data: Dict[str, Any]) -> None:
        """Validate mapping v2 payload against the bundled schema (best effort)."""
        try:
            schema_path = Path(__file__).resolve().parents[5] / "docs" / "mappings-v2" / "schema.json"
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            jsonschema.Draft7Validator(schema).validate(data)
        except FileNotFoundError:
            return
        except jsonschema.ValidationError as exc:
            path = "/".join([str(x) for x in exc.absolute_path])
            raise ValueError(f"Mappings validation error at '{path}': {exc.message}") from exc
        except Exception:
            # Defensive: do not block runtime on schema load failures
            return

    def _default_field_map(self) -> Dict[str, Dict[str, str]]:
        return {
            "compositions": {
                "ehr_id": "ehr_id",
                "composition_id": "comp_id",
                "template_id": "tid",
                "version": "v",
                "composition_nodes": "cn",
            },
            "composition_nodes": {
                "path": "p",
                "keyPath": "kp",
                "lineIndex": "li",
                "data": "data",
            },
            "search": {
                "ehr_id": "ehr_id",
                "composition_id": "comp_id",
                "template_id": "tid",
                "search_nodes": "sn",
            },
            "search_nodes": {
                "path": "p",
                "data": "data",
            },
        }

    def _apply_field_map_to_doc(
        self,
        doc: Dict[str, Any],
        root_map: Dict[str, str],
        node_map: Dict[str, str],
    ) -> Dict[str, Any]:
        """Return a shallow copy of the document with keys renamed per map."""
        if not doc:
            return doc

        renamed = {}
        for key, val in doc.items():
            new_key = root_map.get(key, key)
            renamed[new_key] = val

        # Harmonise node array names
        if "cn" in doc and "composition_nodes" in root_map:
            new_nodes_key = root_map["composition_nodes"]
            if new_nodes_key != "cn":
                renamed[new_nodes_key] = renamed.pop("cn")
        if "sn" in doc and "search_nodes" in root_map:
            new_nodes_key = root_map["search_nodes"]
            if new_nodes_key != "sn":
                renamed[new_nodes_key] = renamed.pop("sn")

        # Rename node-level keys
        nodes_candidates = []
        if "cn" in doc:
            nodes_candidates.append(renamed.get(root_map.get("composition_nodes", "cn")))
        if "sn" in doc:
            nodes_candidates.append(renamed.get(root_map.get("search_nodes", "sn")))
        for nodes_field in nodes_candidates:
            if isinstance(nodes_field, list):
                for node in nodes_field:
                    for k in list(node.keys()):
                        if k in node_map:
                            node[node_map[k]] = node.pop(k)

        return renamed

    def _at_code_to_int(self, at: str) -> Any:
        s = at.lower()
        if s in self.code_book["at"]:
            return self.code_book["at"][s]
        if self.role != "primary":
            raise UnknownCodeError("at", s)

        strategy = self.at_strategy

        if strategy == "literal":
            code: Any = s
        elif strategy == "compact_prefix":
            code = self._encode_at_compact(s)
        elif strategy == "sequential":
            self.seq["at"] = self.seq.get("at", 0) + 1
            code = self.seq["at"]
        else:  # negative_int (default)
            m_root = _AT_ROOT.match(s)
            if m_root:
                code = -int(m_root.group(1))
                self.seq["at"] = min(self.seq["at"], code)
            elif _AT_DOTTED.match(s):
                self.seq["at"] = self.seq["at"] - 1 if self.seq["at"] <= _START_DOTTED else _START_DOTTED
                code = self.seq["at"]
            else:
                raise ValueError(f"Could not parse at-code: {at}")

        self.code_book["at"][s] = code
        if self.store_original_at:
            self.code_book.setdefault("at_orig", {})[code] = s
        return code

    def _encode_at_compact(self, s: str) -> str:
        if s.startswith("at000"):
            return "A" + s[5:]
        if s.startswith("at00"):
            return "B" + s[4:]
        if s.startswith("at0"):
            return "C" + s[3:]
        return s

    def _alloc_code(self, key: str, sid: str) -> Any:
        sid_lower = sid.lower()
        if key == "ar_code" and sid_lower.startswith("at"):
            return self._at_code_to_int(sid)
        
        book = self.code_book.setdefault(key, {})
        if sid in book:
            return book[sid]

        if self.role != "primary":
            raise UnknownCodeError(key, sid)
        
        if key == "ar_code":
            if self.ar_strategy == "short_prefix":
                code = self._encode_ar_short(sid)
            elif self.ar_strategy == "literal":
                code = self._apply_ar_prefix_replace(sid)
            else:
                self.seq.setdefault(key, 0)
                self.seq[key] += 1
                code = self.seq[key]
        else:
            self.seq.setdefault(key, 0)
            self.seq[key] += 1
            code = self.seq[key]

        book[sid] = code
        return code

    def _encode_ar_short(self, sid: str) -> str:
        """
        Shorten archetype ids: openEHR-EHR-OBSERVATION.height.v2 -> O-height.v2
        """
        base = sid
        if sid.lower().startswith("openehr-ehr-"):
            base = sid[len("openehr-ehr-") :]
        parts = base.split(".", 1)
        rm = parts[0]
        suffix = parts[1] if len(parts) > 1 else ""
        rm_map = {
            "COMPOSITION": "C",
            "SECTION": "S",
            "OBSERVATION": "O",
            "EVALUATION": "E",
            "INSTRUCTION": "I",
            "ACTION": "A",
            "ADMIN_ENTRY": "D",
            "CLUSTER": "K",
            "ITEM_TREE": "T",
            "ITEM_LIST": "L",
            "ITEM_SINGLE": "U",
            "ITEM_TABLE": "B",
            "HISTORY": "H",
        }
        abbr = rm_map.get(rm.upper(), rm[:1].upper() or "X")
        return f"{abbr}-{suffix}" if suffix else abbr

    def _apply_ar_prefix_replace(self, sid: str) -> str:
        """
        Apply first matching prefix replacement for literal archetype ids.
        """
        for rule in self.ar_prefix_replace:
            src = rule.get("from") or ""
            dst = rule.get("to") or ""
            if src and sid.startswith(src):
                return dst + sid[len(src):]
        return sid

    def _walk(self, node: dict, ancestors: Tuple[int, ...], cn: List[dict], *, kp_chain: List[str], list_index: Optional[int]):
        """Flatten *node* into the cn[ ] array. Direct adaptation from proven ingestionOptimized.py"""

        # ── 0.  numeric code of *this* node ─────────────────────────────
        aid = self._archetype_id(node)
        if aid:
            code = self._at_code_to_int(aid) if aid.lower().startswith("at") \
                   else self._alloc_code("ar_code", aid)
        else:
            code = 0

        is_root = not ancestors
        emit = (list_index is not None or is_root or self._split_me_as_a_new_node(node)) and code is not None

        scalars: Dict[str, Any] = {}
        for k, v in node.items():

            # 1a – drop attributes explicitly skipped
            if k in SKIP_ATTRS:
                continue

            # 1b – child is its own locatable node → don't duplicate here
            if isinstance(v, dict) and self._is_locatable(v) and self._split_me_as_a_new_node(v):
                continue

            # 1c – EVENT_CONTEXT, HISTORY … keep only **their** scalars
            if isinstance(v, dict) and self._is_locatable(v) and v.get("_type") in NON_ARCHETYPED_RM:
                scalars[k] = self._strip_locatables(v)
                continue

            # 1d – list that contains any splittable locatable → skip whole list
            if isinstance(v, list) and any(self._is_locatable(x) and self._split_me_as_a_new_node(x) for x in v):
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
                    num = self._alloc_code("ar_code", sid)
                    if num is not None:
                        # always store the int under the canonical long key;
                        # later apply_sc will rename 'archetype_id' → 'ai'
                        ad["archetype_id"] = num

        payload = scalars                  

        # ── 3.  emit the node (if required) ────────────────────────────
        if emit:
            leaf_path = self.path_separator.join([str(code)] + [str(a) for a in reversed(ancestors)])

            cn_node: Dict[str, Any] = {
                self.cf_data: payload,  
                self.cf_path: leaf_path, 
                "_anc": ancestors,      
            }

            if self.cf_ap and aid:
                cn_node[self.cf_ap] = aid

            if kp_chain:
                cn_node["kp"] = kp_chain[:]
            if list_index is not None:
                cn_node["li"] = list_index

            cn.append(cn_node)

        # ── 4.  recurse into children  ─────────────────────────────────
        new_anc = ancestors + ((code,) if emit else ())
        base_kp = [] if emit else kp_chain 

        for k, v in node.items():
            # Use shortcut keys if available
            k_long = self._sc_key(k)

            if isinstance(v, dict) and self._is_locatable(v):
                self._walk(v, new_anc, cn, kp_chain=base_kp + [k_long], list_index=None)

            elif isinstance(v, list):
                for idx, itm in enumerate(v):
                    if self._is_locatable(itm):
                        self._walk(itm, new_anc, cn, kp_chain=base_kp + [k_long], list_index=idx)

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
    def _template_id(obj: dict) -> str | None:
        ad = obj.get("archetype_details", {})
        tid = ad.get("template_id")
        if isinstance(tid, dict):
            val = tid.get("value") or tid.get("v")
            if isinstance(val, str):
                return val.strip()
        if isinstance(tid, str):
            return tid.strip()
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
        
        raw_block = self.raw_rules.get(original_num, {})
        if not raw_block:
            # Try version-flexible matching as fallback
            base_name = original_num.rsplit('.v', 1)[0] if '.v' in original_num else original_num
            for rule_key in self.raw_rules.keys():
                if rule_key.startswith(base_name + '.v'):
                    raw_block = self.raw_rules[rule_key]
                    break
        
        compiled: list[dict] = []
        for r in raw_block.get("rules", []):
            w = r["when"]
            p_raw = w.get("pathChain", [])
            c_raw = w.get("contains", [])
            extra = [(k, v) for k, v in w.items() if k not in ("pathChain", "contains")]
            
            try:
                compiled_rule = {
                    "_path": tuple(self._seg_to_int(x) for x in p_raw),
                    "_cont": tuple(self._seg_to_int(x) for x in reversed(c_raw)),
                    "_extra": extra,
                    "copy": r["copy"],
                }
                if "label" in r:
                    compiled_rule["label"] = r["label"]
                if "rmType" in r:
                    compiled_rule["rmType"] = r["rmType"]
                if "index" in r:
                    compiled_rule["index"] = r["index"]
                compiled.append(compiled_rule)
            except (ValueError, UnknownCodeError) as e:
                print(f"Warning: Skipping rule due to code error: {e}")

        self.active_rules[template] = compiled
        return compiled

    def _compiled_rules_for_template(self, template_name: str) -> List[dict]:
        """
        Compile and cache rules for a given template name (templateId).
        """
        if template_name in self.compiled_template_rules:
            return self.compiled_template_rules[template_name]
        compiled: List[dict] = []
        for fld in self._get_simple_fields_for(template_name) or []:
            rule = self._compile_field_rule(fld, infer_extract=True)
            if rule:
                compiled.append(rule)
        self.compiled_template_rules[template_name] = compiled
        return compiled

    def _compile_field_rule(self, field: dict, *, infer_extract: bool = False) -> Optional[dict]:
        """
        Compile a single field definition (path + extract) into the internal rule shape used by _get_rules_for.
        """
        path = field.get("path")
        if not path:
            return None
        extract = field.get("extract") or field.get("valuePath") or ""
        selectors = self._parse_path_selectors(path)
        if not selectors:
            return None

        if infer_extract and not extract:
            extract = self._infer_extract_from_path(path)

        path_chain = [sel["selector"] for sel in reversed(selectors)]
        contains: list[str] = []

        copy_fields = ["p"]
        copy_fields.append(f"data.{extract}" if extract else "data")

        try:
            compiled_rule = {
                "_path": tuple(self._seg_to_int(x) for x in path_chain),
                "_cont": tuple(self._seg_to_int(x) for x in contains),
                "_extra": [],
                "copy": copy_fields,
            }
            return compiled_rule
        except (ValueError, UnknownCodeError):
            return None

    def _infer_extract_from_path(self, path: str) -> str:
        """
        Derive the relative data path (post-leaf archetype) from an openEHR path.
        """
        segments = [seg for seg in path.split("/") if seg]
        last_bracket_idx = -1
        for idx, seg in enumerate(segments):
            if "[" in seg and "]" in seg:
                last_bracket_idx = idx
        if last_bracket_idx == -1 or last_bracket_idx + 1 >= len(segments):
            return ""
        tail = segments[last_bracket_idx + 1 :]
        pieces: List[str] = []
        for seg in tail:
            base, _ = self._split_segment(seg)
            if base:
                pieces.append(base)
        return ".".join(pieces)

    def _parse_path_selectors(self, path: str) -> List[dict]:
        """
        Parse a canonical openEHR path into selector tokens with archetype hints.
        """
        selectors: List[dict] = []
        segments = [seg for seg in path.split("/") if seg]
        for seg in segments:
            _, selector = self._split_segment(seg)
            if selector:
                selectors.append(
                    {"selector": selector, "is_archetype": selector.upper().startswith("OPENEHR-EHR-")}
                )
        return selectors
        
    def _seg_to_int(self, seg: str | int) -> Any:
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
        if rule["_path"] and self.path_separator.join(str(x) for x in rule["_path"]) not in pc_set:
            return False
        if rule["_cont"]:
            j = 0
            for code in reversed(parts[1:]):
                if code == str(rule["_cont"][j]):
                    j += 1
                    if j == len(rule["_cont"]): break
            if j != len(rule["_cont"]): return False
        
        # Wrap dblock to match "data.value" expectation in rules
        if any(self._dpath_get({"data": dblock}, path) != val_req for path, val_req in rule["_extra"]):
            return False
        return True

    def _get_simple_fields_for(self, template_name: str) -> Optional[List[dict]]:
        if template_name in self.simple_fields:
            return self.simple_fields[template_name]
        # fallback on versionless match
        base = template_name.rsplit(".v", 1)[0] if ".v" in template_name else template_name
        for tid, fields in self.simple_fields.items():
            if tid.startswith(base + ".v"):
                return fields
        return None

    def _extract_by_openehr_path(self, comp: dict, path: str) -> Any:
        """
        Minimal path evaluator for openEHR-like paths with selectors:
        /content[openEHR-EHR-...,'optional name']/items[at0001]/value
        """
        if not path:
            return None
        segments = [seg for seg in path.split("/") if seg]
        current: List[Any] = [comp]

        for seg in segments:
            next_items: List[Any] = []
            key, selector = self._split_segment(seg)
            for item in current:
                if not isinstance(item, dict):
                    continue
                child = item.get(key)
                if child is None:
                    continue
                candidates = []
                if isinstance(child, list):
                    candidates = child
                elif isinstance(child, dict):
                    candidates = [child]
                else:
                    candidates = [child]

                if selector:
                    selector_id = selector
                    filtered = []
                    for cand in candidates:
                        if isinstance(cand, dict) and cand.get("archetype_node_id") == selector_id:
                            filtered.append(cand)
                    candidates = filtered
                next_items.extend(candidates)
            current = next_items
            if not current:
                return None

        if not current:
            return None
        if len(current) == 1:
            return current[0]
        return current

    def _split_segment(self, seg: str) -> tuple[str, Optional[str]]:
        """
        Parse a path segment like "content[openEHR-EHR-SECTION.diagnostic_reports.v0,'Label']"
        into ("content", "openEHR-EHR-SECTION.diagnostic_reports.v0")
        or "items[at0001]" -> ("items", "at0001")
        or "value" -> ("value", None)
        """
        if "[" not in seg:
            return seg, None
        base, rest = seg.split("[", 1)
        sel = rest.split("]", 1)[0]
        if "," in sel:
            sel = sel.split(",", 1)[0]
        sel = sel.strip().strip("'\"")
        return base, sel

    def _apply_rule(self, rule, node, dblock) -> dict:
        """
        Creates a 'slim' search node.
        Reads from 'node' using Composition Keys (cf_*).
        Writes to 'slim' using Search Keys (sf_*).
        """
        slim: dict = {}
        
        # 1. Path
        if self.sf_path and "p" in rule["copy"]: 
             slim[self.sf_path] = self._encode_path_for_profile(node.get(self.cf_path), self.search_encoding_profile)

        # 2. Archetype Path
        if self.sf_ap and self.cf_ap and self.cf_ap in node:
            slim[self.sf_ap] = node[self.cf_ap]

        # 3. Ancestors
        if self.sf_anc and "_anc" in node:
             slim[self.sf_anc] = node["_anc"]

        # 4. Data
        for expr in rule["copy"]:
            if expr.startswith("data."):
                sub = expr[5:] # Remove "data."
                val = self._dpath_get(dblock, sub)
                
                if val is None: continue
                
                cur = slim.setdefault(self.sf_data, {})
                path_parts = sub.split(".")
                for part in path_parts[:-1]:
                    cur = cur.setdefault(part, {})
                cur[path_parts[-1]] = val

        # 5. Labels / RM type (if provided on rule)
        if rule.get("label"):
            slim["label"] = rule["label"]
        if rule.get("rmType"):
            slim["rmType"] = rule["rmType"]
        if rule.get("index"):
            slim["index"] = rule["index"]

        return slim

    def get_index_hints(self) -> Dict[str, List[dict]]:
        """
        Return index hints aggregated from mapping rules (label + index spec).
        Useful for driving index creation (Atlas Search/B-tree) downstream.
        """
        hints: Dict[str, List[dict]] = {}
        for tid, block in self.raw_rules.items():
            for rule in block.get("rules", []):
                if not isinstance(rule, dict):
                    continue
                idx = rule.get("index")
                if not idx:
                    continue
                name = rule.get("label") or ".".join(rule.get("when", {}).get("pathChain", []))
                hints.setdefault(tid, []).append({"name": name, "index": idx, "pathChain": rule.get("when", {}).get("pathChain")})
        return hints

    def _encode_path_for_profile(self, path: Optional[str], profile: Optional[str]) -> Optional[str]:
        if not path:
            return path
        return self.path_codec.encode_path_from_string(str(path), profile)

    def _encode_id(self, value: Any, field: str) -> Any:
        policy = (self.id_encoding.get(field) or "string").lower()
        if policy == "string":
            return str(value)
        if policy == "objectid":
            try:
                return ObjectId(str(value))
            except Exception:
                return value
        if policy in ("uuid", "uuidbin", "uuid_bin"):
            try:
                u = uuid.UUID(str(value))
                return u.bytes
            except Exception:
                return value
        return value

    def _refresh_codec(self):
        """Refresh path codec with current dictionaries and shortcuts."""
        self.path_codec = PathCodec(
            ar_codes=self.code_book.get("ar_code") or {},
            at_codes=self.code_book.get("at") or {},
            separator=self.path_separator,
            shortcuts=self.shortcut_keys,
        )
        
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
