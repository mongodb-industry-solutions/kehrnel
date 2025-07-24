# kehrnel/transform/_bulk_flatten.py

import copy
import threading
import re
from datetime import timezone
from dateutil import parser
from typing import Any, Dict, List, Optional, Tuple

from .at_code_codec import alloc, at_code_to_int  # your at ⇄ int codec
from .core import get_rules_for                        # your YAML rules engine

# thread‐local flag used by alloc when running in “secondary” mode
_local = threading.local()

# ─── constants ────────────────────────────────────────────────────────────────
LOCATABLE = {
    "COMPOSITION","SECTION","ADMIN_ENTRY","OBSERVATION","EVALUATION",
    "INSTRUCTION","ACTION","CLUSTER","ITEM_TREE","ITEM_LIST","ITEM_SINGLE",
    "ITEM_TABLE","ELEMENT","HISTORY","EVENT","POINT_EVENT","INTERVAL_EVENT",
    "ACTIVITY","ISM_TRANSITION","INSTRUCTION_DETAILS","CARE_ENTRY",
    "PARTY_PROXY","EVENT_CONTEXT"
}

NON_ARCHETYPED_RM = {
    "HISTORY","ISM_TRANSITION","EVENT_CONTEXT"
}

LOC_HINT   = {"archetype_node_id","archetype_details"}
SKIP_ATTRS = set()   # e.g. {"archetype_details","uid"} if you wish

# ─── JSON‐path helper ──────────────────────────────────────────────────────────
def dpath_get(obj: Any, path: str) -> Any:
    """
    Follow a dotted path into dictionaries/lists.
    """
    parts = path.split(".")
    def walk(cur, idx):
        if idx == len(parts):
            return cur
        part = parts[idx]
        if isinstance(cur, dict):
            return walk(cur.get(part), idx+1)
        if isinstance(cur, list):
            # numeric index
            if part.isdigit():
                i = int(part)
                if 0 <= i < len(cur):
                    return walk(cur[i], idx+1)
                return None
            # single‐element list
            if len(cur)==1:
                return walk(cur[0], idx)
            # multiple: collect
            coll = [walk(item, idx) for item in cur]
            coll = [c for c in coll if c is not None]
            if not coll:
                return None
            if len(coll)==1:
                return coll[0]
            return coll
        return None
    return walk(obj, 0)

# ─── “locatable” detection ────────────────────────────────────────────────────
def is_locatable(o: Any) -> bool:
    if not isinstance(o, dict):
        return False
    t = o.get("_type")
    if isinstance(t, str) and t in LOCATABLE:
        return True
    return any(k in o for k in LOC_HINT)

def split_me_as_a_new_node(o: dict) -> bool:
    return isinstance(o, dict) and "archetype_node_id" in o

def strip_locatables(d: dict) -> dict:
    out: dict = {}
    for k, v in d.items():
        if isinstance(v, dict) and is_locatable(v):
            continue
        if isinstance(v, list) and any(is_locatable(x) for x in v):
            continue
        out[k] = v
    return out

# ─── archetype_node_id ↔ numeric code ─────────────────────────────────────────
def archetype_id(o: dict) -> Optional[str]:
    ad = o.get("archetype_details") or {}
    ai = ad.get("archetype_id")
    if isinstance(ai, dict) and ai.get("value"):
        return ai["value"].strip()
    if isinstance(ai, str) and ai.strip():
        return ai.strip()
    ani = o.get("archetype_node_id")
    if isinstance(ani, str) and ani.strip():
        return ani.strip()
    return None

# ─── date‐coercion ─────────────────────────────────────────────────────────────
def to_bson_dates(o: Any) -> Any:
    if isinstance(o, dict):
        t0 = o.get("_type")
        if t0 in ("DV_DATE_TIME","DV_DATE") and isinstance(o.get("value"),str):
            try:
                dt = parser.isoparse(o["value"])
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                new = o.copy()
                new["value"] = dt
                return new
            except Exception:
                return o
        # recurse
        return {k: to_bson_dates(v) for k,v in o.items()}
    if isinstance(o, list):
        return [to_bson_dates(x) for x in o]
    return o

# ─── flattening ───────────────────────────────────────────────────────────────
def walk(
    node: dict,
    ancestors: Tuple[int, ...],
    cn: List[dict],
    *,
    kp_chain: List[str],
    list_index: Optional[int]
) -> None:
    """
    Flatten `node` into the accumulator `cn`.
    """
    # 0️⃣ compute numeric code for this node
    aid = archetype_id(node)
    if aid and aid.lower().startswith("at"):
        code = at_code_to_int(aid)
    elif aid:
        code = alloc("ar_code", aid)
    else:
        code = 0

    is_root = not ancestors
    emit    = (list_index is not None or is_root or split_me_as_a_new_node(node)) \
              and code is not None

    # 1️⃣ collect scalars
    scalars: Dict[str,Any] = {}
    for k,v in node.items():
        if k in SKIP_ATTRS:
            continue
        if isinstance(v, dict) and is_locatable(v) and split_me_as_a_new_node(v):
            continue
        if isinstance(v, dict) and is_locatable(v) and v.get("_type") in NON_ARCHETYPED_RM:
            scalars[k] = strip_locatables(v)
            continue
        if isinstance(v, list) and any(is_locatable(x) and split_me_as_a_new_node(x) for x in v):
            continue
        scalars[k] = v

    # 2️⃣ coerce dates & inject archetype_node_id numeric
    scalars = to_bson_dates(scalars)
    scalars["archetype_node_id"] = code

    payload = scalars

    # 3️⃣ emit this node
    if emit:
        leaf_path = ".".join([str(code)] + [str(a) for a in reversed(ancestors)])
        node_rec: Dict[str,Any] = {
            "data": payload,
            "p":    leaf_path,
            "_anc": ancestors
        }
        if kp_chain:
            node_rec["kp"] = kp_chain[:]
        if list_index is not None:
            node_rec["li"] = list_index
        cn.append(node_rec)

    # 4️⃣ recurse children
    new_anc = ancestors + ((code,) if emit else ())
    base_kp = [] if emit else kp_chain

    for k,v in node.items():
        k_long = k
        if isinstance(v, dict) and is_locatable(v):
            walk(v, new_anc, cn, kp_chain=base_kp+[k_long], list_index=None)
        elif isinstance(v, list):
            for idx,item in enumerate(v):
                if is_locatable(item):
                    walk(item, new_anc, cn,
                         kp_chain=base_kp+[k_long],
                         list_index=idx)

# ─── entry‐point ───────────────────────────────────────────────────────────────
def transform(raw: dict) -> Optional[Tuple[dict,dict]]:
    """
    Given one `raw` record with keys:
      - `_id`
      - `ehr_id`
      - `canonicalJSON`
      - optionally `composition_version`
    returns a pair `(full_doc, search_doc)` ready for insertion,
    or `None` if a code‐allocation error occurred.
    """
    comp       = raw["canonicalJSON"]
    root_ani   = archetype_id(comp) or "unknown"
    template_id= alloc("ar_code", root_ani)

    # 1️⃣ flatten
    cn: List[dict] = []
    walk(comp, (), cn, kp_chain=[], list_index=None)

    # if secondary and saw unknown code, bail out
    if getattr(_local, "bad_code", False):
        return None

    # 2️⃣ apply mapping rules to build `sn`
    rules = get_rules_for(template_id, root_ani)
    sn: List[dict] = []

    for node in cn:
        parts = node["p"].split(".")
        prefixes = {".".join(parts[:i]) for i in range(1, len(parts)+1)}
        dblock = node["data"]

        for rule in rules:
            rc1 = rule["_path"]    # leaf-first
            rc2 = rule["_cont"]    # root-first
            # pathChain test
            if rc1 and ".".join(str(x) for x in rc1) not in prefixes:
                continue
            # contains test
            if rc2:
                j=0
                for code in reversed(parts[1:]):
                    if code == str(rc2[j]):
                        j += 1
                        if j==len(rc2): break
                if j!=len(rc2):
                    continue
            # extra predicates
            if any(dpath_get({"data":dblock},path)!=val for path,val in rule["_extra"]):
                continue
            # copy fields
            slim: Dict[str,Any] = {}
            for expr in rule["copy"]:
                if expr.startswith("data."):
                    sub = expr[5:]
                    val = dpath_get(dblock, sub)
                    if val is None:
                        continue
                    cur = slim.setdefault("data", {})
                    for seg in sub.split(".")[:-1]:
                        cur = cur.setdefault(seg, {})
                    cur[sub.split(".")[-1]] = val
                elif expr=="p":
                    slim["p"] = node["p"]
            if slim:
                sn.append(slim)

    # drop ancestry for storage
    for node in cn:
        node.pop("_anc", None)

    # 3️⃣ assemble docs
    full = {
        "_id":    raw["_id"],
        "ehr_id": raw["ehr_id"],
        "comp_id":raw["_id"],
        "v":      raw.get("composition_version"),
        "tid":    template_id,
        "cn":     cn
    }
    search = {
        "_id":    raw["_id"],
        "ehr_id": raw["ehr_id"],
        "tid":    template_id,
        "sn":     sn
    }

    return full, search