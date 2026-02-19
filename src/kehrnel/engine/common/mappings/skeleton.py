# src/mapper/skeleton.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Callable
import contextlib, io

from kehrnel.engine.domains.openehr.templates.parser import TemplateParser
from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator

ENTRY_TYPES = {"OBSERVATION", "EVALUATION", "INSTRUCTION", "ACTION", "ADMIN_ENTRY"}
SECTION = "SECTION"

# ───────────────────────── helpers ─────────────────────────
def _is_dv(node: Any) -> bool:
    return isinstance(node, dict) and isinstance(node.get("_type"), str) and node["_type"].startswith("DV_")

def _selector_for(item: dict) -> Optional[str]:
    aid = (item.get("archetype_details", {}).get("archetype_id", {}) or {}).get("value")
    if aid: return aid
    code = item.get("archetype_node_id")
    if code: return code
    return None

def _join(parent: str, key: str) -> str:
    return f"{parent.rstrip('/')}/{key}" if parent else f"/{key}"

def _label_of(node: Optional[dict]) -> Optional[str]:
    if not node:
        return None
    name = node.get("name")
    if isinstance(name, dict):
        return name.get("value")
    if isinstance(name, str):
        return name
    return None

def _rm(node: dict) -> str:
    return node.get("_type", "")

def _content_list(comp: dict) -> list:
    return comp.get("content", [])

def _child_items(node: dict) -> list:
    return node.get("items") or []

def _nearest(anc: List[dict], types: set[str]) -> Optional[dict]:
    for n in reversed(anc or []):
        if _rm(n) in types:
            return n
    return None

# ────────────────────── traversal ──────────────────────
def collect_targets(comp: Dict, *, include_header: bool = False, minimal: bool = True) -> Iterable[tuple[str, dict, dict]]:
    """
    Yield (json_path, dv_object, ctx) for:
      - ELEMENT.value DV_*
      - direct DV_* attributes (e.g., EVENT.time, HISTORY.origin)
    Notes:
      - minimal=True skips noisy /name/value
      - we DO NOT descend into ELEMENT.value to avoid DV_ORDINAL.symbol DV_CODED_TEXT noise
    """
    def under_content(p: str) -> bool: return "/content[" in p

    def _walk(node: Any, path: str, ancestors: List[dict]):
        if isinstance(node, list):
            for item in node:
                if isinstance(item, dict):
                    sel = _selector_for(item)
                    sel_path = f"{path}[{sel}]" if sel else f"{path}[0]"
                    yield from _walk(item, sel_path, ancestors + [item])
            return

        if not isinstance(node, dict):
            return

        rm_type = node.get("_type")

        # ELEMENT.value = DV_*
        if rm_type == "ELEMENT" and "value" in node and _is_dv(node["value"]):
            p = _join(path, "value")
            if not (minimal and p.endswith("/name/value")):
                if include_header or under_content(path):
                    ctx = {"rm_parent": "ELEMENT", "element_name": _label_of(node), "ancestors": ancestors[:]}
                    yield (p, node["value"], ctx)
            # IMPORTANT: do not descend into 'value' to avoid ordinal.symbol targets
            for k, v in node.items():
                if k == "value":
                    continue
                if isinstance(v, (dict, list)):
                    yield from _walk(v, _join(path, k), ancestors + [node])
            return

        # direct DV_* attributes (EVENT.time, HISTORY.origin, etc.)
        for k, v in list(node.items()):
            if _is_dv(v):
                p = _join(path, k)
                if minimal and p.endswith("/name/value"):
                    continue
                if include_header or "/content[" in path:
                    ctx = {"rm_parent": rm_type, "field": k, "ancestors": ancestors[:]}
                    yield (p, v, ctx)

        # descend
        for k, v in list(node.items()):
            if isinstance(v, list):
                yield from _walk(v, _join(path, k), ancestors + [node])
            elif isinstance(v, dict):
                yield from _walk(v, _join(path, k), ancestors + [node])

    yield from _walk(comp, "", [])

# ───────────────────── mapping stubs ─────────────────────
def _rule_stub(path: str, dv: Dict[str, Any], use_macros: bool) -> tuple[str, Any]:
    """
    Produce an editable mapping stub for the DV_* at *path*.
    Neutral key 'from' keeps handlers agnostic; a GUI can later swap 'from' to
    'xpath'/'csv'/'json' etc.
    """
    t = dv.get("_type", "")
    if t == "DV_QUANTITY":   return (f"{path}/magnitude", {"from": ""})
    if t == "DV_COUNT":      return (f"{path}/magnitude", {"from": ""})
    if t == "DV_DATE_TIME":  return (f"{path}/value", {"from": ""})
    if t == "DV_DATE":       return (f"{path}/value", {"from": ""})
    if t == "DV_BOOLEAN":    return (f"{path}/value", {"from": "", "map": {"yes": True, "no": False}})
    if t == "DV_TEXT":       return (f"{path}/value", {"from": ""})
    if t == "DV_CODED_TEXT":
        return (path, {"code": "", "term": ""}) if use_macros else (f"{path}/defining_code/code_string", {"from": ""})
    if t == "DV_ORDINAL":    return (f"{path}/symbol/defining_code/code_string", {"from": "", "map": {}})
    if "value" in dv:        return (f"{path}/value", {"from": ""})
    return (path, {"from": ""})

def _field_descriptor(out_path: str, dv: dict, ctx: dict, ordinal_codes_global: dict) -> dict:
    entry = _nearest(ctx.get("ancestors", []), ENTRY_TYPES)
    section = _nearest(ctx.get("ancestors", []), {SECTION})
    entry_aid = (entry or {}).get("archetype_details", {}).get("archetype_id", {}).get("value")
    entry_label = _label_of(entry) or entry_aid or ("COMPOSITION" if entry is None else _rm(entry))
    section_label = _label_of(section) if section else None

    desc = {
        "path": out_path,
        "dvType": dv.get("_type"),
        "label": ctx.get("element_name") or ctx.get("field") or "Value",
        "entry": {"rmType": _rm(entry or {}), "archetype_id": entry_aid, "label": entry_label},
        "section": {"label": section_label} if section_label else None,
        "required": None,
    }
    if dv.get("_type") == "DV_ORDINAL":
        codes = [k for k in ordinal_codes_global.keys() if isinstance(k, str) and k.startswith("at")]
        desc["choices_hint"] = [{"code": c} for c in sorted(set(codes))]
    elif dv.get("_type") == "DV_BOOLEAN":
        desc["choices_hint"] = [{"code": True, "label": "Yes"}, {"code": False, "label": "No"}]
    return desc

def _gui_from_composition(comp: dict, fields: List[dict]) -> dict:
    """Compact GUI model: sections → entries → fields (or rootEntries)."""
    def _key(fd: dict):
        sec = (fd.get("section") or {}).get("label")
        ent = (fd.get("entry") or {}).get("archetype_id") or (fd.get("entry") or {}).get("label")
        return (sec, ent)

    bucket: Dict[tuple, List[dict]] = {}
    for f in fields:
        bucket.setdefault(_key(f), []).append(f)

    sections: Dict[str, dict] = {}
    root_entries: Dict[str, dict] = {}

    for item in _content_list(comp):
        rm = _rm(item)
        if rm == SECTION:
            sec_label = _label_of(item) or "Section"
            sec = sections.setdefault(sec_label, {"label": sec_label, "entries": []})
            for child in _child_items(item) or []:
                ent_aid = (child.get("archetype_details", {}).get("archetype_id", {}) or {}).get("value")
                ent_label = _label_of(child) or ent_aid or _rm(child)
                k = (sec_label, ent_aid or ent_label)
                ent_fields = sorted(bucket.get(k, []), key=lambda x: x["label"])
                sec["entries"].append({
                    "rmType": _rm(child),
                    "archetype_id": ent_aid,
                    "label": ent_label,
                    "fields": ent_fields
                })
        elif rm in ENTRY_TYPES:
            ent_aid = (item.get("archetype_details", {}).get("archetype_id", {}) or {}).get("value")
            ent_label = _label_of(item) or ent_aid or rm
            k = (None, ent_aid or ent_label)
            ent_fields = sorted(bucket.get(k, []), key=lambda x: x["label"])
            root_entries[ent_aid or ent_label] = {
                "rmType": rm, "archetype_id": ent_aid, "label": ent_label, "fields": ent_fields
            }

    return {
        "composition": {
            "template_id": comp.get("archetype_details", {}).get("template_id", {}).get("value") or "",
            "label": _label_of(comp) or ""
        },
        "sections": list(sections.values()),
        "rootEntries": list(root_entries.values())
    }

# ───────────────────── public API ─────────────────────
def build_skeleton(template_path: Path | str,
                   *,
                   use_macros: bool = True,
                   include_header: bool = False,
                   include_helpers: bool = False,
                   on_status: Callable[[str], None] | None = None,
                   suppress_generator_noise: bool = True) -> dict:
    """
    Create a mapping skeleton from an OPT (or web-template JSON):
      - top-level stubs for every DV_* leaf (under canonical JSON paths)
      - optional '_hints' (labels, ordinal code hints)
      - optional '_gui' (sections → entries → fields) for form rendering
      - '_options.prune_empty=True' by default
    """
    say = on_status or (lambda *_: None)

    say("reading template")
    tpl = TemplateParser(template_path)
    gen = kehrnelGenerator(tpl)

    if suppress_generator_noise:
        _buf = io.StringIO()
        with contextlib.redirect_stdout(_buf):
            comp = gen.generate_minimal()
    else:
        comp = gen.generate_minimal()

    mapping: Dict[str, Any] = {"_options": {"prune_empty": True}}
    hints: Dict[str, Any] = {}
    gui_fields: List[dict] = []

    global_ord = getattr(gen, "_ordinal_by_code", {}) or {}
    for path, dv, ctx in collect_targets(comp, include_header=include_header, minimal=True):
        out_path, rule = _rule_stub(path, dv, use_macros)
        if out_path not in mapping:
            mapping[out_path] = rule

        if include_helpers:
            h = {"rm_type": dv.get("_type")}
            nm = ctx.get("element_name")
            if nm: h["label"] = nm
            if dv.get("_type") == "DV_ORDINAL":
                h["ordinal_codes_hint"] = [k for k in sorted(global_ord.keys()) if str(k).startswith("at")]
            hints[out_path] = h
            gui_fields.append(_field_descriptor(out_path, dv, ctx, global_ord))

    if include_helpers:
        mapping["_hints"] = hints
        mapping["_gui"] = _gui_from_composition(comp, gui_fields)

    fields_count = sum(1 for k in mapping.keys() if k.startswith("/"))
    say(f"done: {fields_count}" + (f" fields, {len(gui_fields)} helpers" if include_helpers else " fields"))
    return mapping