# src/mapper/mapping_engine.py (patches)
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Protocol, Iterable, Tuple, List
from mapper.utils.macro_expander import expand_macros
from mapper.utils.jinja_env      import env as _JINJA

class SourceHandler(Protocol):
    def can_handle(self, path: Path) -> bool: ...
    def load_source(self, path: Path) -> Any: ...
    def extract_value(self, src: Any, rule: Dict | str | Any) -> Any: ...
    def preprocess_mapping(self, mapping: Dict, src: Any) -> Dict | List[Tuple[Any, Dict]] | None: ...

NO_TX_SUFFIXES = (
    "/language/code_string",
    "/territory/code_string",
    "/defining_code/code_string",
)

def _render_template(tpl: str, ctx: dict) -> str:
    return _JINJA.from_string(tpl).render(ctx)

def apply_mapping(generator, mapping, handler, source_tree, skeleton):
    mapping = expand_macros(mapping)
    src_subtree, effective_map = source_tree, mapping

    for jpath, rule in effective_map.items():
        if jpath.startswith("_"):
            continue

        # pull per-rule knobs
        translate_mode = None
        if isinstance(rule, dict):
            translate_mode = rule.get("translate")  # "on" (default), "off", "no-cache"
            default = rule.pop("default", None)
            nlf     = rule.pop("null_flavour", None)
        else:
            default = None
            nlf     = None
        
        # never translate control-code targets
        auto_no_tx = any(jpath.endswith(sfx) for sfx in NO_TX_SUFFIXES)
        if translate_mode is None and auto_no_tx:
            translate_mode = "off"
        # --------------------------------------------

        try:
            value = handler.extract_value(src_subtree, rule)

            # If we render Jinja, do it before translation
            if isinstance(value, str) and "{{" in value and "}}" in value:
                value = _render_template(value, {"maps_to": value})

            # defaults/null-flavour
            if value in (None, "", []) and default is not None:
                value = default
            elif value in (None, "", []) and nlf:
                value = {"null_flavour": {"value": nlf, "_type": "DV_CODED_TEXT"}}

            # ── TRANSLATION (strings only) ─────────────────────────────
            tx = getattr(generator, "translator", None)
            if (
                tx
                and isinstance(value, str)
                and (translate_mode is None or translate_mode == "on" or translate_mode == "no-cache")
            ):
                persist = (translate_mode != "no-cache")
                value = tx.translate(value, persist=persist)

            if value not in (None, "", []):
                generator._set_value_at_path(skeleton, jpath, value)

        except Exception as exc:
            print(f"[WARN] {jpath}: {exc}")

    return skeleton