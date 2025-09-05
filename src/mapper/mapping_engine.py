# src/mapper/mapping_engine.py   

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Protocol


class SourceHandler(Protocol):
    def can_handle(self, path: Path) -> bool: ...
    def load_source(self, path: Path) -> Any: ...
    def extract_value(self, src: Any, rule: Dict | str) -> Any: ...
    def preprocess_mapping(self, mapping: Dict, src: Any) -> Dict: ...

from mapper.utils.macro_expander import expand_macros
from mapper.utils.jinja_env      import env as _JINJA

def _render_template(tpl: str, ctx: dict) -> str:
    return _JINJA.from_string(tpl).render(ctx)

def apply_mapping(generator, mapping, handler, source_tree, skeleton):
    # ① expand @code/@term/… macros once
    mapping = expand_macros(mapping)

    for jpath, rule in mapping.items():
        if jpath.startswith("_"):
            continue

        # ② default / null_flavour bookkeeping
        default   = rule.pop("default",   None) if isinstance(rule, dict) else None
        nlf       = rule.pop("null_flavour", None) if isinstance(rule, dict) else None

        # ③ evaluate rule (Jinja2 template strings supported)
        try:
            value = handler.extract_value(source_tree, rule)

            # treat pure strings starting with "{{" "}}" as Jinja templates
            if isinstance(value, str) and "{{" in value and "}}" in value:
                value = _render_template(value, {"maps_to": value})

            if value in (None, "", []) and default is not None:
                value = default
            elif value in (None, "", []) and nlf:
                value = {
                    "null_flavour": {
                        "value": nlf,
                        "_type": "DV_CODED_TEXT"
                    }
                }

            if value not in (None, "", []):
                generator._set_value_at_path(skeleton, jpath, value)

        except Exception as exc:
            print(f"[WARN] {jpath}: {exc}")

    return skeleton