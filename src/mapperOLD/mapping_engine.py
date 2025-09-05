#src/mapper/mapping_engine.py
"""
apply_mapping() drives the JSON-path ⇢ value assignment.

The rules themselves are interpreted by a concrete *SourceHandler*
(see xml_handler.py for an example).
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Protocol


class SourceHandler(Protocol):
    def can_handle(self, path: Path) -> bool: ...
    def load_source(self, path: Path) -> Any: ...
    def extract_value(self, src: Any, rule: Dict | str) -> Any: ...
    def preprocess_mapping(self, mapping: Dict, src: Any) -> Dict: ...


# ---------------------------------------------------------------------------


def apply_mapping(generator, mapping: Dict, handler: SourceHandler,
                  source_tree: Any, skeleton: Dict) -> Dict:
    """
    generator  –  core.OpenEHRGenerator (needs its private _set_value_at_path)
    mapping    –  mapping dict WITHOUT meta keys
    handler    –  concrete handler (XML, CSV, …)
    source_tree–  already loaded/parsed source data
    skeleton   –  composition returned by generator.generate_minimal()
    """
    for jpath, rule in mapping.items():
        if jpath.startswith("_"):
            continue                           # meta (_metadata, _options …)

        try:
            value = handler.extract_value(source_tree, rule)
            if value is not None:
                generator._set_value_at_path(skeleton, jpath, value)
        except Exception as exc:
            print(f"[WARN] {jpath}: {exc}")

    return skeleton