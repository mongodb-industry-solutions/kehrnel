# src/mapper/mapping_engine.py
from __future__ import annotations
from typing import Any, Dict, Optional, Callable
import os
import sys

NO_TRANSLATE_SUFFIX = (
    "/language/code_string",
    "/territory/code_string",
    "/defining_code/code_string",
)

def _log_tx(jpath: str, mode: Any, src: str, dst: str) -> None:
    if not os.environ.get("KEHRNEL_TRANSLATE_LOG"):
        return
    def _short(s: str) -> str:
        s = s.replace("\n", " ")
        return (s[:120] + "…") if len(s) > 121 else s
    print(f"[tx] {mode or 'auto'}  {jpath} :: '{_short(src)}' -> '{_short(dst)}'", file=sys.stderr)


def _maybe_translate(gen: Any, jpath: str, value: Any, mode: Any) -> Any:
    """
    Translate only when the mapping rule explicitly asks for it:
      translate: true | "on" | "no-cache" | "nocache" | "no_cache"
    Any other value (None, False, "off") → no translation.
    """
    if not isinstance(value, str):
        return value

    # opt-in only
    if mode in (True, "on", "no-cache", "nocache", "no_cache"):
        tx = getattr(gen, "translator", None)
        if not tx:
            return value
        persist = (mode not in ("no-cache", "nocache", "no_cache"))
        try:
            out = tx.translate(value, persist=persist)
            if out != value:
                _log_tx(jpath, mode, value, out)
            return out
        except Exception:
            return value

    # default: do not translate
    return value

# ⬇️ Restore this – used by core/generator.py
def apply_mapping(gen: Any, flat_map: Dict[str, Dict[str, Any]], composition: Dict, *_, **__) -> Dict:
    """
    Apply path-keyed rules to the pre-built composition skeleton.

    flat_map: { "/json/path": {"literal": <value>, "translate": <mode?>, ...}, ... }
    """
    set_fn: Optional[Callable[[Dict, str, Any], None]] = getattr(gen, "set_at_path", None)
    if set_fn is None:
        set_fn = getattr(gen, "_set_value_at_path", None)
    if set_fn is None:
        raise AttributeError("Generator has no set_at_path/_set_value_at_path")

    for path, spec in (flat_map or {}).items():
        if not isinstance(spec, dict):
            continue
        if "literal" not in spec:
            continue
        val = spec["literal"]
        val = _maybe_translate(gen, path, val, spec.get("translate", None))
        set_fn(composition, path, val)

    return composition