"""Domain-agnostic ingest encoding helpers."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


class PathCodec:
    """
    Encode/decode between canonical selector chains and stored path strings according
    to an encoding profile.

    - profile.fullpath: human-readable selectors joined by separator
    - profile.codedpath / profile.search_shortcuts: numeric codes with configurable separator
    """

    def __init__(
        self,
        ar_codes: Dict[str, int] | None = None,
        at_codes: Dict[str, int] | None = None,
        separator: str = ".",
        shortcuts: Dict[str, str] | None = None,
    ):
        self.ar_codes = ar_codes or {}
        self.at_codes = at_codes or {}
        self.separator = separator or "."
        self.shortcuts = shortcuts or {}

    def encode_path_from_chain(self, chain: List[str] | List[int], profile: Optional[str]) -> str:
        """Encode a leaf-first selector chain into a path string for the given profile."""
        if profile == "profile.fullpath":
            return self.separator.join([self._selector_from_code(seg) for seg in chain])
        if profile in ("profile.codedpath", "profile.search_shortcuts"):
            codes = [self._selector_to_code(seg) for seg in chain]
            return self.separator.join([str(c) for c in codes if c is not None])
        return self.separator.join([str(x) for x in chain])

    def encode_path_from_string(self, path: str, profile: Optional[str]) -> str:
        """Encode an existing stored path (usually numeric dotted) into profile output."""
        if not path:
            return path
        parts = str(path).split(".")
        return self.encode_path_from_chain(parts, profile)

    def decode_path(self, path: str, profile: Optional[str]) -> List[str]:
        """Decode a path string into selector tokens."""
        if not path:
            return []
        parts = str(path).split(self.separator)
        if profile == "profile.fullpath":
            return parts
        selectors: List[str] = []
        for seg in parts:
            selectors.append(self._selector_from_code(seg))
        return selectors

    def shorten_keys(self, obj: Any) -> Any:
        """Recursively replace keys using shortcuts."""
        if isinstance(obj, dict):
            return {self.shortcuts.get(k, k): self.shorten_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.shorten_keys(x) for x in obj]
        return obj

    def expand_keys(self, obj: Any) -> Any:
        """Recursively expand shortcut keys back to canonical keys."""
        inverse = {v: k for k, v in self.shortcuts.items()}
        if isinstance(obj, dict):
            return {inverse.get(k, k): self.expand_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.expand_keys(x) for x in obj]
        return obj

    def _selector_to_code(self, selector: str | int) -> int | str:
        if isinstance(selector, int):
            return selector
        sel = str(selector)
        if sel.lower().startswith("at"):
            return self.at_codes.get(sel, sel)
        return self.ar_codes.get(sel, sel)

    def _selector_from_code(self, code: str | int) -> str:
        inv_ar = {v: k for k, v in self.ar_codes.items()}
        inv_at = {v: k for k, v in self.at_codes.items()}
        if code in inv_ar:
            return inv_ar[code]
        if code in inv_at:
            return inv_at[code]
        try:
            ival = int(code)
        except Exception:
            return str(code)
        if ival in inv_ar:
            return inv_ar[ival]
        if ival in inv_at:
            return inv_at[ival]
        if ival < 0:
            return f"at{abs(ival):04d}"
        return str(code)
