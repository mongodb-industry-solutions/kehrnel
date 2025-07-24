# kehrnel/transform/at_code_codec.py

import re
import threading
from typing import Optional

CACHE_LOCK = threading.Lock()
_local = threading.local()

# initial sequences
SEQ = {
    "ar_code": 0,    # positive codes
    "at":      -1,   # negative codes
}
CODE_BOOK = {
    "ar_code": {},   # sid -> int
    "at":      {},   # at-code -> negative int
}

_AT_ROOT    = re.compile(r"^at0*([1-9][0-9]*)$", re.I)
_AT_DOTTED  = re.compile(r"^at[0-9]+(?:\.[0-9]{1,4})+$", re.I)
_START_DOTTED = -10_000

class AtCodeCodec:
    def __init__(self, role: str = "primary"):
        self.role = role

    def alloc(self, key: str, sid: str) -> Optional[int]:
        sid_l = sid.lower()
        if key == "ar_code" and sid_l.startswith("at"):
            return self.at_code_to_int(sid)
        book = CODE_BOOK.setdefault(key, {})
        if sid in book:
            return book[sid]
        if self.role != "primary":
            _local.bad_code = True
            return None
        SEQ.setdefault(key, 0)
        SEQ[key] += 1
        book[sid] = SEQ[key]
        return SEQ[key]

    def at_code_to_int(self, at: str) -> Optional[int]:
        s = at.lower()
        # already known
        if s in CODE_BOOK["at"]:
            return CODE_BOOK["at"][s]
        # only primary can allocate
        if self.role != "primary":
            _local.bad_code = True
            return None
        # root form → -n
        m = _AT_ROOT.match(s)
        if m:
            code = -int(m.group(1))
            with CACHE_LOCK:
                CODE_BOOK["at"][s] = code
                SEQ["at"] = min(SEQ["at"], code)
            return code
        # dotted → sequential from -10000 down
        if _AT_DOTTED.match(s):
            with CACHE_LOCK:
                base = SEQ["at"]
                nxt = base - 1 if base <= _START_DOTTED else _START_DOTTED
                SEQ["at"] = nxt
                CODE_BOOK["at"][s] = nxt
            return nxt
        return None

    def decode_map(self, ar_map: dict, at_map: dict) -> dict:
        """ Utility if needed for reverse stage. """
        return {**{v:k for k,v in ar_map.items()}, **{v:k for k,v in at_map.items()}}

_SHARED = AtCodeCodec(role="primary")

def set_shared_role(role: str) -> None:
    """Call this once from `transform.core` so flatten/reverse know the role."""
    global _SHARED
    _SHARED = AtCodeCodec(role=role)

def alloc(key: str, sid: str) -> int | None:          # noqa:  F401
    """Exactly the helper the old monolith expected."""
    return _SHARED.alloc(key, sid)

def at_code_to_int(at: str) -> int | None:            # noqa:  F401
    return _SHARED.at_code_to_int(at)