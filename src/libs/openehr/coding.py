from __future__ import annotations

from typing import Any, Dict

from src.transform.at_code_codec import AtCodeCodec, load_codes_from_db as _load_codes_from_db, set_shared_role


def get_codec(role: str = "primary") -> AtCodeCodec:
    """
    Factory for AtCodeCodec; keeps strategies decoupled from import paths.
    """
    set_shared_role(role)
    return AtCodeCodec(role)


async def load_codes(codec: AtCodeCodec, db, config: Dict[str, Any]):
    """
    Load codes into the shared AtCodeCodec from the database for reverse/transform use-cases.
    """
    await _load_codes_from_db(db, config)
