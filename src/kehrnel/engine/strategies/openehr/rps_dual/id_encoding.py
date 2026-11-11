from __future__ import annotations

import uuid
from typing import Any

from bson import Binary, ObjectId
from bson.binary import UUID_SUBTYPE
from bson.errors import InvalidId


_ENCODING_ALIASES = {
    "string": "string",
    "objectid": "objectid",
    "uuid": "uuidbin",
    "uuidbin": "uuidbin",
    "uuid_bin": "uuidbin",
}


def normalize_id_encoding(encoding: Any) -> str:
    """Return the canonical ID encoding name or reject an unknown policy."""
    policy = str(encoding or "string").strip().lower()
    try:
        return _ENCODING_ALIASES[policy]
    except KeyError as exc:
        raise ValueError(f"Unsupported ID encoding policy: {policy!r}") from exc


def encode_id(value: Any, encoding: Any, *, field_name: str) -> Any:
    """Encode an identifier without silently changing the configured BSON type."""
    policy = normalize_id_encoding(encoding)
    if value is None:
        raise ValueError(f"{field_name} cannot be null")

    if policy == "string":
        return value if isinstance(value, str) else str(value)

    if policy == "objectid":
        if isinstance(value, ObjectId):
            return value
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError, ValueError) as exc:
            raise ValueError(
                f"{field_name} is not compatible with the configured objectid encoding"
            ) from exc

    if isinstance(value, Binary):
        if value.subtype == UUID_SUBTYPE and len(value) == 16:
            return value
        raise ValueError(
            f"{field_name} is not compatible with the configured uuidbin encoding"
        )
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
        if len(raw) != 16:
            raise ValueError(
                f"{field_name} is not compatible with the configured uuidbin encoding"
            )
        return Binary.from_uuid(uuid.UUID(bytes=raw))
    try:
        return Binary.from_uuid(
            value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} is not compatible with the configured uuidbin encoding"
        ) from exc


def atlas_search_type_for_id_encoding(encoding: Any) -> str:
    """Return the Atlas Search type corresponding to the persisted BSON type."""
    policy = normalize_id_encoding(encoding)
    if policy == "objectid":
        return "objectId"
    if policy == "uuidbin":
        return "uuid"
    return "token"
