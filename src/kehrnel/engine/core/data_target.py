"""Non-secret identity for a resolved persistence target.

Bindings can be resolved more than once during a request.  A resolver is
allowed to rotate credentials, but it must not silently redirect a reviewed
operation to another cluster or database.  This module derives a stable digest
from the addressable MongoDB target while deliberately excluding credentials.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Optional
from urllib.parse import parse_qsl, urlsplit


_IDENTITY_QUERY_OPTIONS = frozenset(
    {
        "directconnection",
        "loadbalanced",
        "replicaset",
        "srvservicename",
    }
)


def resolved_data_target_digest(
    bindings: Mapping[str, Any] | None,
) -> Optional[str]:
    """Return a credential-free digest for the resolved MongoDB target."""

    if not isinstance(bindings, Mapping):
        return None
    db = bindings.get("db")
    if not isinstance(db, Mapping):
        return None
    provider = str(db.get("provider") or "").strip().lower()
    uri = str(db.get("uri") or "").strip()
    database = str(db.get("database") or "").strip()
    if provider not in {"mongo", "mongodb"} or not uri or not database:
        return None

    parsed = urlsplit(uri)
    if parsed.scheme.lower() not in {"mongodb", "mongodb+srv"}:
        return None
    # The last @ separates user-info from the seed list even when user-info
    # contains percent-encoded punctuation.  No credential material enters
    # the digest or an error response.
    authority = parsed.netloc.rsplit("@", 1)[-1].strip().lower()
    if not authority:
        return None
    options = sorted(
        (key.lower(), value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() in _IDENTITY_QUERY_OPTIONS
    )
    canonical = json.dumps(
        {
            "provider": "mongodb",
            "scheme": parsed.scheme.lower(),
            "authority": authority,
            "database": database,
            "identityOptions": options,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


__all__ = ["resolved_data_target_digest"]
