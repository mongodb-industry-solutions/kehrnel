"""Redaction helpers for logs and API error messages.

Keep this conservative: only redact obvious credential patterns.
"""

from __future__ import annotations

import re


_MONGO_URI_CREDS = re.compile(
    r"(mongodb(?:\\+srv)?://)([^\\s:/@]+):([^\\s/@]+)@",
    flags=re.IGNORECASE,
)


def redact_secrets(text: str | None) -> str | None:
    """
    Redact credentials embedded in connection strings.

    Examples:
    - mongodb+srv://user:pass@host -> mongodb+srv://***:***@host
    - mongodb://user:pass@host -> mongodb://***:***@host
    """
    if text is None:
        return None
    if not isinstance(text, str) or not text:
        return text  # type: ignore[return-value]
    return _MONGO_URI_CREDS.sub(r"\\1***:***@", text)

