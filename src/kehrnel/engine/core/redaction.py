"""Redaction helpers for logs and API error messages.

Goal:
- prevent accidental leakage of credentials (e.g. MongoDB URIs)
- prevent filesystem information disclosure (absolute paths) in API errors

Keep this conservative: only redact obvious patterns.
"""

from __future__ import annotations

import re


_MONGO_URI_CREDS = re.compile(
    r"(mongodb(?:\\+srv)?://)([^\\s:/@]+):([^\\s/@]+)@",
    flags=re.IGNORECASE,
)

_ABS_UNIX_PATH_IN_QUOTES = re.compile(r"(?P<q>['\"])(?P<p>/[^'\"\\n]+)(?P=q)")
_ABS_WIN_PATH_IN_QUOTES = re.compile(r"(?P<q>['\"])(?P<p>[A-Za-z]:\\\\[^'\"\\n]+)(?P=q)")


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


def redact_paths(text: str | None) -> str | None:
    """Redact absolute filesystem paths (typically leaked via exception messages).

    Only redacts:
    - Unix absolute paths inside quotes: '/app/tmp/file'
    - Windows absolute paths inside quotes: 'C:\\temp\\file'

    This avoids interfering with AQL paths like 'c/uid/value' which do not start with '/'.
    """
    if text is None:
        return None
    if not isinstance(text, str) or not text:
        return text  # type: ignore[return-value]
    out = _ABS_UNIX_PATH_IN_QUOTES.sub(lambda m: f"{m.group('q')}<path>{m.group('q')}", text)
    out = _ABS_WIN_PATH_IN_QUOTES.sub(lambda m: f"{m.group('q')}<path>{m.group('q')}", out)
    return out


def redact_sensitive(text: str | None) -> str | None:
    """Convenience wrapper for redacting secrets and sensitive internals from a string."""
    return redact_paths(redact_secrets(text))
