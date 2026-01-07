"""Error types for Kehrnel runtime."""

from __future__ import annotations

from typing import Any, Dict, Optional


class KehrnelError(Exception):
    def __init__(self, code: str, status: int, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.details = details or {}
