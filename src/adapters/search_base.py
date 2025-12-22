from __future__ import annotations

from typing import Any, Dict, List, Protocol


class SearchAdapter(Protocol):
    """Minimal search adapter contract for strategy search capability."""

    def search(self, query: Dict[str, Any]) -> Any: ...
    def aggregate(self, pipeline: List[Dict[str, Any]]) -> Any: ...
