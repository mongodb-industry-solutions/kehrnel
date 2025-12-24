from __future__ import annotations

from typing import Any, Iterable, Protocol


class StorageAdapter(Protocol):
    """Minimal storage adapter contract used by strategy ingest/transform steps."""

    def insert_one(self, doc: dict, *, search: bool | None = None) -> Any: ...
    def insert_many(self, docs: Iterable[dict], *, search: bool | None = None) -> Any: ...


class SearchAdapter(Protocol):
    """Placeholder for search/query adapters."""

    def search(self, query: dict) -> Any: ...
    def aggregate(self, pipeline: list) -> Any: ...
