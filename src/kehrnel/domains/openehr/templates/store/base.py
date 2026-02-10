"""
Abstract persistence interface.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable


class Store(ABC):
    """Generic CRUD interface for compositions."""

    @abstractmethod
    def save(self, composition: dict, *, uid: str | None = None) -> str: ...

    @abstractmethod
    def get(self, uid: str) -> dict | None: ...

    @abstractmethod
    def delete(self, uid: str) -> bool: ...

    @abstractmethod
    def list(self, template_id: str | None = None) -> Iterable[dict]: ...