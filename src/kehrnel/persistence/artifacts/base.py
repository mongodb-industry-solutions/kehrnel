"""Storage-neutral protocol for immutable binary artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Protocol


@dataclass(frozen=True)
class ArtifactLocation:
    """Result of storing one immutable object."""

    key: str
    uri: str
    provider: str
    size: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class ArtifactStoreAdapter(Protocol):
    async def put(
        self,
        key: str,
        content: bytes,
        *,
        media_type: str,
        metadata: Dict[str, Any] | None = None,
    ) -> ArtifactLocation: ...

    async def get(self, key: str) -> bytes: ...

    async def stat(self, key: str) -> ArtifactLocation: ...
