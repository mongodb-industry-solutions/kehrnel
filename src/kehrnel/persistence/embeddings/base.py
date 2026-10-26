from __future__ import annotations

from typing import List, Protocol


class EmbeddingAdapter(Protocol):
    async def embed(self, texts: List[str]) -> List[List[float]]: ...
