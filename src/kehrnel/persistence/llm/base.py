from __future__ import annotations

from typing import Any, Dict, List, Protocol


class LlmAdapter(Protocol):
    """Provider-neutral text completion used for extraction and adjudication."""

    async def complete(self, prompt: str, *, system: str | None = None) -> Dict[str, Any]: ...


class RerankAdapter(Protocol):
    """Provider-neutral cross-encoder reranker."""

    async def rerank(self, query: str, documents: List[str], *, top_k: int | None = None) -> List[Dict[str, Any]]: ...
