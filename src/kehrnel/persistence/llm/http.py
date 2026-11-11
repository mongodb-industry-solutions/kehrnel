"""Provider-neutral HTTP clients for text completion and reranking.

Both clients speak the OpenAI-compatible surface most gateways expose, so a
tenant can point Kehrnel at an in-house gateway, a cloud vendor, or a local
model without a code change. Neither client decides anything clinical: the
completion client returns text, and the reranker returns an ordering over
documents the strategy already retrieved.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from typing import Any, Dict, List

_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def parse_loose_json(text: str) -> Any:
    """Parse JSON that a model wrapped in prose or a code fence."""
    payload = str(text or "").strip()
    fence = payload.find("```")
    if fence >= 0:
        end = payload.find("```", fence + 3)
        fenced = payload[fence + 3 : end if end >= 0 else len(payload)]
        payload = fenced.lstrip("json").strip() if fenced.lstrip().startswith("json") else fenced.strip()
    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        payload = payload[start : end + 1]
    return json.loads(payload)


class HttpLlmAdapter:
    """Text completion over an OpenAI-compatible ``/chat/completions`` endpoint."""

    def __init__(
        self,
        endpoint: str,
        *,
        model: str,
        api_key: str | None = None,
        timeout_seconds: int = 60,
        max_tokens: int = 4096,
        headers: Dict[str, str] | None = None,
    ):
        if not endpoint.startswith(("https://", "http://")):
            raise ValueError("llm endpoint must use HTTP or HTTPS")
        self.endpoint = endpoint.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_seconds = int(timeout_seconds)
        self.max_tokens = int(max_tokens)
        self.headers = dict(headers or {})

    async def complete(self, prompt: str, *, system: str | None = None) -> Dict[str, Any]:
        messages: List[Dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        body = {"model": self.model, "messages": messages, "max_tokens": self.max_tokens}

        def call() -> Dict[str, Any]:
            headers = {"Content-Type": "application/json", **self.headers}
            if self.api_key:
                headers.setdefault("Authorization", f"Bearer {self.api_key}")
            request = urllib.request.Request(
                self.endpoint,
                data=json.dumps(body).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    content = response.read(_MAX_RESPONSE_BYTES)
            except urllib.error.HTTPError as exc:
                detail = exc.read(_MAX_RESPONSE_BYTES).decode("utf-8", "replace")[:500]
                raise RuntimeError(f"llm gateway returned HTTP {exc.code}: {detail}") from exc
            parsed = json.loads(content.decode("utf-8"))
            choices = parsed.get("choices")
            if not isinstance(choices, list) or not choices:
                raise RuntimeError("llm gateway response has no choices")
            message = choices[0].get("message") if isinstance(choices[0], dict) else None
            text = (message or {}).get("content") if isinstance(message, dict) else None
            if not isinstance(text, str):
                raise RuntimeError("llm gateway response has no message content")
            usage = parsed.get("usage") if isinstance(parsed.get("usage"), dict) else {}
            return {
                "text": text,
                "model": parsed.get("model") or self.model,
                "usage": {
                    "inputTokens": usage.get("prompt_tokens"),
                    "outputTokens": usage.get("completion_tokens"),
                    "totalTokens": usage.get("total_tokens"),
                },
            }

        return await asyncio.to_thread(call)


class HttpRerankAdapter:
    """Cross-encoder reranking over a Voyage-compatible ``/rerank`` endpoint."""

    def __init__(
        self,
        endpoint: str,
        *,
        model: str,
        api_key: str | None = None,
        timeout_seconds: int = 15,
        headers: Dict[str, str] | None = None,
    ):
        if not endpoint.startswith(("https://", "http://")):
            raise ValueError("rerank endpoint must use HTTP or HTTPS")
        self.endpoint = endpoint.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_seconds = int(timeout_seconds)
        self.headers = dict(headers or {})

    async def rerank(self, query: str, documents: List[str], *, top_k: int | None = None) -> List[Dict[str, Any]]:
        if not documents:
            return []
        body: Dict[str, Any] = {"model": self.model, "query": query, "documents": documents}
        if top_k:
            body["top_k"] = int(top_k)

        def call() -> List[Dict[str, Any]]:
            headers = {"Content-Type": "application/json", **self.headers}
            if self.api_key:
                headers.setdefault("Authorization", f"Bearer {self.api_key}")
            request = urllib.request.Request(
                f"{self.endpoint}/rerank",
                data=json.dumps(body).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                content = response.read(_MAX_RESPONSE_BYTES)
            parsed = json.loads(content.decode("utf-8"))
            data = parsed.get("data")
            if not isinstance(data, list):
                return []
            normalized = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                index = item.get("index")
                if index is None:
                    continue
                normalized.append({"index": int(index), "relevanceScore": item.get("relevance_score")})
            normalized.sort(key=lambda entry: -(entry["relevanceScore"] or 0))
            return normalized

        try:
            return await asyncio.to_thread(call)
        except Exception:  # noqa: BLE001 - reranking is best-effort by contract
            return []
