"""Small provider-neutral HTTP embedding adapter."""

from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from typing import Any, Dict, List


class _RedirectGuard(urllib.request.HTTPRedirectHandler):
    """urlopen follows redirects with the Authorization header attached, so a
    302 could forward the bearer credential to another destination. Once a
    credential is configured, only same-origin HTTPS redirects are accepted."""

    def __init__(self, credential: bool, origin: str):
        super().__init__()
        self._credential = credential
        self._origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if self._credential:
            parts = urllib.parse.urlsplit(newurl)
            if parts.scheme != "https" or f"{parts.scheme}://{parts.netloc}" != self._origin:
                raise ValueError(
                    f"embedding endpoint redirect would send the bearer credential to {newurl}"
                )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HttpEmbeddingAdapter:
    def __init__(
        self,
        endpoint: str,
        *,
        model: str | None = None,
        api_key: str | None = None,
        timeout_seconds: int = 60,
        headers: Dict[str, str] | None = None,
    ):
        if not endpoint.startswith(("https://", "http://")):
            raise ValueError("embedding endpoint must use HTTP or HTTPS")
        self.endpoint = endpoint
        self.model = model
        self.api_key = api_key
        self.timeout_seconds = int(timeout_seconds)
        self.headers = dict(headers or {})
        # A bearer credential over plain HTTP exposes the key and the clinical
        # text on the wire, so HTTP stays reserved for credential-free endpoints.
        credential = bool(self.api_key) or any(name.lower() == "authorization" for name in self.headers)
        if credential and not self.endpoint.startswith("https://"):
            raise ValueError("embedding endpoint must use HTTPS when a bearer credential is sent")
        parts = urllib.parse.urlsplit(self.endpoint)
        self._opener = urllib.request.build_opener(
            _RedirectGuard(credential, f"{parts.scheme}://{parts.netloc}")
        )

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        def request_embeddings() -> List[List[float]]:
            body: Dict[str, Any] = {"input": texts}
            if self.model:
                body["model"] = self.model
            headers = {"Content-Type": "application/json", **self.headers}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            request = urllib.request.Request(
                self.endpoint,
                data=json.dumps(body).encode(),
                headers=headers,
                method="POST",
            )
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                content = response.read(20_000_001)
            if len(content) > 20_000_000:
                raise RuntimeError("embedding response exceeds 20 MB")
            parsed = json.loads(content.decode("utf-8"))
            if isinstance(parsed.get("data"), list):
                vectors = [item.get("embedding") for item in parsed["data"]]
            else:
                vectors = parsed.get("embeddings")
            if not isinstance(vectors, list) or len(vectors) != len(texts):
                raise RuntimeError("embedding response count does not match input count")
            normalized = []
            for vector in vectors:
                if not isinstance(vector, list) or not vector or not all(isinstance(item, (int, float)) for item in vector):
                    raise RuntimeError("embedding response contains an invalid vector")
                normalized.append([float(item) for item in vector])
            return normalized

        return await asyncio.to_thread(request_embeddings)
