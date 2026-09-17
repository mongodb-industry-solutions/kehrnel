"""Safe local filesystem implementation of the artifact adapter."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any, Dict

from .base import ArtifactLocation


class FileSystemArtifactStore:
    """Store immutable objects beneath a configured root.

    Keys are deliberately restricted to slash-separated alphanumeric tokens.
    This adapter is intended for local and single-node deployments; cloud object
    stores can implement the same protocol without changing strategy code.
    """

    provider = "filesystem"

    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        parts = key.split("/")
        if not parts or any(
            not part or part in {".", ".."}
            or not part.replace("-", "").replace("_", "").replace(".", "").isalnum()
            for part in parts
        ):
            raise ValueError("artifact key contains an unsafe path segment")
        path = self.root.joinpath(*parts).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("artifact key escapes the configured root")
        return path

    async def put(
        self,
        key: str,
        content: bytes,
        *,
        media_type: str,
        metadata: Dict[str, Any] | None = None,
    ) -> ArtifactLocation:
        if not isinstance(content, bytes):
            raise TypeError("artifact content must be bytes")
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            existing = path.read_bytes()
            if existing != content:
                raise ValueError("immutable artifact key already contains different bytes")
        else:
            descriptor, temporary_name = tempfile.mkstemp(prefix=".artifact-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.link(temporary_name, path)
                except FileExistsError:
                    if path.read_bytes() != content:
                        raise ValueError("immutable artifact key already contains different bytes")
            except Exception:
                raise
            finally:
                try:
                    os.unlink(temporary_name)
                except FileNotFoundError:
                    pass
        return ArtifactLocation(
            key=key,
            uri=path.as_uri(),
            provider=self.provider,
            size=len(content),
            metadata={"sha256": hashlib.sha256(content).hexdigest(), **(metadata or {})},
        )

    async def get(self, key: str) -> bytes:
        path = self._resolve(key)
        if not path.is_file():
            raise FileNotFoundError(f"artifact object not found: {key}")
        return path.read_bytes()

    async def stat(self, key: str) -> ArtifactLocation:
        path = self._resolve(key)
        if not path.is_file():
            raise FileNotFoundError(f"artifact object not found: {key}")
        def inspect() -> tuple[int, str]:
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return path.stat().st_size, digest.hexdigest()

        size, sha256 = await asyncio.to_thread(inspect)
        return ArtifactLocation(
            key=key,
            uri=path.as_uri(),
            provider=self.provider,
            size=size,
            metadata={"sha256": sha256},
        )
