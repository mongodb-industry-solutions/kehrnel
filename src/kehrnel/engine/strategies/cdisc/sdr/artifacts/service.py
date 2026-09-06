"""Immutable artifact registration and verified replay."""

from __future__ import annotations

import base64
import binascii
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from ..common import collections, config, replace_documents, storage_adapter


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def normalize_sha256(value: str) -> str:
    normalized = value.strip().lower().removeprefix("sha256:")
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise KehrnelError(code="INVALID_CHECKSUM", status=400, message="expectedSha256 must be a SHA-256 hex digest")
    return normalized


class ArtifactService:
    async def initiate_upload(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        digest = normalize_sha256(str(payload.get("sha256") or ""))
        media_type = str(payload.get("mediaType") or "application/octet-stream").strip()
        size = int(payload.get("size") or 0)
        if size < 1:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="size must be greater than zero")
        artifact_store = (ctx.adapters or {}).get("artifact_store")
        create_upload = getattr(artifact_store, "create_upload", None)
        if not callable(create_upload):
            raise KehrnelError(
                code="DIRECT_UPLOAD_UNAVAILABLE",
                status=501,
                message="The configured artifact adapter does not support direct uploads; use cdisc_store_artifact within the inline size limit.",
            )
        object_key = f"sha256/{digest[:2]}/{digest}"
        expires_in = min(max(int(payload.get("expiresIn") or 900), 60), 3600)
        try:
            target = await create_upload(
                object_key,
                media_type=media_type,
                size=size,
                sha256=digest,
                expires_in=expires_in,
            )
        except Exception as exc:
            raise KehrnelError(code="ARTIFACT_UPLOAD_INIT_FAILED", status=502, message=str(exc)) from exc
        return {
            "ok": True,
            "uploadId": f"sha256:{digest}",
            "objectKey": object_key,
            "sha256": digest,
            "size": size,
            "mediaType": media_type,
            "expiresIn": expires_in,
            "target": target,
        }

    async def finalize_upload(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        digest = normalize_sha256(str(payload.get("sha256") or ""))
        upload_id = str(payload.get("uploadId") or "").strip()
        if upload_id != f"sha256:{digest}":
            raise KehrnelError(code="INVALID_UPLOAD_ID", status=400, message="uploadId does not match sha256")
        return await self.register_external(
            ctx,
            {
                "objectKey": f"sha256/{digest[:2]}/{digest}",
                "sha256": digest,
                "size": payload.get("size"),
                "mediaType": payload.get("mediaType") or "application/octet-stream",
                "sourceName": payload.get("sourceName"),
                "kind": payload.get("kind") or "source",
                "artifactId": payload.get("artifactId"),
                "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            },
        )

    async def prepare_download(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        artifact_id = str(payload.get("artifactId") or "").strip()
        if not artifact_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="artifactId is required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        artifact = await storage.find_one(
            collections(cfg)["artifacts"],
            {"_id": artifact_id, "tenantId": str(cfg["tenant_id"])},
        )
        if not artifact:
            raise KehrnelError(code="ARTIFACT_NOT_FOUND", status=404, message=f"Artifact {artifact_id} was not found.")
        artifact_store = (ctx.adapters or {}).get("artifact_store")
        create_download = getattr(artifact_store, "create_download", None)
        if not callable(create_download):
            raise KehrnelError(
                code="DIRECT_DOWNLOAD_UNAVAILABLE",
                status=501,
                message="The configured artifact adapter does not support direct downloads; use cdisc_replay_artifact within the replay size limit.",
            )
        expires_in = min(max(int(payload.get("expiresIn") or 900), 60), 3600)
        try:
            target = await create_download(artifact["objectKey"], expires_in=expires_in)
        except Exception as exc:
            raise KehrnelError(code="ARTIFACT_DOWNLOAD_INIT_FAILED", status=502, message=str(exc)) from exc
        return {"ok": True, "artifact": artifact, "expiresIn": expires_in, "target": target}

    async def register_external(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        object_key = str(payload.get("objectKey") or "").strip()
        expected = normalize_sha256(str(payload.get("sha256") or ""))
        media_type = str(payload.get("mediaType") or "application/octet-stream")
        if not object_key:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="objectKey is required")
        artifact_store = (ctx.adapters or {}).get("artifact_store")
        stat = getattr(artifact_store, "stat", None)
        if not callable(stat):
            raise KehrnelError(code="ARTIFACT_STAT_UNAVAILABLE", status=501, message="Artifact adapter cannot verify external objects.")
        try:
            location = await stat(object_key)
        except Exception as exc:
            raise KehrnelError(code="ARTIFACT_READ_FAILED", status=502, message=str(exc)) from exc
        actual = str((location.metadata or {}).get("sha256") or "").lower().removeprefix("sha256:")
        if actual != expected:
            raise KehrnelError(
                code="ARTIFACT_CHECKSUM_MISMATCH",
                status=422,
                message="Object-store SHA-256 metadata does not match the declared checksum.",
                details={"expectedSha256": expected, "actualSha256": actual or None},
            )
        declared_size = payload.get("size")
        if declared_size is not None and int(declared_size) != int(location.size):
            raise KehrnelError(code="ARTIFACT_SIZE_MISMATCH", status=422, message="Object-store size does not match the declared size.")
        cfg, storage = config(ctx), storage_adapter(ctx)
        requested_id = str(payload.get("artifactId") or "").strip()
        artifact_id = f"{cfg['tenant_id']}:artifact:{requested_id}" if requested_id else f"{cfg['tenant_id']}:sha256:{expected}"
        document = {
            "_id": artifact_id, "artifactId": artifact_id, "tenantId": str(cfg["tenant_id"]),
            "provider": location.provider, "uri": location.uri, "objectKey": location.key,
            "mediaType": media_type, "size": int(location.size),
            "digest": {"algorithm": "sha256", "value": expected},
            "acquiredAt": datetime.now(timezone.utc).isoformat(), "sourceName": payload.get("sourceName"),
            "metadata": {"kind": str(payload.get("kind") or "source"), **(payload.get("metadata") or {})},
        }
        collection = collections(cfg)["artifacts"]
        existing = await storage.find_one(collection, {"_id": artifact_id})
        if existing:
            immutable = ("tenantId", "objectKey", "mediaType", "size", "digest")
            if any(existing.get(field) != document.get(field) for field in immutable):
                raise KehrnelError(code="ARTIFACT_IDENTITY_CONFLICT", status=409, message="Artifact ID is already bound to different content.")
            return {"ok": True, "artifact": existing, "created": False}
        await replace_documents(storage, collection, [document])
        return {"ok": True, "artifact": document, "created": True}

    async def store(
        self,
        ctx: StrategyContext,
        *,
        content: bytes,
        media_type: str,
        source_name: str | None,
        kind: str,
        expected_sha256: str | None = None,
        artifact_id: str | None = None,
        metadata: Dict[str, Any] | None = None,
        enforce_inline_limit: bool = True,
    ) -> Dict[str, Any]:
        cfg = config(ctx)
        maximum = int((cfg.get("artifact") or {}).get("max_inline_bytes", 25_000_000))
        if enforce_inline_limit and len(content) > maximum:
            raise KehrnelError(code="ARTIFACT_TOO_LARGE", status=413, message="Artifact exceeds the configured inline size limit.")
        digest = sha256_bytes(content)
        if expected_sha256 and normalize_sha256(expected_sha256) != digest:
            raise KehrnelError(
                code="ARTIFACT_CHECKSUM_MISMATCH",
                status=422,
                message="Artifact bytes do not match expectedSha256.",
                details={"actualSha256": digest},
            )
        artifact_store = (ctx.adapters or {}).get("artifact_store")
        if artifact_store is None:
            raise KehrnelError(code="ARTIFACT_STORE_UNAVAILABLE", status=500, message="artifact_store adapter is required")
        storage = storage_adapter(ctx)
        object_key = f"sha256/{digest[:2]}/{digest}"
        try:
            location = await artifact_store.put(
                object_key,
                content,
                media_type=media_type,
                metadata={"kind": kind, **(metadata or {})},
            )
        except Exception as exc:
            raise KehrnelError(code="ARTIFACT_WRITE_FAILED", status=502, message=str(exc)) from exc
        if int(location.size) != len(content):
            raise KehrnelError(code="ARTIFACT_WRITE_FAILED", status=502, message="Artifact adapter returned an invalid size.")
        requested_id = str(artifact_id or "").strip()
        resolved_id = f"{cfg['tenant_id']}:artifact:{requested_id}" if requested_id else f"{cfg['tenant_id']}:sha256:{digest}"
        doc = {
            "_id": resolved_id,
            "artifactId": resolved_id,
            "tenantId": str(cfg["tenant_id"]),
            "provider": location.provider,
            "uri": location.uri,
            "objectKey": location.key,
            "mediaType": media_type,
            "size": len(content),
            "digest": {"algorithm": "sha256", "value": digest},
            "acquiredAt": datetime.now(timezone.utc).isoformat(),
            "sourceName": source_name,
            "metadata": {"kind": kind, **(metadata or {})},
        }
        collection = collections(cfg)["artifacts"]
        existing = await storage.find_one(collection, {"_id": resolved_id})
        if existing:
            immutable = ("tenantId", "objectKey", "mediaType", "size", "digest")
            if any(existing.get(field) != doc.get(field) for field in immutable):
                raise KehrnelError(code="ARTIFACT_IDENTITY_CONFLICT", status=409, message="Artifact ID is already bound to different content.")
            return {"artifact": existing, "created": False}
        await replace_documents(storage, collection, [doc])
        return {"artifact": doc, "created": True}

    async def replay(self, ctx: StrategyContext, artifact_id: str) -> tuple[Dict[str, Any], bytes]:
        cfg = config(ctx)
        storage = storage_adapter(ctx)
        artifact_store = (ctx.adapters or {}).get("artifact_store")
        if artifact_store is None:
            raise KehrnelError(code="ARTIFACT_STORE_UNAVAILABLE", status=500, message="artifact_store adapter is required")
        artifact = await storage.find_one(
            collections(cfg)["artifacts"],
            {"_id": artifact_id, "tenantId": str(cfg["tenant_id"])},
        )
        if not artifact or artifact.get("tenantId") != str(cfg["tenant_id"]):
            raise KehrnelError(code="ARTIFACT_NOT_FOUND", status=404, message=f"Artifact {artifact_id} was not found.")
        try:
            content = await artifact_store.get(artifact["objectKey"])
        except Exception as exc:
            raise KehrnelError(code="ARTIFACT_READ_FAILED", status=502, message=str(exc)) from exc
        actual = sha256_bytes(content)
        expected = artifact.get("digest", {}).get("value")
        if actual != expected or len(content) != artifact.get("size"):
            raise KehrnelError(
                code="ARTIFACT_INTEGRITY_FAILED",
                status=500,
                message="Replayed artifact bytes failed digest or size verification.",
                details={"artifactId": artifact_id, "expectedSha256": expected, "actualSha256": actual},
            )
        return artifact, content

    async def store_base64(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        encoded = payload.get("contentBase64") or payload.get("content_base64")
        if not isinstance(encoded, str) or not encoded:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="contentBase64 is required")
        maximum = int((config(ctx).get("artifact") or {}).get("max_inline_bytes", 25_000_000))
        estimated_size = (len(encoded.rstrip("=")) * 3) // 4
        if estimated_size > maximum:
            raise KehrnelError(code="ARTIFACT_TOO_LARGE", status=413, message="Artifact exceeds the configured inline size limit.")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise KehrnelError(code="INVALID_BASE64", status=400, message="contentBase64 is not valid base64") from exc
        stored = await self.store(
            ctx,
            content=content,
            media_type=str(payload.get("mediaType") or "application/octet-stream"),
            source_name=payload.get("sourceName"),
            kind=str(payload.get("kind") or "source"),
            expected_sha256=payload.get("expectedSha256"),
            artifact_id=payload.get("artifactId"),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
        )
        return {"ok": True, **stored}

    async def replay_base64(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        artifact_id = str(payload.get("artifactId") or "").strip()
        if not artifact_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="artifactId is required")
        artifact, content = await self.replay(ctx, artifact_id)
        maximum = int((config(ctx).get("artifact") or {}).get("max_replay_bytes", 25_000_000))
        if len(content) > maximum:
            raise KehrnelError(code="ARTIFACT_TOO_LARGE", status=413, message="Artifact exceeds the configured replay size limit.")
        return {
            "ok": True,
            "artifact": artifact,
            "contentBase64": base64.b64encode(content).decode("ascii"),
            "integrityVerified": True,
        }
