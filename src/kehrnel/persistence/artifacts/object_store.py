"""Optional cloud object-store implementations of the artifact protocol."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Dict

from .base import ArtifactLocation


def _safe_key(key: str) -> str:
    parts = key.split("/")
    if not parts or any(
        not part or not part.replace("-", "").replace("_", "").replace(".", "").isalnum()
        for part in parts
    ):
        raise ValueError("artifact key contains an unsafe path segment")
    return "/".join(parts)


def _prefixed(prefix: str, key: str) -> str:
    safe = _safe_key(key)
    normalized = _safe_key(prefix.strip("/")) if prefix.strip("/") else ""
    return f"{normalized}/{safe}" if normalized else safe


class S3ArtifactStore:
    """Immutable S3-compatible storage with digest metadata verification."""

    provider = "s3"

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        client: Any | None = None,
        region: str | None = None,
        endpoint_url: str | None = None,
        server_side_encryption: str | None = None,
        kms_key_id: str | None = None,
        client_options: Dict[str, Any] | None = None,
    ):
        if not bucket.strip():
            raise ValueError("S3 artifact storage requires a bucket")
        self.bucket = bucket.strip()
        self.prefix = prefix.strip("/")
        self.server_side_encryption = server_side_encryption
        self.kms_key_id = kms_key_id
        if client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("Install kehrnel-core[cdisc-cloud] for S3 artifacts") from exc
            options = dict(client_options or {})
            if region:
                options["region_name"] = region
            if endpoint_url:
                options["endpoint_url"] = endpoint_url
            client = boto3.client("s3", **options)
        self.client = client

    async def create_upload(
        self,
        key: str,
        *,
        media_type: str,
        size: int,
        sha256: str,
        expires_in: int,
    ) -> Dict[str, Any]:
        """Create a signed PUT whose checksum metadata is verified at finalization."""
        object_key = _prefixed(self.prefix, key)
        params: Dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": object_key,
            "ContentType": media_type,
            "Metadata": {"sha256": sha256},
            "IfNoneMatch": "*",
        }
        headers = {
            "Content-Type": media_type,
            "If-None-Match": "*",
            "x-amz-meta-sha256": sha256,
        }
        if self.server_side_encryption:
            params["ServerSideEncryption"] = self.server_side_encryption
            headers["x-amz-server-side-encryption"] = self.server_side_encryption
        if self.kms_key_id:
            params["SSEKMSKeyId"] = self.kms_key_id
            headers["x-amz-server-side-encryption-aws-kms-key-id"] = self.kms_key_id
        url = await asyncio.to_thread(
            lambda: self.client.generate_presigned_url(
                "put_object", Params=params, ExpiresIn=expires_in, HttpMethod="PUT"
            )
        )
        return {"url": url, "method": "PUT", "headers": headers, "contentLength": size}

    async def create_download(self, key: str, *, expires_in: int) -> Dict[str, Any]:
        object_key = _prefixed(self.prefix, key)
        url = await asyncio.to_thread(
            lambda: self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_key},
                ExpiresIn=expires_in,
                HttpMethod="GET",
            )
        )
        return {"url": url, "method": "GET", "headers": {}}

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
        object_key = _prefixed(self.prefix, key)
        digest = hashlib.sha256(content).hexdigest()

        def store() -> None:
            try:
                existing = self.client.head_object(Bucket=self.bucket, Key=object_key)
            except Exception as exc:
                response = getattr(exc, "response", {})
                code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
                if code not in {"404", "NoSuchKey", "NotFound"} and exc.__class__.__name__ not in {
                    "NoSuchKey", "NotFound"
                } and not isinstance(exc, FileNotFoundError):
                    raise
            else:
                existing_digest = (existing.get("Metadata") or {}).get("sha256")
                if existing.get("ContentLength") != len(content) or existing_digest != digest:
                    raise ValueError("immutable artifact key already contains different bytes")
                return
            options: Dict[str, Any] = {
                "Bucket": self.bucket,
                "Key": object_key,
                "Body": content,
                "ContentType": media_type,
                "Metadata": {"sha256": digest, **{str(k): str(v) for k, v in (metadata or {}).items()}},
            }
            if self.server_side_encryption:
                options["ServerSideEncryption"] = self.server_side_encryption
            if self.kms_key_id:
                options["SSEKMSKeyId"] = self.kms_key_id
            self.client.put_object(**options)

        await asyncio.to_thread(store)
        return ArtifactLocation(
            key=key,
            uri=f"s3://{self.bucket}/{object_key}",
            provider=self.provider,
            size=len(content),
            metadata={"sha256": digest, **(metadata or {})},
        )

    async def get(self, key: str) -> bytes:
        object_key = _prefixed(self.prefix, key)

        def load() -> bytes:
            response = self.client.get_object(Bucket=self.bucket, Key=object_key)
            body = response["Body"]
            return body.read() if hasattr(body, "read") else bytes(body)

        return await asyncio.to_thread(load)

    async def stat(self, key: str) -> ArtifactLocation:
        object_key = _prefixed(self.prefix, key)
        response = await asyncio.to_thread(
            lambda: self.client.head_object(Bucket=self.bucket, Key=object_key)
        )
        return ArtifactLocation(
            key=key,
            uri=f"s3://{self.bucket}/{object_key}",
            provider=self.provider,
            size=int(response.get("ContentLength") or 0),
            metadata=dict(response.get("Metadata") or {}),
        )


class AzureBlobArtifactStore:
    """Immutable Azure Blob storage adapter."""

    provider = "azure-blob"

    def __init__(
        self,
        container: str,
        *,
        prefix: str = "",
        service_client: Any | None = None,
        connection_string: str | None = None,
    ):
        if not container.strip():
            raise ValueError("Azure artifact storage requires a container")
        self.container = container.strip()
        self.prefix = prefix.strip("/")
        if service_client is None:
            if not connection_string:
                raise ValueError("Azure artifact storage requires connection_string")
            try:
                from azure.storage.blob import BlobServiceClient
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("Install kehrnel-core[cdisc-cloud] for Azure artifacts") from exc
            service_client = BlobServiceClient.from_connection_string(connection_string)
        self.service_client = service_client

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
        object_key = _prefixed(self.prefix, key)
        digest = hashlib.sha256(content).hexdigest()
        blob = self.service_client.get_blob_client(container=self.container, blob=object_key)

        def store() -> None:
            if blob.exists():
                properties = blob.get_blob_properties()
                existing_metadata = getattr(properties, "metadata", {}) or {}
                existing_size = getattr(properties, "size", None)
                if existing_size != len(content) or existing_metadata.get("sha256") != digest:
                    raise ValueError("immutable artifact key already contains different bytes")
                return
            from azure.storage.blob import ContentSettings

            blob.upload_blob(
                content,
                overwrite=False,
                metadata={"sha256": digest, **{str(k): str(v) for k, v in (metadata or {}).items()}},
                content_settings=ContentSettings(content_type=media_type),
            )

        await asyncio.to_thread(store)
        return ArtifactLocation(
            key=key,
            uri=f"azure://{self.container}/{object_key}",
            provider=self.provider,
            size=len(content),
            metadata={"sha256": digest, **(metadata or {})},
        )

    async def get(self, key: str) -> bytes:
        object_key = _prefixed(self.prefix, key)
        blob = self.service_client.get_blob_client(container=self.container, blob=object_key)
        return await asyncio.to_thread(lambda: blob.download_blob().readall())

    async def stat(self, key: str) -> ArtifactLocation:
        object_key = _prefixed(self.prefix, key)
        blob = self.service_client.get_blob_client(container=self.container, blob=object_key)
        properties = await asyncio.to_thread(blob.get_blob_properties)
        return ArtifactLocation(
            key=key,
            uri=f"azure://{self.container}/{object_key}",
            provider=self.provider,
            size=int(getattr(properties, "size", 0)),
            metadata=dict(getattr(properties, "metadata", {}) or {}),
        )
