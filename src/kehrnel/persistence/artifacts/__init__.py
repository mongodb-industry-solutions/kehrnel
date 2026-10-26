"""Checksum-addressed artifact persistence adapters."""

from .base import ArtifactLocation, ArtifactStoreAdapter
from .filesystem import FileSystemArtifactStore
from .object_store import AzureBlobArtifactStore, S3ArtifactStore

__all__ = [
    "ArtifactLocation",
    "ArtifactStoreAdapter",
    "FileSystemArtifactStore",
    "S3ArtifactStore",
    "AzureBlobArtifactStore",
]
