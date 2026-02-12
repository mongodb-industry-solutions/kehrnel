"""OpenEHR ingestion helpers."""

from .bulk import run, from_mongo

__all__ = ["run", "from_mongo"]
