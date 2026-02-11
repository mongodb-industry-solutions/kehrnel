"""
URI-based configuration resolver for RPS-Dual strategy.

Supports:
- file://path/to/file.json - Load from local file (relative to strategy root)
- collection://collection_name - Load from MongoDB collection
- collection://collection_name?filter=value - Load with filter
- Inline objects (passed through as-is)
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import parse_qs, urlparse


STRATEGY_ROOT = Path(__file__).parent


def resolve_uri(
    uri_or_object: Union[str, Dict[str, Any], None],
    db: Optional[Any] = None,
    base_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """
    Resolve a URI reference to its content.

    Args:
        uri_or_object: A URI string (file://, collection://) or inline object
        db: Optional MongoDB database for collection:// URIs
        base_path: Base path for file:// URIs (defaults to strategy root)

    Returns:
        Resolved content as a dictionary, or None if not found
    """
    if uri_or_object is None:
        return None

    # Inline object - return as-is
    if isinstance(uri_or_object, dict):
        return uri_or_object

    if not isinstance(uri_or_object, str):
        return None

    uri = uri_or_object.strip()

    if uri.startswith("file://"):
        return _resolve_file_uri(uri, base_path or STRATEGY_ROOT)

    if uri.startswith("collection://"):
        return _resolve_collection_uri(uri, db)

    # Treat as file path for backwards compatibility
    return _resolve_file_path(uri, base_path or STRATEGY_ROOT)


async def resolve_uri_async(
    uri_or_object: Union[str, Dict[str, Any], None],
    db: Optional[Any] = None,
    base_path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """
    Async version of resolve_uri for collection:// URIs.
    """
    if uri_or_object is None:
        return None

    if isinstance(uri_or_object, dict):
        return uri_or_object

    if not isinstance(uri_or_object, str):
        return None

    uri = uri_or_object.strip()

    if uri.startswith("file://"):
        return _resolve_file_uri(uri, base_path or STRATEGY_ROOT)

    if uri.startswith("collection://"):
        return await _resolve_collection_uri_async(uri, db)

    return _resolve_file_path(uri, base_path or STRATEGY_ROOT)


def _resolve_file_uri(uri: str, base_path: Path) -> Optional[Dict[str, Any]]:
    """Resolve file://path/to/file.json"""
    path_str = uri[7:]  # Remove "file://"
    return _resolve_file_path(path_str, base_path)


def _resolve_file_path(path_str: str, base_path: Path) -> Optional[Dict[str, Any]]:
    """Resolve a file path (absolute or relative to base_path)"""
    path = Path(path_str)

    if not path.is_absolute():
        path = base_path / path
    else:
        allow_abs = os.getenv("KEHRNEL_ALLOW_ABSOLUTE_CONFIG_PATHS", "false").lower() in ("1", "true", "yes")
        if not allow_abs:
            raise ValueError("Absolute file paths are not allowed for strategy config resolution")

    resolved_base = base_path.resolve()
    resolved_path = path.resolve()
    if resolved_base not in resolved_path.parents and resolved_path != resolved_base:
        raise ValueError("Config file path escapes strategy base directory")

    if not resolved_path.exists():
        return None

    content = resolved_path.read_text(encoding="utf-8")

    # Strip comments for JSONC files
    if resolved_path.suffix.lower() in (".jsonc", ".json"):
        content = _strip_json_comments(content)

    return json.loads(content)


def _resolve_collection_uri(uri: str, db: Optional[Any]) -> Optional[Dict[str, Any]]:
    """Synchronous collection resolver - returns None, use async version"""
    # For sync code, return None and let caller handle
    return None


async def _resolve_collection_uri_async(uri: str, db: Optional[Any]) -> Optional[Dict[str, Any]]:
    """Resolve collection://name?filter=value"""
    if db is None:
        return None

    parsed = urlparse(uri)
    collection_name = parsed.netloc or parsed.path.lstrip("/")

    if not collection_name:
        return None

    # Parse query string for filters
    query_params = parse_qs(parsed.query)
    filter_doc: Dict[str, Any] = {}

    for key, values in query_params.items():
        if values:
            filter_doc[key] = values[0] if len(values) == 1 else values

    collection = db[collection_name]

    if filter_doc:
        doc = await collection.find_one(filter_doc)
    else:
        # Return all documents as a list under "items" key
        docs = await collection.find().to_list(length=1000)
        return {"items": docs} if docs else None

    return doc


def _strip_json_comments(content: str) -> str:
    """Strip // and /* */ comments from JSON content"""
    # Remove single-line comments
    content = re.sub(r"//.*?$", "", content, flags=re.MULTILINE)
    # Remove multi-line comments
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    return content


def get_collection_name(cfg: Dict[str, Any], key: str, default: str = "") -> str:
    """
    Extract collection name from config, supporting nested structure.

    Args:
        cfg: Configuration dictionary
        key: Key path like "collections.codes.name" or "codes"
        default: Default value if not found
    """
    parts = key.split(".")
    current = cfg

    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default

    return current if isinstance(current, str) else default


def merge_configs(strategy_cfg: Dict[str, Any], bulk_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Merge strategy and bulk configs for operations that need both.

    Strategy config takes precedence for overlapping keys.
    Bulk config is added under a 'bulk' key.
    """
    merged = dict(strategy_cfg)
    if bulk_cfg:
        merged["_bulk"] = bulk_cfg
    return merged


def extract_bulk_config(merged: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract bulk config from a merged configuration."""
    return merged.get("_bulk")
