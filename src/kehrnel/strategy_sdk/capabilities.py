from __future__ import annotations

from enum import Enum


class StrategyCapability(str, Enum):
    """
    Standardized capability flags. A strategy can implement any subset.
    These are intentionally coarse-grained; finer-grained detail lives in the manifest.
    """

    INGEST = "ingest"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    MAP = "map"
    IDENTIFY = "identify"
    QUERY = "query"
    SEARCH = "search"
    GENERATE = "generate"
    ENRICH = "enrich"
    EMBED = "embed"
    SYNTHETIC = "synthetic"
    CATALOG = "catalog"
