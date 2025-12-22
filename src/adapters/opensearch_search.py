from __future__ import annotations

from typing import Any, Dict, List
from opensearchpy import OpenSearch


class OpenSearchAdapter:
    """
    Thin adapter over OpenSearch for search/aggregate.
    """

    def __init__(self, client: OpenSearch, index: str):
        self.client = client
        self.index = index

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "OpenSearchAdapter":
        client = OpenSearch(
            hosts=cfg.get("hosts"),
            http_auth=cfg.get("auth"),
            use_ssl=cfg.get("use_ssl", True),
            verify_certs=cfg.get("verify_certs", True),
            ssl_assert_hostname=cfg.get("ssl_assert_hostname", True),
            ssl_show_warn=cfg.get("ssl_show_warn", False),
        )
        return cls(client, cfg.get("index"))

    def search(self, query: Dict[str, Any]) -> Any:
        return self.client.search(index=self.index, body=query)

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> Any:
        # OpenSearch aggregates embedded in search body
        body = {"size": 0, "aggs": pipeline[0].get("aggs", {}) if pipeline else {}}
        return self.client.search(index=self.index, body=body)
