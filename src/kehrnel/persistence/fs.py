"""Filesystem persistence helpers and driver."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator


def write_jsonl(path: str | Path, docs: Iterable[Dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, default=str))
            f.write("\n")


def read_jsonl(path: str | Path) -> Iterator[Dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


class FileStore:
    """Simple JSONL-backed sink with the same interface as ``MongoStore``."""

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self.stats = type("S", (), {"inserted": 0})()
        self._base_path: Path | None = None
        self._compositions_path: Path | None = None
        self._search_path: Path | None = None

    def connect(self) -> None:
        base_path = Path(
            self.cfg.get("base_path")
            or self.cfg.get("path")
            or self.cfg.get("output_dir")
            or ".kehrnel/persistence"
        )
        compositions_name = self.cfg.get("compositions_file") or "compositions.jsonl"
        search_name = self.cfg.get("search_file") or "search.jsonl"
        base_path.mkdir(parents=True, exist_ok=True)
        self._base_path = base_path
        self._compositions_path = base_path / compositions_name
        self._search_path = base_path / search_name
        # Ensure files exist to simplify downstream tooling.
        self._compositions_path.touch(exist_ok=True)
        self._search_path.touch(exist_ok=True)

    def insert_one(self, doc: Dict[str, Any], *, search: bool = False) -> None:
        if self._compositions_path is None or self._search_path is None:
            self.connect()
        target = self._search_path if search else self._compositions_path
        assert target is not None
        with target.open("a", encoding="utf-8") as f:
            f.write(json.dumps(doc, default=str))
            f.write("\n")
        self.stats.inserted += 1

    def insert_many(self, docs: Iterable[Dict[str, Any]], workers: int = 4) -> None:
        # The FS sink is single-writer by design; keep workers for interface parity.
        for d in docs:
            self.insert_one(d, search=bool(d.get("sn") is not None))
