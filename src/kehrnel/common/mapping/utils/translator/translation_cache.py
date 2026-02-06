# src/mapper/utils/translator/translation_cache.py
from __future__ import annotations
from pathlib import Path
import json
from typing import Optional, Dict, Tuple

class TranslationCache:
    def __init__(self, path: str | Path = ".kehrnel/translations.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[Tuple[str, str, str], str] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._save()
            return
        try:
            obj = json.loads(self.path.read_text(encoding="utf-8"))
            # keys are tuples serialized as "sl|tl|src"
            for k, v in obj.items():
                sl, tl, src = k.split("|", 2)
                self._data[(sl, tl, src)] = v
        except Exception:
            self._data = {}

    def _save(self) -> None:
        out = {"|".join(k): v for k, v in self._data.items()}
        self.path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    def lookup(self, sl: str, tl: str, src: str) -> Optional[str]:
        return self._data.get((sl, tl, src))

    def put(self, sl: str, tl: str, src: str, dst: str) -> None:
        self._data[(sl, tl, src)] = dst
        self._save()