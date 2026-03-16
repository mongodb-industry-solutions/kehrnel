# ──────────────────────────────────────────────────────────────────────────────
#  core/webtemplate_parser.py
# 
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

class WebTemplate:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.wt   = json.loads(self.path.read_text(encoding="utf-8"))
        self.template_id = self.wt.get("templateId") or ""
        self._code_to_ordinal: Dict[str,int] = {}
        self._code_to_label  : Dict[str,str] = {}
        self._index()

    def _index(self) -> None:
        def walk(node: Dict[str, Any]):
            if not isinstance(node, dict): return
            # collect CODED_TEXT lists on DV_ORDINAL items
            if node.get("rmType") == "DV_ORDINAL":
                for inp in (node.get("inputs") or []):
                    if inp.get("type") == "CODED_TEXT":
                        for opt in (inp.get("list") or []):
                            code = str(opt.get("value"))
                            self._code_to_ordinal[code] = int(opt.get("ordinal", 0))
                            self._code_to_label[code]   = str(opt.get("label") or "")
            for ch in (node.get("children") or []):
                walk(ch)
        walk(self.wt.get("tree") or {})

    def ordinal_for(self, code: str) -> Optional[int]:
        return self._code_to_ordinal.get(str(code))

    def label_for(self, code: str) -> Optional[str]:
        return self._code_to_label.get(str(code))