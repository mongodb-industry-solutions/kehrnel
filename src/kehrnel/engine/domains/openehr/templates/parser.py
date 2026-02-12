# parser.py
"""
Thin wrapper around xml.etree that parses an OPT once and exposes
cached helpers used by both the generator and validator.
"""
from functools import cached_property
from pathlib import Path
from xml.etree import ElementTree as ET


class TemplateParser:
    NS = {
        "opt": "http://schemas.openehr.org/v1",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }

    def __init__(self, opt_path: Path):
        self.opt_path = Path(opt_path)
        self.tree     = ET.parse(self.opt_path)

    # ───────────────────────────────────────────────────── cached helpers ──

    @cached_property
    def template_id(self) -> str:
        return self.tree.findtext(".//opt:template_id/opt:value",
                                  default="",
                                  namespaces=self.NS).strip()

    @cached_property
    def term_definitions(self) -> dict[str, str]:
        """Template-level term map (code → text)."""
        term_map: dict[str, str] = {}
        for term_def in self.tree.findall(".//opt:term_definitions", self.NS):
            code = term_def.get("code", "")
            for item in term_def.findall("opt:items", self.NS):
                if item.get("id") == "text" and item.text:
                    term_map[code] = item.text.strip()
                    break
        return term_map

    # ↓ Add here any other helper your generator / validator need