#src/mapper/document_identifier.py
"""
Deterministic document-type identifier.

Each pattern in *patterns.yaml* is an “all-or-nothing” rule-set:
every condition must hold for the pattern to match.

Supported handlers: xml | csv | json | hl7v2
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml
from dataclasses import dataclass, field
from lxml import etree
import logging

log = logging.getLogger("kehrnel.identifier")
log.addHandler(logging.StreamHandler())     
log.setLevel(logging.INFO)                  


# ─────────────────────────────────────────────────────────────────────────────
# Pattern definition
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class DocumentPattern:
    name: str
    handler: str                        # xml | csv | json | hl7v2
    priority: int = 50                 # higher = evaluated first

    # handler-specific mandatory checks
    required_elements: List[str] = field(default_factory=list)   # XML / JSON
    xpath_patterns: List[str] = field(default_factory=list)      # XML
    namespaces: Dict[str, str] = field(default_factory=dict)     # XML
    csv_headers: List[str] = field(default_factory=list)         # CSV
    exclude_elements: List[str] = field(default_factory=list)    # all types


# ─────────────────────────────────────────────────────────────────────────────
# Identifier class
# ─────────────────────────────────────────────────────────────────────────────
class DocumentIdentifier:
    """
    Deterministic document identifier: first pattern that passes wins.

    Patterns are gathered from, in that order :
      1. the hard-coded *patterns.yaml* next to this file
      2. one or more **extra config files** (YAML or JSON) passed by the
         caller – use this for customer-specific overrides
      3. the FastAPI layer may append patterns coming from Mongo
    """

    DEFAULT_CONFIG_FILE = Path(__file__).with_name("patterns.yaml")

    # ───────────── loader ──────────────────────────────────────────────────
    @staticmethod
    def _deserialize(doc: Dict[str, Any]) -> "DocumentPattern":
        """Build a DocumentPattern from a raw dict, filling defaults."""
        return DocumentPattern(
            name=doc.get("name"),
            handler=doc.get("handler"),
            priority=doc.get("priority", 50),
            required_elements=doc.get("required_elements", []),
            xpath_patterns=doc.get("xpath_patterns", []),
            namespaces=doc.get("namespaces", {}),
            csv_headers=doc.get("csv_headers", []),
            exclude_elements=doc.get("exclude_elements", []),
        )

    @classmethod
    def _read_config_file(cls, path: Path) -> List["DocumentPattern"]:
        """Read *one* YAML or JSON config file → list[DocumentPattern]."""
        if not path.exists():
            raise FileNotFoundError(path)

        if path.suffix.lower() in {".yml", ".yaml"}:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        elif path.suffix.lower() == ".json":
            raw = json.loads(path.read_text(encoding="utf-8")) or []
        else:
            raise ValueError(f"Unsupported pattern file: {path}")

        if not isinstance(raw, list):
            raise ValueError("Pattern file must contain a YAML/JSON *sequence*")
        return [cls._deserialize(item) for item in raw]

    @classmethod
    def _load_patterns(cls,
                   extra_files: Sequence[Path] | None = None,
                   include_default: bool = True) -> list[DocumentPattern]:
        files = ([] if not include_default else [cls.DEFAULT_CONFIG_FILE]) + \
            list(extra_files or [])
        patterns: list[DocumentPattern] = []
        for f in files:
            if f:
                patterns.extend(cls._read_config_file(f))
        return sorted(patterns, key=lambda p: p.priority, reverse=True)

    # ───────────── ctor ────────────────────────────────────────────────────
    def __init__(
        self,
        patterns:       Optional[List[DocumentPattern]] = None,
        *,
        pattern_files:  Optional[Sequence[str | Path]] = None,
        include_default: bool = True,
        debug:          bool = False,
    ) -> None:
        self.patterns: list[DocumentPattern] = (
            sorted(patterns, key=lambda p: p.priority, reverse=True)
            if patterns                               
            else self._load_patterns(
                            extra_files=[Path(p) for p in pattern_files] if pattern_files else [],
                            include_default=include_default,
                        )
        )
        if debug:
            log.setLevel(logging.DEBUG)     
        self.debug = debug

    # ───────────── public API ──────────────────────────────────────────────
    def identify_document(self, file_path: Path) -> Dict[str, Any]:
        ext = file_path.suffix.lower()

        if self._is_hl7v2(file_path):
            return self._identify_hl7v2(file_path)
        if ext in {".xml", ".cda"}:
            return self._identify_xml(file_path)
        if ext in {".csv", ".tsv", ".txt"}:
            return self._identify_csv(file_path)
        if ext == ".json":
            return self._identify_json(file_path)

        return {
            "documentType": "unknown",
            "handler": "unknown",
            "sampleData": {},
            "structure": {"error": f"Unsupported file type: {ext}"},
        }

    # ───────────── XML ─────────────────────────────────────────────────────
    def _identify_xml(self, path: Path) -> Dict[str, Any]:
        root = etree.parse(str(path), etree.XMLParser(remove_blank_text=True, recover=True)).getroot()

        for pat in self.patterns:
            if pat.handler != "xml":
                continue
            if self._matches_xml(root, pat):
                return {
                    "documentType": pat.name,
                    "handler": "xml",
                    "sampleData": self._extract_xml_sample(root, pat),
                    "structure": self._extract_xml_structure(root),
                }

        return {
            "documentType": "unknown_xml",
            "handler": "xml",
            "sampleData": {},
            "structure": self._extract_xml_structure(root),
        }

    def _matches_xml(self, root: etree._Element,
                     pat: DocumentPattern) -> bool:
        """Return True when *all* deterministic conditions of *pat* hold."""

        # 1. Excluded elements --------------------------------------------------
        for ex in pat.exclude_elements:
            if root.xpath(f"//*[local-name()='{ex}']"):
                log.debug("⨯ %-20s  element <%s> excluded", pat.name, ex)
                return False

        # 2. Deterministic XPath tests -----------------------------------------
        for xp in pat.xpath_patterns:
            try:
                ok = bool(root.xpath(xp, namespaces=pat.namespaces))
            except (etree.XPathSyntaxError,
                    etree.XPathEvalError,
                    etree.XPathError) as err:
                log.debug("⚠  %-20s  bad XPath → %s", pat.name, err)
                return False            # treat the whole pattern as non-match
            if not ok:
                log.debug("⨯ %-20s  XPath NO-MATCH: %s", pat.name, xp)
                return False

        # 3. Required elements --------------------------------------------------
        for el in pat.required_elements:
            if not root.xpath(f"//*[local-name()='{el}']"):
                log.debug("⨯ %-20s  missing element <%s>", pat.name, el)
                return False

        log.debug("✓ %-20s  all tests passed", pat.name)
        return True

    # ───────────── CSV ─────────────────────────────────────────────────────
    # ───────────── CSV ─────────────────────────────────────────────────────
# ───────────── CSV ─────────────────────────────────────────────────────
    def _identify_csv(self, path: Path) -> Dict[str, Any]:

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(4096)
            f.seek(0)
            dialect  = csv.Sniffer().sniff(sample)
            reader   = csv.DictReader(f, delimiter=dialect.delimiter)
            headers  = {h.lower().strip() for h in (reader.fieldnames or [])}
            firstrow = next(reader, None)

        matches: list[DocumentPattern] = [
            p for p in self.patterns
            if p.handler == "csv"
            and set(h.lower() for h in p.csv_headers).issubset(headers)
        ]

        if not matches:
            return {
                "documentType": "unknown_csv",
                "handler": "csv",
                "sampleData": firstrow or {},
                "structure": {"headers": sorted(headers), "delimiter": dialect.delimiter},
            }

        matches.sort(key=lambda p: (-len(p.csv_headers), p.name))
        pat = matches[0]

        return {
            "documentType": pat.name,
            "handler": "csv",
            "sampleData": firstrow or {},
            "structure": {"headers": sorted(headers), "delimiter": dialect.delimiter},
        }

    # ───────────── JSON ────────────────────────────────────────────────────
    def _identify_json(self, path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return {
                "documentType": "invalid_json",
                "handler": "json",
                "sampleData": {},
                "structure": {"type": type(data).__name__},
            }

        for pat in self.patterns:
            if pat.handler != "json":
                continue
            # required keys
            if any(k not in data for k in pat.required_elements):
                continue
            # exclusions
            if any(k in data for k in pat.exclude_elements):
                continue
            return {
                "documentType": pat.name,
                "handler": "json",
                "sampleData": {k: data[k] for k in pat.required_elements},
                "structure": {"topLevelKeys": list(data.keys())},
            }

        return {
            "documentType": "unknown_json",
            "handler": "json",
            "sampleData": {},
            "structure": {"topLevelKeys": list(data.keys())},
        }

    # ───────────── HL7 v2 ──────────────────────────────────────────────────
    def _identify_hl7v2(self, path: Path) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln.strip() for ln in f if ln.strip()]

        segs = [ln[:3] for ln in lines]

        for pat in self.patterns:
            if pat.handler != "hl7v2":
                continue
            if all(req in segs for req in pat.required_elements):
                return {
                    "documentType": pat.name,
                    "handler": "hl7v2",
                    "sampleData": {"segments": segs[:10]},
                    "structure": {"segments": segs, "count": len(lines)},
                }

        return {
            "documentType": "unknown_hl7v2",
            "handler": "hl7v2",
            "sampleData": {},
            "structure": {"segments": segs, "count": len(lines)},
        }

    # ───────────── helpers ─────────────────────────────────────────────────
    @staticmethod
    def _is_hl7v2(path: Path) -> bool:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                return f.readline().startswith("MSH|")
        except Exception:
            return False

    # minimal sample/structure helpers – unchanged logic
        # ───────────── helpers ──────────────────────────────────────────────
    # ───────────── helpers ─────────────────────────────────────────────────
    def _extract_xml_sample(
        self,
        root: etree._Element,
        *_,
    ) -> Dict[str, str]:
        """
        Return the same four high-level CDA fields for *every* document,
        regardless of which pattern matched.
        """
        ns = {"cda": "urn:hl7-org:v3"}

        def grab(xp: str) -> str | None:
            """Return the first node’s string-value (or None if missing)."""
            value = root.xpath(f"string({xp})", namespaces=ns)
            return value if value else None

        return {
            "code":          grab("//cda:code/@code"),
            "displayName":   grab("//cda:code/@displayName"),
            "title":         grab("//cda:title"),
            "effectiveTime": grab("//cda:effectiveTime/@value"),
        }

    def _extract_xml_structure(self, root: etree._Element) -> Dict[str, Any]:
        elems = {etree.QName(n).localname for n in root.iter()}
        return {
            "rootElement": etree.QName(root).localname,
            "elementCount": len(elems),
            "elements": sorted(elems)[:50],
        }