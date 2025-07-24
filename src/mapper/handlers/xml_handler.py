from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List

from lxml import etree                           # type: ignore
from mapper.mapping_engine import SourceHandler
from mapper.transforms import REGISTRY


class XMLHandler(SourceHandler):
    """Generic CDA / XML source handler with multiply_-helpers."""

    NS_DEFAULT: Dict[str, str] = {"cda": "urn:hl7-org:v3"}

    # ──────────────────────────── life-cycle ────────────────────────────
    def can_handle(self, path: Path) -> bool:
        return path.suffix.lower() in {".xml", ".cda"}

    def load_source(self, path: Path) -> etree._Element:
        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        return etree.parse(str(path), parser).getroot()

    # ────────────────────────── rule evaluation ─────────────────────────
    def extract_value(self, src: etree._Element, rule: Any) -> Any:
        # 1) one-liner shorthand  ("constant: foo", "xpath: //bar")
        if isinstance(rule, str):
            rule = rule.strip()
            if rule.startswith("constant:"):
                return rule.split("constant:", 1)[1].lstrip()
            if rule.startswith("xpath:"):
                return self._eval_xpath(src, rule.split("xpath:", 1)[1].strip())
            return rule  # literal

        # 2) YAML mapping ----------------------------------------------
        if isinstance(rule, dict):
            # ── base value ────────────────────────────────────────────
            if "xpath" in rule:
                val = self._eval_xpath(src, rule["xpath"])
            elif "constant" in rule:
                val = rule["constant"]
            else:
                raise ValueError("Rule needs 'xpath' or 'constant'")

            # ── aggregate (if list) ───────────────────────────────────
            if "aggregate" in rule and isinstance(val, list):
                how = rule["aggregate"]
                if how == "first":
                    val = val[0]
                elif how == "last":
                    val = val[-1]
                elif how.startswith("join:"):
                    sep = how.split("join:", 1)[1]
                    val = sep.join(str(v) for v in val)
                else:
                    raise ValueError(f"Unknown aggregate '{how}'")

            # ── transform ─────────────────────────────────────────────
            if "transform" in rule:
                try:
                    val = REGISTRY[rule["transform"]](val)
                except KeyError as e:
                    raise ValueError(f"Unknown transform '{rule['transform']}'") from e

            # ── map (element-wise if still a list) ────────────────────
            if "map" in rule:
                m = rule["map"]
                if isinstance(val, list):
                    val = [m.get(str(v), v) for v in val]
                else:
                    val = m.get(str(val), val)

            return val

        # 3) already a literal -----------------------------------------
        return rule

    # ──────────────────────────── helpers ──────────────────────────────
    def _eval_xpath(self, root: etree._Element, expr: str) -> Any:
        res: List[Any] = root.xpath(expr, namespaces=self.namespaces)
        if not res:
            return None
        if len(res) == 1:
            item = res[0]
            return item.text if isinstance(item, etree._Element) else item
        return res

    @property
    def namespaces(self) -> Dict[str, str]:
        """Default CDA ns  +  ones declared in mapping._metadata.namespaces"""
        extra = getattr(self, "_metadata", {}).get("namespaces", {})
        return {**self.NS_DEFAULT, **extra}

    def count_elements(self, src: etree._Element, xpath: str) -> int:
        return len(src.xpath(xpath, namespaces=self.namespaces))

    # ─────────────────────── preprocessing helpers ─────────────────────
    def preprocess_mapping(
        self, mapping: Dict[str, Any], src: etree._Element
    ) -> Dict[str, Any]:
        # cache metadata for namespaces()
        self._metadata = mapping.get("_metadata", {})

        steps = mapping.get("_preprocessing", [])
        expanded: Dict[str, Any] = {}

        # 1) copy rules w/o placeholder untouched
        for p, r in mapping.items():
            if "{i}" not in p:
                expanded[p] = r

        def _subst(obj: Any, i: int) -> Any:
            if isinstance(obj, str):
                return obj.replace("{i}", str(i))
            if isinstance(obj, dict):
                return {k: _subst(v, i) for k, v in obj.items()}
            return obj

        # 2) multiply_content (makes copies of the stub ADMIN_ENTRY)
        for step in steps:
            if step.get("type") != "multiply_content":
                continue
            count = self.count_elements(src, step["xpath"])
            for i in range(count):
                for path, rule in mapping.items():
                    if "{i}" not in path:
                        continue
                    expanded[_subst(path, i)] = _subst(rule, i)

        return expanded