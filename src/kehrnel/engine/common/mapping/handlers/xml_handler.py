#src/mapper/handlers/xml_handler.py

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List
from lxml import etree
from kehrnel.engine.common.mapping.utils.expr import evaluate as eval_expr
from kehrnel.engine.common.mapping.utils.jinja_env import env as JINJA
from kehrnel.engine.common.mapping.utils.transform import REGISTRY as TREG
import re

class XMLHandler:
    NS_DEFAULT = {"cda":"urn:hl7-org:v3"}

    def can_handle(self, path: Path) -> bool:
        return path.suffix.lower() in {".xml",".cda"}

    def load_source(self, path: Path) -> etree._Element:
        parser = etree.XMLParser(
            remove_blank_text=True,
            recover=False,
            resolve_entities=False,
            no_network=True,
            load_dtd=False,
            dtd_validation=False,
            huge_tree=False,
        )
        return etree.parse(str(path), parser).getroot()

    def preprocess_mapping_new(self, mapping: Dict, root: etree._Element) -> List[Dict[str, Any]]:
        meta    = mapping.get("meta") or {}
        inp     = mapping.get("input", {}) or {}
        out     = mapping.get("output", {}) or {}
        rules   = mapping.get("mappings") or {}
        ns      = (mapping.get("sources") or {}).get(meta.get("default_source","cda1"),{}).get("namespaces") or \
                  (mapping.get("input", {}) or {}).get("namespaces") or self.NS_DEFAULT

        def xp(expr: str):
            res = root.xpath(expr, namespaces=ns)
            if not res: return None
            if len(res) == 1:
                r = res[0]
                return r.text if isinstance(r, etree._Element) else r
            out_vals = []
            for e in res:
                out_vals.append(e.text if isinstance(e, etree._Element) else e)
            return out_vals

        groups = [ [ {"_root": root} ] ]  # nominal group
        norm_rules = [{**(rule or {}), "path": str(path_key)} for path_key, rule in (rules or {}).items()]

        result: List[Dict[str, Any]] = []
        for grp in groups:
            flat, env = {}, {}
            def _apply_transforms(val, trans: List[str]):
                v = val
                for t in (trans or []):
                    if t == "strip":
                        v = None if v is None else str(v).strip()
                    else:
                        fn = TREG.get(t); v = fn(v) if fn else v
                return v

            for r in norm_rules:
                path = r["path"].lstrip("/")
                when = r.get("when")
                if when and not eval_expr(when, row={"_xml":True}, vars={"xpath": xp}):
                    continue
                if "set" in r:
                    val = r["set"]
                elif "expr" in r:
                    val = JINJA.from_string(str(r["expr"])).render({"xpath": xp})
                elif "get" in r and "selector" in r["get"]:
                    sel = r["get"]["selector"]
                    val = xp(sel)
                    if isinstance(val, list):
                        if not val:
                            continue
                        val = val[-1] if r.get("overwrite") == "last" else val[0]
                else:
                    continue
                val = _apply_transforms(val, r.get("transform") or [])
                if r.get("null_if_empty") and (val is None or str(val).strip() == ""):
                    continue
                flat[path] = {"literal": val}

            for k, er in (out.get("envelope") or {}).items():
                if "get" in er and "selector" in er["get"]:
                    env[k] = xp(er["get"]["selector"])
                elif "set" in er:
                    env[k] = er["set"]
                elif "expr" in er:
                    env[k] = JINJA.from_string(str(er["expr"])).render({"xpath": xp})

            fname = None
            if out.get("filename"):
                fname = JINJA.from_string(str(out["filename"])).render({"rows": grp, "envelope": env})

            result.append({"rows": grp, "map": flat, "envelope": env or None, "filename": fname, "prune_empty": bool(out.get("prune_empty", False))})
        return result
