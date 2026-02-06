# src/cli/map.py  →  `kehrnel-map`
# src/cli/map.py  →  `kehrnel-map`
from __future__ import annotations

import json
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
import yaml

# ── core (OPT-based canonical generation + validation) ──────────────────────
try:
    from kehrnel.domains.openehr.templates import TemplateParser, kehrnelGenerator, kehrnelValidator
except Exception:
    from kehrnel.domains.openehr.templates.parser import TemplateParser
    from kehrnel.domains.openehr.templates.generator import kehrnelGenerator
    from kehrnel.domains.openehr.templates.validator import kehrnelValidator

# WebTemplate only to assert -t is valid (mapping is path-keyed already)
from kehrnel.domains.openehr.templates.webtemplate_parser import WebTemplate

from kehrnel.common.mapping.mapping_engine import apply_mapping

# Jinja + transforms (singular)
from kehrnel.common.mapping.utils.jinja_env import env as JINJA
from kehrnel.common.mapping.utils.transform import REGISTRY as TRANSFORMS
from kehrnel.common.mapping.utils.transform import attach_to_jinja

app = typer.Typer(add_completion=False, rich_markup_mode="rich")


def _default_strategies_root() -> Path:
    """Root where built-in strategy packs live (src/kehrnel/strategies)."""
    return Path(__file__).resolve().parent.parent / "kehrnel" / "strategies"


def _load_manifest_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _find_strategy_pack(strategy_id: str, strategies_root: Path) -> Optional[Path]:
    """Locate a strategy pack folder (containing manifest.json) by id."""
    if not strategies_root.exists():
        return None
    for manifest_path in strategies_root.rglob("manifest.json"):
        data = _load_manifest_json(manifest_path)
        if data and data.get("id") == strategy_id:
            return manifest_path.parent
    return None


def _resolve_mapping_path(
    mapping: Optional[Path],
    strategy: Optional[str],
    strategies_root: Optional[Path],
) -> Path:
    """
    Resolve the mapping file path.
    - If an explicit path exists, use it.
    - If a strategy id is provided, look inside that pack (default: ingest/config/mappings.*).
    """
    if mapping and mapping.exists():
        return mapping

    pack_root: Optional[Path] = None
    if strategy:
        base = strategies_root or _default_strategies_root()
        pack_root = _find_strategy_pack(strategy, base)
        if not pack_root:
            raise typer.BadParameter(f"Strategy '{strategy}' not found under {base}")

        # Relative mapping hint → resolve against pack root
        if mapping:
            candidate = pack_root / mapping
            if candidate.exists():
                return candidate

        # Try common defaults inside the pack
        for rel in (
            "ingest/config/mappings.yaml",
            "ingest/config/mappings.yml",
            "ingest/config/mappings.json",
        ):
            candidate = pack_root / rel
            if candidate.exists():
                return candidate

        raise typer.BadParameter(f"No mapping file found for strategy '{strategy}' (checked ingest/config/mappings.*)")

    if mapping:
        raise typer.BadParameter(f"Mapping file not found: {mapping}")

    raise typer.BadParameter("Provide --mapping or --strategy to locate a mapping file")


def _slug(s: str) -> str:
    import re
    s = re.sub(r"[^\w.-]+", "_", s, flags=re.UNICODE)
    return s.strip("._") or "file"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _resolve_output(out_base: Path, suggested: Optional[str]) -> Path:
    if out_base == Path("-"):
        return out_base
    if out_base.suffix:
        _ensure_dir(out_base.parent)
        return out_base
    _ensure_dir(out_base)
    if not suggested:
        suggested = f"composition_{dt.datetime.now():%Y%m%d_%H%M%S_%f}.json"
    suggested = _slug(suggested)
    if not Path(suggested).suffix:
        suggested += ".json"
    return out_base / suggested


def _render_expr(tpl: str, ctx: Dict[str, Any]) -> str:
    return JINJA.from_string(str(tpl)).render(ctx)


def _apply_transforms(val: Any, tlist: Optional[List[str]]) -> Any:
    out = val
    for t in (tlist or []):
        name = str(t).strip()
        fn = TRANSFORMS.get(name)
        if fn:
            out = fn(out)
            continue
        if name in {"int", "to_int"}:
            try:
                out = None if out in ("", None) else int(float(out))
            except Exception:
                pass
            continue
        if name in {"float", "to_float"}:
            try:
                out = None if out in ("", None) else float(out)
            except Exception:
                pass
            continue
        if name == "strip":
            out = None if out is None else str(out).strip()
            continue
        # date/datetime coercions are handled by handlers; pass-through here
        if name in {"date_iso", "datetime_iso"}:
            out = out
            continue
    return out


def _map_value(val: Any, mapping: Optional[Dict[str, Any]]) -> Any:
    if not mapping:
        return val
    import re
    s = str(val)
    for k, v in mapping.items():
        try:
            if re.fullmatch(k, s) or re.search(k, s):
                return v
        except re.error:
            pass
    return mapping.get(s, val)


def _map_ranges(val: Any, mapping: Optional[Dict[str, Any]]) -> Any:
    if not mapping:
        return val
    import re
    try:
        x = float(val)
    except Exception:
        return val
    for rng, mv in mapping.items():
        m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*\.\.\s*(-?\d+(?:\.\d+)?)\s*$", str(rng))
        if not m:
            continue
        lo, hi = float(m.group(1)), float(m.group(2))
        if lo <= x <= hi:
            return mv
    return val


@app.command()
def main(
    mapping: Optional[Path] = typer.Option(None, "-m", "--mapping", help="Mapping YAML/JSON file (optional when --strategy is set)"),
    strategy: Optional[str] = typer.Option(None, "-S", "--strategy", help="Strategy id to auto-resolve mapping under strategies/<...>/ingest/config"),
    strategies_root: Optional[Path] = typer.Option(None, "--strategies-root", help="Base directory to search strategy packs (default: src/kehrnel/strategies)"),
    source: Path = typer.Option(..., "-s", help="Source data to transform"),
    webtemplate: Path = typer.Option(..., "-t", help="WebTemplate JSON (.json)"),
    opt: Path = typer.Option(..., "-p", help="OPT template (.opt) for canonical COMPOSITION"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout, else file or folder"),
    strict: bool = typer.Option(False, help="Fail on validation errors"),
):
    """
    Build canonical openEHR COMPOSITIONs from a source dataset using the new path-keyed grammar.
    """
    # Locate mapping (explicit path or resolved from strategy pack)
    try:
        mapping_path = _resolve_mapping_path(mapping, strategy, strategies_root)
    except typer.BadParameter as exc:
        typer.secho(str(exc), fg="red", err=True)
        raise typer.Exit(2)

    # Mapping
    try:
        if mapping_path.suffix.lower() in {".yaml", ".yml"}:
            raw = yaml.safe_load(mapping_path.read_text())
        else:
            raw = json.loads(mapping_path.read_text())
    except Exception as e:
        typer.secho(f"Error reading mapping file: {e}", fg="red", err=True)
        raise typer.Exit(2)

    # WebTemplate (just sanity-checks -t)
    if not webtemplate.exists() or webtemplate.suffix.lower() != ".json":
        typer.secho("Provide a WebTemplate JSON via -t", fg="red", err=True)
        raise typer.Exit(2)
    _ = WebTemplate(webtemplate)

    # OPT → generator/validator
    if not opt.exists() or opt.suffix.lower() != ".opt":
        typer.secho("Provide an OPT template via -p", fg="red", err=True)
        raise typer.Exit(2)
    tpl = TemplateParser(opt)
    gen = kehrnelGenerator(tpl)
    validator = kehrnelValidator(tpl)

    # ── Translator hookup (from mapping meta.translation) ─────────────
    tx_cfg = (raw.get("meta") or {}).get("translation") or {}
    if tx_cfg.get("enabled"):
        try:
            from kehrnel.common.mapping.handlers.common import Translator  # re-export
        except Exception:
            from kehrnel.common.mapping.utils.translator.translator import Translator
        gen.translator = Translator(
            source_lang = tx_cfg.get("source_lang", "es"),
            target_lang = tx_cfg.get("target_lang", "en"),
            cache_path  = tx_cfg.get("cache_file", ".kehrnel/translations.json"),
        )
        typer.echo(
            f"[translation] enabled {tx_cfg.get('source_lang','es')}→{tx_cfg.get('target_lang','en')} "
            f"(cache: {tx_cfg.get('cache_file','.kehrnel/translations.json')})"
        )

    # Handler
    from kehrnel.common.mapping.handlers.csv_handler import CSVHandler
    from kehrnel.common.mapping.handlers.xml_handler import XMLHandler
    handlers = [CSVHandler(), XMLHandler()]
    handler = next((h for h in handlers if h.can_handle(source)), None)
    if not handler:
        typer.secho(f"No handler for '{source.suffix}'", fg="red", err=True)
        raise typer.Exit(2)

    try:
        src_tree = handler.load_source(source)
    except Exception as e:
        typer.secho(f"Error loading source: {e}", fg="red", err=True)
        raise typer.Exit(2)

    # Grouping/preprocess (new grammar)
    try:
        groups = handler.preprocess_mapping_new(raw, src_tree)
    except Exception as e:
        typer.secho(f"Error preprocessing mapping: {e}", fg="red", err=True)
        raise typer.Exit(2)

    out_cfg: Dict[str, Any] = (raw.get("output") or {})
    env_spec: Dict[str, Any] = (out_cfg.get("envelope") or {})
    comp_key: str = out_cfg.get("composition_key") or "canonicalJSON"

    # Output strategy
    is_stdout = (output == Path("-"))
    is_dir = (not is_stdout) and (output.suffix == "" or output.is_dir())
    if is_dir:
        output.mkdir(parents=True, exist_ok=True)

    written = 0

    for idx, grp in enumerate(groups, start=1):
        rows: List[Dict[str, Any]] = grp.get("rows") or []
        first: Dict[str, Any] = rows[0] if rows else {}
        flat_map: Dict[str, Dict[str, Any]] = grp["map"]
        prune: bool = bool(grp.get("prune_empty") or out_cfg.get("prune_empty"))

        # Filename
        filename_tpl = out_cfg.get("filename")
        if filename_tpl:
            try:
                fname = _render_expr(filename_tpl, {"first": first, "rows": rows, "index": idx})
            except Exception:
                fname = None
        else:
            fname = grp.get("filename")

        # Build canonical from OPT, apply mapping
        try:
            comp: Dict[str, Any] = gen.generate_minimal()
            comp = apply_mapping(gen, flat_map, comp)
        except Exception as e:
            typer.secho(f"[group {idx}] Error applying mapping: {e}", fg="red", err=True)
            raise typer.Exit(1)

        # Normalize/prune
        try:
            comp = gen._normalize_for_rm(comp)
            comp = gen._prune_incomplete_datavalues(comp)
            if prune:
                gen._prune_empty(comp)

            root_lang = (comp.get("language") or {}).get("code_string") or "en"
            for entry in comp.get("content", []):
                if entry.get("_type") in {"OBSERVATION", "EVALUATION", "INSTRUCTION", "ACTION", "ADMIN_ENTRY"}:
                    entry["language"] = {
                        "_type": "CODE_PHRASE",
                        "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                        "code_string": root_lang
                    }
        except Exception as e:
            typer.secho(f"[group {idx}] Error normalizing/pruning: {e}", fg="red", err=True)
            raise typer.Exit(1)

        # Validate
        try:
            issues = validator.validate(comp)
        except Exception as e:
            typer.secho(f"[group {idx}] Validation failed: {e}", fg="red", err=True)
            raise typer.Exit(1)

        if issues and strict:
            for i in issues:
                path = getattr(i, "path", "")
                message = getattr(i, "message", str(i))
                severity = getattr(getattr(i, "severity", None), "value", "ERROR")
                typer.echo(f"[{severity}] {path}: {message}")
            raise typer.Exit(1)

        # Envelope (AFTER we have comp)
        def _eval_rule(rule: Any) -> Any:
            if rule is None:
                return None
            if not isinstance(rule, dict):
                return rule

            if "set" in rule:
                val = rule["set"]
            elif "expr" in rule:
                val = _render_expr(rule["expr"], {"rows": rows, "first": first, "comp": comp})
            elif "get" in rule:
                src = rule["get"]
                if isinstance(src, dict):
                    col = src.get("column") or src.get("from")
                else:
                    col = str(src)
                val = (first.get(col) if isinstance(first, dict) else None)
            else:
                val = None

            val = _apply_transforms(val, rule.get("transform"))
            val = _map_value(val, rule.get("map"))
            val = _map_ranges(val, rule.get("map_ranges"))

            if rule.get("null_if_empty") and (val is None or str(val).strip() == ""):
                return None
            return val

        root_doc: Dict[str, Any] = {}
        for k, r in env_spec.items():
            try:
                v = _eval_rule(r)
                if v is not None:
                    root_doc[k] = v
            except Exception as e:
                typer.secho(f"[group {idx}] Envelope key '{k}' failed: {e}", fg="yellow", err=True)

        root_doc[comp_key] = comp

        # Write
        try:
            if is_stdout:
                typer.echo(json.dumps(root_doc, ensure_ascii=False, indent=2))
                written += 1
            else:
                out_path = _resolve_output(output, fname)
                out_path.write_text(json.dumps(root_doc, ensure_ascii=False, indent=2))
                written += 1
        except Exception as e:
            typer.secho(f"[group {idx}] Writing output failed: {e}", fg="red", err=True)
            raise typer.Exit(1)

    if not is_stdout:
        typer.echo(f"✓ {written} file(s) written to {output}")


if __name__ == "__main__":
    attach_to_jinja(JINJA)
    app()
