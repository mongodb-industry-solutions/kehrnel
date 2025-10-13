# src/cli/map.py   →  `kehrnel-map`
"""Apply a YAML/JSON mapping to a source document and emit a composition."""
from __future__ import annotations

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True), override=False)

import json
import sys
from pathlib import Path
import re
import datetime as dt
from typing import Any, Dict, List

import typer
import yaml
from tabulate import tabulate

from core import kehrnelGenerator, kehrnelValidator, TemplateParser
from mapper import mapping_engine
from mapper.handlers.xml_handler import XMLHandler
from mapper.handlers.csv_handler import CSVHandler
from mapper.utils.jinja_env import env as _JINJA
from mapper.utils.macro_expander import expand_macros
from mapper.utils.translator import Translator


def _slug(s: str) -> str:
    s = re.sub(r"[^\w.-]+", "_", s, flags=re.UNICODE)
    return s.strip("._") or "file"


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _resolve_output_file(out_base: Path, suggested_filename: str | None) -> Path:
    """
    If out_base has a suffix → treat as a file path.
    Otherwise treat as a directory and join with suggested_filename (or a default).
    """
    if out_base.suffix:                           # ".json", ".xml", etc. → file path
        _ensure_dir(out_base.parent)
        return out_base

    # directory path
    _ensure_dir(out_base)
    if not suggested_filename:
        suggested_filename = f"composition_{dt.datetime.now():%Y%m%d_%H%M%S_%f}.json"
    suggested_filename = _slug(suggested_filename)
    if not Path(suggested_filename).suffix:
        suggested_filename += ".json"
    return out_base / suggested_filename


def _dedupe_path(p: Path) -> Path:
    """Avoid clobbering if multiple groups produce the same name."""
    if not p.exists():
        return p
    base, suf = p.stem, p.suffix
    i = 1
    while True:
        candidate = p.with_name(f"{base}_{i}{suf}")
        if not candidate.exists():
            return candidate
        i += 1


def _prune_empty(obj: Any) -> Any:
    """Recursively prune empty strings, lists, and dicts."""
    if isinstance(obj, dict):
        return {k: _prune_empty(v) for k, v in obj.items() if v not in (None, "", [], {})}
    if isinstance(obj, list):
        return [ _prune_empty(v) for v in obj if v not in (None, "", [], {}) ]
    return obj


app = typer.Typer(add_completion=False, rich_markup_mode="rich")


@app.command()
def main(
    mapping: Path = typer.Option(..., "-m", help="Mapping YAML/JSON file"),
    source: Path = typer.Option(..., "-s", help="Source data to transform"),
    template: Path | None = typer.Option(None, "-t", help="OPT template (.opt)"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout (otherwise file or folder)"),
    strict: bool = typer.Option(False, help="Fail on validation issues"),
    trace: bool = typer.Option(False, help="Print mapping trace table (legacy grammar only)"),
):
    """Generate a composition from *source* + *mapping*."""
    load_dotenv()

    # ── load mapping ────────────────────────────────────────────────────────
    raw_map = yaml.safe_load(mapping.read_text()) \
        if mapping.suffix.lower() in {".yaml", ".yml"} else json.loads(mapping.read_text())

    # ── derive template when -t omitted (supports both 'meta' and 'at') ────
    tpl_path = template
    meta = raw_map.get("meta") or raw_map.get("at") or raw_map.get("_metadata") or {}
    if not tpl_path:
        target_tpl = meta.get("target_template")
        if not target_tpl:
            typer.secho("Template not given and meta.target_template (or _metadata.target_template) missing",
                        fg="red", err=True)
            raise typer.Exit(1)
        tpl_path = (mapping.parent / ".." / "templates" / f"{target_tpl}.opt").resolve()
        if not tpl_path.exists():
            typer.secho(f"Derived template {tpl_path} not found", fg="red", err=True)
            raise typer.Exit(1)
        typer.echo(f"[auto] Using template {tpl_path}")

    # ── init core objects ──────────────────────────────────────────────────
    tpl = TemplateParser(tpl_path)
    gen = kehrnelGenerator(tpl)
    gen.register_handler(XMLHandler())
    gen.register_handler(CSVHandler())

    # ── translation config (global) ────────────────────────────────────────
    tr = (meta.get("translation") or raw_map.get("_translation") or {})
    if tr.get("enabled") or tr.get("enable"):
        gen.translator = Translator(
            source_lang = tr.get("source_lang", "es"),
            target_lang = tr.get("target_lang", "en"),
            cache_path  = tr.get("cache_file") or tr.get("cache") or ".kehrnel/translations.jsonl",
        )
    else:
        gen.translator = None

    # ── choose handler & preprocess ────────────────────────────────────────
    handler = next((h for h in gen.handlers if h.can_handle(source)), None)
    if not handler:
        typer.secho(f"No handler registered for {source.suffix}", fg="red", err=True)
        raise typer.Exit(1)

    src_tree = handler.load_source(source)
    prepped = handler.preprocess_mapping(raw_map, src_tree)  # supports new & legacy

    # Normalize to a list of group dicts with keys:
    #   rows, map, envelope (optional), filename (optional), prune_empty (bool)
    groups_norm: List[Dict[str, Any]] = []

    def _legacy_to_groups(obj) -> List[Dict[str, Any]]:
        # legacy returns: Dict (flat map) or List[ (rows, flat_map) ]
        if obj is None:
            return [{"rows": [], "map": expand_macros(raw_map)}]
        if isinstance(obj, dict):
            return [{"rows": [], "map": expand_macros(obj)}]
        # list
        out = []
        for tup in obj:
            rows, flat = tup
            out.append({"rows": rows, "map": expand_macros(flat)})
        return out

    if isinstance(prepped, list) and prepped and isinstance(prepped[0], dict) and "map" in prepped[0]:
        groups_norm = prepped  # new grammar path
    else:
        groups_norm = _legacy_to_groups(prepped)

    # output knobs (new grammar) or legacy
    out_cfg = raw_map.get("output") or raw_map.get("_output") or {}
    prune_empty = bool(out_cfg.get("prune_empty"))
    fname_tmpl  = out_cfg.get("filename") or out_cfg.get("filename_template")  # Jinja template
    include_envelope = "envelope" in out_cfg

    # if -o is a directory (or no suffix), write one file per group
    is_dir_out = (output != Path("-") and (output.suffix == "" or output.is_dir()))
    if is_dir_out:
        output.mkdir(parents=True, exist_ok=True)

    written = 0
    for idx, grp in enumerate(groups_norm, start=1):
        rows   = grp.get("rows") or []
        gmap   = grp.get("map") or {}
        env    = grp.get("envelope")
        gfile  = grp.get("filename")
        gprune = prune_empty if grp.get("prune_empty") is None else bool(grp.get("prune_empty"))

        comp = gen.generate_from_mapping(gmap, source)

        issues = kehrnelValidator(tpl).validate(comp)
        if issues:
            typer.secho(f"⚠  {len(issues)} validation issues (group {idx})", fg="yellow")
            for i in issues:
                typer.echo(f"[{i.severity}] {i.path}: {i.message}")
            if strict:
                raise typer.Exit(1)

        # build output document (optionally wrap with envelope)
        out_doc: Any = comp
        if env is not None or include_envelope:
            out_doc = {"envelope": env or {}, "composition": comp}

        if gprune:
            out_doc = _prune_empty(out_doc)

        # resolve filename
        if is_dir_out:
            if gfile:
                fname = gfile
            elif fname_tmpl:
                first = (rows[0] if isinstance(rows, list) and rows else {})
                fname = _JINJA.from_string(fname_tmpl).render({
                    "index": idx,
                    "first": first,
                    "rows": rows,
                    "envelope": env or {},
                })
            else:
                fname = f"composition_{idx:04d}.json"

            out_path = _resolve_output_file(output, fname)
            out_path = _dedupe_path(out_path)
            out_path.write_text(json.dumps(out_doc, indent=2, ensure_ascii=False), encoding="utf-8")
            written += 1
        else:
            if output == Path("-"):
                sys.stdout.write(json.dumps(out_doc, indent=2, ensure_ascii=False))
            else:
                out_path = _resolve_output_file(output, None)
                out_path.write_text(json.dumps(out_doc, indent=2, ensure_ascii=False), encoding="utf-8")
                typer.echo(f"✓ written to {out_path}")
            break  # single output when not a directory

    if is_dir_out:
        typer.echo(f"✓ {written} file(s) written to {output}")


if __name__ == "__main__":
    app()