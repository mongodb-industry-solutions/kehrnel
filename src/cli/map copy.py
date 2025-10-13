# ──────────────────────────────────────────────────────────────────────────────
# src/cli/map.py   →  `kehrnel-map`
# ──────────────────────────────────────────────────────────────────────────────
"""Apply a YAML/JSON mapping to a source document and emit a composition."""
from __future__ import annotations

import json
import sys
from pathlib import Path
import re
import datetime as dt

import typer
import yaml
from tabulate import tabulate
from mapper.utils.translation_cache import Translator


from core import kehrnelGenerator, kehrnelValidator, TemplateParser
from mapper import mapping_engine
from mapper.handlers.xml_handler import XMLHandler 
from mapper.handlers.csv_handler import CSVHandler
from mapper.utils.jinja_env import env as _JINJA

from mapper.utils.macro_expander import expand_macros


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



app = typer.Typer(add_completion=False, rich_markup_mode="rich")


@app.command()
def main(
    mapping: Path = typer.Option(..., "-m", help="Mapping YAML/JSON file"),
    source: Path = typer.Option(..., "-s", help="Source data to transform"),
    template: Path | None = typer.Option(None, "-t", help="OPT template (.opt)"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    strict: bool = typer.Option(False, help="Fail on unmapped / validation"),
    trace: bool = typer.Option(False, help="Print mapping trace table"),
):
    """Generate a composition from *source* + *mapping*.
    """

    # ── derive template when -t omitted ────────────────────────────────────
    tpl_path = template
    if not tpl_path:
        meta = yaml.safe_load(mapping.read_text()) or {}
        target_tpl = meta.get("_metadata", {}).get("target_template")
        if not target_tpl:
            typer.secho("Template not given and _metadata.target_template missing",
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

    # ── load mapping & preprocess ──────────────────────────────────────────
    raw_map = yaml.safe_load(mapping.read_text()) \
        if mapping.suffix in {".yaml", ".yml"} else json.loads(mapping.read_text())

    tr_cfg = (raw_map.get("_translation") or {})
    translator = None
    if tr_cfg.get("enabled"):
        translator = Translator(
            source_lang = tr_cfg.get("source_lang", "es"),
            target_lang = tr_cfg.get("target_lang", "en"),
            cache_path  = Path(tr_cfg.get("cache_file", ".kehrnel/translations.jsonl")),
            # plug your LLM here later: llm_call=my_translate
        )
        
    handler = next((h for h in gen.handlers if h.can_handle(source)), None)
    if not handler:
        typer.secho(f"No handler registered for {source.suffix}", fg="red", err=True)
        raise typer.Exit(1)

    src_tree = handler.load_source(source)
    prepped  = handler.preprocess_mapping(raw_map, src_tree)

    # normalize to a list of groups with expanded mappings
    if prepped is None:
        groups = [({}, expand_macros(raw_map))]
    elif isinstance(prepped, dict):
        groups = [({}, expand_macros(prepped))]
    else:
        groups = [(g, expand_macros(m)) for (g, m) in prepped]

    # optional filename template provided by YAML (generic)
    out_cfg = (raw_map.get("_output") or {})
    fname_tmpl = out_cfg.get("filename_template")  # Jinja template (optional)

    # if -o is a directory (or no suffix), write one file per group
    is_dir_out = (output != Path("-") and (output.suffix == "" or output.is_dir()))
    if is_dir_out:
        output.mkdir(parents=True, exist_ok=True)

    written = 0
    for idx, (grp_rows, grp_map) in enumerate(groups, start=1):
        comp = gen.generate_from_mapping(grp_map, source)

        issues = kehrnelValidator(tpl).validate(comp)
        if issues:
            typer.secho(f"⚠  {len(issues)} validation issues (group {idx})", fg="yellow")
            for i in issues:
                typer.echo(f"[{i.severity}] {i.path}: {i.message}")
            if strict:
                raise typer.Exit(1)

                # ── write one file per group (directory) or a single file/stdout ──
        if is_dir_out:
            if fname_tmpl:
                first = (grp_rows[0] if isinstance(grp_rows, list) and grp_rows else {})
                name  = _JINJA.from_string(fname_tmpl).render({
                    "index": idx,
                    "first": first,
                    "rows": grp_rows,
                })
            else:
                name = f"composition_{idx:04d}.json"

            out_path = _resolve_output_file(output, name)  # joins directory + filename
            out_path = _dedupe_path(out_path)
            out_path.write_text(
                json.dumps(comp, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            written += 1
        else:
            if output == Path("-"):
                sys.stdout.write(json.dumps(comp, indent=2, ensure_ascii=False))
            else:
                out_path = _resolve_output_file(output, None)  # ensures parent dir exists
                out_path.write_text(
                    json.dumps(comp, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )
                typer.echo(f"✓ composition written to {out_path}")
            break  # single output when not a directory

    if is_dir_out:
        typer.echo(f"✓ {written} composition(s) written to {output}")


if __name__ == "__main__":
    app()
