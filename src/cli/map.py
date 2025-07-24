# ──────────────────────────────────────────────────────────────────────────────
# src/cli/map.py   →  `kehrnel-map`
# ──────────────────────────────────────────────────────────────────────────────
"""Apply a YAML/JSON mapping to a source document and emit a composition."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import typer
import yaml
from tabulate import tabulate

from core import kehrnelGenerator, kehrnelValidator, TemplateParser
from mapper import mapping_engine
from mapper.handlers.xml_handler import XMLHandler  # existing handler

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

    # ── load mapping & preprocess ──────────────────────────────────────────
    raw_map = yaml.safe_load(mapping.read_text()) \
        if mapping.suffix in {".yaml", ".yml"} else json.loads(mapping.read_text())

    handler = next((h for h in gen.handlers if h.can_handle(source)), None)
    if not handler:
        typer.secho(f"No handler registered for {source.suffix}", fg="red", err=True)
        raise typer.Exit(1)

    src_tree = handler.load_source(source)
    proc_map = handler.preprocess_mapping(raw_map, src_tree)

    # ── generate composition ───────────────────────────────────────────────
    comp = gen.generate_from_mapping(proc_map, source)

    # strict: unmapped jsonpaths ------------------------------------------------
    if strict:
        used = {p for p, _ in gen.iter_leaves(comp)}
        missed = [jp for jp in proc_map if not jp.startswith("_") and jp not in used]
        if missed:
            typer.secho("Unmapped CDA entries:", fg="red")
            for jp in missed:
                typer.echo(f"  • {jp}")
            raise typer.Exit(1)

    # validate composition -----------------------------------------------------
    issues = kehrnelValidator(tpl).validate(comp)
    if issues:
        typer.secho(f"⚠  {len(issues)} validation issues", fg="yellow")
        for i in issues:
            typer.echo(f"[{i.severity}] {i.path}: {i.message}")
        if strict:
            raise typer.Exit(1)

    # mapping trace table ------------------------------------------------------
    if trace:
        rows = gen.trace(proc_map, source)
        typer.echo(tabulate(rows, headers="keys", tablefmt="github"))

    # ── output ----------------------------------------------------------------
    text = json.dumps(comp, indent=2, ensure_ascii=False)
    (sys.stdout if output == Path("-") else output.open("w")).write(text)
    if output != Path("-"):
        typer.echo(f"✓ composition written to {output}")


if __name__ == "__main__":
    app()
