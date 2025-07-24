# ──────────────────────────────────────────────────────────────────────────────
# src/cli/generate.py   →  `kehrnel-generate`
# ──────────────────────────────────────────────────────────────────────────────
"""Generate a composition skeleton (random or minimal).

Examples
--------
$ kehrnel-generate -t templates/T-IGR-PMSI-EXTRACT.opt -o out.json
$ kehrnel-generate -t x.opt --random | jq .
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from core import kehrnelGenerator, kehrnelValidator, TemplateParser

app = typer.Typer(add_completion=False, rich_markup_mode="rich")


@app.command()
def main(
    opt: Path = typer.Option(..., "-t", help="OPT template file"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    random: bool = typer.Option(
        False, "--random", "-r", help="Fill leaves with random demo values"
    ),
):
    """Generate **minimal** (default) or **random** composition skeleton."""

    tpl = TemplateParser(opt)
    gen = kehrnelGenerator (tpl)
    comp = gen.generate_random() if random else gen.generate_minimal()

    # quick validation feedback
    issues = kehrnelValidator(tpl).validate(comp)
    if issues:
        typer.secho(f"⚠  {len(issues)} validation issues", fg="yellow")

    text = json.dumps(comp, indent=2, ensure_ascii=False)
    (sys.stdout if output == Path("-") else output.open("w")).write(text)
    if output != Path("-"):
        typer.echo(f"✓ composition written to {output}")


if __name__ == "__main__":  # allow `python -m cli.generate …`
    app()


