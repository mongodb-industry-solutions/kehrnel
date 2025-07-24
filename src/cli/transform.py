# cli/transform.py  →  console-script: kehrnel-transform
from __future__ import annotations
import json, sys, typer
from pathlib import Path
from transform.core import Transformer, load_default_cfg

app = typer.Typer(help="Canonical ↔ flattened transformer")

@app.command("flatten")
def flatten(
    source: Path = typer.Argument(..., exists=True, readable=True,
                                  help="Canonical composition JSON file"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    cfg:    Path = typer.Option(None, "-c", help="override config JSON")
):
    """Convert *canonical* JSON (one file) to the **flattened** representation
    expected by the ingest driver.  Emits **two** docs (base + search)."""
    t = Transformer(load_default_cfg(cfg))
    docs = t.flatten(json.loads(source.read_text(encoding="utf-8")))
    text = json.dumps(docs, indent=2, default=str)
    (sys.stdout if output == Path("-") else output.open("w")).write(text)

@app.command("expand")
def expand(
    flat_file: Path = typer.Argument(..., exists=True, readable=True,
                                     help="Flattened JSON file"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    cfg:    Path = typer.Option(None, "-c", help="override config JSON")
):
    """Reverse a flattened doc back to canonical JSON."""
    t = Transformer(load_default_cfg(cfg), role="secondary")
    comp = t.reverse(json.loads(flat_file.read_text(encoding="utf-8")))
    text = json.dumps(comp, indent=2, default=str)
    (sys.stdout if output == Path("-") else output.open("w")).write(text)


if __name__ == "__main__":
    app()