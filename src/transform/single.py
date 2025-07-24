# src/transform/single.py
from __future__ import annotations
import json
from pathlib import Path
import typer

from .core import Transformer           # ← your existing flatten/reverse logic

# ----------------------------------------------------------------------
# Public helpers (importable from Python or tests)
# ----------------------------------------------------------------------
def flatten_one(path: str | Path, role: str = "primary"):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    tr  = Transformer(role=role)
    return tr.flatten(raw)              # (full_doc, search_doc)

def expand_one(path: str | Path):
    flat = json.loads(Path(path).read_text(encoding="utf-8"))
    tr   = Transformer(role="secondary")
    return tr.reverse(flat)             # canonical JSON


# ----------------------------------------------------------------------
# Typer CLI
# ----------------------------------------------------------------------
app = typer.Typer(rich_markup_mode="rich", no_args_is_help=True)

@app.command()
def flatten(
    src: Path = typer.Argument(..., help="Canonical composition JSON"),
    out: Path = typer.Option("flat.json", "-o", help="Output file"),
    role: str = typer.Option("primary", help="primary|secondary (code allocation)"),
):
    """Flatten **one** canonical composition."""
    full, _search = flatten_one(src, role)
    out.write_text(json.dumps(full, default=str, indent=2))
    typer.echo(f"✓ flattened → {out}")

@app.command()
def expand(
    src: Path = typer.Argument(..., help="Flattened JSON"),
    out: Path = typer.Option("canonical.json", "-o", help="Output file"),
):
    """Expand **one** flattened doc back to canonical JSON."""
    comp = expand_one(src)
    out.write_text(json.dumps(comp, indent=2, ensure_ascii=False))
    typer.echo(f"✓ expanded → {out}")