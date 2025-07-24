# kehrnel/transform/single.py

import json
import typer
from .core import Transformer

app = typer.Typer()

@app.command("to-mongo")
def to_mongo(
    input_file: str,
    mappings: str = "transform/config/mappings.yaml",
    shortcuts: str = "transform/config/shortcuts.jsonc",
    role: str = "primary"
):
    raw = json.load(open(input_file,encoding="utf-8"))
    t   = Transformer(mappings, shortcuts, role=role)
    base, search = t.flatten(raw)
    typer.echo(json.dumps({"base":base,"search":search}, default=str))

@app.command("reverse")
def reverse(
    flat_file: str,
    shortcuts: str = "transform/config/shortcuts.jsonc",
    codes: str = "transform/config/_codes.json"
):
    flat = json.load(open(flat_file,encoding="utf-8"))
    t = Transformer("", shortcuts, role="secondary")
    comp = t.reverse(flat)
    typer.echo(json.dumps(comp, indent=2, default=str))