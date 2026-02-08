# cli/transform.py  →  console-script: kehrnel-transform
from __future__ import annotations
import json, sys, typer
from pathlib import Path
from kehrnel.strategies.openehr.rps_dual.ingest.flattener_f import CompositionFlattener

app = typer.Typer(help="Canonical ↔ flattened transformer")

@app.command("flatten")
def flatten(
    source: Path = typer.Argument(..., exists=True, readable=True,
                                  help="Canonical composition JSON file"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    cfg:    Path = typer.Option(None, "-c", help="override config JSON (unused in legacy-compatible flatten mode)")
):
    """Convert canonical JSON to flattened docs (base + optional search)."""
    raw = json.loads(source.read_text(encoding="utf-8"))
    comp = {
        "_id": raw.get("_id", "comp-1"),
        "ehr_id": raw.get("ehr_id", "ehr-1"),
        "composition_version": raw.get("composition_version"),
        "canonicalJSON": raw.get("canonicalJSON") or raw,
    }
    flattener = CompositionFlattener(
        db=None,
        config={
            "role": "primary",
            "apply_shortcuts": False,
            "paths": {"separator": "."},
            "collections": {},
        },
        mappings_path=str(
            Path(__file__).resolve().parents[1]
            / "kehrnel"
            / "strategies"
            / "openehr"
            / "rps_dual"
            / "ingest"
            / "config"
            / "flattener_mappings_f.jsonc"
        ),
        mappings_content=None,
        coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )
    base, search = flattener.transform_composition(comp)
    docs = {"base": base}
    if search:
        docs["search"] = search
    text = json.dumps(docs, indent=2, default=str)
    (sys.stdout if output == Path("-") else output.open("w")).write(text)

@app.command("expand")
def expand(
    flat_file: Path = typer.Argument(..., exists=True, readable=True,
                                     help="Flattened JSON file"),
    output: Path = typer.Option(Path("-"), "-o", help="'-' = stdout"),
    cfg:    Path = typer.Option(None, "-c", help="override config JSON")
):
    """Reverse command is currently unavailable in this CLI."""
    raise typer.BadParameter(
        "expand is temporarily disabled in this CLI. "
        "Use the strategy/API reverse-transform path instead."
    )


if __name__ == "__main__":
    app()
