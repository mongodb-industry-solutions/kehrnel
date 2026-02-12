# src/kehrnel/legacy/transform/single.py
from __future__ import annotations
import json
from pathlib import Path
import typer

from .flattener_f import CompositionFlattener

# ----------------------------------------------------------------------
# Public helpers (importable from Python or tests)
# ----------------------------------------------------------------------
def flatten_one(path: str | Path, role: str = "primary"):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    comp = {
        "_id": raw.get("_id", "comp-1"),
        "ehr_id": raw.get("ehr_id", "ehr-1"),
        "composition_version": raw.get("composition_version"),
        "canonicalJSON": raw.get("canonicalJSON") or raw,
    }
    cfg = {
        "role": role,
        "apply_shortcuts": False,
        "paths": {"separator": "."},
        "collections": {},
    }
    flattener = CompositionFlattener(
        db=None,
        config=cfg,
        mappings_path=str(Path(__file__).parent / "config" / "flattener_mappings_f.jsonc"),
        mappings_content=None,
        coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )
    base, search = flattener.transform_composition(comp)
    return base, search

def expand_one(path: str | Path, config_path: Path | None = None):
    flat = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg = {}
    if config_path and Path(config_path).exists():
        cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    from .unflattener import CompositionUnflattener
    from .flattener_f import CompositionFlattener
    # build flattener to hydrate dictionaries/shortcuts
    fl_cfg = {
        "role": cfg.get("role", "secondary"),
        "apply_shortcuts": cfg.get("apply_shortcuts", False),
        "paths": cfg.get("paths", {"separator": "."}),
        "ids": cfg.get("ids", {}),
        "collections": cfg.get("collections", {}),
        "target": cfg.get("target", {}),
    }
    flattener = CompositionFlattener(
        db=None,
        config=fl_cfg,
        mappings_path=str(Path(__file__).parent / "config" / "flattener_mappings_f.jsonc"),
        mappings_content=None,
        coding_opts=cfg.get("coding_opts") or {"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )
    unfl = CompositionUnflattener(
        codec=flattener.path_codec,
        shortcuts=flattener.shortcut_keys,
        nodes_field=flattener.cf_nodes,
        path_field=flattener.cf_path,
        data_field=flattener.cf_data,
        list_index_field="li",
        kp_field="kp",
    )
    return unfl.unflatten(flat)


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
    comp = expand_one(src, config)
    out.write_text(json.dumps(comp, indent=2, ensure_ascii=False, default=str))
    typer.echo(f"✓ expanded → {out}")
