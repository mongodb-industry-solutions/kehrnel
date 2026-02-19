"""Ingestion helpers used by unified CLI commands."""
from __future__ import annotations
import json
import logging
import typer
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import yaml

from kehrnel.persistence import get_driver, list_drivers, MongoSource
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener

app = typer.Typer(help="Bulk-ingest flattened docs into a persistence driver")
log = logging.getLogger(__name__)


def _coerce_cli_value(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    try:
        return json.loads(text)
    except Exception:
        return text


def _parse_set_values(items: list[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid --set entry '{item}'. Use KEY=VALUE.")
        key, raw = item.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"Invalid --set entry '{item}'. KEY cannot be empty.")
        out[key] = _coerce_cli_value(raw)
    return out


def _normalize_driver_name(driver: str) -> str:
    raw = (driver or "").strip().lower()
    aliases = {
        "mongo": "mongodb",
        "file": "filesystem",
        "fs": "filesystem",
    }
    return aliases.get(raw, raw)


@app.command("file")
def from_file(
    jsonl: Path = typer.Argument(..., exists=True, readable=True,
                                 help="NDJSON file with flattened docs"),
    driver_cfg: Path = typer.Option(..., "-d", help="YAML/JSON driver config"),
    workers:    int  = typer.Option(4, help="parallel insert workers")
):
    """Read an **NDJSON** file (one flattened doc per line) and ingest it."""
    drv = get_driver(driver_cfg)
    drv.connect()

    def producer() -> Iterator[Dict[str, Any]]:
        with jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)

    drv.insert_many(producer(), workers=workers)
    log.info("done - inserted %d docs", getattr(getattr(drv, "stats", None), "inserted", 0))


@app.command("mongo-catchup")
def from_mongo(
    src_cfg:  Path = typer.Option(..., help="Source Mongo config JSON"),
    driver_cfg: Path = typer.Option(..., help="Target driver YAML/JSON"),
    limit: int = typer.Option(None, help="patient limit")
):
    """A convenience command that mimics the old *ingestion.py* behaviour
    (read canonical comps from one Mongo collection, flatten & write to
    another)."""
    source_cfg = json.loads(src_cfg.read_text(encoding="utf-8"))
    source = MongoSource(source_cfg, limit=limit)
    drv = get_driver(driver_cfg)
    drv.connect()

    # Canonical "catchup" uses literal encoding (no dictionaries required).
    flattener = CompositionFlattener(
        db=None,
        config={
            "role": "primary",
            "apply_shortcuts": False,
            "paths": {"separator": "."},
            "collections": {},
        },
        mappings_path=str(
            Path(__file__).resolve().parents[2]
            / "engine"
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

    inserted = 0
    for raw in source.iter_compositions():
        base_doc, search_doc = flattener.transform_composition(raw)
        drv.insert_one(base_doc)
        inserted += 1
        if search_doc and search_doc.get("sn"):
            drv.insert_one(search_doc, search=True)
            inserted += 1
    log.info("done - inserted %d docs", inserted)


@app.command("drivers")
def drivers(
    include_aliases: bool = typer.Option(False, "--aliases", help="Include aliases"),
):
    """List registered persistence drivers."""
    rows = list_drivers(include_aliases=include_aliases)
    for row in rows:
        typer.echo(row)


@app.command("init-driver")
def init_driver(
    driver: str = typer.Option("mongodb", "--driver", "-t", help="Driver type (mongodb, filesystem, or custom)"),
    out: Optional[Path] = typer.Option(None, "--out", "-o", help="Where to write the driver YAML"),
    use_env_var: bool = typer.Option(True, "--env/--no-env", help="Use an env var placeholder for MongoDB URI"),
    env_var_name: str = typer.Option("MONGODB_URI", "--env-var", help="Env var name to reference when --env"),
    connection_string: Optional[str] = typer.Option(None, "--uri", prompt=False, hide_input=True, help="MongoDB URI (only used for mongodb + --no-env)"),
    database_name: Optional[str] = typer.Option(None, "--db", help="MongoDB database name"),
    compositions_collection: Optional[str] = typer.Option(None, "--compositions", help="MongoDB base compositions collection"),
    search_collection: Optional[str] = typer.Option(None, "--search", help="MongoDB search collection"),
    base_path: str = typer.Option(".kehrnel/persistence", "--base-path", help="Filesystem base path"),
    compositions_file: str = typer.Option("compositions.jsonl", "--compositions-file", help="Filesystem base compositions file"),
    search_file: str = typer.Option("search.jsonl", "--search-file", help="Filesystem search file"),
    set_values: list[str] = typer.Option([], "--set", help="Extra KEY=VALUE entries (JSON values accepted)"),
):
    """
    Create a persistence driver config for `kehrnel common ingest`.

    Same CLI UX across drivers:
    - mongodb: URI + database/collections
    - filesystem: path + JSONL filenames
    - custom driver: base `driver` + optional `--set`
    """
    driver_name = _normalize_driver_name(driver)
    if not driver_name:
        raise typer.BadParameter("Driver cannot be empty.")

    cfg: Dict[str, Any] = {"driver": driver_name}
    if driver_name == "mongodb":
        db_name = (database_name or "").strip()
        if not db_name:
            db_name = typer.prompt("MongoDB database name")

        base_col = (compositions_collection or "").strip() or "compositions_rps"
        search_col = (search_collection or "").strip() or "compositions_search"
        if use_env_var:
            uri_value = f"${{{env_var_name}}}"
        else:
            uri_value = connection_string or typer.prompt("MongoDB connection string", hide_input=True)
        cfg.update(
            {
                "connection_string": uri_value,
                "database_name": db_name,
                "compositions_collection": base_col,
                "search_collection": search_col,
            }
        )
    elif driver_name == "filesystem":
        cfg.update(
            {
                "base_path": base_path,
                "compositions_file": compositions_file,
                "search_file": search_file,
            }
        )

    extra = _parse_set_values(set_values)
    if extra:
        cfg.update(extra)

    target = out or Path(f".kehrnel/driver.{driver_name}.yaml")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    typer.echo(f"✓ wrote {target}")
    typer.echo("Use with: kehrnel common ingest -- -- file <batch.ndjson> -d <driver.yaml>")
    if driver_name == "mongodb" and use_env_var:
        typer.echo(f"Set {env_var_name} in your environment before running ingest.")


@app.command("init-source-mongo")
def init_source_mongo(
    out: Path = typer.Option(Path(".kehrnel/source.mongo.json"), "--out", "-o", help="Where to write the source JSON"),
    use_env_var: bool = typer.Option(True, "--env/--no-env", help="Use an env var placeholder for the connection string"),
    env_var_name: str = typer.Option("MONGODB_URI", "--env-var", help="Env var name to reference when --env"),
    connection_string: Optional[str] = typer.Option(None, "--uri", prompt=False, hide_input=True, help="MongoDB connection string (only used when --no-env)"),
    database_name: str = typer.Option(..., "--db", prompt=True, help="Database name"),
    source_collection: str = typer.Option(..., "--collection", prompt=True, help="Source compositions collection"),
):
    """Create a source Mongo config JSON for `kehrnel ingest mongo-catchup`."""
    if use_env_var:
        uri_value = f"${{{env_var_name}}}"
    else:
        uri_value = connection_string or typer.prompt("MongoDB connection string", hide_input=True)

    cfg = {
        "connection_string": uri_value,
        "database_name": database_name,
        "source_collection": source_collection,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    typer.echo(f"✓ wrote {out}")
    if use_env_var:
        typer.echo(f"Set {env_var_name} in your environment before running mongo-catchup.")


if __name__ == "__main__":
    app()
