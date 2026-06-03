"""
Command-line interface for the ``fhir-search-to-mql`` library.

Installed as the ``fhir-mql`` console script via
``[project.scripts]`` in ``pyproject.toml``. After ``pip install
fhir-search-to-mql`` you get a system-wide ``fhir-mql`` command that
exposes the library's denormalization, indexing, and search-conversion
capabilities against a MongoDB instance.

Design goals
------------
* **Multi-resource by default.** Every bulk subcommand
  (``denormalize``, ``indexes``, ``reset``, ``stats``) accepts ONE OR
  MORE resource types in a single invocation, plus an ``--all``
  switch that targets every resource the configuration knows about.
* **Bundled configs out of the box.** When ``--config-dir`` is
  omitted, the CLI uses the YAML configs that ship with this package
  — same as the Python API. ``--config-dir`` may be supplied multiple
  times to layer host-project configs on top of (or instead of) the
  bundled ones.
* **Connection via env or flag.** The MongoDB URI is resolved from
  ``--uri`` first, then ``$MONGODB_URI``, then a sensible local
  default. The same precedence applies to ``--db`` /
  ``$MONGODB_DB``.
* **Safe by default.** ``--dry-run`` is honored on every mutating
  subcommand and reports what WOULD have happened without touching
  the database.
* **Scriptable output.** ``--format json`` (default for ``search``)
  emits machine-readable JSON; ``--format table`` emits a compact
  human-readable summary. Errors and progress logs always go to
  stderr so JSON on stdout stays clean.

The CLI is a thin shell around the library's public surface
(``ResourceDenormalizer``, ``MongoDBHandler``, ``FHIRSearchConverter``,
``ConfigLoader``); every exit code and behavior is documented below
so it can be wired into shell scripts and CI pipelines without
surprises.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from fhir_search_to_mql import (
    __version__,
    ConfigLoader,
    FHIRSearchConverter,
    MongoDBHandler,
    ResourceDenormalizer,
)
from fhir_search_to_mql.core.exceptions import (
    ConfigurationError,
    FHIRSearchToMQLError,
    MissingConfigurationError,
)


# ---------------------------------------------------------------------------
# Defaults & exit codes
# ---------------------------------------------------------------------------

DEFAULT_MONGODB_URI = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "fhir_synthetic"

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_CONFIG = 3
EXIT_RUNTIME = 4
EXIT_NO_PYMONGO = 5


# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------


def _json_default(obj: Any) -> Any:
    """JSON encoder fallback for datetime/date/ObjectId/etc."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # ObjectId / bytes / sets / anything stringable.
    return str(obj)


def _dump_json(value: Any) -> str:
    """
    Render a JSON document for stdout.

    ``ensure_ascii=True`` escapes non-ASCII characters into ``\\uXXXX``
    sequences. This keeps output safe on terminals whose default
    encoding can't render high code points (notably Windows
    ``cp1252``, which raises on ``\\uffff`` — the sentinel used by
    string prefix range queries).
    """
    return json.dumps(
        value, default=_json_default, indent=2, ensure_ascii=True
    )


def _eprint(*args: Any, **kwargs: Any) -> None:
    """Print to stderr so stdout stays scriptable."""
    print(*args, file=sys.stderr, **kwargs)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _resolve_uri(args: argparse.Namespace) -> str:
    return (
        args.uri
        or os.environ.get("MONGODB_URI")
        or DEFAULT_MONGODB_URI
    )


def _resolve_db_name(args: argparse.Namespace) -> str:
    return (
        args.db
        or os.environ.get("MONGODB_DB")
        or DEFAULT_DB_NAME
    )


def _open_database(args: argparse.Namespace):
    """
    Open a pymongo database handle using args + env-var precedence.

    pymongo is an optional runtime dependency for callers who only
    need ``convert`` (pure FHIR → MQL translation), so we import it
    lazily and surface a clean error when it's missing.
    """
    try:
        from pymongo import MongoClient  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised only without pymongo
        raise SystemExit(
            "pymongo is required for this subcommand. Install with "
            "`pip install pymongo` or `pip install fhir-search-to-mql[mongo]`."
        ) from exc

    uri = _resolve_uri(args)
    db_name = _resolve_db_name(args)
    client = MongoClient(uri)
    return client[db_name], client


def _resolve_collection_name(args: argparse.Namespace, resource_type: str) -> str:
    """
    Resolve the MongoDB collection name for a resource type.

    Defaults to the FHIR resource name itself (matching this
    project's convention and tests). ``--collection-prefix`` allows
    schemas like ``fhir_Patient`` without bespoke per-resource flags.
    """
    prefix = getattr(args, "collection_prefix", "") or ""
    return f"{prefix}{resource_type}"


# ---------------------------------------------------------------------------
# Library factory helpers
# ---------------------------------------------------------------------------


def _build_loader(args: argparse.Namespace) -> ConfigLoader:
    config_dirs = list(args.config_dir or [])
    return ConfigLoader(config_dir=config_dirs or None)


def _build_denormalizer(args: argparse.Namespace) -> ResourceDenormalizer:
    config_dirs = list(args.config_dir or [])
    return ResourceDenormalizer(config_dir=config_dirs or None)


def _build_converter(args: argparse.Namespace) -> FHIRSearchConverter:
    config_dirs = list(args.config_dir or [])
    compartment_dir = getattr(args, "compartment_definitions_dir", None)
    return FHIRSearchConverter(
        config_dir=config_dirs or None,
        compartment_definitions_dir=compartment_dir,
    )


def _expand_resource_list(
    args: argparse.Namespace, loader: ConfigLoader
) -> List[str]:
    """
    Resolve the resource-types list from CLI flags.

    Honors ``--all`` (every configured resource), an explicit
    positional list, or — failing both — fails with a helpful usage
    error so users don't accidentally rewrite half a database.
    """
    if getattr(args, "all", False):
        return sorted(loader.list_resources())
    resources = list(getattr(args, "resources", []) or [])
    if not resources:
        raise SystemExit(
            "Specify one or more resource types or pass --all "
            "(e.g. `fhir-mql denormalize Patient Observation`)."
        )
    # Validate each is configured before we touch the database.
    unknown = [r for r in resources if not loader.has_config(r)]
    if unknown:
        configured = ", ".join(sorted(loader.list_resources())) or "(none)"
        raise SystemExit(
            f"No configuration found for: {', '.join(unknown)}. "
            f"Configured resources: {configured}."
        )
    return resources


# ---------------------------------------------------------------------------
# Subcommand: resources (introspection)
# ---------------------------------------------------------------------------


def cmd_resources(args: argparse.Namespace) -> int:
    loader = _build_loader(args)
    resources = sorted(loader.list_resources())
    if args.format == "json":
        out = []
        for r in resources:
            cfg = loader.get_config(r)
            out.append({
                "resource": r,
                "fhir_version": cfg.get("fhir_version", "?"),
                "search_parameters": sorted(
                    list((cfg.get("search_parameters") or cfg.get("parameters") or {}).keys())
                ),
                "denormalization_rules": sorted(
                    list((cfg.get("denormalization") or {}).keys())
                ),
                "indexes": len(cfg.get("indexes") or []),
            })
        print(_dump_json(out))
    else:
        if not resources:
            print("(no configured resources)")
            return EXIT_OK
        print(f"{'Resource':<16} {'FHIR':<6} {'Params':<7} {'Denorm':<7} {'Indexes':<8}")
        print("-" * 48)
        for r in resources:
            cfg = loader.get_config(r)
            params = cfg.get("search_parameters") or cfg.get("parameters") or {}
            denorm = cfg.get("denormalization") or {}
            indexes = cfg.get("indexes") or []
            print(
                f"{r:<16} "
                f"{cfg.get('fhir_version', '?'):<6} "
                f"{len(params):<7} "
                f"{len(denorm):<7} "
                f"{len(indexes):<8}"
            )
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: convert (pure conversion, no DB needed)
# ---------------------------------------------------------------------------


def cmd_convert(args: argparse.Namespace) -> int:
    converter = _build_converter(args)
    if args.compartment_type and args.compartment_id:
        mql = converter.convert_with_compartment(
            compartment_type=args.compartment_type,
            compartment_id=args.compartment_id,
            resource_type=args.resource,
            query_string=args.query,
        )
    else:
        mql = converter.convert(
            resource_type=args.resource, query_string=args.query
        )
    print(_dump_json(mql))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: search (convert + execute)
# ---------------------------------------------------------------------------


def _execute_search(
    db, collection_name: str, mql: Dict[str, Any], limit: int
) -> List[Dict[str, Any]]:
    """
    Execute the converter's MQL output against MongoDB.

    Handles both plain MQL dicts and the ``{_query, _multi_step}``
    envelope produced by ``:identifier`` / ``_has`` parameters. The
    multi-step branch is intentionally minimal — it materializes the
    plan into the standard ``$and``-of-id-lists shape — because full
    plan execution is host-application territory (transactions,
    pagination, security filters, etc.).
    """
    if isinstance(mql, dict) and "_multi_step" in mql:
        # Materialize each step into an _id list, then AND them in.
        composed = dict(mql.get("_query") or {})
        and_clauses: List[Dict[str, Any]] = []
        for step in mql["_multi_step"]:
            step_coll = db[step.get("collection") or collection_name]
            ids = list(
                step_coll.find(
                    step.get("query") or {},
                    {step.get("project_field", "_id"): 1, "_id": 0},
                )
            )
            field = step.get("project_field", "_id")
            id_values = [d.get(field) for d in ids if d.get(field) is not None]
            target_field = step.get("target_field") or field
            if id_values:
                and_clauses.append({target_field: {"$in": id_values}})
            else:
                # No matches in the auxiliary step → enforce empty result.
                and_clauses.append({"_id": {"$in": []}})
        if and_clauses:
            composed = (
                {"$and": [composed] + and_clauses}
                if composed
                else {"$and": and_clauses}
            )
        mql = composed
    cursor = db[collection_name].find(mql)
    if limit and limit > 0:
        cursor = cursor.limit(limit)
    return list(cursor)


def cmd_search(args: argparse.Namespace) -> int:
    converter = _build_converter(args)
    if args.compartment_type and args.compartment_id:
        mql = converter.convert_with_compartment(
            compartment_type=args.compartment_type,
            compartment_id=args.compartment_id,
            resource_type=args.resource,
            query_string=args.query,
        )
    else:
        mql = converter.convert(
            resource_type=args.resource, query_string=args.query
        )

    if args.explain:
        print(_dump_json({"mql": mql}))
        return EXIT_OK

    db, client = _open_database(args)
    try:
        collection_name = _resolve_collection_name(args, args.resource)
        results = _execute_search(db, collection_name, mql, args.limit)
    finally:
        client.close()

    if args.format == "json":
        print(_dump_json({
            "resource": args.resource,
            "collection": collection_name,
            "mql": mql,
            "count": len(results),
            "limit": args.limit,
            "results": results,
        }))
    else:
        _eprint(f"Resource:   {args.resource}")
        _eprint(f"Collection: {collection_name}")
        _eprint(f"MQL:        {_dump_json(mql)}")
        _eprint(f"Matched:    {len(results)} (limit={args.limit})")
        for doc in results:
            print(f"- {doc.get('id') or doc.get('_id')}")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: denormalize (bulk re-denormalization)
# ---------------------------------------------------------------------------


def cmd_denormalize(args: argparse.Namespace) -> int:
    loader = _build_loader(args)
    denormalizer = _build_denormalizer(args)
    resources = _expand_resource_list(args, loader)

    if args.dry_run:
        db = None
        client = None
    else:
        db, client = _open_database(args)

    summary: List[Dict[str, Any]] = []
    try:
        for resource_type in resources:
            collection_name = _resolve_collection_name(args, resource_type)
            if args.dry_run:
                _eprint(
                    f"[dry-run] would denormalize {resource_type} "
                    f"in collection '{collection_name}'"
                )
                summary.append({
                    "resource": resource_type,
                    "collection": collection_name,
                    "dry_run": True,
                })
                continue

            collection = db[collection_name]
            query: Dict[str, Any] = {}
            if args.limit and args.limit > 0:
                # Process only the first N docs (useful for testing).
                ids = [
                    d["_id"]
                    for d in collection.find({}, {"_id": 1}).limit(args.limit)
                ]
                if not ids:
                    summary.append({
                        "resource": resource_type,
                        "collection": collection_name,
                        "processed": 0,
                        "updated": 0,
                        "failed": 0,
                    })
                    continue
                query = {"_id": {"$in": ids}}

            stats = MongoDBHandler.update_search_fields(
                collection=collection,
                query=query,
                processor=denormalizer.denormalize,
                batch_size=args.batch_size,
            )
            summary.append({
                "resource": resource_type,
                "collection": collection_name,
                **stats,
            })
    finally:
        if client is not None:
            client.close()

    if args.format == "json":
        print(_dump_json(summary))
    else:
        _print_summary_table(
            summary,
            columns=[
                ("resource", "Resource", 16),
                ("collection", "Collection", 18),
                ("processed", "Processed", 10),
                ("updated", "Updated", 8),
                ("failed", "Failed", 7),
                # Per-FIELD failures: documents that updated
                # successfully overall but had at least one rule
                # silently skipped. Surface them so a user spotting
                # warnings on stderr can correlate them to the
                # final count instead of seeing "0 failed" and
                # assuming the run was clean.
                ("field_failures", "FieldWarn", 10),
                ("documents_with_field_failures", "Docs", 6),
            ],
        )
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: indexes (ensure indexes from config)
# ---------------------------------------------------------------------------


def cmd_indexes(args: argparse.Namespace) -> int:
    loader = _build_loader(args)
    resources = _expand_resource_list(args, loader)

    if args.dry_run:
        db = None
        client = None
    else:
        db, client = _open_database(args)

    summary: List[Dict[str, Any]] = []
    try:
        for resource_type in resources:
            cfg = loader.get_config(resource_type)
            indexes = cfg.get("indexes") or []
            collection_name = _resolve_collection_name(args, resource_type)

            if args.dry_run:
                _eprint(
                    f"[dry-run] would create {len(indexes)} index(es) on "
                    f"'{collection_name}'"
                )
                summary.append({
                    "resource": resource_type,
                    "collection": collection_name,
                    "indexes_planned": len(indexes),
                    "dry_run": True,
                })
                continue

            collection = db[collection_name]
            created = MongoDBHandler.ensure_indexes(collection, indexes)
            summary.append({
                "resource": resource_type,
                "collection": collection_name,
                "indexes_created": len(created),
                "names": created,
            })
    finally:
        if client is not None:
            client.close()

    if args.format == "json":
        print(_dump_json(summary))
    else:
        _print_summary_table(
            summary,
            columns=[
                ("resource", "Resource", 16),
                ("collection", "Collection", 18),
                ("indexes_created", "Created", 8),
                ("indexes_planned", "Planned", 8),
            ],
        )
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: reset (clear _search and _compartments)
# ---------------------------------------------------------------------------


def cmd_reset(args: argparse.Namespace) -> int:
    loader = _build_loader(args)
    resources = _expand_resource_list(args, loader)

    if args.dry_run:
        for resource_type in resources:
            collection_name = _resolve_collection_name(args, resource_type)
            _eprint(
                f"[dry-run] would unset _search and _compartments on "
                f"'{collection_name}'"
            )
        return EXIT_OK

    db, client = _open_database(args)
    summary: List[Dict[str, Any]] = []
    try:
        for resource_type in resources:
            collection_name = _resolve_collection_name(args, resource_type)
            modified = MongoDBHandler.remove_search_fields(db[collection_name])
            summary.append({
                "resource": resource_type,
                "collection": collection_name,
                "modified": modified,
            })
    finally:
        client.close()

    if args.format == "json":
        print(_dump_json(summary))
    else:
        _print_summary_table(
            summary,
            columns=[
                ("resource", "Resource", 16),
                ("collection", "Collection", 18),
                ("modified", "Cleared", 8),
            ],
        )
    return EXIT_OK


# ---------------------------------------------------------------------------
# Subcommand: stats (collection statistics)
# ---------------------------------------------------------------------------


def cmd_stats(args: argparse.Namespace) -> int:
    loader = _build_loader(args)
    resources = _expand_resource_list(args, loader)

    db, client = _open_database(args)
    summary: List[Dict[str, Any]] = []
    try:
        for resource_type in resources:
            collection_name = _resolve_collection_name(args, resource_type)
            stats = MongoDBHandler.get_collection_stats(
                db[collection_name], resource_type=resource_type
            )
            summary.append({
                "resource": resource_type,
                "collection": collection_name,
                **stats,
            })
    finally:
        client.close()

    if args.format == "json":
        print(_dump_json(summary))
    else:
        _print_summary_table(
            summary,
            columns=[
                ("resource", "Resource", 16),
                ("collection", "Collection", 18),
                ("total_count", "Total", 9),
                ("with_search", "Search", 9),
                ("without_search", "Missing", 9),
            ],
        )
    return EXIT_OK


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------


def _print_summary_table(
    rows: Sequence[Dict[str, Any]],
    columns: Sequence[Sequence[Any]],
) -> None:
    if not rows:
        print("(no rows)")
        return
    header = " ".join(f"{title:<{width}}" for _, title, width in columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        print(" ".join(
            f"{str(row.get(key, '')):<{width}}"
            for key, _, width in columns
        ))


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Flags shared by every subcommand (config, output)."""
    parser.add_argument(
        "--config-dir",
        action="append",
        metavar="DIR",
        help=(
            "Directory of resource YAML configs. Pass multiple times to "
            "layer overrides; later directories win for the same "
            "resource type. When omitted, the configs bundled with "
            "the package are used."
        ),
    )
    parser.add_argument(
        "--compartment-definitions-dir",
        metavar="DIR",
        default=None,
        help=(
            "Directory of FHIR CompartmentDefinition JSON files. "
            "Defaults to the bundled definitions."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "table"),
        default="table",
        help="Output format (default: table).",
    )


def _add_db_args(parser: argparse.ArgumentParser) -> None:
    """Flags for subcommands that talk to MongoDB."""
    parser.add_argument(
        "--uri",
        metavar="URI",
        default=None,
        help=(
            "MongoDB connection URI. Falls back to $MONGODB_URI, then "
            f"{DEFAULT_MONGODB_URI}."
        ),
    )
    parser.add_argument(
        "--db",
        metavar="NAME",
        default=None,
        help=(
            f"MongoDB database name. Falls back to $MONGODB_DB, then "
            f"{DEFAULT_DB_NAME!r}."
        ),
    )
    parser.add_argument(
        "--collection-prefix",
        metavar="PREFIX",
        default="",
        help=(
            "Prefix prepended to the resource type to derive the "
            "collection name (default: empty, i.e. 'Patient' → "
            "collection 'Patient')."
        ),
    )


def _add_bulk_args(parser: argparse.ArgumentParser) -> None:
    """Flags for bulk-mutation subcommands (denormalize / indexes / reset / stats)."""
    parser.add_argument(
        "resources",
        nargs="*",
        metavar="RESOURCE",
        help=(
            "One or more FHIR resource types (e.g. Patient Observation "
            "Appointment). Use --all for every configured resource."
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Apply to every configured resource type.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without touching MongoDB.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fhir-mql",
        description=(
            "Convert FHIR search queries to MongoDB Query Language and "
            "manage FHIR-resource denormalization, indexing, and search "
            "from the command line."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"fhir-mql {__version__}",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # resources
    p = sub.add_parser(
        "resources",
        help="List configured resource types and their feature counts.",
    )
    _add_common_args(p)
    p.set_defaults(func=cmd_resources)

    # convert (pure conversion, no DB)
    p = sub.add_parser(
        "convert",
        help=(
            "Convert a FHIR search query string to a MongoDB query "
            "(no database access)."
        ),
    )
    _add_common_args(p)
    p.add_argument("resource", help="FHIR resource type (e.g. Patient).")
    p.add_argument(
        "query",
        help="FHIR search query string (e.g. 'name=Smith&gender=male').",
    )
    p.add_argument("--compartment-type", default=None, help="Compartment type (e.g. Patient).")
    p.add_argument("--compartment-id", default=None, help="Compartment id.")
    p.set_defaults(func=cmd_convert)

    # search (convert + execute)
    p = sub.add_parser(
        "search",
        help="Convert a FHIR search query and execute it against MongoDB.",
    )
    _add_common_args(p)
    _add_db_args(p)
    p.add_argument("resource", help="FHIR resource type (e.g. Patient).")
    p.add_argument(
        "query",
        help="FHIR search query string (e.g. 'name=Smith&gender=male').",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum results to return (default: 20).",
    )
    p.add_argument("--compartment-type", default=None, help="Compartment type (e.g. Patient).")
    p.add_argument("--compartment-id", default=None, help="Compartment id.")
    p.add_argument(
        "--explain",
        action="store_true",
        help="Print the generated MQL but do not execute it.",
    )
    p.set_defaults(func=cmd_search, format="json")

    # denormalize
    p = sub.add_parser(
        "denormalize",
        help=(
            "Recompute and persist _search and _compartments fields for "
            "one or more resources."
        ),
    )
    _add_common_args(p)
    _add_db_args(p)
    _add_bulk_args(p)
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="MongoDB cursor batch size (default: 100).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process only the first N documents per resource (0=all).",
    )
    p.set_defaults(func=cmd_denormalize)

    # indexes
    p = sub.add_parser(
        "indexes",
        help="Create the indexes declared in each resource's YAML config.",
    )
    _add_common_args(p)
    _add_db_args(p)
    _add_bulk_args(p)
    p.set_defaults(func=cmd_indexes)

    # reset
    p = sub.add_parser(
        "reset",
        help=(
            "Clear _search and _compartments fields for one or more "
            "resources (keeps the source FHIR documents intact)."
        ),
    )
    _add_common_args(p)
    _add_db_args(p)
    _add_bulk_args(p)
    p.set_defaults(func=cmd_reset)

    # stats
    p = sub.add_parser(
        "stats",
        help=(
            "Show document counts and denormalization coverage per "
            "resource."
        ),
    )
    _add_common_args(p)
    _add_db_args(p)
    _add_bulk_args(p)
    p.set_defaults(func=cmd_stats)

    return parser


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point.

    Returns an integer exit code. Wired into ``console_scripts`` via
    the ``[project.scripts]`` table in ``pyproject.toml`` so it's
    invoked as ``fhir-mql ...`` after a ``pip install``.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if not getattr(args, "command", None):
        parser.print_help(sys.stderr)
        return EXIT_USAGE

    try:
        return args.func(args)
    except SystemExit:
        raise
    except (ConfigurationError, MissingConfigurationError) as exc:
        _eprint(f"configuration error: {exc}")
        return EXIT_CONFIG
    except FHIRSearchToMQLError as exc:
        _eprint(f"error: {exc}")
        return EXIT_RUNTIME
    except KeyboardInterrupt:
        _eprint("interrupted")
        return EXIT_RUNTIME


if __name__ == "__main__":  # pragma: no cover - module-as-script convenience
    raise SystemExit(main())
