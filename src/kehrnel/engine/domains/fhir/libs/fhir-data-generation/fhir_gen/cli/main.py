"""FHIR R5 synthetic data generator CLI."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import click

from ..config import settings
from ..generators.base import ResourceGenerator
from ..schema.versions import resolve_schema_path, supported_schema_versions
from ..generators.poly_catalog import poly_coverage_summary, poly_groups_for, resources_with_poly_groups
from ..generators.scenarios import NAMED_SCENARIO_CATALOG, named_scenario_catalog, scenario_catalog
from ..persistence.mongo import FHIRMongoStore
from ..schema.registry import registry


def _collect_store_resources(gen: ResourceGenerator) -> list[dict]:
    """All resources registered in the session (including auto-generated dependencies)."""
    return gen.store.all_resources()


def _parse_count_pair(pair: str) -> tuple[str, int]:
    """Parse ``ResourceType=10`` or ``ResourceType:10``."""
    for sep in ("=", ":"):
        if sep in pair:
            name, _, raw = pair.partition(sep)
            name = name.strip().strip('"').strip("'")
            if name:
                return name, int(raw.strip())
    raise click.ClickException(
        f"Invalid --count value {pair!r}; use ResourceType=10 (e.g. Patient=10)"
    )


def _parse_counts_option(counts: str) -> dict[str, int]:
    """
    Parse --counts JSON or shell-friendly ``{Patient:10,Encounter:20}``.

    PowerShell often strips JSON double quotes, so brace/key:value form is accepted.
    """
    text = counts.strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise click.ClickException("--counts must be a JSON object")
        return {str(k): int(v) for k, v in data.items()}
    except json.JSONDecodeError:
        pass

    inner = text
    if inner.startswith("{") and inner.endswith("}"):
        inner = inner[1:-1]
    result: dict[str, int] = {}
    for part in re.split(r",\s*", inner):
        part = part.strip()
        if not part:
            continue
        try:
            name, value = _parse_count_pair(part)
            result[name] = value
        except click.ClickException:
            continue
    if result:
        return result

    raise click.ClickException(
        f"Could not parse --counts {counts!r}. "
        "Use JSON like {\"Patient\":10} or Patient=10,Encounter=20, "
        "or repeat --count Patient=10 --count Encounter=20."
    )


def _build_count_map(
    resource_types: tuple[str, ...],
    counts: str | None,
    count_pairs: tuple[str, ...],
) -> dict[str, int]:
    count_map: dict[str, int] = {}
    if counts:
        count_map.update(_parse_counts_option(counts))
    for pair in count_pairs:
        name, value = _parse_count_pair(pair)
        count_map[name] = value
    if not count_map:
        count_map = {rtype: 1 for rtype in resource_types}
    return count_map


@click.group()
@click.option("--seed", type=int, default=None, help="Random seed for reproducibility")
@click.option(
    "--schema-version",
    type=click.Choice(list(supported_schema_versions()), case_sensitive=False),
    default="R5",
    show_default=True,
    envvar="FHIR_GEN_SCHEMA_VERSION",
    help="FHIR release for JSON Schema (R5 default, R6 optional)",
)
@click.option(
    "--schema-path",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    hidden=True,
    help="Advanced: override bundled schema file (ignores --schema-version)",
)
@click.option(
    "--mongo-uri",
    default=None,
    envvar="FHIR_GEN_MONGODB_URI",
    help="MongoDB URI (default: mongodb://localhost:27017)",
)
@click.option(
    "--db",
    default=None,
    envvar="FHIR_GEN_MONGODB_DB",
    help="MongoDB database name (default: fhir_synthetic)",
)
@click.pass_context
def cli(
    ctx: click.Context,
    seed: int | None,
    schema_version: str,
    schema_path: Path | None,
    mongo_uri: str | None,
    db: str | None,
) -> None:
    """FHIR synthetic data generator (default schema: FHIR R5)."""
    ctx.ensure_object(dict)
    ctx.obj["seed"] = seed
    ctx.obj["mongo_uri"] = mongo_uri or settings.mongodb_uri
    ctx.obj["db"] = db or settings.mongodb_db

    try:
        resolved = resolve_schema_path(
            schema_version=schema_version,
            schema_path=schema_path,
        )
    except (ValueError, FileNotFoundError) as exc:
        raise click.ClickException(str(exc)) from exc

    settings.schema_version = schema_version
    settings.schema_path = schema_path
    from ..schema.registry import SchemaRegistry

    SchemaRegistry.reload(resolved)
    ctx.obj["schema_version"] = settings.fhir_version
    ctx.obj["schema_path"] = str(resolved)


@cli.command()
def version() -> None:
    """Show package version."""
    from fhir_gen import __version__

    click.echo(f"fhir-gen {__version__}")


@cli.command()
@click.argument("resource_type")
@click.option("-n", "--count", default=1, show_default=True, help="Number of resources to generate")
@click.option("--save/--no-save", default=True, show_default=True, help="Save to MongoDB")
@click.option("--output", type=click.Path(), default=None, help="Output JSON file path (- for stdout)")
@click.option("--pretty/--no-pretty", default=True, show_default=True, help="Pretty-print JSON output")
@click.option(
    "--with-deps/--no-deps",
    default=True,
    show_default=True,
    help="Auto-generate required dependency resources",
)
@click.option(
    "--variants/--no-variants",
    default=False,
    show_default=True,
    help="Emit one resource per polymorphic field variant (INSTRUCTIONS #7)",
)
@click.option(
    "--scenario",
    "scenario_id",
    default=None,
    help="Named lifecycle scenario (e.g. deceased_datetime). See: fhir-gen list-scenarios Patient",
)
@click.option(
    "--scenarios/--no-scenarios",
    default=False,
    show_default=True,
    help="One resource per scenario (named lifecycle + schema choice variants)",
)
@click.option(
    "--scenarios-named-only/--no-scenarios-named-only",
    default=False,
    show_default=True,
    help="With --scenarios: only hand-crafted lifecycle scenarios (no poly_* variants)",
)
@click.pass_context
def generate(
    ctx: click.Context,
    resource_type: str,
    count: int,
    save: bool,
    output: str | None,
    pretty: bool,
    with_deps: bool,
    variants: bool,
    scenario_id: str | None,
    scenarios: bool,
    scenarios_named_only: bool,
) -> None:
    """Generate FHIR resources of a given type.

    \b
    Examples:
      fhir-gen generate Patient --count 10
      fhir-gen generate Observation --count 5 --no-save --output obs.json
      fhir-gen generate MedicationRequest -n 20
    """
    valid_resources = registry.all_resources()
    if resource_type not in valid_resources:
        prefix = resource_type.lower()[:3]
        similar = [r for r in valid_resources if r.lower().startswith(prefix)]
        click.echo(f"Unknown resource: {resource_type}", err=True)
        if similar:
            click.echo(f"Did you mean: {', '.join(similar[:5])}", err=True)
        sys.exit(1)

    if sum([variants, scenarios, bool(scenario_id)]) > 1:
        raise click.ClickException(
            "Use only one of: --variants, --scenarios, --scenario <id>"
        )

    gen = ResourceGenerator(seed=ctx.obj["seed"])
    schema_path = ctx.obj.get("schema_path")

    if variants:
        click.echo(f"Generating {resource_type} polymorphic variants...", err=True)
        resources = gen.generate_variants(resource_type)
    elif scenarios:
        catalog = scenario_catalog(
            resource_type,
            include_poly_variants=not scenarios_named_only,
        )
        if not catalog:
            raise click.ClickException(
                f"No scenarios for {resource_type}. "
                f"Try: fhir-gen list-poly-groups {resource_type}"
            )
        click.echo(
            f"Generating {len(catalog)} {resource_type} scenario(s)...",
            err=True,
        )
        resources = gen.generate_scenarios(
            resource_type,
            include_poly_variants=not scenarios_named_only,
            named_only=scenarios_named_only,
        )
    elif scenario_id:
        click.echo(
            f"Generating {count} {resource_type} "
            f"(scenario={scenario_id})...",
            err=True,
        )
        resources = [
            gen.generate_scenario(resource_type, scenario_id)
            for _ in range(count)
        ]
    else:
        click.echo(f"Generating {count} {resource_type} resource(s)...", err=True)
        if with_deps:
            resources = gen.generate(resource_type, count=count, schema_path=schema_path)
        else:
            resources = [
                gen._generate_one(resource_type)  # noqa: SLF001 — CLI no-deps mode
                for _ in range(count)
            ]

    if save:
        store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
        counts = store_obj.save_many(_collect_store_resources(gen))
        click.echo(f"Saved to MongoDB: {counts}", err=True)
        store_obj.close()

    indent = 2 if pretty else None
    if output and output != "-":
        Path(output).write_text(json.dumps(resources, indent=indent), encoding="utf-8")
        click.echo(f"Written to {output}", err=True)
    elif output == "-" or not save:
        click.echo(json.dumps(resources, indent=indent))
    else:
        click.echo(f"Generated {len(resources)} {resource_type} resource(s)", err=True)


@cli.command("generate-many")
@click.argument("resource_types", nargs=-1, required=True)
@click.option(
    "--counts",
    default=None,
    help='Count map: JSON {"Patient":10} or Patient=10,Encounter=20 (PowerShell-friendly)',
)
@click.option(
    "--count",
    "count_pairs",
    multiple=True,
    help="Per-type count (repeatable), e.g. --count Patient=10 --count Encounter=20",
)
@click.option("--save/--no-save", default=True, show_default=True)
@click.option("--output-dir", type=click.Path(), default=None, help="Directory for per-type JSON files")
@click.pass_context
def generate_many_cmd(
    ctx: click.Context,
    resource_types: tuple[str, ...],
    counts: str | None,
    count_pairs: tuple[str, ...],
    save: bool,
    output_dir: str | None,
) -> None:
    """Generate multiple resource types in dependency order.

    \b
    Examples:
      fhir-gen generate-many Patient Encounter Observation
      fhir-gen generate-many Patient Encounter --counts '{"Patient":5,"Encounter":10}'
      fhir-gen generate-many Patient Encounter --count Patient=5 --count Encounter=10
    """
    count_map = _build_count_map(resource_types, counts, count_pairs)

    click.echo(f"Generating resources: {', '.join(resource_types)}", err=True)
    gen = ResourceGenerator(seed=ctx.obj["seed"])
    results = gen.generate_many(list(resource_types), counts=count_map)

    if save:
        store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
        saved = store_obj.save_many(_collect_store_resources(gen))
        click.echo(f"Saved: {saved}", err=True)
        store_obj.close()

    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        for rtype, resources in results.items():
            file_path = out_path / f"{rtype}.json"
            file_path.write_text(json.dumps(resources, indent=2), encoding="utf-8")
            click.echo(f"  Written {rtype}: {file_path}", err=True)

    summary = {rtype: len(res) for rtype, res in results.items()}
    click.echo(json.dumps(summary, indent=2))


@cli.command("list-resources")
@click.pass_context
def list_resources(_ctx: click.Context) -> None:
    """List all available FHIR resource types."""
    resources = sorted(registry.all_resources())
    click.echo(f"Available resources ({len(resources)} total):")
    for i, rname in enumerate(resources, 1):
        click.echo(f"  {i:3d}. {rname}")


@cli.command("list-scenarios")
@click.argument("resource_type", required=False, default=None)
@click.option(
    "--named-only",
    is_flag=True,
    help="List only hand-crafted lifecycle scenarios (exclude poly_* variants)",
)
@click.option(
    "--all-types",
    is_flag=True,
    help="List scenario counts for every resource with named and/or poly scenarios",
)
def list_scenarios(
    resource_type: str | None,
    named_only: bool,
    all_types: bool,
) -> None:
    """List generation scenarios (lifecycle + schema polymorphic variants)."""
    if all_types:
        click.echo("Scenario coverage by resource type:\n")
        click.echo(f"{'Resource':<32} {'Named':>6} {'Poly':>6} {'Total':>6}")
        click.echo("-" * 54)
        named_types = set(NAMED_SCENARIO_CATALOG)
        poly_types = set(resources_with_poly_groups())
        all_types_set = sorted(named_types | poly_types)
        for rt in all_types_set:
            n_named = len(named_scenario_catalog(rt))
            n_poly = len(scenario_catalog(rt)) - n_named if not named_only else 0
            if named_only:
                total = n_named
            else:
                total = len(scenario_catalog(rt, include_poly_variants=True))
                n_poly = total - n_named
            click.echo(f"{rt:<32} {n_named:>6} {n_poly:>6} {total:>6}")
        click.echo(
            f"\n{len(named_types)} resource(s) with named scenarios; "
            f"{len(poly_types)} with schema choice groups."
        )
        return

    if resource_type:
        catalog = scenario_catalog(
            resource_type,
            include_poly_variants=not named_only,
        )
        if not catalog:
            click.echo(
                f"No scenarios for {resource_type}. "
                f"Use: fhir-gen list-poly-groups {resource_type}",
                err=True,
            )
            sys.exit(1)
        n_named = len(named_scenario_catalog(resource_type))
        click.echo(f"{resource_type} scenarios ({len(catalog)} total, {n_named} named):")
        for entry in catalog:
            kind = "named" if not entry.id.startswith("poly_") else "poly "
            poly = f" forced={entry.forced_poly}" if entry.forced_poly else ""
            click.echo(f"  [{kind}] {entry.id:28s} {entry.description}{poly}")
        return

    click.echo("Resources with named lifecycle scenario catalogs:")
    for rtype, entries in sorted(NAMED_SCENARIO_CATALOG.items()):
        click.echo(f"\n  {rtype} ({len(entries)} named):")
        for entry in entries:
            click.echo(f"    {entry.id:22s} {entry.description}")
    click.echo(
        f"\nFor polymorphic choice groups on 49 resource types: "
        f"fhir-gen list-poly-groups"
    )


@cli.command("list-poly-groups")
@click.argument("resource_type", required=False, default=None)
def list_poly_groups(resource_type: str | None) -> None:
    """List FHIR schema polymorphic choice groups (value[x], onset[x], deceased[x], …)."""
    if resource_type:
        try:
            groups = poly_groups_for(resource_type)
        except KeyError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)
        if not groups:
            click.echo(f"{resource_type} has no polymorphic choice groups in the schema.")
            return
        total = sum(len(v) for v in groups.values())
        click.echo(f"{resource_type}: {len(groups)} group(s), {total} variant(s)\n")
        for base, keys in sorted(groups.items()):
            click.echo(f"  {base}:")
            for key in keys:
                click.echo(f"    - {key}")
        click.echo(
            f"\nGenerate all variants: fhir-gen generate {resource_type} --variants"
        )
        click.echo(
            f"Or: fhir-gen generate {resource_type} --scenarios"
        )
        return

    rows = poly_coverage_summary()
    click.echo(f"Resources with polymorphic choice groups ({len(rows)}):\n")
    click.echo(f"{'Resource':<32} {'Groups':>7} {'Variants':>9}")
    click.echo("-" * 50)
    for row in rows:
        click.echo(
            f"{row['resource_type']:<32} {row['group_count']:>7} {row['variant_count']:>9}"
        )
    click.echo(
        "\nDetail: fhir-gen list-poly-groups Observation"
    )


@cli.command("schema-info")
@click.argument("resource_type")
@click.pass_context
def schema_info(_ctx: click.Context, resource_type: str) -> None:
    """Show schema info for a resource type."""
    try:
        defn = registry.definition(resource_type)
    except KeyError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo(f"\n{resource_type}")
    click.echo("=" * len(resource_type))
    click.echo(f"Description: {defn.description}")
    click.echo(f"Required fields: {defn.required}")
    click.echo(f"Total fields: {len(defn.fields)}")
    click.echo(f"Polymorphic groups: {list(defn.poly_groups.keys())}")
    click.echo("\nFields:")
    for fname, fdef in list(defn.fields.items())[:30]:
        req = "* " if fname in defn.required else "  "
        arr = "[]" if fdef.is_array else ""
        click.echo(f"  {req}{fname}: {fdef.ref or 'any'}{arr}")


@cli.command("db-stats")
@click.pass_context
def db_stats(ctx: click.Context) -> None:
    """Show MongoDB database statistics."""
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    try:
        stats = store_obj.stats()
        if not stats:
            click.echo("No data found in database.")
            return
        total = sum(stats.values())
        click.echo(f"Database: {ctx.obj['db']}")
        click.echo(f"{'Resource Type':<35} {'Count':>10}")
        click.echo("-" * 46)
        for rtype, count in sorted(stats.items()):
            click.echo(f"{rtype:<35} {count:>10,}")
        click.echo("-" * 46)
        click.echo(f"{'TOTAL':<35} {total:>10,}")
    finally:
        store_obj.close()


@cli.command()
@click.argument("resource_type", required=False)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def clear(ctx: click.Context, resource_type: str | None, yes: bool) -> None:
    """Clear resources from MongoDB database."""
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    try:
        target = resource_type or "ALL RESOURCES"
        if not yes:
            click.confirm(f"Delete {target} from {ctx.obj['db']}?", abort=True)
        store_obj.delete_all(resource_type)
        click.echo(f"Deleted: {target}")
    finally:
        store_obj.close()


@cli.command()
@click.argument("resource_type")
@click.option("--patient-id", help="Filter by patient ID")
@click.option("--code", help="Filter by code (LOINC/SNOMED)")
@click.option("--status", help="Filter by status")
@click.option("--limit", default=10, show_default=True)
@click.option("--pretty/--no-pretty", default=True)
@click.pass_context
def search(
    ctx: click.Context,
    resource_type: str,
    patient_id: str | None,
    code: str | None,
    status: str | None,
    limit: int,
    pretty: bool,
) -> None:
    """Search resources in MongoDB.

    \b
    Examples:
      fhir-gen search Observation --patient-id abc-123
      fhir-gen search Condition --code 73211009 --status active
    """
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    try:
        query: dict = {}
        if patient_id:
            if resource_type == "Patient":
                query["id"] = patient_id
            else:
                query["subject.reference"] = f"Patient/{patient_id}"
        if code:
            query["$or"] = [
                {"code.coding.code": code},
                {"vaccineCode.coding.code": code},
            ]
        if status:
            query["status"] = status

        results = store_obj.find(resource_type, query, limit=limit)
        indent = 2 if pretty else None
        click.echo(json.dumps(results, indent=indent, default=str))
    finally:
        store_obj.close()


def main() -> None:
    cli(obj={})


if __name__ == "__main__":
    main()
