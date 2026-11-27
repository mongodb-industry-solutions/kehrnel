"""Accenture/Databricks entrypoint: inspect, plan, prepare, backfill, or one record."""

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from bson import json_util
from .connections import read_environment, connect, create_converter
from .setup import worker_environment
from .jobs import Backfill, run_backfill
from .projection import utc


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env.analytics.local")
    parser.add_argument(
        "--config",
        help="Portable strategy config JSON: source profile and immutable rules",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect")
    sub.add_parser("plan")
    sub.add_parser("prepare")
    p = sub.add_parser("pin-config")
    p.add_argument("--output", required=True)
    p = sub.add_parser("backfill")
    p.add_argument("--job", required=True)
    p.add_argument("--max-batches", type=int)
    p = sub.add_parser("one")
    p.add_argument("--document", required=True)
    p.add_argument("--accepted-at", required=True)
    p.add_argument("--write", action="store_true")
    p.add_argument("--deleted", action="store_true")
    p = sub.add_parser(
        "propose-defaults",
        help="Generate an offline all-clinical-fields proposal from one OPT",
    )
    p.add_argument("--opt", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--epoch", type=int, default=1)
    p.add_argument("--effective-at", required=True)
    p = sub.add_parser(
        "finalize-defaults",
        help="Write a reviewed rule after explicit field exclusions",
    )
    p.add_argument("--proposal", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--exclude-field", action="append", default=[])
    args = parser.parse_args(argv)
    if args.command in ("propose-defaults", "finalize-defaults"):
        from .default_mapping import propose_from_opt, finalize_proposal

        try:
            output = Path(args.output)
            if output.exists():
                raise ValueError(
                    "Choose a new output path; existing proposals/rules are preserved"
                )
            if args.command == "propose-defaults":
                result = propose_from_opt(
                    Path(args.opt).read_bytes(),
                    epoch=args.epoch,
                    effective_at=args.effective_at,
                )
            else:
                result = finalize_proposal(
                    json.loads(Path(args.proposal).read_text()),
                    exclude_field_ids=args.exclude_field,
                )
            output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
            print(json.dumps({"output": str(output), "database_writes": False}))
            return 0
        except ValueError as exc:
            parser.error(str(exc))
    env = read_environment(args.env)
    src_client = dst_client = None
    try:
        cfg = json_util.loads(Path(args.config).read_text()) if args.config else {}
        env = worker_environment(env, cfg)
        if args.command in ("inspect", "backfill") and cfg.get("source", {}).get("mode") == "upload":
            raise ValueError("Choose a MongoDB source for inspect/backfill")
        src_client, dst_client, source, store = connect(
            env,
            target=args.command not in ("inspect", "plan", "pin-config") and not (args.command == "one" and not args.write),
            settings=cfg,
        )
        if args.command == "inspect":
            # Deliberately bounded: this is schema discovery, not a billion-document GROUP BY.
            docs = list(
                source.find({}, {"_id": 0, "template": 1, "template_name": 1, "tid": 1})
                .limit(1000)
                .max_time_ms(15000)
            )
            first = source.find_one({}, max_time_ms=15000) or {}
            result = {
                "source_database": source.database.name,
                "source_collection": source.name,
                "sampled_compositions": len(docs),
                "sampled_templates": sorted(
                    {
                        str(
                            d.get(
                                "template",
                                d.get("template_name", d.get("tid", "unknown")),
                            )
                        )
                        for d in docs
                    }
                ),
                "envelope_keys": sorted(first),
                "first_node_keys": sorted((first.get("cn") or [{}])[0]),
                "note": "Read-only bounded sample. Template coverage is not exhaustive.",
            }
        else:
            if not args.config:
                raise ValueError(
                    "Provide --config with the source profile and immutable rule revisions"
                )
            converter = create_converter(env, source, cfg["rules"], cfg["source"])
            if args.command == "plan":
                result = {
                    "rule_set_revision": converter.rules.revision,
                    "rules": {
                        t: [r.revision for r in rs]
                        for t, rs in converter.rules.by_template.items()
                    },
                    "decoder_revision": (
                        converter.decoder.revision
                        if converter.decoder
                        else "canonical-openEHR/v1"
                    ),
                    "source_index_recommendations": [
                        {
                            converter.fields.template: 1,
                            converter.fields.composition_id: 1,
                        }
                    ],
                    "note": "Source indexes are recommendations only; this process never creates them.",
                }
            elif args.command == "pin-config":
                from .profiles import pin_source_profile

                output = Path(args.output)
                if output.exists():
                    raise ValueError(
                        "Choose a new output path; pinned configurations are immutable"
                    )
                output.write_text(
                    json_util.dumps(pin_source_profile(cfg, converter), indent=2) + "\n"
                )
                result = {
                    "output": str(output),
                    "rules_revision": converter.rules.revision,
                    "credentials_exported": False,
                }
            elif args.command == "prepare":
                result = {
                    "collections": store.prepare(converter.rules.by_revision.values())
                }
            elif args.command == "one":
                p = converter.convert(
                    json_util.loads(Path(args.document).read_text()),
                    accepted_at=utc(args.accepted_at),
                    deleted=args.deleted,
                )
                result = store.write(p) if args.write else p.documents()
            else:
                spec = json_util.loads(Path(args.job).read_text())
                spec["accepted_at"] = utc(spec["accepted_at"])
                for key in ("committed_from", "committed_until"):
                    if spec.get(key):
                        spec[key] = utc(spec[key])
                result = run_backfill(
                    source,
                    store,
                    converter,
                    Backfill(**spec),
                    max_batches=args.max_batches,
                )
        print(json_util.dumps(result, indent=2))
        return 0
    except ValueError as exc:
        parser.error(str(exc))
    except Exception as exc:
        # Driver errors can contain hosts/connection details. Do not echo credentials.
        print(
            json.dumps(
                {
                    "error": type(exc).__name__,
                    "message": "Operation failed. Verify configuration, permissions and replica-set connectivity.",
                }
            )
        )
        return 1
    finally:
        if src_client is not None:
            src_client.close()
        if dst_client is not None:
            dst_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
