#!/usr/bin/env python3
"""Bounded, read-only IBM validation with a private per-composition review.

The local review cache is not an analytics destination or an alternative model.
Only build connects to MongoDB; serve reads the completed local cache.
"""

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import sqlite3
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
INDEX = "template_1_creation_date_-1"
ENTRY_TYPES = {"OBSERVATION", "EVALUATION", "ACTION", "INSTRUCTION", "ADMIN_ENTRY"}


def dumps(value):
    from bson.json_util import dumps as bson_dumps, RELAXED_JSON_OPTIONS

    return bson_dumps(value, json_options=RELAXED_JSON_OPTIONS)


def iso(value):
    return value.isoformat() if isinstance(value, datetime) else str(value)


def sampled_documents(source, template, limit):
    """Use ten positions in the recorded-date range, then fill sparse windows.

    Every query is limited, index-hinted and has a server time budget. This is
    deliberately a structural/date coverage sample, not a prevalence sample.
    """

    def query(extra=None, direction=1, count=1, header=False):
        criteria = {"template": template, **(extra or {})}
        projection = {"_id": 0, "creation_date": 1} if header else None
        return (
            source.find(criteria, projection)
            .hint(INDEX)
            .sort("creation_date", direction)
            .limit(count)
            .batch_size(min(count, 100))
            .max_time_ms(15000)
        )

    with query(header=True) as cursor:
        oldest = next(cursor, None)
    with query(direction=-1, header=True) as cursor:
        newest = next(cursor, None)
    if oldest is None:
        return
    if not isinstance(oldest.get("creation_date"), datetime) or not isinstance(
        newest.get("creation_date"), datetime
    ):
        raise ValueError(
            "Date-stratified validation requires BSON creation_date values"
        )
    start, end = oldest["creation_date"], newest["creation_date"]
    windows = [(None, 1), (None, -1)] + [
        ({"creation_date": {"$gte": start + (end - start) * i / 9}}, 1)
        for i in range(1, 9)
    ]
    seen = set()
    for extra, direction in windows:
        with query(extra, direction, math.ceil(limit / 10)) as cursor:
            for document in cursor:
                if document["_id"] in seen:
                    continue
                seen.add(document["_id"])
                yield document
                if len(seen) >= limit:
                    return
    # At most limit prior IDs can overlap; 2*limit suffices to fill to the cap.
    with query(count=2 * limit) as cursor:
        for document in cursor:
            if document["_id"] not in seen:
                seen.add(document["_id"])
                yield document
                if len(seen) >= limit:
                    return


def clinical_coverage(canonical, fields):
    from kehrnel.engine.domains.openehr.analytics.source import resolve

    selected, clinical = set(), set()
    for field in fields:
        if field["selected"]:
            selected.update(id(node) for node, _ in resolve(canonical, field["path"]))

    def visit(node, in_entry=False):
        if isinstance(node, dict):
            in_entry = in_entry or node.get("_type") in ENTRY_TYPES
            if (
                in_entry
                and node.get("_type") == "ELEMENT"
                and ("value" in node or "null_flavour" in node)
            ):
                clinical.add(id(node))
            for value in node.values():
                visit(value, in_entry)
        elif isinstance(node, list):
            for value in node:
                visit(value, in_entry)

    visit(canonical)
    return len(clinical), len(clinical - selected)


def error_category(error):
    message = str(error)
    for prefix, category in [
        ("Conflicting source", "conflicting_source_metadata"),
        ("A related", "related_subject_requires_mapping"),
        ("Binding crosses", "repeated_container_requires_mapping"),
        ("The fact", "fact_boundary_requires_mapping"),
        ("Declared ENTRY", "entry_subject_requires_mapping"),
        ("Fact boundary", "fact_boundary_requires_mapping"),
        ("Required interpretation", "interpretation_context_missing"),
    ]:
        if message.startswith(prefix):
            return category
    return "other_conversion_error"


def initialize(db):
    db.executescript("""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE compositions (
            row_id INTEGER PRIMARY KEY, composition_id TEXT NOT NULL UNIQUE,
            template TEXT NOT NULL, version TEXT NOT NULL, recorded_at TEXT NOT NULL,
            status TEXT NOT NULL, fact_count INTEGER, source_nodes INTEGER NOT NULL,
            clinical_elements INTEGER, unmapped_elements INTEGER,
            error_category TEXT, error_message TEXT, detail TEXT NOT NULL
        );
        CREATE TABLE facts (
            row_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, document TEXT NOT NULL,
            PRIMARY KEY(row_id, ordinal)
        );
        CREATE INDEX template_status ON compositions(template, status, row_id);
    """)


def save_result(db, document, result, error, coverage):
    """Rejected conversions have no fact count and no partial projection."""
    status = "rejected" if error else result.status
    count = len(result.facts) if status == "projected" else None
    detail = (
        {
            "activity": result.activity,
            "source_ref": result.source_ref,
            "state": result.state(),
        }
        if result
        else {}
    )
    cursor = db.execute(
        """INSERT INTO compositions
        (composition_id,template,version,recorded_at,status,fact_count,source_nodes,
         clinical_elements,unmapped_elements,error_category,error_message,detail)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            str(document["_id"]),
            document["template"],
            str(document.get("version", "")),
            iso(document.get("creation_date", "")),
            status,
            count,
            len(document.get("cn", [])),
            *(coverage if coverage is not None else (None, None)),
            error_category(error) if error else None,
            str(error) if error else None,
            dumps(detail),
        ),
    )
    if status == "projected":
        db.executemany(
            "INSERT INTO facts VALUES (?,?,?)",
            [(cursor.lastrowid, i, dumps(fact)) for i, fact in enumerate(result.facts)],
        )


def summarize(db):
    rows = db.execute("""SELECT template, count(*) AS sampled,
        sum(status='projected') AS converted, sum(status='rejected') AS rejected,
        sum(status='needs_rules') AS needs_rules, sum(coalesce(fact_count,0)) AS facts,
        sum(status='projected' AND fact_count=0) AS zero_facts,
        sum(coalesce(clinical_elements,0)) AS clinical_elements,
        sum(coalesce(unmapped_elements,0)) AS unmapped_elements
        FROM compositions GROUP BY template ORDER BY template""").fetchall()
    templates = [dict(row) for row in rows]
    totals = {
        key: sum(row[key] for row in templates)
        for key in (
            "sampled",
            "converted",
            "rejected",
            "needs_rules",
            "facts",
            "zero_facts",
            "clinical_elements",
            "unmapped_elements",
        )
    }
    errors = [
        dict(row)
        for row in db.execute(
            """SELECT template, error_category, count(*) AS count
        FROM compositions WHERE error_category IS NOT NULL GROUP BY template, error_category"""
        )
    ]
    return {"templates": templates, "totals": totals, "errors": errors}


def build(args):
    from kehrnel.engine.domains.openehr.analytics.connections import (
        read_environment,
        connect,
        create_converter,
    )
    from kehrnel.engine.domains.openehr.analytics.default_mapping import (
        finalize_proposal,
    )

    output = args.output.resolve()
    if output.is_relative_to(ROOT):
        raise ValueError("Keep customer review data outside the repository")
    os.umask(0o077)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    index = json.loads((args.proposals / "index.json").read_text())
    proposals = {
        item["template"]: json.loads((args.proposals / item["file"]).read_text())
        for item in index
    }
    rules = [finalize_proposal(proposal) for proposal in proposals.values()]
    cfg = json.loads(args.config.read_text())
    env = read_environment(args.env_file)
    client = None
    db = sqlite3.connect(output / "review.sqlite3")
    db.row_factory = sqlite3.Row
    initialize(db)
    accepted_at = datetime.now(timezone.utc).isoformat()
    metadata = {
        "started_at": accepted_at,
        "complete": False,
        "read_only_source": True,
        "source_database": env["ANALYTICS_SOURCE_DATABASE"],
        "source_collection": env["ANALYTICS_SOURCE_COLLECTION"],
        "per_template_limit": args.per_template,
        "source_index": INDEX,
        "method": "Up to 1,000 per template by default, across ten recorded-date positions; indexed fill for sparse windows. Not a random or production-representative sample.",
        "rule_revisions": [rule["revision"] for rule in rules],
        "scope": f"All selected clinical ELEMENTs from {len(proposals)} exact OPT proposals; complete recorded DV values. Not all source templates or every RM attribute.",
        "query_errors": [],
    }
    try:
        client, _, source, _ = connect(env, target=False)
        converter = create_converter(env, source, rules, cfg["source"])
        metadata["decoder_revision"] = converter.decoder.revision
        for template, proposal in proposals.items():
            n = 0
            try:
                for document in sampled_documents(source, template, args.per_template):
                    result, error, coverage = None, None, None
                    try:
                        canonical = converter.decoder.decode(document)
                        coverage = clinical_coverage(canonical, proposal["fields"])
                        result = converter.convert(document, accepted_at=accepted_at)
                    except Exception as exc:
                        error = exc
                    save_result(db, document, result, error, coverage)
                    n += 1
                    if n % 100 == 0:
                        db.commit()
                        print(
                            json.dumps({"template": template, "reviewed": n}),
                            flush=True,
                        )
            except Exception as exc:
                metadata["query_errors"].append(
                    {"template": template, "type": type(exc).__name__}
                )
                raise
            finally:
                db.commit()
            print(json.dumps(summarize(db)["totals"]), flush=True)
        metadata["complete"] = True
    finally:
        metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
        metadata.update(summarize(db))
        db.execute(
            "INSERT OR REPLACE INTO meta VALUES ('summary',?)", (json.dumps(metadata),)
        )
        db.commit()
        (output / "summary.json").write_text(json.dumps(metadata, indent=2) + "\n")
        db.close()
        if client is not None:
            client.close()
    print(
        json.dumps(
            {
                "complete": metadata["complete"],
                "totals": metadata["totals"],
                "review": str(output),
            }
        ),
        flush=True,
    )


def read_rows(db, params):
    where, values = [], []
    for key in ("template", "status"):
        if params.get(key):
            where.append(key + "=?")
            values.append(params[key])
    if params.get("q"):
        where.append("instr(composition_id,?) > 0")
        values.append(params["q"][:150])
    if params.get("zero") == "1":
        where.append("status='projected' AND fact_count=0")
    clause = " WHERE " + " AND ".join(where) if where else ""
    page = max(0, int(params.get("page", 0)))
    sort = {"facts": "fact_count DESC, row_id", "date": "recorded_at DESC, row_id"}.get(
        params.get("sort"), "row_id"
    )
    total = db.execute("SELECT count(*) FROM compositions" + clause, values).fetchone()[
        0
    ]
    rows = db.execute(
        """SELECT row_id,composition_id,template,version,recorded_at,status,
        fact_count,source_nodes,clinical_elements,unmapped_elements,error_category
        FROM compositions""" + clause + " ORDER BY " + sort + " LIMIT 50 OFFSET ?",
        [*values, page * 50],
    ).fetchall()
    return {
        "rows": [dict(row) for row in rows],
        "total": total,
        "page": page,
        "page_size": 50,
    }


def read_detail(db, row_id, page=0):
    row = db.execute("SELECT * FROM compositions WHERE row_id=?", (row_id,)).fetchone()
    if row is None:
        return None
    detail = dict(row)
    detail["projection"] = json.loads(detail.pop("detail"))
    detail["facts"] = [
        json.loads(row[0])
        for row in db.execute(
            "SELECT document FROM facts WHERE row_id=? ORDER BY ordinal LIMIT 20 OFFSET ?",
            (row_id, max(0, page) * 20),
        )
    ]
    detail["page"] = page
    return detail


def serve(args):
    path = args.review.resolve() / "review.sqlite3"
    if not path.is_file():
        raise ValueError("Choose a completed review directory")
    asset = ROOT / "examples/openehr_analytics/review.html"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Requests can contain clinical identifiers; do not log them.

        def respond(self, data, content_type="application/json", status=200):
            payload = data if isinstance(data, bytes) else json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.headers.get("Host") not in {
                f"127.0.0.1:{args.port}",
                f"localhost:{args.port}",
            }:
                return self.respond({"error": "Local review only"}, status=403)
            route = urlsplit(self.path)
            params = {k: v[-1] for k, v in parse_qs(route.query).items()}
            if route.path == "/":
                return self.respond(asset.read_bytes(), "text/html; charset=utf-8")
            private_assets = {
                "/conflicts": ("ibm-conflict-examples.html", "text/html; charset=utf-8"),
                "/conflicts.md": ("ibm-conflict-examples.md", "text/markdown; charset=utf-8"),
                "/conflicts.json": ("ibm-conflict-examples.json", "application/json"),
            }
            if route.path in private_assets:
                filename, content_type = private_assets[route.path]
                report = path.parent / filename
                if not report.is_file() or report.is_symlink():
                    return self.respond({"error": "No conflict examples in this review"}, status=404)
                return self.respond(report.read_bytes(), content_type)
            db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            db.row_factory = sqlite3.Row
            try:
                if route.path == "/api/summary":
                    result = json.loads(
                        db.execute(
                            "SELECT value FROM meta WHERE key='summary'"
                        ).fetchone()[0]
                    )
                    result["has_conflict_examples"] = (
                        path.parent / "ibm-conflict-examples.html"
                    ).is_file()
                elif route.path == "/api/compositions":
                    result = read_rows(db, params)
                elif route.path.startswith("/api/compositions/"):
                    result = read_detail(
                        db,
                        int(route.path.rsplit("/", 1)[-1]),
                        max(0, int(params.get("page", 0))),
                    )
                    if result is None:
                        return self.respond(
                            {"error": "Composition not found"}, status=404
                        )
                else:
                    return self.respond({"error": "Not found"}, status=404)
                self.respond(result)
            except (ValueError, TypeError):
                self.respond({"error": "Invalid review request"}, status=400)
            finally:
                db.close()

    print(f"Private composition review: http://127.0.0.1:{args.port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser(
        "build", help="Read a bounded source sample; save projections locally only"
    )
    p.add_argument("--env-file", type=Path, default=ROOT / ".env.analytics.local")
    p.add_argument(
        "--config",
        type=Path,
        default=ROOT / "examples/openehr_analytics/config.ctti.json",
    )
    p.add_argument(
        "--proposals",
        type=Path,
        default=ROOT / "examples/openehr_analytics/default-proposals",
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--per-template", type=int, default=1000)
    p = sub.add_parser(
        "serve", help="Browse a private local review; no database connection"
    )
    p.add_argument("--review", type=Path, required=True)
    p.add_argument("--port", type=int, default=4326)
    args = parser.parse_args()
    if args.command == "build":
        if not 1 <= args.per_template <= 10000:
            parser.error(
                "Use 1–10,000 records per template for this bounded validation tool"
            )
        build(args)
    else:
        serve(args)


if __name__ == "__main__":
    main()
