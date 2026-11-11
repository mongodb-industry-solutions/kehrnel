"""Prepare licensed RF2 Snapshots for the existing JSON release ingest path.

Components are joined in a temporary SQLite store, not a second terminology
database. No archive extraction, classifier, invented identifiers or mappings.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import zipfile
from collections import Counter, deque
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from .model import parse_release_date
from .sidecar import build_term_documents

ISA = "116680003"
INFERRED = "900000000000011006"
CONTRACT = "snomedct-preparation/v1"
MAX_BYTES = 20_000_000_000
MAX_FILES = 10_000


class RF2PreparationError(ValueError):
    """The input cannot safely produce a coherent Snapshot projection."""


def _kind(fields: set[str]) -> str | None:
    if "definitionStatusId" in fields:
        return "concept"
    if {"conceptId", "term", "languageCode"} <= fields:
        return "description"
    if {"sourceId", "destinationId", "typeId"} <= fields:
        return "relationship"
    if {"sourceId", "value", "typeId"} <= fields:
        return "concrete"
    if {"refsetId", "referencedComponentId"} <= fields:
        if "acceptabilityId" in fields:
            return "language"
        if "owlExpression" in fields:
            return "owl"
        if "mapTarget" in fields:
            return "mapping"
        return "refset"
    return None


@contextmanager
def _files(source: Path):
    """Only Snapshot text files. ZIP members are streamed without extraction."""
    archive = None
    entries = []
    try:
        if source.is_dir():
            for path in sorted(source.rglob("*.txt")):
                if not path.resolve().is_relative_to(source.resolve()) or path.is_symlink():
                    raise RF2PreparationError("RF2 files must remain inside the source directory; symlinks are unsupported.")
                if "snapshot" in path.name.lower() or "snapshot" in [p.lower() for p in path.parts]:
                    entries.append((path.relative_to(source).as_posix(), path.stat().st_size, path))
        elif source.suffix.lower() == ".zip":
            archive = zipfile.ZipFile(source)
            for member in archive.infolist():
                path = PurePosixPath(member.filename)
                if path.is_absolute() or ".." in path.parts or "\\" in member.filename or stat.S_ISLNK(member.external_attr >> 16):
                    raise RF2PreparationError("Unsafe ZIP member path or symlink.")
                if not member.is_dir() and path.suffix.lower() == ".txt" and "snapshot" in member.filename.lower():
                    entries.append((member.filename, member.file_size, member))
        else:
            raise RF2PreparationError("Use an RF2 Snapshot directory or ZIP archive.")
        if not entries:
            raise RF2PreparationError("No RF2 Snapshot text files found. Full and Delta imports are unsupported.")
        if len(entries) > MAX_FILES or sum(e[1] for e in entries) > MAX_BYTES:
            raise RF2PreparationError("RF2 input exceeds the preparation file or uncompressed size limit.")

        def opened():
            for name, size, entry in sorted(entries, key=lambda e: e[0]):
                with (archive.open(entry) if archive else entry.open("rb")) as stream:
                    yield name, size, stream

        yield opened()
    finally:
        if archive:
            archive.close()


def _ancestors(focus: str, parents: dict[str, set[str]]) -> list[str]:
    found: set[str] = set()
    todo = list(parents.get(focus, ()))
    while todo:
        node = todo.pop()
        if node not in found:
            found.add(node)
            todo.extend(parents.get(node, ()))
    return sorted(found)


def _taxonomy(db: sqlite3.Connection) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    parents: dict[str, set[str]] = {}
    children: dict[str, set[str]] = {}
    # Only active, supplied inferred is-a edges participate in the closure.
    for (body,) in db.execute("SELECT body FROM components WHERE kind='relationship'"):
        row = json.loads(body)
        if row["active"] == "1" and row["typeId"] == ISA and row["characteristicTypeId"] == INFERRED:
            source, target = row["sourceId"], row["destinationId"]
            for code in (source, target):
                if not db.execute("SELECT 1 FROM components WHERE kind='concept' AND id=?", (code,)).fetchone():
                    raise RF2PreparationError(f"Missing inferred hierarchy dependency {code}; include the required edition Snapshot.")
            parents.setdefault(source, set()).add(target)
            children.setdefault(target, set()).add(source)
    # Kahn's algorithm detects cycles without recursive depth limits.
    nodes = set(parents) | set(children)
    degrees = {n: len(parents.get(n, ())) for n in nodes}
    queue = deque(n for n, d in degrees.items() if not d)
    visited = 0
    while queue:
        n = queue.popleft()
        visited += 1
        for child in children.get(n, ()):
            degrees[child] -= 1
            if not degrees[child]:
                queue.append(child)
    if visited != len(nodes):
        raise RF2PreparationError("Cycle in supplied inferred is-a relationships; preparation stopped.")
    return parents, children


def _rows(db: sqlite3.Connection, kind: str, owner: str) -> list[dict[str, Any]]:
    return [json.loads(r[0]) for r in db.execute("SELECT body FROM components WHERE kind=? AND owner=? ORDER BY id", (kind, owner))]


def prepare_rf2(
    source: str | Path, *, release_id: str, output: str | Path | None = None,
    languages: list[str] | None = None, license_acknowledged: bool = False,
) -> dict[str, Any]:
    """Validate the whole Snapshot; optionally write an atomic JSON + receipt.

    ``output=None`` is a complete dry run with no retained source or database
    writes. Full releases require temporary disk roughly proportional to RF2.
    Existing outputs are never overwritten. Ingest is a separate operation.
    """
    if parse_release_date(release_id) is None:
        raise RF2PreparationError("release_id must be a valid YYYYMMDD date.")
    if output is not None and not license_acknowledged:
        raise RF2PreparationError("Writing licensed derivatives requires license_acknowledged=true.")
    source = Path(source).resolve()
    destination = Path(output) if output is not None else None
    receipt_path = Path(str(destination) + ".receipt") if destination else None
    if destination and (destination.exists() or receipt_path.exists()):
        raise RF2PreparationError("Prepared output already exists; choose a new output name.")
    counts: Counter = Counter()
    language_counts: Counter = Counter()
    modules: set[str] = set()
    inventory = []
    duplicate_count = 0
    with tempfile.TemporaryDirectory(prefix="kehrnel-rf2-") as temp:
        db = sqlite3.connect(Path(temp) / "components.sqlite")
        try:
            db.execute("CREATE TABLE components(kind TEXT, id TEXT, owner TEXT, body TEXT, PRIMARY KEY(kind,id))")
            with _files(source) as files:
                for name, size, stream in files:
                    digest = hashlib.sha256()
                    # Hash exactly the bytes consumed by the parser in one pass.
                    def lines():
                        first = True
                        for line in stream:
                            digest.update(line)
                            yield line.decode("utf-8-sig" if first else "utf-8")
                            first = False
                    reader = csv.DictReader(lines(), delimiter="\t", quoting=csv.QUOTE_NONE)
                    fields = set(reader.fieldnames or ())
                    kind = _kind(fields)
                    if kind is None:
                        for _ in reader:
                            pass
                        inventory.append({"name": name, "bytes": size, "sha256": digest.hexdigest(), "kind": "unsupported", "rows": 0})
                        continue
                    if not {"id", "effectiveTime", "active", "moduleId"} <= fields:
                        raise RF2PreparationError(f"Missing RF2 columns in {name}.")
                    seen = 0
                    for row in reader:
                        if None in row or any(v is None for v in row.values()):
                            raise RF2PreparationError(f"Malformed tab-separated row in {name}, line {reader.line_num}.")
                        if not row["id"] or row["active"] not in {"0", "1"} or parse_release_date(row["effectiveTime"]) is None:
                            raise RF2PreparationError(f"Invalid released component in {name}, line {reader.line_num}.")
                        if row["effectiveTime"] > release_id:
                            raise RF2PreparationError("A component effectiveTime is newer than the declared release.")
                        if kind in {"relationship", "concrete"} and not {"characteristicTypeId", "relationshipGroup", "modifierId"} <= fields:
                            raise RF2PreparationError(f"Missing relationship columns in {name}.")
                        owner = row.get("conceptId") or row.get("sourceId") or row.get("referencedComponentId") or row["id"]
                        body = json.dumps(row, sort_keys=True, ensure_ascii=False)
                        previous = db.execute("SELECT body FROM components WHERE kind=? AND id=?", (kind, row["id"])).fetchone()
                        if previous:
                            if previous[0] != body:
                                raise RF2PreparationError(f"Conflicting Snapshot component {row['id']}; use a coherent edition and its dependencies.")
                            duplicate_count += 1
                        else:
                            db.execute("INSERT INTO components VALUES (?,?,?,?)", (kind, row["id"], owner, body))
                            counts[kind] += 1
                            modules.add(row["moduleId"])
                            if kind == "description" and row["active"] == "1":
                                language_counts[row["languageCode"]] += 1
                        seen += 1
                    db.commit()
                    inventory.append({"name": name, "bytes": size, "sha256": digest.hexdigest(), "kind": kind, "rows": seen})
            if not counts["concept"] or not counts["description"] or not counts["relationship"]:
                raise RF2PreparationError("Concept, Description and supplied inferred Relationship Snapshot components are required.")
            db.execute("CREATE INDEX components_owner ON components(kind,owner)")
            for kind in ("description", "relationship", "concrete", "owl", "mapping"):
                missing = db.execute("SELECT owner FROM components c WHERE kind=? AND NOT EXISTS (SELECT 1 FROM components x WHERE x.kind='concept' AND x.id=c.owner) LIMIT 1", (kind,)).fetchone()
                if missing:
                    raise RF2PreparationError(f"Missing concept dependency {missing[0]} for {kind} components.")
            missing = db.execute("SELECT owner FROM components c WHERE kind='language' AND NOT EXISTS (SELECT 1 FROM components x WHERE x.kind='description' AND x.id=c.owner) LIMIT 1").fetchone()
            if missing:
                raise RF2PreparationError(f"Language member references missing description {missing[0]}.")
            parents, children = _taxonomy(db)
            if not parents:
                raise RF2PreparationError("No active supplied inferred is-a edges found; stated axioms cannot substitute for a classified Snapshot.")
            selected_languages = languages or sorted(language_counts)
            out = Path(temp) / "prepared.json"
            prepared_hash = hashlib.sha256()
            active_count = term_count = 0
            with out.open("wb") as handle:
                def emit(value: bytes):
                    handle.write(value)
                    prepared_hash.update(value)
                emit(b"[\n")
                first = True
                for (body,) in db.execute("SELECT body FROM components WHERE kind='concept' ORDER BY id"):
                    doc = json.loads(body)
                    code = doc.pop("id")
                    doc["conceptId"] = code
                    doc["active"] = doc["active"] == "1"
                    active_count += int(doc["active"])
                    descriptions = _rows(db, "description", code)
                    for desc in descriptions:
                        desc["descriptionId"] = desc["id"]
                        desc["acceptabilityMap"] = {r["refsetId"]: r["acceptabilityId"] for r in _rows(db, "language", desc["id"]) if r["active"] == "1"}
                    doc["descriptions"] = descriptions
                    for kind, key in (("relationship", "relationships"), ("concrete", "concreteRelationships"), ("owl", "owlAxioms"), ("mapping", "mappings")):
                        doc[key] = _rows(db, kind, code)
                    doc["memberOfRefsetIds"] = sorted({r["refsetId"] for k in ("refset", "owl", "mapping") for r in _rows(db, k, code) if r["active"] == "1"})
                    doc["refsetMembers"] = _rows(db, "refset", code)
                    doc["inferredParentIds"] = sorted(parents.get(code, ()))
                    doc["inferredChildIds"] = sorted(children.get(code, ()))
                    doc["inferredAncestorIds"] = _ancestors(code, parents)
                    doc["sourceFormat"] = "rf2-snapshot"
                    term_count += len(build_term_documents(doc, release_id=release_id, language_codes=selected_languages))
                    if not first:
                        emit(b",\n")
                    emit(json.dumps(doc, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
                    first = False
                emit(b"\n]\n")
            source_hash = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            warnings = []
            if not counts["language"]:
                warnings.append("No language acceptability refset: preferred terms use the existing sidecar fallback.")
            if any(i["kind"] == "unsupported" for i in inventory):
                warnings.append("Unsupported Snapshot files are inventoried but not projected.")
            absent_languages = sorted(set(selected_languages) - set(language_counts))
            if absent_languages:
                warnings.append(f"No active descriptions for configured languages: {', '.join(absent_languages)}.")
            report = {
                "ok": True, "contract": CONTRACT, "dry_run": destination is None,
                "releaseId": release_id, "source_format": "rf2-snapshot",
                "source_digest": "sha256:" + source_hash,
                "output_digest": "sha256:" + prepared_hash.hexdigest(),
                "files": inventory, "component_counts": dict(counts),
                "concepts": counts["concept"], "active_concepts": active_count,
                "duplicate_components": duplicate_count, "modules": sorted(modules),
                "active_description_languages": dict(language_counts), "projection_languages": selected_languages,
                "estimated_term_documents": term_count,
                "hierarchy": {"basis": "supplied-inferred-is-a", "validated": True, "edges": sum(map(len, parents.values())), "cycles": 0},
                "warnings": warnings,
                "output_path": str(destination) if destination else None,
                "receipt_path": str(receipt_path) if receipt_path else None,
                "next_step": "inspect-diff-dry-run-ingest" if destination else "prepare-to-json",
            }
            if destination:
                destination.parent.mkdir(parents=True, exist_ok=True)
                # Publish complete files atomically on the destination filesystem.
                # Hard links create exclusively; an existing output is never replaced.
                created = False
                with tempfile.TemporaryDirectory(prefix=".rf2-publish-", dir=destination.parent) as publishing:
                    staged = Path(publishing) / "concepts"
                    staged_receipt = Path(publishing) / "receipt"
                    shutil.copyfile(out, staged)
                    staged.chmod(0o600)
                    staged_receipt.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                    staged_receipt.chmod(0o600)
                    try:
                        os.link(staged, destination)
                        created = True
                        os.link(staged_receipt, receipt_path)
                    except Exception:
                        if created:
                            destination.unlink(missing_ok=True)
                        raise
            return report
        finally:
            db.close()


def main(argv=None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Validate RF2 Snapshot and prepare Kehrnel concept JSON.")
    parser.add_argument("source", type=Path)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output", type=Path, help="Omit for a complete dry run.")
    parser.add_argument("--language", action="append", dest="languages")
    parser.add_argument("--license-acknowledged", action="store_true")
    args = parser.parse_args(argv)
    try:
        print(json.dumps(prepare_rf2(args.source, release_id=args.release_id, output=args.output, languages=args.languages, license_acknowledged=args.license_acknowledged), ensure_ascii=False, indent=2))
        return 0
    except (RF2PreparationError, zipfile.BadZipFile, UnicodeError, csv.Error) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
