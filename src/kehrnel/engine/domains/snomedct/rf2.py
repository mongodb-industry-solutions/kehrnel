"""RF2 Snapshot -> derived concept JSON converter for SNOMED CT ingest.

The kehrnel SNOMED CT ingest operation (``snomed_ingest_release``) consumes a
*derived* JSON array of concept objects -- not the official RF2 tab-delimited
distribution. This module is the missing bridge: it reads a licensed RF2
Snapshot (directory, loose ``.txt`` files, or a release ``.zip``) and emits the
JSON array that :func:`kehrnel.engine.domains.snomedct.iter_concepts_from_json`
streams and :func:`kehrnel.engine.domains.snomedct.normalize_concept` canonicalizes.

Nothing here downloads anything and no SNOMED CT content is embedded: the caller
supplies a release they are licensed to use.

Critical correctness note
-------------------------
``normalize_concept`` does **not** compute transitive closure -- it only
stringifies whatever list it is handed (``stringify_list``). ECL ``<X`` / ``<<X``
expansion compiles to a flat Mongo match on ``{"inferredAncestorIds": X}``.
Therefore ``inferredAncestorIds`` **must** be the full transitive closure of
IS-A ancestors or every descendant query silently under-returns. This module
computes that closure itself, iteratively (never unbounded recursion), is
cycle-safe, and shares parent ancestor-sets so large graphs do not blow up
quadratically in work.
"""

from __future__ import annotations

import json
import os
import re
import zipfile
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, IO, Iterable, Iterator, Sequence

__all__ = [
    "ACCEPTABILITY_ACCEPTABLE_ID",
    "ACCEPTABILITY_PREFERRED_ID",
    "CHARACTERISTIC_INFERRED_ID",
    "DEFINITION_STATUS_DEFINED_ID",
    "DEFINITION_STATUS_PRIMITIVE_ID",
    "IS_A_TYPE_ID",
    "TYPE_FSN_ID",
    "TYPE_SYNONYM_ID",
    "Rf2FileSet",
    "Rf2Source",
    "ConversionStats",
    "HierarchyIndex",
    "build_hierarchy_index",
    "compute_transitive_ancestors",
    "convert_rf2_to_json",
    "iter_rf2_rows",
    "is_active",
    "locate_rf2_files",
    "open_rf2_source",
]

# --- RF2 / SNOMED CT metadata concept ids (structural identifiers, not content) ---

IS_A_TYPE_ID = "116680003"
CHARACTERISTIC_INFERRED_ID = "900000000000011006"
CHARACTERISTIC_ADDITIONAL_ID = "900000000000227009"
DEFINITION_STATUS_PRIMITIVE_ID = "900000000000074008"
DEFINITION_STATUS_DEFINED_ID = "900000000000073002"
TYPE_FSN_ID = "900000000000003001"
TYPE_SYNONYM_ID = "900000000000013009"
ACCEPTABILITY_PREFERRED_ID = "900000000000548007"
ACCEPTABILITY_ACCEPTABLE_ID = "900000000000549004"

TRUE_VALUES = {"1", "true", "TRUE", "True", "y", "Y", "yes"}

#: Glob patterns used to locate each RF2 Snapshot file. Release file names carry
#: edition/date suffixes (e.g. ``sct2_Concept_Snapshot_INT_20240101.txt``), so we
#: always resolve by glob and never by an exact file name.
CONCEPT_GLOBS = ("sct2_Concept_Snapshot*.txt", "sct2_Concept_Snapshot*.TXT")
DESCRIPTION_GLOBS = ("sct2_Description_Snapshot*.txt", "sct2_Description_Snapshot*.TXT")
RELATIONSHIP_GLOBS = ("sct2_Relationship_Snapshot*.txt", "sct2_Relationship_Snapshot*.TXT")
LANGUAGE_REFSET_GLOBS = ("der2_cRefset_Language*Snapshot*.txt", "der2_cRefset_Language*Snapshot*.TXT")


class Rf2Error(RuntimeError):
    """Raised when an RF2 release cannot be read or is missing required files."""


def is_active(value: Any) -> bool:
    """True when an RF2 ``active`` column marks the row as active."""
    return str(value).strip() in TRUE_VALUES


def _is_inferred(characteristic_type_id: str) -> bool:
    """Only inferred relationships define the classified (queryable) hierarchy.

    Stated and 'additional' relationships must never contribute IS-A edges: the
    inferred view is what ECL descendant queries resolve against.
    """
    return characteristic_type_id.strip() == CHARACTERISTIC_INFERRED_ID


# --------------------------------------------------------------------------- #
# Source abstraction: directory / loose files / zip
# --------------------------------------------------------------------------- #


@dataclass
class Rf2FileSet:
    """Resolved locations of the RF2 Snapshot files inside a source."""

    concepts: str
    descriptions: str
    relationships: str
    language_refsets: list[str] = field(default_factory=list)

    def describe(self) -> dict[str, Any]:
        return {
            "concepts": self.concepts,
            "descriptions": self.descriptions,
            "relationships": self.relationships,
            "languageRefsets": list(self.language_refsets),
        }


class Rf2Source:
    """Read-only access to RF2 member files, backed by a directory or a zip.

    Members are addressed by the *relative* path string returned from
    :meth:`list_members`, so callers do not care which backing store is in use.
    """

    def __init__(self, root: Path, *, zip_file: zipfile.ZipFile | None = None) -> None:
        self.root = root
        self._zip = zip_file

    # -- discovery ---------------------------------------------------------
    def list_members(self) -> list[str]:
        if self._zip is not None:
            return [name for name in self._zip.namelist() if not name.endswith("/")]
        if self.root.is_file():
            return [self.root.name]
        return [
            str(path.relative_to(self.root)).replace(os.sep, "/")
            for path in sorted(self.root.rglob("*"))
            if path.is_file()
        ]

    # -- reading -----------------------------------------------------------
    @contextmanager
    def open_text(self, member: str) -> Iterator[Iterable[str]]:
        """Open a member as a stream of text lines (never loads the whole file)."""
        if self._zip is not None:
            with self._zip.open(member, "r") as raw:
                yield _decode_lines(raw)
            return
        path = self.root if self.root.is_file() and member == self.root.name else self.root / member
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            yield handle

    def close(self) -> None:
        if self._zip is not None:
            self._zip.close()
            self._zip = None


def _decode_lines(raw: IO[bytes]) -> Iterator[str]:
    """Decode a binary zip member lazily, line by line."""
    trailing = ""
    first = True
    while True:
        chunk = raw.read(1 << 20)
        if not chunk:
            break
        text = trailing + chunk.decode("utf-8", errors="replace")
        lines = text.splitlines(keepends=True)
        trailing = ""
        if lines and not lines[-1].endswith(("\n", "\r")):
            trailing = lines.pop()
        for line in lines:
            if first:
                line = line.lstrip("\ufeff")
                first = False
            yield line
    if trailing:
        if first:
            trailing = trailing.lstrip("\ufeff")
        yield trailing


@contextmanager
def open_rf2_source(path: str | Path) -> Iterator[Rf2Source]:
    """Open an RF2 release: a ``.zip`` archive, a directory, or a single file."""
    target = Path(path)
    if not target.exists():
        raise Rf2Error(f"RF2 source does not exist: {target}")
    if target.is_file() and zipfile.is_zipfile(target):
        archive = zipfile.ZipFile(target, "r")
        source = Rf2Source(target, zip_file=archive)
        try:
            yield source
        finally:
            source.close()
        return
    source = Rf2Source(target)
    try:
        yield source
    finally:
        source.close()


def _match_members(members: Sequence[str], patterns: Sequence[str]) -> list[str]:
    """Match members against glob patterns on the *base name*, at any depth."""
    import fnmatch

    matched: list[str] = []
    for member in members:
        base = member.rsplit("/", 1)[-1]
        for pattern in patterns:
            if fnmatch.fnmatch(base, pattern) or fnmatch.fnmatch(base.lower(), pattern.lower()):
                matched.append(member)
                break
    return sorted(set(matched))


def locate_rf2_files(source: Rf2Source) -> Rf2FileSet:
    """Locate the required RF2 Snapshot files by glob pattern."""
    members = source.list_members()

    def _one(patterns: Sequence[str], label: str) -> str:
        found = _match_members(members, patterns)
        if not found:
            raise Rf2Error(
                f"No {label} RF2 Snapshot file found (looked for {', '.join(patterns)}). "
                f"Found {len(members)} file(s) in the source."
            )
        return found[0]

    return Rf2FileSet(
        concepts=_one(CONCEPT_GLOBS, "Concept"),
        descriptions=_one(DESCRIPTION_GLOBS, "Description"),
        relationships=_one(RELATIONSHIP_GLOBS, "Relationship"),
        language_refsets=_match_members(members, LANGUAGE_REFSET_GLOBS),
    )


# --------------------------------------------------------------------------- #
# Row streaming (column resolution by header NAME, never by fixed index)
# --------------------------------------------------------------------------- #


def iter_rf2_rows(source: Rf2Source, member: str) -> Iterator[dict[str, str]]:
    """Stream one RF2 file as dict rows keyed by its header column names.

    Column positions vary between editions and derivative tools, so every value
    is resolved by header name. Rows are yielded one at a time -- nothing is
    accumulated here.
    """
    with source.open_text(member) as lines:
        iterator = iter(lines)
        try:
            header_line = next(iterator)
        except StopIteration:
            return
        header = [name.strip().lstrip("\ufeff") for name in header_line.rstrip("\r\n").split("\t")]
        width = len(header)
        for line in iterator:
            line = line.rstrip("\r\n")
            if not line:
                continue
            values = line.split("\t")
            if len(values) < width:
                values = values + [""] * (width - len(values))
            yield dict(zip(header, values))


def _require_columns(row: dict[str, str], required: Sequence[str], member: str) -> None:
    missing = [name for name in required if name not in row]
    if missing:
        raise Rf2Error(f"{member} is missing required column(s): {', '.join(missing)}")


# --------------------------------------------------------------------------- #
# Hierarchy: IS-A graph + full transitive closure
# --------------------------------------------------------------------------- #


@dataclass
class HierarchyIndex:
    """Direct IS-A adjacency, built from active inferred relationship rows."""

    parents: dict[str, set[str]] = field(default_factory=dict)
    children: dict[str, set[str]] = field(default_factory=dict)

    def add_edge(self, child_id: str, parent_id: str) -> None:
        if not child_id or not parent_id or child_id == parent_id:
            # A self IS-A edge carries no information and would only seed a cycle.
            return
        self.parents.setdefault(child_id, set()).add(parent_id)
        self.children.setdefault(parent_id, set()).add(child_id)

    def parents_of(self, concept_id: str) -> set[str]:
        return self.parents.get(concept_id, set())

    def children_of(self, concept_id: str) -> set[str]:
        return self.children.get(concept_id, set())

    def nodes(self) -> set[str]:
        return set(self.parents) | set(self.children)


def build_hierarchy_index(rows: Iterable[dict[str, str]]) -> HierarchyIndex:
    """Build the IS-A adjacency from streamed relationship rows.

    Only rows that are active **and** inferred **and** typed IS-A participate.
    """
    index = HierarchyIndex()
    for row in rows:
        if not is_active(row.get("active", "")):
            continue
        if row.get("typeId", "").strip() != IS_A_TYPE_ID:
            continue
        if not _is_inferred(row.get("characteristicTypeId", "")):
            continue
        index.add_edge(row.get("sourceId", "").strip(), row.get("destinationId", "").strip())
    return index


def compute_transitive_ancestors(index: HierarchyIndex) -> dict[str, set[str]]:
    """Compute the FULL transitive ancestor closure for every concept.

    Strategy: an explicit-stack post-order traversal that memoises each node's
    completed ancestor set and unions in each parent's *already-completed* set.
    That makes the work proportional to the number of (edge, ancestor-set-union)
    operations rather than re-walking the graph per concept, so deep or wide
    graphs do not degrade quadratically in traversal count.

    Cycle safety: nodes currently on the stack are tracked in ``in_progress``.
    An edge back into an in-progress node is recorded as an ancestor but is not
    descended into, so a cyclic IS-A graph terminates instead of recursing
    forever. After the main pass, members of each cycle are reconciled so that
    every node in a cycle sees the union of the cycle's ancestors.
    """
    ancestors: dict[str, set[str]] = {}
    cycle_edges: list[tuple[str, str]] = []

    for root in index.nodes():
        if root in ancestors:
            continue
        stack: list[tuple[str, Iterator[str]]] = [(root, iter(sorted(index.parents_of(root))))]
        in_progress: set[str] = {root}
        while stack:
            node, parent_iter = stack[-1]
            advanced = False
            for parent in parent_iter:
                if parent in ancestors:
                    continue  # already complete; folded in during finalisation
                if parent in in_progress:
                    # Cycle: do not descend, but remember the edge.
                    cycle_edges.append((node, parent))
                    continue
                stack.append((parent, iter(sorted(index.parents_of(parent)))))
                in_progress.add(parent)
                advanced = True
                break
            if advanced:
                continue
            # All parents of `node` are resolved (or deliberately skipped).
            stack.pop()
            in_progress.discard(node)
            node_parents = index.parents_of(node)
            if not node_parents:
                ancestors[node] = set()
                continue
            # Seed from the largest parent closure and union the rest in, so a
            # long single-parent chain copies one set per level instead of
            # rebuilding the whole closure element by element.
            base_parent = max(node_parents, key=lambda p: len(ancestors.get(p, ())))
            collected = set(ancestors.get(base_parent, ()))
            collected.add(base_parent)
            for parent in node_parents:
                if parent is base_parent or parent == base_parent:
                    continue
                collected.add(parent)
                parent_ancestors = ancestors.get(parent)
                if parent_ancestors:
                    collected |= parent_ancestors
            collected.discard(node)
            ancestors[node] = collected

    # Reconcile cycles: propagate ancestor sets around back-edges until stable.
    if cycle_edges:
        queue = deque({node for node, _ in cycle_edges})
        seeded = set(queue)
        while queue:
            node = queue.popleft()
            seeded.discard(node)
            current = ancestors.setdefault(node, set())
            merged = set(current)
            for parent in index.parents_of(node):
                merged.add(parent)
                merged |= ancestors.get(parent, set())
            merged.discard(node)
            if merged != current:
                ancestors[node] = merged
                for child in index.children_of(node):
                    if child not in seeded:
                        seeded.add(child)
                        queue.append(child)

    return ancestors


# --------------------------------------------------------------------------- #
# Conversion
# --------------------------------------------------------------------------- #


@dataclass
class ConversionStats:
    """Counters describing one conversion run."""

    concepts_total: int = 0
    concepts_written: int = 0
    concepts_inactive_skipped: int = 0
    descriptions_total: int = 0
    descriptions_kept: int = 0
    relationships_total: int = 0
    relationships_kept: int = 0
    isa_edges: int = 0
    language_refset_rows: int = 0
    acceptability_entries: int = 0
    orphan_descriptions: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "conceptsTotal": self.concepts_total,
            "conceptsWritten": self.concepts_written,
            "conceptsInactiveSkipped": self.concepts_inactive_skipped,
            "descriptionsTotal": self.descriptions_total,
            "descriptionsKept": self.descriptions_kept,
            "relationshipsTotal": self.relationships_total,
            "relationshipsKept": self.relationships_kept,
            "isaEdges": self.isa_edges,
            "languageRefsetRows": self.language_refset_rows,
            "acceptabilityEntries": self.acceptability_entries,
            "orphanDescriptions": self.orphan_descriptions,
        }


def _concept_payload(row: dict[str, str]) -> dict[str, Any]:
    return {
        "conceptId": row.get("id", "").strip(),
        "active": is_active(row.get("active", "")),
        "effectiveTime": row.get("effectiveTime", "").strip(),
        "moduleId": row.get("moduleId", "").strip(),
        "definitionStatusId": row.get("definitionStatusId", "").strip(),
    }


def _description_payload(row: dict[str, str]) -> dict[str, Any]:
    return {
        "descriptionId": row.get("id", "").strip(),
        "conceptId": row.get("conceptId", "").strip(),
        "active": is_active(row.get("active", "")),
        "effectiveTime": row.get("effectiveTime", "").strip(),
        "moduleId": row.get("moduleId", "").strip(),
        "typeId": row.get("typeId", "").strip(),
        "languageCode": row.get("languageCode", "").strip().lower(),
        "term": row.get("term", ""),
        "caseSignificanceId": row.get("caseSignificanceId", "").strip(),
        "acceptabilityMap": {},
    }


def _relationship_payload(row: dict[str, str]) -> dict[str, Any]:
    return {
        "relationshipId": row.get("id", "").strip(),
        "sourceId": row.get("sourceId", "").strip(),
        "destinationId": row.get("destinationId", "").strip(),
        "typeId": row.get("typeId", "").strip(),
        "moduleId": row.get("moduleId", "").strip(),
        "characteristicTypeId": row.get("characteristicTypeId", "").strip(),
        "modifierId": row.get("modifierId", "").strip(),
        "active": is_active(row.get("active", "")),
        "effectiveTime": row.get("effectiveTime", "").strip(),
    }


def convert_rf2_to_json(
    source_path: str | Path,
    output_path: str | Path,
    *,
    include_inactive_concepts: bool = False,
    indent: int | None = 2,
    progress: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    """Convert an RF2 Snapshot release into the derived concept JSON array.

    Parameters
    ----------
    source_path:
        A release directory, a Snapshot ``Terminology`` folder, or a ``.zip``.
    output_path:
        Destination ``.json`` file. Written as a top-level JSON array, one
        concept object per item, streamed item by item.
    include_inactive_concepts:
        When False (default) inactive concept rows are skipped entirely.
        Inactive description/relationship rows are always skipped: they must
        never contribute terms or hierarchy.
    indent:
        Pretty-print indent for each concept object, or ``None`` for compact.
    progress:
        Optional ``callback(stage, count)`` invoked periodically.

    Returns
    -------
    dict
        A report with the resolved files and :class:`ConversionStats` counters.
    """
    out = Path(output_path)
    stats = ConversionStats()

    def _tick(stage: str, count: int) -> None:
        if progress is not None and count % 100_000 == 0:
            progress(stage, count)

    with open_rf2_source(source_path) as source:
        files = locate_rf2_files(source)

        # -- pass 1: concepts (the only rows we must hold; one small dict each)
        concepts: dict[str, dict[str, Any]] = {}
        for row in iter_rf2_rows(source, files.concepts):
            if stats.concepts_total == 0:
                _require_columns(row, ["id", "effectiveTime", "active", "moduleId", "definitionStatusId"], files.concepts)
            stats.concepts_total += 1
            _tick("concepts", stats.concepts_total)
            concept_id = row.get("id", "").strip()
            if not concept_id:
                continue
            active = is_active(row.get("active", ""))
            if not active and not include_inactive_concepts:
                stats.concepts_inactive_skipped += 1
                continue
            concepts[concept_id] = _concept_payload(row)

        # -- pass 2: descriptions, streamed and bucketed per concept
        descriptions: dict[str, list[dict[str, Any]]] = {}
        description_index: dict[str, dict[str, Any]] = {}
        for row in iter_rf2_rows(source, files.descriptions):
            if stats.descriptions_total == 0:
                _require_columns(
                    row,
                    ["id", "effectiveTime", "active", "moduleId", "conceptId", "languageCode", "typeId", "term", "caseSignificanceId"],
                    files.descriptions,
                )
            stats.descriptions_total += 1
            _tick("descriptions", stats.descriptions_total)
            if not is_active(row.get("active", "")):
                continue
            concept_id = row.get("conceptId", "").strip()
            if concept_id not in concepts:
                stats.orphan_descriptions += 1
                continue
            payload = _description_payload(row)
            if not payload["descriptionId"]:
                continue
            descriptions.setdefault(concept_id, []).append(payload)
            description_index[payload["descriptionId"]] = payload
            stats.descriptions_kept += 1

        # -- pass 3: language refsets -> acceptabilityMap on the referenced description
        for member in files.language_refsets:
            for row in iter_rf2_rows(source, member):
                if stats.language_refset_rows == 0:
                    _require_columns(row, ["refsetId", "referencedComponentId", "acceptabilityId", "active"], member)
                stats.language_refset_rows += 1
                _tick("language", stats.language_refset_rows)
                if not is_active(row.get("active", "")):
                    continue
                description = description_index.get(row.get("referencedComponentId", "").strip())
                if description is None:
                    continue
                refset_id = row.get("refsetId", "").strip()
                acceptability_id = row.get("acceptabilityId", "").strip()
                if not refset_id or not acceptability_id:
                    continue
                description["acceptabilityMap"][refset_id] = acceptability_id
                stats.acceptability_entries += 1

        # -- pass 4: relationships, streamed; hierarchy edges collected alongside
        relationships: dict[str, list[dict[str, Any]]] = {}
        hierarchy = HierarchyIndex()
        for row in iter_rf2_rows(source, files.relationships):
            if stats.relationships_total == 0:
                _require_columns(
                    row,
                    ["id", "effectiveTime", "active", "moduleId", "sourceId", "destinationId", "typeId", "characteristicTypeId", "modifierId"],
                    files.relationships,
                )
            stats.relationships_total += 1
            _tick("relationships", stats.relationships_total)
            if not is_active(row.get("active", "")):
                continue
            source_id = row.get("sourceId", "").strip()
            if source_id not in concepts:
                continue
            payload = _relationship_payload(row)
            relationships.setdefault(source_id, []).append(payload)
            stats.relationships_kept += 1
            if payload["typeId"] == IS_A_TYPE_ID and _is_inferred(payload["characteristicTypeId"]):
                destination_id = payload["destinationId"]
                # An IS-A to a concept we are not emitting cannot be a usable
                # hierarchy edge, so keep the graph restricted to emitted nodes.
                if destination_id in concepts:
                    hierarchy.add_edge(source_id, destination_id)
                    stats.isa_edges += 1

    # -- closure (the correctness-critical step)
    ancestors = compute_transitive_ancestors(hierarchy)

    # -- emit
    out.parent.mkdir(parents=True, exist_ok=True)
    item_indent = (" " * indent) if indent else ""
    with out.open("w", encoding="utf-8") as handle:
        handle.write("[")
        first = True
        for concept_id in sorted(concepts):
            concept = dict(concepts[concept_id])
            concept["descriptions"] = descriptions.get(concept_id, [])
            concept["relationships"] = relationships.get(concept_id, [])
            concept["concreteRelationships"] = []
            concept["inferredParentIds"] = sorted(hierarchy.parents_of(concept_id))
            concept["inferredAncestorIds"] = sorted(ancestors.get(concept_id, set()))
            concept["inferredChildIds"] = sorted(hierarchy.children_of(concept_id))
            concept["memberOfRefsetIds"] = []
            text = json.dumps(concept, ensure_ascii=False, indent=indent)
            if indent:
                text = "\n".join(item_indent + line for line in text.splitlines())
                handle.write(("\n" if first else ",\n") + text)
            else:
                handle.write(("" if first else ",") + text)
            first = False
            stats.concepts_written += 1
        handle.write("\n]\n" if (indent and not first) else "]\n")

    return {
        "ok": True,
        "source": str(Path(source_path)),
        "output": str(out),
        "files": files.describe(),
        "stats": stats.as_dict(),
    }
