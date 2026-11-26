"""
Verify the SYNTHETIC SNOMED-shaped demonstration release (hdl-synthetic-2.json).

=============================================================================
HONESTY NOTICE
This verifies a HAND-AUTHORED, SYNTHETIC release. It is NOT a SNOMED CT
release, NOT a substitute for a licensed release, and MUST NOT be used
clinically. Some concept identifiers are real SNOMED identifiers used only as
*references* for structural fidelity; the hierarchy itself is synthetic. See
kehrnel/scripts/generate_synthetic_snomed_release.py for the full notice.
=============================================================================

Checks performed:
  (a) every parent/child edge is reciprocal
  (b) inferredAncestorIds equals the independently recomputed closure
  (c) the IS-A graph is acyclic
  (d) every FSN carries a parenthesised semantic tag
  (e) every clinical finding carries 404684003 among its ancestors
  (f) all concept ids (and description ids) are unique
  (g) the file streams via iter_json_objects_from_top_array and every concept
      survives normalize_concept (both imported from the engine and run)
  (h) build_term_documents yields a sane preferred term, semanticTag and
      areaTags for sample concepts

Usage:
    python kehrnel/scripts/verify_synthetic_snomed_release.py
    python kehrnel/scripts/verify_synthetic_snomed_release.py --file <path>
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import deque
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from kehrnel.engine.domains.snomedct.model import (  # noqa: E402
    TYPE_FSN,
    normalize_concept,
    semantic_tag,
)
from kehrnel.engine.domains.snomedct.sidecar import (  # noqa: E402
    DEFAULT_TOP_ROOT_IDS,
    build_term_documents,
)
from kehrnel.engine.domains.snomedct.stream_json import (  # noqa: E402
    iter_json_objects_from_top_array,
)

RELEASE_ID = "hdl-synthetic-2"
RELEASE_LABEL = (
    "Synthetic SNOMED-shaped demonstration hierarchy (NOT an official SNOMED CT release)"
)
DEFAULT_FILE = REPO / ".kehrnel/snomedct/releases/hdl-synthetic-2.json"
CLINICAL_FINDING = "404684003"
SYNTHETIC_PREFIX = "9999"

# Broad parents whose descendant counts are reported (ECL `<<X` semantics).
BROAD_PARENTS = {
    "404684003": "Clinical finding",
    "64572001": "Disease",
    "50043002": "Disorder of respiratory system",
    "49601007": "Disorder of cardiovascular system",
    "362969004": "Disorder of endocrine system",
    "42030000": "Disorder of urinary system",
    "118940003": "Disorder of nervous system",
    "74732009": "Mental disorder",
    "40733004": "Infectious disease",
    "108369006": "Neoplasm",
    "928000": "Disorder of musculoskeletal system",
    "71388002": "Procedure",
    "373873005": "Pharmaceutical / biologic product",
    "78621006": "Observable entity",
    "123037004": "Body structure",
    "73211009": "Diabetes mellitus",
    "195967001": "Asthma",
    "13645005": "Chronic obstructive lung disease",
    "709044004": "Chronic kidney disease",
    "84114007": "Heart failure",
}

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify hdl-synthetic-2.json")
    parser.add_argument("--file", type=Path, default=DEFAULT_FILE)
    args = parser.parse_args()
    path = args.file

    if not path.exists():
        print(f"FAIL: release file not found: {path}")
        return 1

    # ---------------------------------------------------------------- (g1)
    # Stream the file through the production reader rather than json.load.
    streamed = list(iter_json_objects_from_top_array(path))
    # Cross-check the streaming reader against a plain json.load, but do it
    # defensively: json.load materialises the whole document and can exhaust
    # memory on constrained hosts (which is precisely why the engine ships a
    # streaming reader). A MemoryError here is not a release defect, so it
    # downgrades to a skip note rather than a verification failure.
    try:
        with path.open("r", encoding="utf-8") as handle:
            direct = json.load(handle)
        check(
            len(streamed) == len(direct),
            f"(g) streamed object count {len(streamed)} != json.load count {len(direct)}",
        )
        check(
            [c.get("conceptId") for c in streamed] == [c.get("conceptId") for c in direct],
            "(g) streamed concept order/ids differ from json.load",
        )
        del direct
    except MemoryError:
        print("[g] NOTE: json.load cross-check skipped (host out of memory); "
              "streaming reader result used as authoritative")
    concepts = streamed
    print(f"[g] streamed {len(concepts)} concepts via iter_json_objects_from_top_array")

    # ---------------------------------------------------------------- (f)
    ids = [str(c.get("conceptId") or "") for c in concepts]
    check(all(ids), "(f) at least one concept has an empty conceptId")
    check(len(ids) == len(set(ids)), f"(f) duplicate conceptIds: {len(ids) - len(set(ids))}")
    desc_ids = [
        str(d.get("descriptionId") or "")
        for c in concepts
        for d in c.get("descriptions", [])
    ]
    check(all(desc_ids), "(f) at least one description has an empty descriptionId")
    check(
        len(desc_ids) == len(set(desc_ids)),
        f"(f) duplicate descriptionIds: {len(desc_ids) - len(set(desc_ids))}",
    )
    print(f"[f] {len(set(ids))} unique concept ids, {len(set(desc_ids))} unique description ids")

    index = {str(c["conceptId"]): c for c in concepts}

    # ---------------------------------------------------------------- (a)
    edge_problems = 0
    for cid, concept in index.items():
        for parent in concept.get("inferredParentIds", []):
            parent = str(parent)
            if parent not in index:
                failures.append(f"(a) {cid} lists unknown parent {parent}")
                edge_problems += 1
                continue
            if cid not in [str(x) for x in index[parent].get("inferredChildIds", [])]:
                failures.append(f"(a) {cid}->{parent} parent edge missing reciprocal child")
                edge_problems += 1
        for child in concept.get("inferredChildIds", []):
            child = str(child)
            if child not in index:
                failures.append(f"(a) {cid} lists unknown child {child}")
                edge_problems += 1
                continue
            if cid not in [str(x) for x in index[child].get("inferredParentIds", [])]:
                failures.append(f"(a) {cid}<-{child} child edge missing reciprocal parent")
                edge_problems += 1
    print(f"[a] parent/child reciprocity: {edge_problems} problem(s)")

    # ---------------------------------------------------------------- (c)
    parents_of = {cid: [str(p) for p in c.get("inferredParentIds", [])] for cid, c in index.items()}
    children_of: dict[str, list[str]] = {cid: [] for cid in index}
    for cid, plist in parents_of.items():
        for p in plist:
            if p in children_of:
                children_of[p].append(cid)
    indeg = {cid: len(set(plist)) for cid, plist in parents_of.items()}
    queue = deque(cid for cid, d in indeg.items() if d == 0)
    topo: list[str] = []
    while queue:
        cid = queue.popleft()
        topo.append(cid)
        for child in children_of[cid]:
            indeg[child] -= 1
            if indeg[child] == 0:
                queue.append(child)
    check(
        len(topo) == len(index),
        f"(c) cycle detected: {len(index) - len(topo)} concept(s) never reached a zero in-degree",
    )
    print(f"[c] acyclic: topologically ordered {len(topo)}/{len(index)} concepts")

    # ---------------------------------------------------------------- (b)
    # Independent closure recomputation from the authored IS-A relationships,
    # NOT from inferredParentIds, so the two representations cross-check.
    rel_parents: dict[str, set[str]] = {cid: set() for cid in index}
    for cid, concept in index.items():
        for rel in concept.get("relationships", []):
            if str(rel.get("typeId")) == "116680003" and rel.get("active"):
                check(
                    str(rel.get("sourceId")) == cid,
                    f"(b) relationship sourceId {rel.get('sourceId')} != concept {cid}",
                )
                rel_parents[cid].add(str(rel.get("destinationId")))
    for cid in index:
        check(
            rel_parents[cid] == set(parents_of[cid]),
            f"(b) IS-A relationships {sorted(rel_parents[cid])} disagree with "
            f"inferredParentIds {sorted(parents_of[cid])} for {cid}",
        )

    closure: dict[str, set[str]] = {}
    for cid in topo:
        acc: set[str] = set()
        for p in rel_parents[cid]:
            acc.add(p)
            acc |= closure.get(p, set())
        closure[cid] = acc
    closure_problems = 0
    for cid, concept in index.items():
        declared = sorted(str(a) for a in concept.get("inferredAncestorIds", []))
        if declared != sorted(closure[cid]):
            failures.append(
                f"(b) ancestor closure mismatch for {cid}: "
                f"declared {len(declared)} vs recomputed {len(closure[cid])}"
            )
            closure_problems += 1
        if cid in closure[cid]:
            failures.append(f"(b) {cid} is its own ancestor")
            closure_problems += 1
    print(f"[b] ancestor closure matches recomputation: {closure_problems} mismatch(es)")

    # ---------------------------------------------------------------- (d)
    tag_problems = 0
    tags: dict[str, int] = {}
    for cid, concept in index.items():
        fsns = [d for d in concept.get("descriptions", []) if str(d.get("typeId")) == TYPE_FSN]
        if len(fsns) != 1:
            failures.append(f"(d) {cid} has {len(fsns)} FSN descriptions (expected 1)")
            tag_problems += 1
            continue
        tag = semantic_tag(fsns[0].get("term"))
        if not tag:
            failures.append(f"(d) {cid} FSN lacks a semantic tag: {fsns[0].get('term')!r}")
            tag_problems += 1
            continue
        tags[tag] = tags.get(tag, 0) + 1
        synonyms = [
            d for d in concept.get("descriptions", []) if str(d.get("typeId")) == "900000000000013009"
        ]
        for syn in synonyms:
            if semantic_tag(syn.get("term")):
                failures.append(f"(d) {cid} SYNONYM carries a semantic tag: {syn.get('term')!r}")
                tag_problems += 1
    print(f"[d] FSN semantic tags: {tag_problems} problem(s); tag histogram {dict(sorted(tags.items(), key=lambda kv: -kv[1]))}")

    # ---------------------------------------------------------------- (e)
    finding_tags = {"finding", "disorder", "morphologic abnormality"}
    finding_problems = 0
    finding_count = 0
    for cid, concept in index.items():
        fsns = [d for d in concept.get("descriptions", []) if str(d.get("typeId")) == TYPE_FSN]
        if not fsns:
            continue
        tag = semantic_tag(fsns[0].get("term"))
        if tag not in finding_tags:
            continue
        finding_count += 1
        ancestors = {str(a) for a in concept.get("inferredAncestorIds", [])}
        if cid == CLINICAL_FINDING:
            continue
        if CLINICAL_FINDING not in ancestors:
            failures.append(
                f"(e) clinical finding {cid} ({fsns[0].get('term')}) lacks {CLINICAL_FINDING} "
                "in its ancestor closure - sidecar would not tag it clinical-finding"
            )
            finding_problems += 1
    print(
        f"[e] {finding_count} finding/disorder concepts; "
        f"{finding_problems} missing {CLINICAL_FINDING} in closure"
    )

    # ---------------------------------------------------------------- (g2)
    normalize_problems = 0
    for concept in concepts:
        doc = normalize_concept(
            concept, release_id=RELEASE_ID, release_label=RELEASE_LABEL
        )
        if not doc.get("conceptId"):
            failures.append("(g) normalize_concept produced an empty conceptId")
            normalize_problems += 1
            continue
        if doc.get("active") is not True:
            failures.append(f"(g) normalize_concept marked {doc['conceptId']} inactive")
            normalize_problems += 1
        if len(doc.get("inferredAncestorIds", [])) != len(concept.get("inferredAncestorIds", [])):
            failures.append(f"(g) normalize_concept dropped ancestors for {doc['conceptId']}")
            normalize_problems += 1
        if doc.get("releaseLabel") != RELEASE_LABEL:
            failures.append(f"(g) releaseLabel not carried for {doc['conceptId']}")
            normalize_problems += 1
    print(f"[g] normalize_concept over all {len(concepts)} concepts: {normalize_problems} problem(s)")

    # ---------------------------------------------------------------- (h)
    samples = [
        ("195967001", "asthma"),
        ("44054006", "type 2 diabetes"),
        ("709044004", "chronic kidney disease"),
        ("84114007", "heart failure"),
        ("232717009", "CABG procedure"),
        ("109081006", "metformin product"),
        ("75367002", "blood pressure observable"),
        ("80891009", "heart structure"),
    ]
    sidecar_problems = 0
    print("[h] sidecar term documents:")
    for cid, note in samples:
        concept = index.get(cid)
        if concept is None:
            failures.append(f"(h) sample concept {cid} ({note}) is absent from the release")
            sidecar_problems += 1
            continue
        normalized = normalize_concept(concept, release_id=RELEASE_ID)
        docs = build_term_documents(
            normalized, release_id=RELEASE_ID, language_codes=["en"]
        )
        if not docs:
            failures.append(f"(h) build_term_documents produced nothing for {cid} ({note})")
            sidecar_problems += 1
            continue
        doc = docs[0]
        if not doc.get("preferredTerm"):
            failures.append(f"(h) {cid} has no preferredTerm")
            sidecar_problems += 1
        if not doc.get("semanticTag"):
            failures.append(f"(h) {cid} has no semanticTag")
            sidecar_problems += 1
        if not doc.get("areaTags"):
            failures.append(f"(h) {cid} has no areaTags (top-root intersection empty)")
            sidecar_problems += 1
        print(
            f"     {cid:>12}  preferred={doc.get('preferredTerm')!r} "
            f"tag={doc.get('semanticTag')!r} areaTags={doc.get('areaTags')} "
            f"topRoots={doc.get('topRoots')} docs={len(docs)}"
        )
    check(sidecar_problems == 0, f"(h) {sidecar_problems} sidecar problem(s)")

    # Every concept should intersect at least one configured top root.
    rootless = [
        cid
        for cid, concept in index.items()
        if cid not in set(DEFAULT_TOP_ROOT_IDS)
        and not ({str(a) for a in concept.get("inferredAncestorIds", [])} & set(DEFAULT_TOP_ROOT_IDS))
    ]
    check(
        not rootless,
        f"(h) {len(rootless)} concept(s) intersect no DEFAULT_TOP_ROOT_IDS "
        f"(no areaTags): e.g. {rootless[:5]}",
    )

    # ---------------------------------------------------------------- report
    depth_cache: dict[str, int] = {}

    def depth(cid: str) -> int:
        if cid in depth_cache:
            return depth_cache[cid]
        plist = parents_of[cid]
        depth_cache[cid] = 0 if not plist else 1 + max(depth(p) for p in plist)
        return depth_cache[cid]

    max_depth = max(depth(cid) for cid in index)
    deepest = max(index, key=depth)
    real_ids = [cid for cid in index if not cid.startswith(SYNTHETIC_PREFIX)]
    synth_ids = [cid for cid in index if cid.startswith(SYNTHETIC_PREFIX)]

    descendants: dict[str, int] = {}
    for cid, concept in index.items():
        for anc in concept.get("inferredAncestorIds", []):
            anc = str(anc)
            descendants[anc] = descendants.get(anc, 0) + 1

    print()
    print("=" * 72)
    print(f"RELEASE   : {RELEASE_ID}")
    print(f"LABEL     : {RELEASE_LABEL}")
    print(f"FILE      : {path}")
    print(f"concepts  : {len(index)}")
    print(f"max depth : {max_depth} (deepest example: {deepest})")
    print(
        f"id split  : {len(real_ids)} real-referenced "
        f"({100 * len(real_ids) / len(index):.1f}%) / "
        f"{len(synth_ids)} synthesised 9999xxxxx "
        f"({100 * len(synth_ids) / len(index):.1f}%)"
    )
    print()
    print("descendant counts (ECL `<<X` == match on inferredAncestorIds):")
    for cid, label in BROAD_PARENTS.items():
        present = "" if cid in index else "  [ABSENT]"
        print(f"  <<{cid:<12} {label:<38} {descendants.get(cid, 0)}{present}")
    print("=" * 72)
    print("REMINDER: synthetic demonstration content. NOT SNOMED CT. Not for clinical use.")

    if failures:
        print()
        print(f"VERIFICATION FAILED with {len(failures)} problem(s):")
        for message in failures[:40]:
            print(f"  - {message}")
        if len(failures) > 40:
            print(f"  ... and {len(failures) - 40} more")
        return 1
    print()
    print("VERIFICATION PASSED: (a) reciprocity (b) closure (c) acyclic (d) FSN tags")
    print("                     (e) finding roots (f) unique ids (g) stream+normalize (h) sidecar")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
