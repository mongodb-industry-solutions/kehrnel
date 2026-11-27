#!/usr/bin/env python3
"""Inventory exact OPTs and generate reviewable analytics defaults without a database.

Input template names come from source discovery, never from the configured rules.
ZIP members are read in memory; composition exports and patient values are not read.
"""

import argparse
from collections import defaultdict
from hashlib import sha256
import json
import os
from pathlib import Path
import zipfile

from lxml import etree
from kehrnel.engine.domains.openehr.analytics.default_mapping import (
    finalize_proposal,
    propose_from_opt,
)
from kehrnel.engine.domains.openehr.analytics.rules import RuleSet

MAX_OPT_BYTES = 20_000_000
NS = "{http://schemas.openehr.org/v1}"


def inspect_opt(xml):
    """Ignore only authoring description/UID when comparing exported definitions.

    Keep the complete definition, language, ontology and annotations. Equal
    fingerprints are not a claim of equivalence across differently named templates.
    """
    if len(xml) > MAX_OPT_BYTES:
        raise ValueError("OPT exceeds the 20 MB inspection bound")
    tree = etree.fromstring(
        xml,
        etree.XMLParser(
            resolve_entities=False, no_network=True, remove_blank_text=True
        ),
    )
    if tree.getroottree().docinfo.doctype:
        raise ValueError("DTD declarations are not accepted")
    name = tree.findtext(NS + "template_id/" + NS + "value")
    if not name:
        raise ValueError("No internal openEHR OPT template_id")
    for child in list(tree):
        if isinstance(child.tag, str) and etree.QName(child).localname in (
            "description",
            "uid",
        ):
            tree.remove(child)
    fingerprint = sha256(
        etree.tostring(tree, method="c14n", with_comments=False)
    ).hexdigest()
    return name, sha256(xml).hexdigest(), fingerprint


def opt_inputs(root):
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        suffix = path.suffix.lower()
        if suffix == ".opt" or (suffix == ".txt" and path.parent.name == "Templates"):
            if path.stat().st_size > MAX_OPT_BYTES:
                yield str(
                    path.relative_to(root)
                ), None, "OPT exceeds the 20 MB inspection bound"
            else:
                yield str(path.relative_to(root)), path.read_bytes(), None
        elif suffix == ".zip":
            try:
                with zipfile.ZipFile(path) as archive:
                    for member in archive.infolist():
                        if (
                            not member.filename.lower().endswith(".opt")
                            or "__MACOSX" in member.filename
                        ):
                            continue
                        origin = str(path.relative_to(root)) + "!" + member.filename
                        if member.file_size > MAX_OPT_BYTES:
                            yield origin, None, "OPT exceeds the 20 MB inspection bound"
                        else:
                            yield origin, archive.read(member), None
            except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
                yield str(path.relative_to(root)), None, str(exc)


def build_catalog(names, inputs, output, aliases=None, *, epoch=1, effective_at):
    """Write drafts for every variant, but compile only unambiguous complete rules."""
    aliases = dict(aliases or {})
    by_name = defaultdict(lambda: defaultdict(list))
    errors = []
    for origin, xml, error in inputs:
        try:
            if error:
                raise ValueError(error)
            name, digest, fingerprint = inspect_opt(xml)
            by_name[name][fingerprint].append((origin, digest, xml))
        except (ValueError, etree.XMLSyntaxError) as exc:
            errors.append({"origin": origin, "error": str(exc)})
    output.mkdir(parents=True, exist_ok=False)
    proposals = output / "proposals"
    proposals.mkdir()
    rows, rules, index = [], [], []
    for name in sorted(set(names)):
        variants = by_name.get(name, {})
        row = {
            "template": name,
            "mapping_status": "opt_not_found_in_supplied_catalog",
            "variants": [],
        }
        if variants:
            row["mapping_status"] = (
                "ambiguous_opt_definitions"
                if len(variants) > 1
                else "default_draft_generated"
            )
        for fingerprint, origins in sorted(variants.items()):
            origin, digest, xml = min(origins, key=lambda item: (item[1], item[0]))
            variant = {
                "opt_digest": digest,
                "operational_digest": fingerprint,
                "origins": [{"path": p, "digest": d} for p, d, _ in origins],
                "compiles": False,
            }
            try:
                proposal = propose_from_opt(
                    xml, epoch=epoch, effective_at=effective_at, aliases=aliases
                )
                filename = digest[:16] + ".json"
                (proposals / filename).write_text(
                    json.dumps(proposal, indent=2, ensure_ascii=False) + "\n"
                )
                variant.update(
                    proposal_file=filename,
                    clinical_fields=sum(f["selected"] for f in proposal["fields"]),
                    block_definitions=len(
                        proposal["candidate_rule"]["catalog"]["templates"][0]["groups"]
                    ),
                    issues=proposal["issues"],
                )
                rule = finalize_proposal(proposal)
                variant["compiles"] = True
                if len(variants) == 1:
                    rules.append(rule)
                    row["proposal_file"] = filename
                    index.append(
                        {
                            "template": name,
                            "file": filename,
                            "clinical_fields": variant["clinical_fields"],
                            "block_definitions": variant["block_definitions"],
                            "unresolved_fields": 0,
                            "status": "draft",
                        }
                    )
            except ValueError as exc:
                variant["error"] = str(exc)
                if len(variants) == 1:
                    row["mapping_status"] = "configuration_review_required"
            row["variants"].append(variant)
        rows.append(row)
    RuleSet.compile(rules)
    manifest = {
        "templates": rows,
        "input_errors": errors,
        "ignored_authoring_sections": ["description", "uid"],
        "unmatched_catalog_names": sorted(set(by_name) - set(names)),
    }
    for path, value in [
        (output / "template-mapping.json", manifest),
        (output / "rules.json", rules),
        (output / "aliases.json", aliases),
        (proposals / "index.json", index),
    ]:
        path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--effective-at", required=True, help="Explicit UTC draft activation date"
    )
    parser.add_argument("--epoch", type=int, default=1)
    parser.add_argument(
        "--source-inventory",
        required=True,
        type=Path,
        help='JSON with templates: [{"template": "exact source name"}]',
    )
    parser.add_argument("--opt-dir", required=True, action="append", type=Path)
    parser.add_argument("--seed-proposals", action="append", default=[], type=Path)
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="New directory outside the checkout; never overwrites",
    )
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(Path(__file__).resolve().parents[1]):
        parser.error(
            "Generate customer configuration outside this checkout, then review before importing"
        )
    if any(not root.is_dir() for root in args.opt_dir):
        parser.error("Every --opt-dir must exist")
    os.umask(0o077)
    aliases = {}
    for root in args.seed_proposals:
        for path in sorted(root.glob("*.json")):
            proposal = json.loads(path.read_text())
            if (
                not isinstance(proposal, dict)
                or proposal.get("contract") != "openehr-default-proposal/v1"
            ):
                continue
            for field in proposal["fields"]:
                if field.get("business_key"):
                    identity, key = field["semantic_key"], field["business_key"]
                    if identity in aliases and aliases[identity] != key:
                        raise ValueError(
                            "Conflicting seed aliases for one semantic identity"
                        )
                    if key in aliases.values() and aliases.get(identity) != key:
                        raise ValueError(
                            "Seed business alias names different semantic identities"
                        )
                    aliases[identity] = key
    names = [
        r["template"]
        for r in json.loads(args.source_inventory.read_text())["templates"]
    ]
    inputs = (
        (root.name + "/" + p, xml, error)
        for root in args.opt_dir
        for p, xml, error in opt_inputs(root)
    )
    report = build_catalog(
        names,
        inputs,
        args.output,
        aliases,
        epoch=args.epoch,
        effective_at=args.effective_at,
    )
    from collections import Counter

    print(
        json.dumps(
            {
                "templates": len(report["templates"]),
                "states": dict(
                    Counter(r["mapping_status"] for r in report["templates"])
                ),
                "input_errors": len(report["input_errors"]),
            }
        )
    )


if __name__ == "__main__":
    main()
