from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)


DEFAULT_SOURCE_COLLECTION = "samples"
DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "generated"
DEFAULT_SALT = "kehrnel-rps-dual-sample-pack"
SYSTEM_ID_PLACEHOLDER = "sample.source"
UID_SYSTEM_PLACEHOLDER = "sample.kehrnel"
DEFAULT_TEXT_REPLACEMENTS: List[Tuple[str, str]] = [
    ("CatSalut", "Sample Health Authority"),
    ("catsalut", "sample-health-authority"),
    ("Catalunya", "Sample Region"),
    ("Admin Salut", "Administrative context"),
]
ACTIVE_MAPPINGS_PATH = (
    Path(__file__).resolve().parents[1] / "ingest" / "config" / "flattener_mappings_f.jsonc"
)
BUNDLED_SHORTCUTS_PATH = (
    Path(__file__).resolve().parents[1] / "bundles" / "shortcuts" / "shortcuts.json"
)


@dataclass(frozen=True)
class SampleSpec:
    source_template: str
    target_template: str
    opt_path: Path | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a neutralized RPS Dual sample pack from a private MongoDB source."
    )
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection string.")
    parser.add_argument("--source-db", required=True, help="Source database name.")
    parser.add_argument(
        "--source-collection",
        default=DEFAULT_SOURCE_COLLECTION,
        help=f"Source collection name. Default: {DEFAULT_SOURCE_COLLECTION}",
    )
    parser.add_argument(
        "--sample",
        action="append",
        required=True,
        metavar="SOURCE|TARGET|OPT_PATH",
        help=(
            "Sample mapping. SOURCE is the private template name, TARGET is the neutral "
            "template name, and OPT_PATH is optional."
        ),
    )
    parser.add_argument(
        "--replace-token",
        action="append",
        default=[],
        metavar="OLD=NEW",
        help="Token replacement applied to canonical JSON and OPT text.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of compositions to export per template.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help=f"Output directory. Default: {DEFAULT_OUT_DIR}",
    )
    parser.add_argument(
        "--mask-salt",
        default=DEFAULT_SALT,
        help="Salt used for deterministic masking.",
    )
    return parser.parse_args()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip()).strip("_").lower()
    return slug or "sample"


def _parse_sample_specs(raw_specs: Iterable[str]) -> List[SampleSpec]:
    specs: List[SampleSpec] = []
    for raw in raw_specs:
        parts = [part.strip() for part in raw.split("|")]
        if len(parts) < 2 or len(parts) > 3 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Invalid --sample value: {raw!r}. Expected SOURCE|TARGET|OPT_PATH"
            )
        opt_path = Path(parts[2]).expanduser().resolve() if len(parts) == 3 and parts[2] else None
        specs.append(SampleSpec(parts[0], parts[1], opt_path))
    return specs


def _parse_replace_map(raw_pairs: Iterable[str]) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for raw in raw_pairs:
        if "=" not in raw:
            raise ValueError(f"Invalid --replace-token value: {raw!r}. Expected OLD=NEW")
        old, new = raw.split("=", 1)
        pairs.append((old, new))
    return pairs


def _apply_replacements(value: str, replacements: List[Tuple[str, str]]) -> str:
    result = value
    for old, new in replacements:
        result = result.replace(old, new)
    return result


def _deterministic_uuid(source_value: str, salt: str, namespace: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{salt}:{namespace}:{source_value}"))


def _mask_identifier(source_value: str, salt: str, namespace: str, prefix: str = "id") -> str:
    digest = hashlib.sha256(f"{salt}:{namespace}:{source_value}".encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def _mask_uid(source_uid: str, salt: str) -> str:
    version = "1"
    if "::" in source_uid:
        parts = source_uid.split("::")
        if len(parts) >= 3 and parts[2]:
            version = parts[2]
    masked_uuid = _deterministic_uuid(source_uid, salt, "composition_uid")
    return f"{masked_uuid}::{UID_SYSTEM_PLACEHOLDER}::{version}"


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _rewrite_scalar_strings(node: Any, replacements: List[Tuple[str, str]]) -> Any:
    if isinstance(node, str):
        return _apply_replacements(node, replacements)
    if isinstance(node, list):
        return [_rewrite_scalar_strings(item, replacements) for item in node]
    if isinstance(node, dict):
        return {key: _rewrite_scalar_strings(value, replacements) for key, value in node.items()}
    return node


def _sanitize_canonical(
    composition: Dict[str, Any],
    target_template: str,
    replacements: List[Tuple[str, str]],
    salt: str,
) -> Dict[str, Any]:
    sanitized = copy.deepcopy(composition)
    sanitized = _rewrite_scalar_strings(sanitized, replacements)

    def visit(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return

        rm_type = node.get("_type")
        if rm_type == "DV_IDENTIFIER" and isinstance(node.get("id"), str):
            node["id"] = _mask_identifier(node["id"], salt, "dv_identifier")
        if rm_type == "OBJECT_VERSION_ID" and isinstance(node.get("value"), str):
            node["value"] = _mask_uid(node["value"], salt)
        if rm_type == "PARTY_IDENTIFIED" and isinstance(node.get("name"), str):
            node["name"] = _mask_identifier(node["name"], salt, "party_name", prefix="party")
        if rm_type == "FEEDER_AUDIT_DETAILS" and "system_id" in node:
            node["system_id"] = SYSTEM_ID_PLACEHOLDER
        template_id = node.get("template_id")
        if isinstance(template_id, dict) and isinstance(template_id.get("value"), str):
            template_id["value"] = target_template

        for value in node.values():
            visit(value)

    visit(sanitized)

    ad = sanitized.setdefault("archetype_details", {})
    ti = ad.setdefault("template_id", {})
    if isinstance(ti, dict):
        ti["value"] = target_template

    uid = sanitized.get("uid")
    if isinstance(uid, dict) and isinstance(uid.get("value"), str):
        uid["value"] = _mask_uid(uid["value"], salt)

    return sanitized


def _build_envelope(
    source_doc: Dict[str, Any],
    canonical: Dict[str, Any],
    target_template: str,
    salt: str,
) -> Dict[str, Any]:
    source_ehr = str(source_doc.get("ehr_id") or canonical.get("ehr_id") or "ehr-unknown")
    masked_ehr = _deterministic_uuid(source_ehr, salt, "ehr_id")

    uid_value = None
    uid = canonical.get("uid")
    if isinstance(uid, dict):
        uid_value = uid.get("value")
    comp_root = uid_value.split("::", 1)[0] if isinstance(uid_value, str) and "::" in uid_value else uid_value
    comp_root = comp_root or _deterministic_uuid(json.dumps(canonical, sort_keys=True, default=_json_default), salt, "composition")
    composition_version = source_doc.get("composition_version") or source_doc.get("version")
    if not composition_version and isinstance(uid_value, str) and "::" in uid_value:
        parts = uid_value.split("::")
        if len(parts) >= 3 and parts[2]:
            composition_version = parts[2]
    composition_version = str(composition_version or "1")

    composition_date = source_doc.get("composition_date")
    if composition_date is None:
        composition_date = (
            canonical.get("context", {})
            .get("start_time", {})
            .get("value")
        )
    time_committed = (
        source_doc.get("time_committed")
        or source_doc.get("time_created")
        or composition_date
    )

    return {
        "_id": comp_root,
        "ehr_id": masked_ehr,
        "composition_version": composition_version,
        "time_committed": time_committed,
        "template_name": target_template,
        "template_id": target_template,
        "archetype_node_id": canonical.get("archetype_node_id"),
        "composition_date": composition_date,
        "canonicalJSON": canonical,
    }


def _rewrite_opt_text(
    opt_text: str,
    source_template: str,
    target_template: str,
    replacements: List[Tuple[str, str]],
) -> str:
    rewritten = _apply_replacements(opt_text, replacements)
    rewritten = rewritten.replace(source_template, target_template)
    return rewritten


def _ensure_dirs(out_dir: Path) -> Dict[str, Path]:
    paths = {
        "root": out_dir,
        "canonical": out_dir / "canonical",
        "envelopes": out_dir / "envelopes",
        "templates": out_dir / "templates",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _write_ndjson(path: Path, docs: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for doc in docs:
            handle.write(json.dumps(doc, ensure_ascii=False, default=_json_default))
            handle.write("\n")


def _load_json_or_jsonc(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"//.*?$|/\*.*?\*/", "", text, flags=re.M | re.S)
    return json.loads(text)


def _build_projection_mappings(specs: Iterable[SampleSpec]) -> Dict[str, Any]:
    payload = _load_json_or_jsonc(ACTIVE_MAPPINGS_PATH)
    templates = payload.get("templates") if isinstance(payload, dict) else None
    if not isinstance(templates, list):
        return {"templates": []}

    target_templates = {spec.target_template for spec in specs}
    filtered: List[Dict[str, Any]] = []
    for entry in templates:
        if not isinstance(entry, dict):
            continue
        template_id = str(entry.get("templateId") or "").strip()
        if template_id in target_templates:
            filtered.append(copy.deepcopy(entry))
    return {"templates": filtered}


async def _build_search_index_definition(mappings: Dict[str, Any]) -> Dict[str, Any]:
    shortcuts = _load_json_or_jsonc(BUNDLED_SHORTCUTS_PATH)
    strategy_cfg = normalize_config({})
    return await build_search_index_definition_from_mappings(
        strategy_cfg,
        mappings,
        shortcuts=shortcuts,
    )


def _load_source_docs(
    mongo_uri: str,
    source_db: str,
    source_collection: str,
    source_template: str,
    limit: int,
) -> List[Dict[str, Any]]:
    try:
        from pymongo import MongoClient
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("pymongo is required to export sample packs") from exc

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=20000)
    collection = client[source_db][source_collection]
    cursor = (
        collection.find({"template_name": source_template})
        .sort("composition_date", 1)
        .limit(limit)
    )
    docs = list(cursor)
    if not docs:
        raise RuntimeError(
            f"No source documents found for template {source_template!r} in "
            f"{source_db}.{source_collection}"
        )
    return docs


def main() -> int:
    args = _parse_args()
    specs = _parse_sample_specs(args.sample)
    replacements = [*DEFAULT_TEXT_REPLACEMENTS, *_parse_replace_map(args.replace_token)]
    out_dir = Path(args.out_dir).expanduser().resolve()
    dirs = _ensure_dirs(out_dir)

    manifest: Dict[str, Any] = {
        "strategy": "openehr.rps_dual",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "masking": {
            "ehr_id": "uuid5",
            "composition_uid": "uuid5 + sample.kehrnel system id",
            "dv_identifier.id": "sha256 prefix",
            "feeder_audit.system_id": SYSTEM_ID_PLACEHOLDER,
        },
        "templates": [],
    }

    for spec in specs:
        docs = _load_source_docs(
            args.mongo_uri,
            args.source_db,
            args.source_collection,
            spec.source_template,
            args.limit,
        )

        target_slug = _slugify(spec.target_template)
        canonical_dir = dirs["canonical"] / target_slug
        canonical_dir.mkdir(parents=True, exist_ok=True)
        envelope_docs: List[Dict[str, Any]] = []

        for index, doc in enumerate(docs, start=1):
            canonical = doc.get("canonicalJSON")
            if not isinstance(canonical, dict):
                raise RuntimeError(
                    f"Source document {doc.get('_id')!r} does not contain canonicalJSON as an object"
                )
            sanitized = _sanitize_canonical(
                canonical,
                spec.target_template,
                replacements,
                args.mask_salt,
            )
            envelope = _build_envelope(doc, sanitized, spec.target_template, args.mask_salt)
            envelope_docs.append(envelope)

            canonical_uid = sanitized.get("uid", {}).get("value", "")
            comp_slug = canonical_uid.split("::", 1)[0] if isinstance(canonical_uid, str) else f"{target_slug}_{index:03d}"
            _write_json(canonical_dir / f"{comp_slug}.json", sanitized)

        _write_ndjson(dirs["envelopes"] / f"{target_slug}.ndjson", envelope_docs)

        opt_output = None
        if spec.opt_path:
            opt_text = spec.opt_path.read_text(encoding="utf-8")
            rewritten_opt = _rewrite_opt_text(
                opt_text,
                spec.source_template,
                spec.target_template,
                replacements,
            )
            opt_output = dirs["templates"] / f"{target_slug}.opt"
            opt_output.write_text(rewritten_opt, encoding="utf-8")

        manifest["templates"].append(
            {
                "target_template": spec.target_template,
                "slug": target_slug,
                "composition_count": len(envelope_docs),
                "canonical_dir": str((dirs["canonical"] / target_slug).relative_to(out_dir)),
                "envelope_file": str((dirs["envelopes"] / f"{target_slug}.ndjson").relative_to(out_dir)),
                "opt_file": str(opt_output.relative_to(out_dir)) if opt_output else None,
            }
        )

    projection_mappings = _build_projection_mappings(specs)
    projection_mappings_path = dirs["root"] / "projection_mappings.json"
    _write_json(projection_mappings_path, projection_mappings)

    search_index_payload = asyncio.run(_build_search_index_definition(projection_mappings))
    search_index_path = dirs["root"] / "search_index.definition.json"
    _write_json(search_index_path, search_index_payload.get("definition") or {})

    manifest["projection_mappings_file"] = str(projection_mappings_path.relative_to(out_dir))
    manifest["search_index_definition_file"] = str(search_index_path.relative_to(out_dir))
    manifest["search_index_metadata"] = search_index_payload.get("metadata") or {}
    if search_index_payload.get("warnings"):
        manifest["search_index_warnings"] = search_index_payload["warnings"]

    _write_json(dirs["root"] / "manifest.json", manifest)
    print(f"Wrote neutralized sample pack to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
