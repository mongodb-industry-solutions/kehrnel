"""Fail-closed maintenance for bundled content-addressed catalogs.

Strategy packages are identified by a digest of their complete directory.  A
legitimate package edit therefore invalidates every exact strategy reference
that points at the previous digest.  This module updates that dependency graph
without weakening the runtime loaders or hand-editing a chain of derived
digests.

The refresher deliberately works on the JSON wire representation.  Key and
array order are retained, and files keep the repository's two-space JSON
format.  All inputs and the complete proposed graph are validated before any
file is replaced.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from pydantic import ValidationError

from kehrnel.contracts.outcome_catalog import IndustryOutcomeCatalog
from kehrnel.contracts.source_architecture import (
    SolutionBlueprintTemplate,
    SolutionBlueprintTemplateCatalog,
)
from kehrnel.engine.core.blueprint_templates import (
    DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
    blueprint_template_content_digest,
)
from kehrnel.engine.core.outcome_catalog import (
    DEFAULT_OUTCOME_CATALOG_PATH,
    DEFAULT_STRATEGY_ROOT,
    assert_outcome_catalog_links,
    outcome_catalog_artifact_digest,
)
from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy_manifest,
)


class CatalogRefreshError(ValueError):
    """The catalogs cannot be refreshed without weakening their guarantees."""


@dataclass(frozen=True)
class InstalledStrategyIdentity:
    strategy_id: str
    version: str
    digest: str
    manifest_path: str

    def artifact_ref(self) -> dict[str, str]:
        return {
            "kind": "strategy",
            "id": self.strategy_id,
            "version": self.version,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class CatalogRefreshReport:
    strategies: tuple[InstalledStrategyIdentity, ...]
    strategy_refs_updated: int
    blueprint_refs_updated: int
    outcome_refs_updated: int
    reference_architecture_refs_updated: int
    blueprint_artifact_digests_updated: int
    outcome_artifact_digests_updated: int
    blueprint_catalog_changed: bool
    outcome_catalog_changed: bool
    written: bool

    @property
    def changed(self) -> bool:
        return self.blueprint_catalog_changed or self.outcome_catalog_changed

    def to_wire(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["strategies"] = [asdict(item) for item in self.strategies]
        payload["changed"] = self.changed
        return payload


_OUTCOME_ATOMIC_SECTIONS: tuple[tuple[str, str], ...] = (
    ("domains", "portfolio-domain"),
    ("dataProducts", "data-product"),
    ("architectureCapabilities", "architecture-capability"),
    ("visualPatterns", "visual-pattern"),
    ("outcomes", "outcome-pattern"),
    ("referenceArchitectures", "reference-architecture"),
    ("demoExperiences", "demo-experience"),
)


def resolve_installed_strategy_identities(
    strategy_ids: Sequence[str],
    *,
    strategy_root: str | Path = DEFAULT_STRATEGY_ROOT,
) -> dict[str, InstalledStrategyIdentity]:
    """Resolve each requested strategy to exactly one validated local package."""

    requested = _normalized_strategy_ids(strategy_ids)
    candidates: dict[str, list[Path]] = {strategy_id: [] for strategy_id in requested}
    root = Path(strategy_root)
    try:
        manifest_paths = tuple(iter_strategy_manifest_paths(root))
    except OSError as exc:
        raise CatalogRefreshError(
            f"cannot inspect strategy root {root}: {exc}"
        ) from exc

    for manifest_path in manifest_paths:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # Unrelated preview packages remain outside the requested refresh.
            # A requested package with unreadable identity is consequently not
            # resolvable and fails as missing below.
            continue
        if not isinstance(payload, dict):
            continue
        strategy_id = payload.get("id")
        if strategy_id in candidates:
            candidates[strategy_id].append(manifest_path)

    resolved: dict[str, InstalledStrategyIdentity] = {}
    for strategy_id in requested:
        matches = candidates[strategy_id]
        if not matches:
            raise CatalogRefreshError(
                f"requested strategy is not installed: {strategy_id}"
            )
        if len(matches) != 1:
            rendered = [str(path) for path in matches]
            raise CatalogRefreshError(
                f"requested strategy has multiple installed manifests: "
                f"{strategy_id}: {rendered}"
            )
        manifest_path = matches[0]
        try:
            manifest = load_strategy_manifest(
                manifest_path,
                strategy_id=strategy_id,
            )
        except Exception as exc:
            raise CatalogRefreshError(
                f"cannot load requested strategy {strategy_id} from "
                f"{manifest_path}: {exc}"
            ) from exc
        if not manifest.package_digest:
            raise CatalogRefreshError(
                f"installed strategy has no package digest: {strategy_id}"
            )
        resolved[strategy_id] = InstalledStrategyIdentity(
            strategy_id=strategy_id,
            version=manifest.version,
            digest=manifest.package_digest,
            manifest_path=str(manifest_path),
        )
    return resolved


def refresh_bundled_catalogs(
    strategy_ids: Sequence[str],
    *,
    strategy_root: str | Path = DEFAULT_STRATEGY_ROOT,
    blueprint_catalog_path: str | Path = DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
    outcome_catalog_path: str | Path = DEFAULT_OUTCOME_CATALOG_PATH,
    write: bool = True,
) -> CatalogRefreshReport:
    """Refresh exact references and every derived digest in dependency order.

    No file is changed until both original catalogs and the fully refreshed
    proposal have passed structural, digest, and local-link validation.
    Individual file replacement is atomic; if the second replacement fails,
    the first is restored from its original bytes before the error is raised.
    """

    identities = resolve_installed_strategy_identities(
        strategy_ids,
        strategy_root=strategy_root,
    )
    blueprint_path = Path(blueprint_catalog_path)
    outcome_path = Path(outcome_catalog_path)
    original_blueprint_bytes = _read_required_file(blueprint_path)
    original_outcome_bytes = _read_required_file(outcome_path)
    blueprint_payload = _decode_json_object(
        original_blueprint_bytes,
        label="solution blueprint template catalog",
    )
    outcome_payload = _decode_json_object(
        original_outcome_bytes,
        label="industry outcome catalog",
    )

    _validate_blueprint_catalog(blueprint_payload)
    _validate_outcome_catalog(outcome_payload)

    strategy_ref_count = _count_refs(
        (blueprint_payload, outcome_payload),
        kind="strategy",
        ids=set(identities),
    )
    missing_refs = sorted(
        strategy_id
        for strategy_id in identities
        if _count_refs(
            (blueprint_payload, outcome_payload),
            kind="strategy",
            ids={strategy_id},
        )
        == 0
    )
    if missing_refs:
        raise CatalogRefreshError(
            f"requested strategies are not referenced by the bundled catalogs: "
            f"{missing_refs}"
        )

    strategy_refs_updated = _replace_refs(
        blueprint_payload,
        kind="strategy",
        replacements={
            strategy_id: identity.artifact_ref()
            for strategy_id, identity in identities.items()
        },
    )
    blueprint_digest_updates = _refresh_blueprint_artifact_digests(
        blueprint_payload
    )
    blueprint_refs = _artifact_ref_index(
        _required_list(blueprint_payload, "templates"),
        expected_kind="solution-blueprint-template",
    )

    strategy_refs_updated += _replace_refs(
        outcome_payload,
        kind="strategy",
        replacements={
            strategy_id: identity.artifact_ref()
            for strategy_id, identity in identities.items()
        },
    )
    blueprint_refs_updated = _replace_refs(
        outcome_payload,
        kind="solution-blueprint-template",
        replacements=blueprint_refs,
    )

    # Outcomes are the exact local artifacts referenced by products,
    # capabilities, architectures, and demos. Refresh them first, propagate
    # their refs, then do architectures before their dependent demos.
    outcome_digest_updates = 0
    for section in ("domains", "visualPatterns", "outcomes"):
        outcome_digest_updates += _refresh_outcome_section_digests(
            outcome_payload,
            section,
        )
    outcome_refs = _artifact_ref_index(
        _required_list(outcome_payload, "outcomes"),
        expected_kind="outcome-pattern",
    )
    outcome_refs_updated = _replace_refs(
        outcome_payload,
        kind="outcome-pattern",
        replacements=outcome_refs,
    )

    for section in ("dataProducts", "architectureCapabilities"):
        outcome_digest_updates += _refresh_outcome_section_digests(
            outcome_payload,
            section,
        )

    outcome_digest_updates += _refresh_outcome_section_digests(
        outcome_payload,
        "referenceArchitectures",
    )
    architecture_refs = _artifact_ref_index(
        _required_list(outcome_payload, "referenceArchitectures"),
        expected_kind="reference-architecture",
    )
    architecture_refs_updated = _replace_refs(
        outcome_payload,
        kind="reference-architecture",
        replacements=architecture_refs,
    )

    outcome_digest_updates += _refresh_outcome_section_digests(
        outcome_payload,
        "demoExperiences",
    )
    outcome_digest_updates += _set_artifact_digest(
        outcome_payload,
        outcome_catalog_artifact_digest(outcome_payload),
        expected_kind="industry-outcome-catalog",
    )

    _assert_target_refs_are_exact(
        (blueprint_payload, outcome_payload), identities
    )
    refreshed_templates = _validate_blueprint_catalog(blueprint_payload)
    refreshed_outcome = _validate_outcome_catalog(outcome_payload)
    try:
        assert_outcome_catalog_links(
            refreshed_outcome,
            strategy_root=strategy_root,
            blueprint_templates=refreshed_templates,
        )
    except Exception as exc:
        raise CatalogRefreshError(
            f"refreshed catalog contains unresolved dependencies: {exc}"
        ) from exc

    rendered_blueprints = _encode_json(blueprint_payload)
    rendered_outcomes = _encode_json(outcome_payload)
    blueprint_changed = rendered_blueprints != original_blueprint_bytes
    outcome_changed = rendered_outcomes != original_outcome_bytes

    if write and (blueprint_changed or outcome_changed):
        final_identities = resolve_installed_strategy_identities(
            tuple(identities),
            strategy_root=strategy_root,
        )
        if final_identities != identities:
            raise CatalogRefreshError(
                "strategy package identity changed while catalogs were being refreshed"
            )
        replacements: list[tuple[Path, bytes, bytes]] = []
        if blueprint_changed:
            replacements.append(
                (blueprint_path, rendered_blueprints, original_blueprint_bytes)
            )
        if outcome_changed:
            replacements.append(
                (outcome_path, rendered_outcomes, original_outcome_bytes)
            )
        _atomic_replace_many(replacements)

    if strategy_ref_count <= 0:  # defensive; missing refs are handled above
        raise CatalogRefreshError("no strategy references were eligible for refresh")
    return CatalogRefreshReport(
        strategies=tuple(identities[strategy_id] for strategy_id in sorted(identities)),
        strategy_refs_updated=strategy_refs_updated,
        blueprint_refs_updated=blueprint_refs_updated,
        outcome_refs_updated=outcome_refs_updated,
        reference_architecture_refs_updated=architecture_refs_updated,
        blueprint_artifact_digests_updated=blueprint_digest_updates,
        outcome_artifact_digests_updated=outcome_digest_updates,
        blueprint_catalog_changed=blueprint_changed,
        outcome_catalog_changed=outcome_changed,
        written=bool(write and (blueprint_changed or outcome_changed)),
    )


def _normalized_strategy_ids(strategy_ids: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(
        sorted({value.strip() for value in strategy_ids if value.strip()})
    )
    if not normalized:
        raise CatalogRefreshError("at least one strategy id is required")
    return normalized


def _read_required_file(path: Path) -> bytes:
    if path.is_symlink():
        raise CatalogRefreshError(f"catalog path must not be a symlink: {path}")
    if not path.is_file():
        raise CatalogRefreshError(f"catalog path is not a regular file: {path}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise CatalogRefreshError(f"cannot read catalog {path}: {exc}") from exc


def _decode_json_object(payload: bytes, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatalogRefreshError(f"invalid {label} JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalogRefreshError(f"{label} must contain a JSON object")
    return value


def _encode_json(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _validate_blueprint_catalog(
    payload: Mapping[str, Any],
) -> tuple[SolutionBlueprintTemplate, ...]:
    try:
        catalog = SolutionBlueprintTemplateCatalog.model_validate(payload)
    except ValidationError as exc:
        raise CatalogRefreshError(
            f"invalid solution blueprint template catalog: {exc}"
        ) from exc
    templates = tuple(catalog.templates)
    for template in templates:
        expected = blueprint_template_content_digest(template)
        if template.artifact_ref.digest != expected:
            raise CatalogRefreshError(
                "solution blueprint template digest mismatch for "
                f"{template.artifact_ref.id}@{template.artifact_ref.version}"
            )
    return templates


def _validate_outcome_catalog(payload: Mapping[str, Any]) -> IndustryOutcomeCatalog:
    try:
        catalog = IndustryOutcomeCatalog.model_validate(payload)
    except ValidationError as exc:
        raise CatalogRefreshError(f"invalid industry outcome catalog: {exc}") from exc
    for section_name, _expected_kind in _OUTCOME_ATOMIC_SECTIONS:
        for artifact in getattr(catalog, _snake_case(section_name)):
            expected = outcome_catalog_artifact_digest(artifact)
            if artifact.artifact_ref.digest != expected:
                raise CatalogRefreshError(
                    "outcome catalog digest mismatch for "
                    f"{artifact.artifact_ref.kind}:{artifact.artifact_ref.id}"
                    f"@{artifact.artifact_ref.version}"
                )
    expected = outcome_catalog_artifact_digest(catalog)
    if catalog.artifact_ref.digest != expected:
        raise CatalogRefreshError(
            "outcome catalog digest mismatch for "
            f"{catalog.artifact_ref.id}@{catalog.artifact_ref.version}"
        )
    return catalog


def _snake_case(value: str) -> str:
    result: list[str] = []
    for char in value:
        if char.isupper():
            result.extend(("_", char.lower()))
        else:
            result.append(char)
    return "".join(result)


def _required_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise CatalogRefreshError(f"catalog field '{key}' must be an array")
    return value


def _artifact_ref(artifact: Mapping[str, Any]) -> MutableMapping[str, Any]:
    reference = artifact.get("artifactRef")
    if not isinstance(reference, MutableMapping):
        raise CatalogRefreshError("catalog artifact is missing artifactRef")
    return reference


def _artifact_ref_index(
    artifacts: Iterable[Any],
    *,
    expected_kind: str,
) -> dict[str, dict[str, str]]:
    index: dict[str, dict[str, str]] = {}
    for value in artifacts:
        if not isinstance(value, Mapping):
            raise CatalogRefreshError(
                f"{expected_kind} catalog entry must be an object"
            )
        reference = _artifact_ref(value)
        if reference.get("kind") != expected_kind:
            raise CatalogRefreshError(
                f"expected {expected_kind} artifact, found {reference.get('kind')}"
            )
        artifact_id = reference.get("id")
        version = reference.get("version")
        digest = reference.get("digest")
        if not all(
            isinstance(item, str) and item
            for item in (artifact_id, version, digest)
        ):
            raise CatalogRefreshError(
                f"{expected_kind} artifactRef must contain id, version, and digest"
            )
        if artifact_id in index:
            raise CatalogRefreshError(
                f"duplicate {expected_kind} artifact id: {artifact_id}"
            )
        index[artifact_id] = {
            "kind": expected_kind,
            "id": artifact_id,
            "version": version,
            "digest": digest,
        }
    return index


def _refresh_blueprint_artifact_digests(payload: Mapping[str, Any]) -> int:
    updates = 0
    for template in _required_list(payload, "templates"):
        if not isinstance(template, MutableMapping):
            raise CatalogRefreshError("blueprint template entry must be an object")
        updates += _set_artifact_digest(
            template,
            blueprint_template_content_digest(template),
            expected_kind="solution-blueprint-template",
        )
    return updates


def _refresh_outcome_section_digests(
    payload: Mapping[str, Any], section: str
) -> int:
    expected_by_section = dict(_OUTCOME_ATOMIC_SECTIONS)
    expected_kind = expected_by_section.get(section)
    if expected_kind is None:
        raise CatalogRefreshError(f"unknown outcome catalog section: {section}")
    updates = 0
    for artifact in _required_list(payload, section):
        if not isinstance(artifact, MutableMapping):
            raise CatalogRefreshError(f"{section} entry must be an object")
        updates += _set_artifact_digest(
            artifact,
            outcome_catalog_artifact_digest(artifact),
            expected_kind=expected_kind,
        )
    return updates


def _set_artifact_digest(
    artifact: MutableMapping[str, Any],
    digest: str,
    *,
    expected_kind: str,
) -> int:
    reference = _artifact_ref(artifact)
    if reference.get("kind") != expected_kind:
        raise CatalogRefreshError(
            f"expected {expected_kind} artifact, found {reference.get('kind')}"
        )
    if reference.get("digest") == digest:
        return 0
    reference["digest"] = digest
    return 1


def _is_artifact_ref(value: Mapping[str, Any], *, kind: str) -> bool:
    return (
        value.get("kind") == kind
        and isinstance(value.get("id"), str)
        and isinstance(value.get("version"), str)
        and isinstance(value.get("digest"), str)
    )


def _replace_refs(
    value: Any,
    *,
    kind: str,
    replacements: Mapping[str, Mapping[str, str]],
) -> int:
    updates = 0
    if isinstance(value, MutableMapping):
        if _is_artifact_ref(value, kind=kind):
            artifact_id = value["id"]
            replacement = replacements.get(artifact_id)
            if replacement is not None:
                for field in ("kind", "id", "version", "digest"):
                    desired = replacement[field]
                    if value.get(field) != desired:
                        value[field] = desired
                        updates += 1
                # Report references, not changed scalar fields.
                return 1 if updates else 0
        return sum(
            _replace_refs(child, kind=kind, replacements=replacements)
            for child in value.values()
        )
    if isinstance(value, list):
        return sum(
            _replace_refs(child, kind=kind, replacements=replacements)
            for child in value
        )
    return 0


def _count_refs(values: Iterable[Any], *, kind: str, ids: set[str]) -> int:
    def count(value: Any) -> int:
        if isinstance(value, Mapping):
            own = int(_is_artifact_ref(value, kind=kind) and value.get("id") in ids)
            return own + sum(count(child) for child in value.values())
        if isinstance(value, list):
            return sum(count(child) for child in value)
        return 0

    return sum(count(value) for value in values)


def _assert_target_refs_are_exact(
    values: Iterable[Any],
    identities: Mapping[str, InstalledStrategyIdentity],
) -> None:
    expected = {
        strategy_id: identity.artifact_ref()
        for strategy_id, identity in identities.items()
    }

    def inspect(value: Any) -> None:
        if isinstance(value, Mapping):
            if (
                _is_artifact_ref(value, kind="strategy")
                and value.get("id") in expected
            ):
                strategy_id = value["id"]
                if any(
                    value.get(key) != item
                    for key, item in expected[strategy_id].items()
                ):
                    raise CatalogRefreshError(
                        f"strategy reference was not refreshed exactly: {strategy_id}"
                    )
            for child in value.values():
                inspect(child)
        elif isinstance(value, list):
            for child in value:
                inspect(child)

    for value in values:
        inspect(value)


def _stage_replacement(path: Path, payload: bytes) -> Path:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        handle = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        )
        staged = Path(handle.name)
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(staged, mode)
        return staged
    except OSError as exc:
        raise CatalogRefreshError(f"cannot stage catalog {path}: {exc}") from exc


def _atomic_replace_many(replacements: Sequence[tuple[Path, bytes, bytes]]) -> None:
    staged: dict[Path, Path] = {}
    replaced: list[tuple[Path, bytes]] = []
    try:
        for path, _payload, original in replacements:
            if _read_required_file(path) != original:
                raise CatalogRefreshError(
                    f"catalog changed while refresh was in progress: {path}"
                )
        for path, payload, _original in replacements:
            staged[path] = _stage_replacement(path, payload)
        for path, _payload, original in replacements:
            os.replace(staged.pop(path), path)
            replaced.append((path, original))
    except Exception as exc:
        rollback_errors: list[str] = []
        for path, original in reversed(replaced):
            try:
                rollback = _stage_replacement(path, original)
                os.replace(rollback, path)
            except Exception as rollback_exc:  # pragma: no cover - catastrophic I/O
                rollback_errors.append(f"{path}: {rollback_exc}")
        detail = f"; rollback failed for {rollback_errors}" if rollback_errors else ""
        raise CatalogRefreshError(f"catalog replacement failed: {exc}{detail}") from exc
    finally:
        for staged_path in staged.values():
            try:
                staged_path.unlink(missing_ok=True)
            except OSError:
                pass
