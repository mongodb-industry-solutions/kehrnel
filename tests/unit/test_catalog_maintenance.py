from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

from kehrnel.contracts.outcome_catalog import IndustryOutcomeCatalog
from kehrnel.engine.core.blueprint_templates import (
    DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
    assert_blueprint_template_refs,
    blueprint_template_content_digest,
    load_solution_blueprint_templates,
)
from kehrnel.engine.core.catalog_maintenance import (
    CatalogRefreshError,
    InstalledStrategyIdentity,
    refresh_bundled_catalogs,
)
from kehrnel.engine.core.outcome_catalog import (
    DEFAULT_OUTCOME_CATALOG_PATH,
    outcome_catalog_artifact_digest,
)
import kehrnel.engine.core.catalog_maintenance as catalog_maintenance


STRATEGY_ID = "openehr.rps_dual"
UPDATED_DIGEST = "sha256:" + "a" * 64


def _copy_inputs(tmp_path: Path) -> tuple[Path, Path]:
    blueprint_path = tmp_path / "blueprints.json"
    outcome_path = tmp_path / "outcomes.json"
    blueprint_path.write_bytes(DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH.read_bytes())
    outcome_path.write_bytes(DEFAULT_OUTCOME_CATALOG_PATH.read_bytes())
    return blueprint_path, outcome_path


def _install_changed_identity(monkeypatch) -> None:
    identity = InstalledStrategyIdentity(
        strategy_id=STRATEGY_ID,
        version="0.2.0",
        digest=UPDATED_DIGEST,
        manifest_path="/test/strategies/openehr/rps_dual/manifest.json",
    )

    def resolve(strategy_ids, *, strategy_root):
        assert set(strategy_ids) == {STRATEGY_ID}
        return {STRATEGY_ID: identity}

    monkeypatch.setattr(
        catalog_maintenance,
        "resolve_installed_strategy_identities",
        resolve,
    )

    def validate_links(catalog, *, strategy_root, blueprint_templates):
        assert strategy_root
        assert_blueprint_template_refs(catalog, templates=blueprint_templates)

    monkeypatch.setattr(
        catalog_maintenance,
        "assert_outcome_catalog_links",
        validate_links,
    )


def _refs(value: Any, *, kind: str, artifact_id: str) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if value.get("kind") == kind and value.get("id") == artifact_id:
            yield value
        for child in value.values():
            yield from _refs(child, kind=kind, artifact_id=artifact_id)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _refs(child, kind=kind, artifact_id=artifact_id)


def test_refresh_updates_exact_refs_and_derived_digests_deterministically(
    tmp_path, monkeypatch
):
    blueprint_path, outcome_path = _copy_inputs(tmp_path)
    _install_changed_identity(monkeypatch)
    before_blueprint = blueprint_path.read_bytes()
    before_outcome = outcome_path.read_bytes()

    preview = refresh_bundled_catalogs(
        [STRATEGY_ID],
        blueprint_catalog_path=blueprint_path,
        outcome_catalog_path=outcome_path,
        write=False,
    )

    assert preview.changed is True
    assert preview.written is False
    assert blueprint_path.read_bytes() == before_blueprint
    assert outcome_path.read_bytes() == before_outcome

    report = refresh_bundled_catalogs(
        [STRATEGY_ID],
        blueprint_catalog_path=blueprint_path,
        outcome_catalog_path=outcome_path,
    )

    assert report.written is True
    assert report.strategy_refs_updated > 0
    assert report.blueprint_artifact_digests_updated == 1
    assert report.blueprint_refs_updated > 0
    assert report.reference_architecture_refs_updated > 0

    blueprint_payload = json.loads(blueprint_path.read_text(encoding="utf-8"))
    outcome_payload = json.loads(outcome_path.read_text(encoding="utf-8"))
    for reference in _refs(
        (blueprint_payload, outcome_payload),
        kind="strategy",
        artifact_id=STRATEGY_ID,
    ):
        assert reference["version"] == "0.2.0"
        assert reference["digest"] == UPDATED_DIGEST

    templates = load_solution_blueprint_templates(blueprint_path)
    outcome_catalog = IndustryOutcomeCatalog.model_validate(outcome_payload)
    assert_blueprint_template_refs(outcome_catalog, templates=templates)
    for template in templates:
        assert template.artifact_ref.digest == blueprint_template_content_digest(
            template
        )
    for section in (
        outcome_catalog.domains,
        outcome_catalog.data_products,
        outcome_catalog.architecture_capabilities,
        outcome_catalog.visual_patterns,
        outcome_catalog.outcomes,
        outcome_catalog.reference_architectures,
        outcome_catalog.demo_experiences,
    ):
        for artifact in section:
            assert artifact.artifact_ref.digest == outcome_catalog_artifact_digest(
                artifact
            )
    assert outcome_catalog.artifact_ref.digest == outcome_catalog_artifact_digest(
        outcome_catalog
    )

    # The writer retains key/array order and the repository's canonical pretty form.
    assert blueprint_path.read_bytes() == (
        json.dumps(blueprint_payload, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    assert outcome_path.read_bytes() == (
        json.dumps(outcome_payload, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")

    second = refresh_bundled_catalogs(
        [STRATEGY_ID],
        blueprint_catalog_path=blueprint_path,
        outcome_catalog_path=outcome_path,
    )
    assert second.changed is False
    assert second.written is False


def test_refresh_rejects_existing_digest_corruption_without_writing(
    tmp_path, monkeypatch
):
    blueprint_path, outcome_path = _copy_inputs(tmp_path)
    _install_changed_identity(monkeypatch)
    payload = json.loads(blueprint_path.read_text(encoding="utf-8"))
    payload["templates"][0]["artifactRef"]["digest"] = "sha256:" + "0" * 64
    blueprint_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    corrupt_blueprint = blueprint_path.read_bytes()
    unchanged_outcome = outcome_path.read_bytes()

    with pytest.raises(CatalogRefreshError, match="digest mismatch"):
        refresh_bundled_catalogs(
            [STRATEGY_ID],
            blueprint_catalog_path=blueprint_path,
            outcome_catalog_path=outcome_path,
        )

    assert blueprint_path.read_bytes() == corrupt_blueprint
    assert outcome_path.read_bytes() == unchanged_outcome
