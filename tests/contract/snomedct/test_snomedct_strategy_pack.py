from pathlib import Path

import pytest

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy


PACK_DIR = Path("src/kehrnel/engine/strategies/snomedct/mongodb")


def test_snomedct_mongodb_pack_loads_with_defaults_schema_and_ops():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)

    assert manifest.domain == "snomedct"
    assert manifest.default_config["collections"]["sidecar_enabled"] is True
    assert "collections" in manifest.config_schema["properties"]

    op_names = {op.name for op in manifest.ops}
    assert {
        "snomed_list_releases",
        "snomed_diff_release",
        "snomed_ingest_release",
        "snomed_rebuild_sidecar",
        "snomed_search",
        "snomed_ground_note",
    }.issubset(op_names)


@pytest.mark.asyncio
async def test_snomedct_transform_builds_canonical_and_sidecar_docs():
    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=manifest.default_config, manifest=manifest)

    result = await strategy.transform(
        ctx,
        {
            "concept": {
                "conceptId": "73211009",
                "active": "1",
                "moduleId": "900000000000207008",
                "effectiveTime": "20260601",
                "definitionStatusId": "900000000000074008",
                "inferredParentIds": ["44054006"],
                "inferredAncestorIds": ["404684003"],
                "inferredDescendantIds": ["1", "2"],
                "descriptions": [
                    {
                        "descriptionId": "100",
                        "active": "1",
                        "languageCode": "en",
                        "typeId": "900000000000003001",
                        "term": "Diabetes mellitus (disorder)",
                        "acceptabilityMap": {"900000000000509007": "900000000000548007"},
                    },
                    {
                        "descriptionId": "101",
                        "active": "1",
                        "languageCode": "en",
                        "typeId": "900000000000013009",
                        "term": "Diabetes mellitus",
                        "acceptabilityMap": {"900000000000509007": "900000000000548007"},
                    },
                ],
            }
        },
    )

    assert result.base["conceptId"] == "73211009"
    assert "inferredDescendantIds" not in result.base
    assert result.search
    assert result.search["terms"][0]["conceptId"] == "73211009"


@pytest.mark.asyncio
async def test_snomedct_local_release_staging_list_and_inspect(tmp_path):
    release_dir = tmp_path / "snomed-releases"
    release_dir.mkdir()
    release_file = release_dir / "edicion_20260601.json"
    release_file.write_text(
        """
        [
          {
            "conceptId": "73211009",
            "active": "1",
            "effectiveTime": "20260601",
            "descriptions": [
              {
                "descriptionId": "101",
                "active": "1",
                "languageCode": "en",
                "typeId": "900000000000013009",
                "term": "Diabetes mellitus"
              }
            ]
          }
        ]
        """,
        encoding="utf-8",
    )

    manifest = load_strategy("snomedct.mongodb", PACK_DIR)
    cfg = dict(manifest.default_config)
    cfg["source"] = {
        "local_dir": str(release_dir),
        "file_name": release_file.name,
        "file_pattern": "*.json",
    }
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    ctx = StrategyContext(environment_id="test", config=cfg, manifest=manifest)

    listed = await strategy.snomed_list_releases(ctx, {})
    inspected = await strategy.snomed_inspect_release(ctx, {})

    assert listed["count"] == 1
    assert listed["files"][0]["name"] == release_file.name
    assert inspected["path"] == str(release_file)
    assert inspected["concepts"] == 1
    assert inspected["active"] == 1
