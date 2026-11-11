"""Synthetic RF2 data: no licensed distribution or clinical data required."""
import csv
import json
import zipfile
from pathlib import Path

import pytest

from kehrnel.engine.domains.snomedct import build_term_documents, iter_concepts_from_json, normalize_concept
from kehrnel.engine.domains.snomedct.rf2 import RF2PreparationError, prepare_rf2

DATE = "20261001"
MODULE = "900000000000207008"
ISA = "116680003"
INFERRED = "900000000000011006"
BASE = ["id", "effectiveTime", "active", "moduleId"]


def snapshot(tmp_path, *, cycle=False, missing=False, stated=False):
    folder = tmp_path / "Snapshot"
    folder.mkdir()

    def write(name, fields, rows):
        with (folder / f"{name}_Snapshot.txt").open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(BASE + fields)
            for code, values in rows:
                writer.writerow([code, DATE, "1", MODULE] + values)

    write("sct2_Concept", ["definitionStatusId"], [(str(i), ["900000000000074008"]) for i in (100, 200, 300)])
    write("sct2_Description", ["conceptId", "languageCode", "typeId", "term", "caseSignificanceId"], [
        ("1001", ["100", "en", "900000000000003001", "Synthetic finding (finding)", "900000000000448009"]),
        ("2001", ["200", "en", "900000000000003001", "Synthetic subtype (finding)", "900000000000448009"]),
        ("2002", ["200", "en", "900000000000013009", "Synthetic preferred subtype", "900000000000448009"]),
        ("3001", ["300", "es", "900000000000013009", "Hallazgo sintético", "900000000000448009"]),
    ])
    relfields = ["sourceId", "destinationId", "relationshipGroup", "typeId", "characteristicTypeId", "modifierId"]
    edges = [("4001", ["200", "999" if missing else "100", "0", ISA, "900000000000010007" if stated else INFERRED, "900000000000451002"]),
             ("4002", ["300", "200", "0", ISA, INFERRED, "900000000000451002"]),
             ("4003", ["200", "100", "2", "246075003", INFERRED, "900000000000451002"])]
    if cycle:
        edges.append(("4004", ["100", "300", "0", ISA, INFERRED, "900000000000451002"]))
    write("sct2_Relationship", relfields, edges)
    write("sct2_RelationshipConcreteValues", ["sourceId", "value", "relationshipGroup", "typeId", "characteristicTypeId", "modifierId"], [
        ("5001", ["200", "#10", "2", "1142135004", INFERRED, "900000000000451002"]),
    ])
    write("der2_Language", ["refsetId", "referencedComponentId", "acceptabilityId"], [("6001", ["900000000000509007", "2002", "900000000000548007"])])
    write("sct2_sRefset_OWLExpression", ["refsetId", "referencedComponentId", "owlExpression"], [("7001", ["733073007", "200", "SubClassOf(:200 :100)"])])
    write("der2_iisssccRefset_ExtendedMap", ["refsetId", "referencedComponentId", "mapGroup", "mapPriority", "mapRule", "mapAdvice", "mapTarget", "correlationId", "mapCategoryId"], [
        ("8001", ["447562003", "200", "1", "1", "TRUE", "SYNTHETIC TEST ONLY", "TEST-200", "0", "0"]),
    ])
    return folder


def test_preparation_feeds_existing_normalizer_sidecar_and_stream_reader(tmp_path):
    source = snapshot(tmp_path)
    output = tmp_path / "concepts.json"
    report = prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    assert report["hierarchy"]["edges"] == 2
    assert report["estimated_term_documents"] == 4
    assert report["active_description_languages"] == {"en": 3, "es": 1}
    assert json.loads(Path(report["receipt_path"]).read_text())["output_digest"] == report["output_digest"]
    docs = {d["conceptId"]: normalize_concept(d, release_id=DATE) for d in iter_concepts_from_json(output)}
    assert docs["300"]["inferredAncestorIds"] == ["100", "200"]
    assert docs["100"]["inferredChildIds"] == ["200"]
    assert docs["200"]["relationships"][1]["relationshipGroup"] == "2"
    assert docs["200"]["concreteRelationships"][0]["value"] == "#10"
    assert docs["200"]["owlAxioms"][0]["owlExpression"] == "SubClassOf(:200 :100)"
    assert docs["200"]["mappings"][0]["mapTarget"] == "TEST-200"
    terms = build_term_documents(docs["200"], release_id=DATE, language_codes=["en"])
    assert {t["preferredTerm"] for t in terms} == {"Synthetic preferred subtype"}
    assert {tuple(t["ancestorIds"]) for t in terms} == {("100",)}


def test_zip_and_directory_have_the_same_checksums_and_dry_run_writes_nothing(tmp_path):
    source = snapshot(tmp_path)
    archive = tmp_path / "release.zip"
    with zipfile.ZipFile(archive, "w") as z:
        for f in sorted(source.iterdir()):
            z.write(f, arcname=f.name)
    before = set(tmp_path.iterdir())
    a = prepare_rf2(source, release_id=DATE)
    b = prepare_rf2(archive, release_id=DATE)
    assert a["output_digest"] == b["output_digest"]
    assert a["source_digest"] == b["source_digest"]
    assert set(tmp_path.iterdir()) == before
    assert b["dry_run"] and b["output_path"] is None


@pytest.mark.parametrize("option,match", [("cycle", "Cycle"), ("missing", "dependency")])
def test_incomplete_or_cyclic_hierarchy_fails_before_publishing(tmp_path, option, match):
    source = snapshot(tmp_path, **{option: True})
    output = tmp_path / "broken.json"
    with pytest.raises(RF2PreparationError, match=match):
        prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    assert not output.exists()


def test_stated_edges_do_not_become_inferred_ancestors(tmp_path):
    source = snapshot(tmp_path, stated=True)
    output = tmp_path / "prepared.json"
    prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    docs = {d["conceptId"]: d for d in iter_concepts_from_json(output)}
    assert docs["200"]["inferredAncestorIds"] == []
    assert docs["300"]["inferredAncestorIds"] == ["200"]


def test_unsafe_archive_and_symlink_are_rejected(tmp_path):
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("../Snapshot/sct2_Concept_Snapshot.txt", "unsafe")
    with pytest.raises(RF2PreparationError, match="Unsafe"):
        prepare_rf2(archive, release_id=DATE)
    source = snapshot(tmp_path)
    (source / "escape_Snapshot.txt").symlink_to(archive)
    with pytest.raises(RF2PreparationError, match="symlink"):
        prepare_rf2(source, release_id=DATE)


def test_license_and_existing_output_are_protected(tmp_path):
    source = snapshot(tmp_path)
    output = tmp_path / "existing.json"
    with pytest.raises(RF2PreparationError, match="license"):
        prepare_rf2(source, release_id=DATE, output=output)
    output.write_text("do not overwrite")
    with pytest.raises(RF2PreparationError, match="already exists"):
        prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    assert output.read_text() == "do not overwrite"


@pytest.mark.asyncio
async def test_strategy_rf2_operation_is_tenant_scoped_and_needs_no_mongo_for_preview(tmp_path):
    from kehrnel.engine.core.pack_loader import load_strategy
    from kehrnel.engine.core.types import StrategyContext
    from kehrnel.engine.core.errors import KehrnelError
    from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy
    manifest = load_strategy("snomedct.mongodb", Path("src/kehrnel/engine/strategies/snomedct/mongodb"))
    config = {**manifest.default_config, "source": {"local_dir": str(tmp_path)}, "release": {"id": DATE}}
    ctx = StrategyContext(environment_id="rf2-test", config=config, manifest=manifest)
    strategy = SNOMEDCTMongoDBStrategy(manifest)
    source = snapshot(tmp_path)
    report = await strategy.snomed_prepare_rf2(ctx, {"path": source.name})
    assert report["dry_run"] and report["concepts"] == 3
    with pytest.raises(KehrnelError) as error:
        await strategy.snomed_prepare_rf2(ctx, {"path": "../../outside.zip"})
    assert error.value.code == "RELEASE_PATH_OUTSIDE_STAGING"
    report = await strategy.snomed_prepare_rf2(ctx, {"path": source.name, "output_file_name": "prepared.json", "dry_run": False, "license_acknowledged": True})
    inspection = await strategy.snomed_inspect_release(ctx, {"path": report["output_path"]})
    assert inspection["concepts"] == 3


@pytest.mark.asyncio
async def test_prepared_rf2_uses_existing_release_scoped_batch_upserts(tmp_path):
    from types import SimpleNamespace
    from kehrnel.engine.core.pack_loader import load_strategy
    from kehrnel.engine.core.types import StrategyContext
    from kehrnel.engine.strategies.snomedct.mongodb.strategy import SNOMEDCTMongoDBStrategy
    source = snapshot(tmp_path)
    output = tmp_path / "prepared.json"
    prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    written = []

    class Collection:
        async def bulk_write(self, operations, ordered=False):
            written.extend(op._doc for op in operations)
            return SimpleNamespace(upserted_count=len(operations), modified_count=0)

    manifest = load_strategy("snomedct.mongodb", Path("src/kehrnel/engine/strategies/snomedct/mongodb"))
    config = {**manifest.default_config, "source": {"local_dir": str(tmp_path)}, "release": {"id": DATE}, "ingest": {"create_indexes_before_ingest": False, "rebuild_sidecar": False}}
    ctx = StrategyContext(environment_id="rf2-test", config=config, manifest=manifest, adapters={"storage": SimpleNamespace(db={"snomed_concepts": Collection()})})
    result = await SNOMEDCTMongoDBStrategy(manifest).snomed_ingest_release(ctx, {"path": str(output), "license_acknowledged": True, "batch_size": 2})
    assert result["upserted"] == 3 and result["concepts_seen"] == 3
    assert {d["_id"] for d in written} == {f"{DATE}|{code}" for code in ("100", "200", "300")}
    assert next(d for d in written if d["conceptId"] == "200")["mappings"][0]["mapRule"] == "TRUE"


def test_preparation_route_publishes_the_typed_contract():
    from fastapi import FastAPI
    from kehrnel.api.domains.snomedct.routes import router
    app = FastAPI()
    app.include_router(router)
    spec = app.openapi()
    op = spec["paths"]["/api/domains/snomedct/releases/prepare-rf2"]["post"]
    assert op["requestBody"]["content"]["application/json"]["schema"]["$ref"].endswith("SnomedRF2PrepareRequest")
    schema = spec["components"]["schemas"]["SnomedRF2PrepareRequest"]
    assert schema["properties"]["dry_run"]["default"] is True
    assert schema["properties"]["license_acknowledged"]["default"] is False


def test_rf2_quote_characters_are_preserved(tmp_path):
    source = snapshot(tmp_path)
    with (source / "sct2_Description_Snapshot.txt").open("a") as f:
        f.write('1002\t20261001\t1\t900000000000207008\t100\ten\t900000000000013009\t"Quoted finding"\t900000000000448009\n')
    with (source / "sct2_RelationshipConcreteValues_Snapshot.txt").open("a") as f:
        f.write('5002\t20261001\t1\t900000000000207008\t200\t"synthetic value"\t2\t1142135004\t900000000000011006\t900000000000451002\n')
    output = tmp_path / "literal-values.json"
    prepare_rf2(source, release_id=DATE, output=output, license_acknowledged=True)
    docs = {d["conceptId"]: d for d in iter_concepts_from_json(output)}
    assert next(d for d in docs["100"]["descriptions"] if d["id"] == "1002")["term"] == '"Quoted finding"'
    assert next(r for r in docs["200"]["concreteRelationships"] if r["id"] == "5002")["value"] == '"synthetic value"'
