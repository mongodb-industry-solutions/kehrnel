import json
from pathlib import Path
import jsonschema
from kehrnel.engine.domains.openehr.analytics.store import COLLECTIONS
PACK = Path(__file__).parents[3] / "src/kehrnel/engine/strategies/openehr/analytics"
def test_analytics_presentation_matches_storage_and_defaults():
    spec = json.loads((PACK / "spec.json").read_text())
    schema = json.loads((PACK / "schema.json").read_text())
    defaults = json.loads((PACK / "defaults.json").read_text())
    jsonschema.validate(defaults, schema)
    assert {s["id"] for s in spec["storageModel"]["stores"]} == set(COLLECTIONS)
    assert {e["store"] for e in spec["visualization"]["collectionModel"]["entities"]} == set(COLLECTIONS)
    assert len(spec["transformModel"]["pipeline"]) == 5
    assert schema["x-ui"]["activationFields"] == ["database", "source_namespace"]
    assert schema["x-ui"]["activeFields"] == []
    assert schema["properties"]["database"]["default"] == defaults["database"] == "openehr_analytics"


def test_public_analytics_description_does_not_advertise_private_adapters():
    manifest = json.loads((PACK / "manifest.json").read_text())
    spec = json.loads((PACK / "spec.json").read_text())
    schema = json.loads((PACK / "schema.json").read_text())
    assert "ibm" not in json.dumps({k: manifest.get(k) for k in ("name", "description", "ui")}).lower()
    assert "ibm" not in json.dumps(spec).lower()
    assert "RPS Dual" in manifest["ui"]["story"]
    # Private source configuration remains supported; it is not a public catalogue dependency.
    assert "ibm" in schema["properties"]["source"]["properties"]["format"]["enum"]
