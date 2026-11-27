"""Offline, synthetic walkthrough of the implemented ingestion rule contract.

No MongoDB connection, customer input or writes. The integration supervisor and
Healthcare Data Lab delivery service are separate implementation responsibilities.
"""

from copy import deepcopy
import json
import math
from pathlib import Path

from kehrnel.engine.domains.openehr.analytics import Converter, RuleRevision, RuleSet
from kehrnel.engine.domains.openehr.analytics.store import decide


def run():
    folder = Path(__file__).resolve().parent
    config = json.loads((folder / "config.canonical.json").read_text())
    source = json.loads((folder / "composition.canonical.json").read_text())
    initial = RuleRevision.compile(config["rules"][0])
    changed = deepcopy(config["rules"][0])
    changed.update(epoch=2, effective_at="2026-12-01T00:00:00Z")
    group = changed["catalog"]["templates"][0]["groups"][0]
    group["fields"] = [
        b for b in group["fields"] if b["key"] == "blood_pressure.systolic"
    ]
    group["definition"]["required_fields"] = ["blood_pressure.systolic"]
    changed["retention"] = {
        "mode": "delete_facts",
        "clock": "clinical_time",
        "days": 1825,
    }
    revised = RuleRevision.compile(changed)
    converter = Converter(
        RuleSet([initial, revised]), source_namespace="synthetic-reference"
    )
    old = converter.convert(source, accepted_at="2026-11-20T00:00:00Z")
    retry = converter.convert(source, accepted_at="2026-11-20T00:00:00Z")
    future = converter.convert(source, accepted_at="2026-12-02T00:00:00Z")
    historical = converter.convert(
        source, accepted_at="2026-11-20T00:00:00Z", rule_revision=revised.revision
    )
    pairs = [f["clinical"]["blood_pressure"] for f in old.facts]
    assert pairs[:2] == [
        {"systolic": 150, "diastolic": 65},
        {"systolic": 127, "diastolic": 92},
    ]
    assert len(old.facts) == len(future.facts) == 4
    assert decide(old.state(), retry.state()) == "unchanged"
    assert all("diastolic" not in f["clinical"]["blood_pressure"] for f in future.facts)
    assert historical.rule_revision == revised.revision
    assert all("expires_at" in f for f in historical.facts)
    # Unit conversion is applied to a separate synthetic input; no source is modified.
    quantity = deepcopy(source)
    first = quantity["canonicalJSON"]["content"][0]["data"]["events"][0]["data"][
        "items"
    ][0]
    first["value"].update(magnitude=20, units="kPa")
    converted = converter.convert(quantity, accepted_at="2026-11-20T00:00:00Z")
    magnitude = converted.facts[0]["clinical"]["blood_pressure"]["systolic"]
    assert math.isclose(magnitude, 20 * 7.50061683)
    # A field formula is not an implemented conversion operator.
    unsupported = deepcopy(config["rules"][0])
    unsupported["catalog"]["templates"][0]["groups"][0]["fields"][0]["conversions"] = {
        "formula": "x * 2"
    }
    try:
        RuleRevision.compile(unsupported)
    except ValueError:
        formula_rejected = True
    else:
        raise AssertionError("Unsupported formula unexpectedly accepted")
    return {
        "synthetic": True,
        "database_writes": 0,
        "facts_per_composition": len(old.facts),
        "first_two_recorded_pairs": pairs[:2],
        "retry_decision": "unchanged",
        "prospective_rule_epoch": future.rule_epoch,
        "historical_rule_epoch": historical.rule_epoch,
        "deselected_field_absent": True,
        "fact_expiry_present": True,
        "converted_20_kpa_in_mmhg": magnitude,
        "unsupported_formula_rejected": formula_rejected,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
