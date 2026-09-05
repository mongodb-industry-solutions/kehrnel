import pytest
from pydantic import ValidationError

from kehrnel.engine.strategies.cdisc.sdr.synthetic import SyntheticStudyGenerator


def test_synthetic_study_is_deterministic_and_cross_domain_consistent():
    generator = SyntheticStudyGenerator()
    recipe = {"studyId": "SYNTH-TEST", "subjects": 12, "seed": 42}

    first = generator.generate(recipe)
    second = generator.generate(recipe)

    assert first == second
    assert set(first["datasets"]) == {"DM", "AE", "LB", "VS"}
    dm_subjects = {row[1] for row in first["datasets"]["DM"]["rows"]}
    for domain in ("AE", "LB", "VS"):
        assert {row[1] for row in first["datasets"][domain]["rows"]} <= dm_subjects
    assert first["datasets"]["DM"]["records"] == 12


def test_synthetic_anomalies_are_declared_in_the_recipe_result():
    result = SyntheticStudyGenerator().generate(
        {"studyId": "SYNTH-ANOM", "subjects": 20, "seed": 7, "anomalyRate": 0.2}
    )

    assert result["expectedAnomalies"]
    assert all(item["ruleId"] == "SDR.AE.AEDECOD.REQUIRED" for item in result["expectedAnomalies"])


def test_send_safety_signal_scenario_has_known_cross_domain_truth():
    result = SyntheticStudyGenerator().generate(
        {
            "studyId": "SYNTH-SAFETY",
            "profile": "send",
            "scenario": "safety-signal",
            "subjects": 50,
            "seed": 42,
        }
    )

    assert set(result["datasets"]) == {"DM", "TX", "MI", "LB"}
    assert result["recipe"]["scenario"] == "safety-signal"
    assert result["expectedSignals"][0]["specimen"] == "THYMUS"
    assert result["expectedSignals"][0]["controlIncidence"] == 0
    assert result["expectedSignals"][0]["treatedIncidence"] > 0
    assert result["expectedSignals"][0]["expectedQueryPath"] == ["TX", "DM", "MI", "LB"]
    assert result["datasets"]["DM"]["records"] == 50
    assert result["datasets"]["TX"]["records"] == 5
    assert result["datasets"]["LB"]["records"] == 200


def test_send_safety_signal_requires_the_cross_domain_evidence_path():
    with pytest.raises(ValidationError, match="requires DM, TX, MI, and LB"):
        SyntheticStudyGenerator().generate(
            {
                "profile": "send",
                "scenario": "safety-signal",
                "domains": ["DM", "TX", "MI"],
            }
        )
