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
