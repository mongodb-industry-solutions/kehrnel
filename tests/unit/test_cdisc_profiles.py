import pytest

from kehrnel.engine.domains.cdisc.models import CdiscProfile
from kehrnel.engine.domains.cdisc.projection import derive_entity_refs, derive_facets
from kehrnel.engine.strategies.cdisc.sdr.synthetic import SyntheticStudyGenerator


@pytest.mark.parametrize(
    ("profile", "domain", "data", "expected"),
    [
        ("send", "MI", {"STUDYID": "S", "USUBJID": "A1", "MISPEC": "LIVER", "MISTRESC": "LESION"}, {"subjectType": "animal", "organ": "LIVER", "finding": "LESION"}),
        ("sdtm", "AE", {"STUDYID": "S", "USUBJID": "P1", "AEDECOD": "HEADACHE"}, {"subjectType": "human", "eventTerm": "HEADACHE"}),
        ("adam", "ADLB", {"STUDYID": "S", "USUBJID": "P1", "PARAMCD": "ALT", "AVAL": 12.0, "SAFFL": "Y"}, {"parameterCode": "ALT", "analysisValue": 12.0}),
        ("tig", "EVID", {"STUDYID": "S", "PRODUCTID": "P", "BATCHID": "B", "EVIDTYPE": "NONCLINICAL"}, {"subjectType": "evidence", "productId": "P", "batchId": "B"}),
    ],
)
def test_profile_facets_and_entities(profile, domain, data, expected):
    facets = derive_facets(CdiscProfile(profile), domain, data)
    refs = {(item.type, item.id) for item in derive_entity_refs(CdiscProfile(profile), data)}

    for key, value in expected.items():
        assert facets[key] == value
    assert ("study", "S") in refs


@pytest.mark.parametrize(
    ("profile", "domains"),
    [
        ("sdtm", {"DM", "AE", "LB", "VS"}),
        ("send", {"DM", "TX", "MI", "LB"}),
        ("adam", {"ADSL", "ADAE", "ADLB"}),
        ("tig", {"PROD", "BATCH", "EVID"}),
    ],
)
def test_all_profile_generators_are_deterministic(profile, domains):
    generator = SyntheticStudyGenerator()
    recipe = {"studyId": f"SYNTH-{profile}", "profile": profile, "subjects": 8, "seed": 9}

    first = generator.generate(recipe)
    second = generator.generate(recipe)

    assert first == second
    assert set(first["datasets"]) == domains
    assert first["watermark"]["recipeDigest"] == first["recipeDigest"]
