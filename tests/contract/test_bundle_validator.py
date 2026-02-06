from kehrnel.core.bundles import validate_bundle


def test_bundle_validator_catches_missing_fields():
    bad = {"domain": "openEHR", "kind": "slim_search_definition", "payload": {}}
    errors = validate_bundle(bad)
    assert errors
    assert any("bundle_id" in e for e in errors)
    assert any("templates" in e for e in errors)


def test_bundle_validator_accepts_minimal_valid():
    bundle = {
        "bundle_id": "openehr.analytics.sample.v1",
        "domain": "openEHR",
        "kind": "slim_search_definition",
        "version": "1.0.0",
        "payload": {
            "templates": [
                {
                    "templateId": "Sample",
                    "analytics_fields": [{"name": "field", "path": "/path", "rmType": "DV_TEXT"}],
                }
            ]
        },
    }
    errors = validate_bundle(bundle)
    assert errors == []
