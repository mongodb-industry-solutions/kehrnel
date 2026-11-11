from kehrnel.api.domains.snomedct.routes import _operation_payload


def test_snomed_direct_operations_keep_the_domain_in_multi_domain_environments():
    assert _operation_payload("snomed_hybrid_search", {"q": "furosemide"}) == {
        "domain": "snomedct",
        "op": "snomed_hybrid_search",
        "payload": {"q": "furosemide"},
    }
