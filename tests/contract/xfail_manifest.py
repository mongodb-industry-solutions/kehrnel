XFAIL_TESTS = {
    # Legacy AQL→MQL parity gaps (search compound/lookup/projections/regex)
    "tests/contract/test_atlas_search_compile.py::test_cross_patient_embedded_document_pipeline",
    "tests/contract/test_atlas_search_compile.py::test_lookup_appended_when_configured",
    "tests/contract/test_atlas_search_compile.py::test_post_match_added_for_unsupported_predicate",
    "tests/contract/test_golden_aql_search.py::test_cross_patient_aql_compiles_to_search_first",
    "tests/contract/test_golden_cross_patient_search_embedded.py::test_cross_patient_search_embedded_and_lookup",
    "tests/contract/test_golden_patient_regex_paths.py::test_patient_regex_and_all_elem_match",
    "tests/contract/test_golden_vaccinations_cross_patient_projection.py::test_vaccination_cross_patient_projection_lookup",
    "tests/contract/test_golden_vaccinations_patient_projection.py::test_vaccination_patient_projection_shape",
}
