"""
T1 — Fail-closed FHIR search conversion.

The governing acceptance test: an unsupported search must raise an error and
must NOT produce a match-all filter (which would let Mongo execute a broader
query than the user requested).
"""

import pytest

from fhir_search_to_mql import FHIRSearchConverter
from fhir_search_to_mql.core.exceptions import (
    ConversionError,
    UnsupportedParameterError,
)


@pytest.fixture
def converter():
    return FHIRSearchConverter()


def test_unsupported_param_strict_raises(converter):
    """Strict (default): an unsupported parameter raises, never match-all."""
    with pytest.raises(UnsupportedParameterError):
        converter.convert("Patient", query_string="totally_unsupported=foo")


def test_all_params_dropped_strict_never_matches_all(converter):
    """Even if the sole param is unknown, strict must not return {} (match-all)."""
    try:
        result = converter.convert("Patient", query_string="totally_unsupported=foo")
    except (UnsupportedParameterError, ConversionError):
        return  # correct: failed closed
    assert result != {}, "unsupported-only search must not compile to a match-all filter"
    pytest.fail("expected a fail-closed error for an unsupported-only search")


def test_lenient_records_ignored_but_never_matches_all(converter):
    """Lenient may skip unknowns, but if ALL filters drop it must still error."""
    ignored: list = []
    with pytest.raises((UnsupportedParameterError, ConversionError)):
        converter.convert(
            "Patient",
            query_string="totally_unsupported=foo",
            handling="lenient",
            ignored_out=ignored,
        )


def test_lenient_keeps_supported_drops_unsupported(converter):
    """Lenient: a supported param survives; the unsupported one is recorded."""
    ignored: list = []
    result = converter.convert(
        "Patient",
        query_string="gender=male&totally_unsupported=foo",
        handling="lenient",
        ignored_out=ignored,
    )
    assert result and result != {}
    assert any(item["name"] == "totally_unsupported" for item in ignored)


def test_empty_query_string_is_rejected_by_parser(converter):
    """An empty query string is rejected upstream (parser), not silently match-all.

    The 'list all' case is represented by query_string=None and handled in
    query.py before the converter is invoked — never by an empty match-all here.
    """
    from fhir_search_to_mql.core.exceptions import ParsingError

    with pytest.raises(ParsingError):
        converter.convert("Patient", query_string="")


def test_supported_param_still_works(converter):
    """Regression: a supported param compiles to a non-empty filter."""
    result = converter.convert("Patient", query_string="gender=male")
    assert result and result != {}
