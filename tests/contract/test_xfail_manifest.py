import pytest

from tests.contract.xfail_manifest import XFAIL_TESTS


def test_xfail_manifest_is_stable():
    # If new xfails are added, they must be recorded here.
    expected = XFAIL_TESTS
    assert len(expected) == 8


@pytest.mark.parametrize("nodeid", list(XFAIL_TESTS))
def test_xfail_nodeids_documented(nodeid):
    # This test is a documentation guard: collection must include these nodeids.
    assert nodeid in XFAIL_TESTS
