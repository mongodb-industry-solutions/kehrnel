from __future__ import annotations

from kehrnel.api.domains.openehr.aql.service import _convert_datetime_objects


def test_convert_datetime_objects_collapses_common_float_artifacts():
    payload = {
        "avgMagnitude": 45.156879999999994,
        "sumMagnitude": 5644.61,
        "nested": [0.30000000000000004],
    }

    normalized = _convert_datetime_objects(payload)

    assert normalized == {
        "avgMagnitude": 45.15688,
        "sumMagnitude": 5644.61,
        "nested": [0.3],
    }
