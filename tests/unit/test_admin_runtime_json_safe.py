from __future__ import annotations

import base64
import uuid

from bson.binary import Binary, UuidRepresentation

from kehrnel.api.core.admin.routes import _json_safe


def test_json_safe_serializes_bson_binary_uuid_as_string():
    expected = uuid.uuid4()
    payload = {
        "ehrId": Binary.from_uuid(expected, uuid_representation=UuidRepresentation.STANDARD),
    }

    encoded = _json_safe(payload)

    assert encoded["ehrId"] == str(expected)


def test_json_safe_falls_back_to_base64_for_non_utf8_bytes():
    payload = {
        "blob": b"\xff\x01\x02",
    }

    encoded = _json_safe(payload)

    assert encoded["blob"] == base64.b64encode(payload["blob"]).decode("ascii")
