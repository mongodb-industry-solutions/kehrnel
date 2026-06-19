from bson import Binary

from kehrnel.api.core.admin.routes import _json_safe


def test_json_safe_encodes_binary_values_as_base64():
    payload = {
        "ok": True,
        "result": {
            "rows": [
                {"raw": b"\xac\x00"},
                {"raw": Binary(b"\xef\x01", subtype=0)},
            ]
        },
    }

    encoded = _json_safe(payload)

    assert encoded["result"]["rows"][0]["raw"] == {
        "$binary": "rAA=",
        "encoding": "base64",
    }
    assert encoded["result"]["rows"][1]["raw"] == {
        "$binary": "7wE=",
        "encoding": "base64",
    }
