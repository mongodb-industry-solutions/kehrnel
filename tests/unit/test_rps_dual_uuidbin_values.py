import uuid

from bson.binary import Binary, UuidRepresentation

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.value_formatter import (
    ValueFormatter,
)


def test_uuidbin_id_values_use_bson_uuid_binary_subtype():
    expected = uuid.UUID("001b7ad3-98da-4d4b-b3c0-7557e97dce22")

    formatted = ValueFormatter.format_id_value(str(expected), "uuidbin")

    assert isinstance(formatted, Binary)
    assert formatted.subtype == 4
    assert formatted.as_uuid(UuidRepresentation.STANDARD) == expected


def test_uuidbin_id_values_convert_raw_uuid_bytes_to_bson_uuid_binary():
    expected = uuid.UUID("001b7ad3-98da-4d4b-b3c0-7557e97dce22")

    formatted = ValueFormatter.format_id_value(expected.bytes, "uuidbin")

    assert isinstance(formatted, Binary)
    assert formatted.subtype == 4
    assert formatted.as_uuid(UuidRepresentation.STANDARD) == expected


def test_uuidbin_ingest_ids_use_bson_uuid_binary_subtype():
    expected = uuid.UUID("001b7ad3-98da-4d4b-b3c0-7557e97dce22")
    flattener = CompositionFlattener(
        db=None,
        config={"role": "primary", "ids": {"ehr_id": "uuidbin"}},
        mappings_path="",
        mappings_content={"templates": []},
    )

    formatted = flattener._encode_id(str(expected), "ehr_id")

    assert isinstance(formatted, Binary)
    assert formatted.subtype == 4
    assert formatted.as_uuid(UuidRepresentation.STANDARD) == expected
