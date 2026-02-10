from kehrnel.strategies.openehr.rps_dual.ingest.encoding import PathCodec


def test_pathcodec_encode_fullpath_and_coded():
    codec = PathCodec(ar_codes={"openEHR-EHR-COMPOSITION.v1": 12}, at_codes={"at0001": -1}, separator=".")
    chain = ["at0001", "openEHR-EHR-COMPOSITION.v1"]
    coded = codec.encode_path_from_chain(chain, "profile.codedpath")
    assert coded == "-1.12"
    human = codec.encode_path_from_chain(chain, "profile.fullpath")
    assert human == "at0001.openEHR-EHR-COMPOSITION.v1"
    decoded = codec.decode_path(coded, "profile.codedpath")
    assert decoded[0].startswith("at")


def test_pathcodec_shortcuts_roundtrip():
    codec = PathCodec(shortcuts={"data": "d", "value": "v"})
    obj = {"data": {"value": 1}, "other": 2}
    short = codec.shorten_keys(obj)
    assert short == {"d": {"v": 1}, "other": 2}
    expanded = codec.expand_keys(short)
    assert expanded == obj
