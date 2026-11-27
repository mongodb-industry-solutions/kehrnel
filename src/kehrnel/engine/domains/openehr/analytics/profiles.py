"""Explicit source encodings. Never infer code meanings from clinical labels."""

from copy import deepcopy
import json
from pathlib import Path
from .decoder import IBMDecoder, IBMFields

LEGACY_CTTI = {
    "separator": "/",
    "instances": "li",
    "instance_mode": "local_preorder",
    "value_prefix": "$>",
    "at_prefixes": {"D": "at000", "C": "at00", "B": "at0", "A": "at"},
}


def load_ibm_decoder(database, environment, profile):
    if "codes" in profile or "shortcuts" in profile:
        return pinned_decoder(profile)
    name = profile.get("profile", "rps_dual_full_chain_v1")
    dictionary_format = profile.get('dictionary_format', 'ctti_rows' if name == 'ctti_ibm_legacy_v1' else 'kehrnel_documents')
    if name not in ('rps_dual_full_chain_v1', 'ctti_ibm_legacy_v1') or (dictionary_format == 'ctti_rows' and name != 'ctti_ibm_legacy_v1'):
        raise ValueError('Unknown IBM source profile or dictionary layout')
    if dictionary_format == 'ctti_rows':
        code_collection = profile.get("codes_collection", "ArchetypeCodes")
        value_collection = profile.get("values_collection", "CodedValues")
        # Small reference dictionaries are loaded once per worker, never once per fact.
        codes = list(database[code_collection].find({}).limit(100001))
        values = list(database[value_collection].find({}).limit(100001))
        if not codes or not values or max(len(codes), len(values)) > 100000:
            raise ValueError(
                "IBM dictionaries are missing or exceed the configured profile bound"
            )
        selectors = {str(row["_id"]): row["archetypeId"] for row in codes}
        packaged = (
            Path(__file__).resolve().parents[3]
            / "strategies/openehr/rps_dual/bundles/shortcuts/shortcuts.json"
        )
        keys = json.loads(packaged.read_text())["keys"]
        shortcuts = {
            "keys": keys,
            "values": {row["value"]: str(row["_id"]) for row in values},
        }
        fields = IBMFields(**{**LEGACY_CTTI, **profile.get("nodes", {})})
        return IBMDecoder({"selectors": selectors}, shortcuts, fields)
    if name not in ("rps_dual_full_chain_v1", "ctti_ibm_legacy_v1") or dictionary_format != 'kehrnel_documents':
        raise ValueError("Unknown IBM source profile")
    codes = database[profile.get('codes_collection') or environment.get("ANALYTICS_CODES_COLLECTION", "_codes")].find_one(
        {"_id": profile.get("codes_document", "ar_code")}
    )
    shortcuts = database[
        profile.get('values_collection') or environment.get("ANALYTICS_SHORTCUTS_COLLECTION", "_shortcuts")
    ].find_one({"_id": profile.get("shortcuts_document", "shortcuts")})
    if not codes or not shortcuts:
        raise ValueError("The pinned IBM code/shortcut dictionaries were not found")
    return IBMDecoder(
        codes, shortcuts, IBMFields(**{**(LEGACY_CTTI if name == 'ctti_ibm_legacy_v1' else {"value_prefix": ""}), **profile.get("nodes", {})})
    )


def pin_source_profile(config, converter):
    """Export resolved dictionaries into an activation artifact; contains no credentials."""
    result = deepcopy(config)
    if converter.decoder:
        decoder = converter.decoder
        result["source"].update(
            nodes=decoder.fields.__dict__,
            codes={"selectors": decoder.codes},
            shortcuts={
                "keys": {v: k for k, v in decoder.keys.items()},
                "values": {v: k for k, v in decoder.values.items()},
            },
        )
    return result


def pinned_decoder(profile):
    name = profile.get("profile", "rps_dual_full_chain_v1")
    if name not in ("ctti_ibm_legacy_v1", "rps_dual_full_chain_v1"):
        raise ValueError("Unknown IBM source profile")
    if not profile.get("codes") or not profile.get("shortcuts"):
        raise ValueError("Pin both IBM code and shortcut dictionaries")
    fields = {
        **(LEGACY_CTTI if name == "ctti_ibm_legacy_v1" else {"value_prefix": ""}),
        **profile.get("nodes", {}),
    }
    return IBMDecoder(profile["codes"], profile["shortcuts"], IBMFields(**fields))
