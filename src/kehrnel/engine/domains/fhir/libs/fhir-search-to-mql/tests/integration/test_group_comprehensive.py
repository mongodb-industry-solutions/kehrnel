"""
Comprehensive integration tests for ALL Group search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/group-search.html
- https://www.hl7.org/fhir/group-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-patient.html  (Group via member)
- https://www.hl7.org/fhir/compartmentdefinition-practitioner.html
                                                  (Group via managing-entity)
- https://www.hl7.org/fhir/compartmentdefinition-device.html
                                  (Group via member + managing-entity)

This suite exercises the 13 search parameters declared in
``configs/Group.yaml``:

  - String (1):     name
  - Tokens (6):     characteristic, code, exclude, identifier,
                    membership, type
  - References (3): characteristic-reference, managing-entity, member
  - Common (2):     _id, _lastUpdated
  - Token + composite parameters `value` and `characteristic-value`
    are deferred (see Group.yaml §Composites note).

Plus:

- FHIR R5 ``Group.member.entity`` is a Reference targeting ANY of 11
  resource types (the broadest reference in any shipped config).
  ReferenceExtractor's pre-resolved mode walks the array; the
  CompartmentMembershipExtractor's ``reference_type`` filter routes
  Patient/Practitioner/Device entities into the precomputed
  ``_compartments.<Type>`` buckets so reverse-compartment queries
  (``Patient/<id>/Group``, ``Device/<id>/Group``) collapse to a
  single indexed lookup.
- ``Group.characteristic.value[x]`` is polymorphic (CodeableConcept |
  boolean | Quantity | Range | Reference). Only the Reference variant
  is denormalized here (powering the ``characteristic-reference``
  parameter); the others remain on the original resource and would
  be searched via the deferred ``value`` parameter.
- ``Group.characteristic.exclude`` is a boolean — denormalized as
  native bools so token searches with ``tokenType: boolean`` match
  via BSON-typed equality (same pattern Patient ``active`` uses).
- Real MongoDB roundtrip against ``localhost:27017`` exercises the
  multi-compartment fast-path: a single Group resource simultaneously
  participates in Patient, Practitioner, and Device compartments.
"""

from __future__ import annotations

import pytest
from datetime import datetime
from typing import Any, Dict, List

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_group() -> Dict[str, Any]:
    """A Group with most R5 fields populated, including 11-target
    member references and polymorphic characteristic.value[x]."""
    return {
        "resourceType": "Group",
        "id": "grp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "active": True,
        "type": "person",
        "membership": "enumerated",
        "name": "Diabetic Patients Cohort A",
        "quantity": 3,
        "identifier": [
            {"system": "http://hospital.org/cohort", "value": "COHORT-A"},
        ],
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "73211009",
                    "display": "Diabetes mellitus",
                }
            ]
        },
        "managingEntity": {"reference": "Organization/hospital-1"},
        "characteristic": [
            {
                "code": {
                    "coding": [
                        {"system": "http://snomed.info/sct", "code": "73211009"}
                    ]
                },
                "valueCodeableConcept": {
                    "coding": [
                        {"system": "http://hl7.org/fhir/v2-0136", "code": "Y"}
                    ]
                },
                "exclude": False,
            },
            {
                "code": {
                    "coding": [
                        {"system": "http://loinc.org", "code": "46241-6"}
                    ]
                },
                "valueReference": {"reference": "Observation/obs-baseline"},
                "exclude": True,
            },
        ],
        "member": [
            {"entity": {"reference": "Patient/pat-1"}, "inactive": False},
            {"entity": {"reference": "Patient/pat-2"}, "inactive": False},
            {"entity": {"reference": "Patient/pat-3"}, "inactive": True},
            {"entity": {"reference": "Practitioner/dr-jones"}},
            {"entity": {"reference": "Device/dev-pump-A"}},
        ],
    }


@pytest.fixture
def minimal_group() -> Dict[str, Any]:
    """Minimal valid R5 Group: just the required `type` + `membership`
    fields per the spec invariants."""
    return {
        "resourceType": "Group",
        "id": "grp-min",
        "type": "person",
        "membership": "definitional",
    }


# ===========================================================================
# 1) String parameter (1)
# ===========================================================================


class TestGroupStringParameters:
    """name. Default = lowercase starts-with; `:exact` = case-sensitive."""

    def test_name_default_starts_with(self, converter):
        q = converter.convert("Group", "name=diabetic")
        s = str(q)
        assert "name_lower" in s
        assert "diabetic" in s

    def test_name_exact_modifier(self, converter):
        q = converter.convert(
            "Group", "name:exact=Diabetic Patients Cohort A"
        )
        s = str(q)
        assert "name" in s
        assert "Diabetic Patients Cohort A" in s

    def test_name_contains_modifier(self, converter):
        q = converter.convert("Group", "name:contains=cohort")
        assert "cohort" in str(q).lower()


# ===========================================================================
# 2) Token parameters (6)
# ===========================================================================


class TestGroupTokenParameters:
    """characteristic, code, exclude, identifier, membership, type."""

    def test_type_person(self, converter):
        q = converter.convert("Group", "type=person")
        assert q == {"type": "person"} or "person" in str(q)

    def test_type_device(self, converter):
        q = converter.convert("Group", "type=device")
        assert "device" in str(q)

    def test_membership_enumerated(self, converter):
        q = converter.convert("Group", "membership=enumerated")
        assert q == {"membership": "enumerated"} or "enumerated" in str(q)

    def test_membership_definitional(self, converter):
        q = converter.convert("Group", "membership=definitional")
        assert "definitional" in str(q)

    def test_identifier_bare_value(self, converter):
        q = converter.convert("Group", "identifier=COHORT-A")
        s = str(q)
        assert "COHORT-A" in s
        assert "identifier" in s.lower()

    def test_identifier_system_pipe_code(self, converter):
        q = converter.convert(
            "Group",
            "identifier=http://hospital.org/cohort|COHORT-A",
        )
        assert "COHORT-A" in str(q)

    def test_code_token(self, converter):
        q = converter.convert("Group", "code=73211009")
        s = str(q)
        assert "73211009" in s
        assert "code" in s.lower()

    def test_characteristic_token(self, converter):
        q = converter.convert("Group", "characteristic=73211009")
        s = str(q)
        assert "73211009" in s
        assert "characteristic" in s.lower()

    def test_exclude_true_coerces_to_bool(self, converter):
        # FHIR R5 boolean token — the converter MUST emit Python bool
        # so it matches the BSON-typed denormalized value (string
        # "true" wouldn't match `_search.exclude_values: true`).
        q = converter.convert("Group", "exclude=true")
        assert q == {"_search.exclude_values": True}

    def test_exclude_false_coerces_to_bool(self, converter):
        q = converter.convert("Group", "exclude=false")
        assert q == {"_search.exclude_values": False}

    def test_id_search_parameter(self, converter):
        q = converter.convert("Group", "_id=grp-rich")
        assert "grp-rich" in str(q)


# ===========================================================================
# 3) Reference parameters (3)
# ===========================================================================


class TestGroupReferenceParameters:
    """characteristic-reference, managing-entity, member."""

    def test_managing_entity_reference(self, converter):
        q = converter.convert("Group", "managing-entity=hospital-1")
        s = str(q)
        assert "managingEntity" in s
        assert "hospital-1" in s

    def test_managing_entity_typed_reference(self, converter):
        q = converter.convert(
            "Group", "managing-entity=Organization/hospital-1"
        )
        assert "hospital-1" in str(q)

    def test_member_reference_bare(self, converter):
        q = converter.convert("Group", "member=pat-1")
        s = str(q)
        assert "memberIds" in s
        assert "pat-1" in s

    def test_member_reference_typed_patient(self, converter):
        q = converter.convert("Group", "member=Patient/pat-1")
        s = str(q)
        assert "pat-1" in s

    def test_member_reference_typed_practitioner(self, converter):
        # Same parameter, different target type — `member` accepts any
        # of the 11 R5 target types.
        q = converter.convert("Group", "member=Practitioner/dr-jones")
        assert "dr-jones" in str(q)

    def test_member_reference_typed_device(self, converter):
        q = converter.convert("Group", "member=Device/dev-pump-A")
        assert "dev-pump-A" in str(q)

    def test_characteristic_reference(self, converter):
        q = converter.convert(
            "Group", "characteristic-reference=obs-baseline"
        )
        s = str(q)
        assert "characteristicRef" in s
        assert "obs-baseline" in s


# ===========================================================================
# 4) Date parameter (_lastUpdated)
# ===========================================================================


class TestGroupDateParameters:
    """_lastUpdated common date parameter — full prefix support."""

    def test_lastupdated_eq(self, converter):
        q = converter.convert("Group", "_lastUpdated=2024-08-22")
        s = str(q)
        assert "lastupdated" in s.lower()

    def test_lastupdated_ge(self, converter):
        q = converter.convert("Group", "_lastUpdated=ge2024-01-01")
        s = str(q)
        assert "$gte" in s
        assert "2024" in s

    def test_lastupdated_lt(self, converter):
        q = converter.convert("Group", "_lastUpdated=lt2025-01-01")
        s = str(q)
        assert "$lt" in s


# ===========================================================================
# 5) Modifiers
# ===========================================================================


class TestGroupModifiers:
    """`:exact`, `:contains`, `:not`, `:missing` modifier coverage."""

    def test_name_contains(self, converter):
        q = converter.convert("Group", "name:contains=cohort")
        assert "cohort" in str(q).lower()

    def test_type_missing_true(self, converter):
        q = converter.convert("Group", "type:missing=true")
        s = str(q)
        assert "type" in s.lower()
        assert "$exists" in s or "$eq" in s or "null" in s.lower()

    def test_code_not_modifier(self, converter):
        q = converter.convert("Group", "code:not=73211009")
        s = str(q)
        assert "73211009" in s
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_member_typed_resource_modifier(self, converter):
        # FHIR R5 reference parameters disallow `:not` (per
        # http://hl7.org/fhir/search.html#modifiers — `:not` is only
        # valid on token-shaped params). The valid disambiguator for
        # multi-target references like `member` is the typed-resource
        # modifier `:Patient` / `:Device` / etc., which restricts the
        # match to a specific target type.
        q = converter.convert("Group", "member:Patient=pat-1")
        s = str(q)
        assert "pat-1" in s


# ===========================================================================
# 6) Combined queries
# ===========================================================================


class TestGroupCombinations:
    """Multi-parameter queries combining strings, tokens, refs."""

    def test_type_and_membership(self, converter):
        q = converter.convert("Group", "type=person&membership=enumerated")
        s = str(q)
        assert "person" in s
        assert "enumerated" in s

    def test_type_and_code_and_member(self, converter):
        q = converter.convert(
            "Group",
            "type=person&code=73211009&member=Patient/pat-1",
        )
        s = str(q)
        assert "person" in s
        assert "73211009" in s
        assert "pat-1" in s

    def test_managing_entity_and_exclude(self, converter):
        q = converter.convert(
            "Group", "managing-entity=hospital-1&exclude=false"
        )
        s = str(q)
        assert "hospital-1" in s
        assert "False" in s or "false" in s.lower()


# ===========================================================================
# 7) Denormalization correctness
# ===========================================================================


class TestGroupDenormalization:
    """Verifies the shape of the `_search` document for the rich fixture."""

    def test_top_level_scalars_preserved(self, denormalizer, rich_group):
        out = denormalizer.denormalize(rich_group)
        assert out["type"] == "person"
        assert out["membership"] == "enumerated"

    def test_identifier_denormalization(self, denormalizer, rich_group):
        out = denormalizer.denormalize(rich_group)["_search"]
        assert "COHORT-A" in out["identifier_values"]
        sysvals = " ".join(out["identifier_systemCode"])
        assert "COHORT-A" in sysvals

    def test_code_codeable_concept(self, denormalizer, rich_group):
        out = denormalizer.denormalize(rich_group)["_search"]
        assert "73211009" in out["code_codes"]
        sysvals = " ".join(out["code_systemCode"])
        assert "73211009" in sysvals

    def test_characteristic_unions_across_entries(
        self, denormalizer, rich_group
    ):
        # FHIR R5 `characteristic` is an array — the extractor walks
        # `characteristic[*].code.coding[*]` and unions every entry.
        out = denormalizer.denormalize(rich_group)["_search"]
        assert "73211009" in out["characteristic_codes"]
        assert "46241-6" in out["characteristic_codes"]

    def test_exclude_native_booleans(self, denormalizer, rich_group):
        # FHIR R5 `Group.characteristic.exclude` is a boolean per entry.
        # We denormalize as native bools so `tokenType: boolean`
        # queries match via BSON-typed equality.
        out = denormalizer.denormalize(rich_group)["_search"]
        assert out["exclude_values"] == [False, True]
        assert all(isinstance(v, bool) for v in out["exclude_values"])

    def test_name_case_preserved_and_lowercased(
        self, denormalizer, rich_group
    ):
        out = denormalizer.denormalize(rich_group)["_search"]
        assert out["name"] == "Diabetic Patients Cohort A"
        assert out["name_lower"] == "diabetic patients cohort a"

    def test_managing_entity_reference(self, denormalizer, rich_group):
        out = denormalizer.denormalize(rich_group)["_search"]
        assert out["managingEntityId"] == "hospital-1"
        assert out["managingEntityType"] == "Organization"

    def test_member_eleven_target_types(self, denormalizer, rich_group):
        # Cohort spans Patient (3), Practitioner (1), Device (1).
        out = denormalizer.denormalize(rich_group)["_search"]
        assert out["memberIds"] == [
            "pat-1", "pat-2", "pat-3", "dr-jones", "dev-pump-A"
        ]
        assert out["memberTypes"] == [
            "Patient", "Patient", "Patient", "Practitioner", "Device"
        ]

    def test_characteristic_reference_only_picks_reference_variant(
        self, denormalizer, rich_group
    ):
        # FHIR R5 `characteristic.value[x]` is polymorphic. Only the
        # `valueReference` entries should land in
        # `_search.characteristicRefId`; `valueCodeableConcept`
        # entries are NOT swept up.
        out = denormalizer.denormalize(rich_group)["_search"]
        assert out["characteristicRefId"] == ["obs-baseline"]
        assert out["characteristicRefType"] == ["Observation"]

    def test_minimal_group_sparse_output(
        self, denormalizer, minimal_group
    ):
        out = denormalizer.denormalize(minimal_group)
        search = out.get("_search", {})
        # No projections should be created when source fields are absent.
        for f in (
            "identifier_values", "code_codes", "characteristic_codes",
            "exclude_values", "name", "memberIds", "managingEntityId",
        ):
            assert f not in search


# ===========================================================================
# 8) Resource purity
# ===========================================================================


class TestGroupResourcePurity:
    """Denormalization MUST NOT mutate the original FHIR resource —
    only `_search` and `_compartments` may be added at the root."""

    def test_root_keys_are_resource_plus_buckets_only(
        self, denormalizer, rich_group
    ):
        original_keys = set(rich_group.keys())
        out = denormalizer.denormalize(rich_group)
        added = set(out.keys()) - original_keys
        assert added.issubset({"_search", "_compartments"}), (
            f"Denormalization added unexpected root keys: {added}"
        )

    def test_no_search_subfields_injected_at_root(
        self, denormalizer, rich_group
    ):
        out = denormalizer.denormalize(rich_group)
        # Projection-only fields must never escape `_search`. NOTE:
        # `name` is the legitimate FHIR root field; the denormalizer's
        # write goes to `_search.name`, not the root, but we can't
        # assert "name not in out" because the original resource has
        # `name` at root. The full purity contract is enforced by
        # `test_real_fhir_fields_preserved_unchanged`.
        forbidden = {
            "identifier_values", "identifier_systemCode",
            "code_codes", "code_systemCode",
            "characteristic_codes", "characteristic_systemCode",
            "exclude_values", "name_lower",
            "memberIds", "memberTypes",
            "managingEntityId", "managingEntityType",
            "characteristicRefId", "characteristicRefType",
        }
        for f in forbidden:
            assert f not in out, (
                f"Denormalization leaked '{f}' to the resource root"
            )

    def test_real_fhir_fields_preserved_unchanged(
        self, denormalizer, rich_group
    ):
        out = denormalizer.denormalize(rich_group)
        for field in (
            "active", "type", "membership", "name", "quantity",
            "identifier", "code", "managingEntity",
            "characteristic", "member",
        ):
            assert out[field] == rich_group[field], (
                f"FHIR field '{field}' was mutated by denormalization"
            )

    def test_input_dict_not_mutated_in_place(
        self, denormalizer, rich_group
    ):
        import copy

        original = copy.deepcopy(rich_group)
        denormalizer.denormalize(rich_group)
        assert rich_group == original, "denormalize() mutated input"


# ===========================================================================
# 9) Multi-compartment fast-path routing
# ===========================================================================


class TestGroupMultiCompartmentRouting:
    """Group is in 3 compartments per R5 (Patient, Practitioner, Device).
    All three are precomputed via `_compartments.<Type>` for fast
    indexed lookups when querying `<Compartment>/<id>/Group`."""

    def test_patient_compartment_populated_from_member(
        self, denormalizer, rich_group
    ):
        # `Group.member.entity` filtered to Patient/* refs only.
        out = denormalizer.denormalize(rich_group)
        patients = sorted(out["_compartments"]["Patient"])
        assert patients == ["pat-1", "pat-2", "pat-3"]

    def test_device_compartment_populated_from_member(
        self, denormalizer, rich_group
    ):
        out = denormalizer.denormalize(rich_group)
        # The fixture has one Device member; the Practitioner-targeted
        # managingEntity is correctly filtered out.
        assert out["_compartments"]["Device"] == ["dev-pump-A"]

    def test_practitioner_compartment_empty_when_org_managing(
        self, denormalizer, rich_group
    ):
        # The fixture's managingEntity is Organization/hospital-1, NOT
        # a Practitioner — so `_compartments.Practitioner` should be
        # ABSENT from output (sparse-output contract).
        out = denormalizer.denormalize(rich_group)
        assert "Practitioner" not in out["_compartments"]

    def test_practitioner_compartment_when_practitioner_managing(
        self, denormalizer
    ):
        g = {
            "resourceType": "Group",
            "id": "grp-pr",
            "type": "person",
            "membership": "definitional",
            "managingEntity": {"reference": "Practitioner/dr-jones"},
        }
        out = denormalizer.denormalize(g)
        assert out["_compartments"]["Practitioner"] == ["dr-jones"]

    def test_compartment_query_patient_uses_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Group"
        )
        assert q == {"_compartments.Patient": "pat-1"}

    def test_compartment_query_practitioner_uses_fast_path(
        self, converter
    ):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Group"
        )
        assert q == {"_compartments.Practitioner": "dr-jones"}

    def test_compartment_query_device_uses_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Device", "dev-1", "Group"
        )
        assert q == {"_compartments.Device": "dev-1"}

    def test_compartment_query_no_or_when_precomputed(self, converter):
        # All three compartments use the fast-path so NONE of the
        # generated queries should fall back to the dynamic `$or` over
        # linking parameters.
        for ctype, cid in [
            ("Patient", "p1"), ("Practitioner", "pr1"), ("Device", "d1")
        ]:
            q = converter.convert_with_compartment(ctype, cid, "Group")
            assert "$or" not in str(q), (
                f"{ctype} compartment fell back to dynamic resolution"
            )


# ===========================================================================
# 10) MongoDB end-to-end
# ===========================================================================


def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://localhost:27017", serverSelectionTimeoutMS=1500
        )
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not running on localhost:27017",
)
class TestGroupMongoDB:
    """Seed denormalized Groups into a MongoDB collection, generate
    MQL via the converter, execute, assert the right docs come back.
    Validates the multi-compartment fast-path end-to-end."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["groups_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        groups: List[Dict[str, Any]] = [
            {
                "resourceType": "Group",
                "id": "grp-cohort-A",
                "active": True,
                "type": "person",
                "membership": "enumerated",
                "name": "Diabetic Cohort A",
                "identifier": [
                    {"system": "http://hospital.org/cohort", "value": "COHORT-A"}
                ],
                "code": {
                    "coding": [
                        {"system": "http://snomed.info/sct", "code": "73211009"}
                    ]
                },
                "managingEntity": {"reference": "Organization/hospital-1"},
                "characteristic": [
                    {
                        "code": {
                            "coding": [
                                {"system": "http://snomed.info/sct", "code": "73211009"}
                            ]
                        },
                        "valueCodeableConcept": {
                            "coding": [{"code": "Y"}]
                        },
                        "exclude": False,
                    }
                ],
                "member": [
                    {"entity": {"reference": "Patient/pat-1"}},
                    {"entity": {"reference": "Patient/pat-2"}},
                ],
            },
            {
                "resourceType": "Group",
                "id": "grp-cohort-B",
                "active": True,
                "type": "person",
                "membership": "enumerated",
                "name": "Hypertension Cohort B",
                "identifier": [
                    {"system": "http://hospital.org/cohort", "value": "COHORT-B"}
                ],
                "code": {
                    "coding": [
                        {"system": "http://snomed.info/sct", "code": "38341003"}
                    ]
                },
                "managingEntity": {"reference": "Practitioner/dr-jones"},
                "characteristic": [
                    {
                        "code": {
                            "coding": [
                                {"system": "http://snomed.info/sct", "code": "38341003"}
                            ]
                        },
                        "exclude": True,
                    }
                ],
                "member": [
                    {"entity": {"reference": "Patient/pat-2"}},
                    {"entity": {"reference": "Patient/pat-3"}},
                ],
            },
            {
                "resourceType": "Group",
                "id": "grp-device-fleet",
                "active": True,
                "type": "device",
                "membership": "enumerated",
                "name": "ICU Pump Fleet",
                "managingEntity": {"reference": "Organization/hospital-1"},
                "member": [
                    {"entity": {"reference": "Device/dev-pump-A"}},
                    {"entity": {"reference": "Device/dev-pump-B"}},
                ],
            },
            {
                "resourceType": "Group",
                "id": "grp-defn-only",
                "active": True,
                "type": "person",
                "membership": "definitional",
                "name": "Definitional Group",
                "characteristic": [
                    {
                        "code": {
                            "coding": [
                                {"system": "http://example.org", "code": "trait-x"}
                            ]
                        },
                        "valueBoolean": True,
                        "exclude": False,
                    }
                ],
            },
        ]
        denorm = [denormalizer.denormalize(g) for g in groups]
        mongo_collection.insert_many(denorm)
        return mongo_collection

    # --- single-param queries ---

    def test_query_type_person(self, seeded, converter):
        q = converter.convert("Group", "type=person")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A", "grp-cohort-B", "grp-defn-only"}

    def test_query_type_device(self, seeded, converter):
        q = converter.convert("Group", "type=device")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-device-fleet"}

    def test_query_membership_enumerated(self, seeded, converter):
        q = converter.convert("Group", "membership=enumerated")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {
            "grp-cohort-A", "grp-cohort-B", "grp-device-fleet"
        }

    def test_query_membership_definitional(self, seeded, converter):
        q = converter.convert("Group", "membership=definitional")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-defn-only"}

    def test_query_identifier(self, seeded, converter):
        q = converter.convert("Group", "identifier=COHORT-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A"}

    def test_query_code(self, seeded, converter):
        q = converter.convert("Group", "code=73211009")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A"}

    def test_query_characteristic(self, seeded, converter):
        q = converter.convert("Group", "characteristic=38341003")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-B"}

    def test_query_exclude_true(self, seeded, converter):
        # FHIR R5 boolean token — Group.characteristic.exclude. Native
        # bool query operand against BSON-typed denormalized values.
        q = converter.convert("Group", "exclude=true")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-B"}

    def test_query_exclude_false(self, seeded, converter):
        q = converter.convert("Group", "exclude=false")
        ids = {r["id"] for r in seeded.find(q)}
        # cohort-A and defn-only both have exclude=false entries.
        assert ids == {"grp-cohort-A", "grp-defn-only"}

    def test_query_name_default(self, seeded, converter):
        q = converter.convert("Group", "name=diabetic")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A"}

    def test_query_name_exact_case_sensitive(self, seeded, converter):
        q = converter.convert(
            "Group", "name:exact=Hypertension Cohort B"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-B"}

    def test_query_managing_entity_organization(self, seeded, converter):
        q = converter.convert("Group", "managing-entity=hospital-1")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A", "grp-device-fleet"}

    def test_query_managing_entity_practitioner(
        self, seeded, converter
    ):
        q = converter.convert(
            "Group", "managing-entity=Practitioner/dr-jones"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-B"}

    def test_query_member_patient(self, seeded, converter):
        q = converter.convert("Group", "member=pat-2")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A", "grp-cohort-B"}

    def test_query_member_device(self, seeded, converter):
        q = converter.convert("Group", "member=dev-pump-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-device-fleet"}

    # --- multi-compartment fast-path queries ---

    def test_compartment_patient_finds_groups_with_member(
        self, seeded, converter
    ):
        # Patient/pat-2/Group should return both cohorts that include
        # pat-2 as a member (precomputed `_compartments.Patient`).
        q = converter.convert_with_compartment(
            "Patient", "pat-2", "Group"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A", "grp-cohort-B"}

    def test_compartment_patient_unique_member(self, seeded, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Group"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A"}

    def test_compartment_practitioner_via_managing_entity(
        self, seeded, converter
    ):
        # Practitioner/dr-jones/Group — only cohort-B is managed by
        # the practitioner.
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Group"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-B"}

    def test_compartment_device_via_member(self, seeded, converter):
        q = converter.convert_with_compartment(
            "Device", "dev-pump-A", "Group"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-device-fleet"}

    def test_compartment_unknown_id_returns_empty(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Patient", "pat-nonexistent", "Group"
        )
        assert list(seeded.find(q)) == []

    # --- combined queries ---

    def test_compartment_patient_plus_code_filter(
        self, seeded, converter
    ):
        compartment_q = converter.convert_with_compartment(
            "Patient", "pat-2", "Group"
        )
        code_q = converter.convert("Group", "code=38341003")
        combined = {"$and": [compartment_q, code_q]}
        ids = {r["id"] for r in seeded.find(combined)}
        assert ids == {"grp-cohort-B"}

    def test_member_and_type_intersection(self, seeded, converter):
        q = converter.convert("Group", "member=pat-2&type=person")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"grp-cohort-A", "grp-cohort-B"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
