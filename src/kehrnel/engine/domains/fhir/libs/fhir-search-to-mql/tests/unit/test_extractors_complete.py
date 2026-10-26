"""Unit tests for every FHIR field extractor.

Targets the lowest-coverage extractors first (period, quantity, extension,
dosage, timing, coding, age_duration, money, availability, ratio,
ratio_range, range_extractor, contact_point, codeable_concept, identifier,
reference, address, human_name) and exercises both the
field_mappings-driven path and the default (no field_mappings) path.

The ResourceDenormalizer hosts the canonical extractor registry; pulling
it via that registry also exercises ResourceDenormalizer.EXTRACTORS lookup.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.extractors import (
    AddressExtractor,
    AgeDurationExtractor,
    AvailabilityExtractor,
    CodeableConceptExtractor,
    CodingExtractor,
    ContactPointExtractor,
    DirectFieldExtractor,
    DosageExtractor,
    ExtensionExtractor,
    HumanNameExtractor,
    IdentifierExtractor,
    MoneyExtractor,
    PeriodExtractor,
    PhoneticExtractor,
    QuantityExtractor,
    RangeExtractor,
    RatioExtractor,
    RatioRangeExtractor,
    ReferenceExtractor,
    TimingExtractor,
)
from fhir_search_to_mql.denormalizer.resource_denormalizer import ResourceDenormalizer


pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Base FieldExtractor utilities
# ---------------------------------------------------------------------------


class TestFieldExtractorBase:
    """Cover utility helpers in FieldExtractor that all extractors depend on."""

    def test_ensure_list_with_none(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._ensure_list(None) == []

    def test_ensure_list_with_scalar(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._ensure_list({"a": 1}) == [{"a": 1}]

    def test_ensure_list_with_list(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._ensure_list([1, 2, 3]) == [1, 2, 3]

    def test_extract_field_dot_path(self) -> None:
        extractor = HumanNameExtractor()
        obj = {"a": {"b": {"c": "value"}}}
        assert extractor._extract_field(obj, "a.b.c") == "value"

    def test_extract_field_missing(self) -> None:
        extractor = HumanNameExtractor()
        obj = {"a": {"b": {}}}
        assert extractor._extract_field(obj, "a.b.c", default="missing") == "missing"

    def test_extract_field_partial_non_dict(self) -> None:
        extractor = HumanNameExtractor()
        obj = {"a": "not-a-dict"}
        assert extractor._extract_field(obj, "a.b.c", default=None) is None

    def test_apply_transformation_lowercase(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._apply_transformation("ABC", "lowercase") == "abc"

    def test_apply_transformation_uppercase(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._apply_transformation("abc", "uppercase") == "ABC"

    def test_apply_transformation_empty_value(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._apply_transformation("", "lowercase") == ""

    def test_apply_transformation_unknown(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._apply_transformation("abc", "weird") == "abc"

    def test_apply_transformation_none(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor._apply_transformation("abc", None) == "abc"

    def test_validate_value_present(self) -> None:
        extractor = HumanNameExtractor()
        assert extractor.validate({"family": "Smith"}) is True
        assert extractor.validate(None) is False

    def test_cannot_instantiate_abstract_directly(self) -> None:
        with pytest.raises(TypeError):
            FieldExtractor()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# PeriodExtractor
# ---------------------------------------------------------------------------


class TestPeriodExtractor:
    """Period: start/end with field_mappings, defaults, datatype variants."""

    def setup_method(self) -> None:
        self.extractor = PeriodExtractor()

    def test_empty_input_returns_empty_dict(self) -> None:
        assert self.extractor.extract(None) == {}
        assert self.extractor.extract([]) == {}

    def test_default_extraction_single_period(self) -> None:
        result = self.extractor.extract({"start": "2024-01-01", "end": "2024-12-31"})
        assert result == {"start": "2024-01-01", "end": "2024-12-31"}

    def test_default_extraction_multiple_periods(self) -> None:
        result = self.extractor.extract([
            {"start": "2024-01-01", "end": "2024-06-30"},
            {"start": "2024-07-01", "end": "2024-12-31"},
        ])
        assert result["start"] == ["2024-01-01", "2024-07-01"]
        assert result["end"] == ["2024-06-30", "2024-12-31"]

    def test_field_mappings_array_datatype(self) -> None:
        mappings = [
            {"source_path": "period.start", "target_field": "starts", "datatype": "array[string]"},
            {"source_path": "period.end", "target_field": "ends", "datatype": "array[string]"},
        ]
        result = self.extractor.extract(
            [{"start": "2024-01-01", "end": "2024-06-30"}], field_mappings=mappings
        )
        assert result["starts"] == ["2024-01-01"]
        assert result["ends"] == ["2024-06-30"]

    def test_field_mappings_scalar_datatype_single_value(self) -> None:
        mappings = [{"source_path": "period.start", "target_field": "start", "datatype": "string"}]
        result = self.extractor.extract({"start": "2024-01-01"}, field_mappings=mappings)
        assert result["start"] == "2024-01-01"

    def test_field_mappings_scalar_datatype_multiple_values(self) -> None:
        mappings = [{"source_path": "period.start", "target_field": "start", "datatype": "string"}]
        result = self.extractor.extract(
            [{"start": "2024-01-01"}, {"start": "2024-02-01"}], field_mappings=mappings
        )
        assert result["start"] == ["2024-01-01", "2024-02-01"]

    def test_field_mappings_no_matching_data_is_sparse(self) -> None:
        """
        When the source side has nothing to extract for a target,
        the extractor MUST omit that target from the result. The
        prior contract wrote ``None``, which then tripped
        ``ResourceDenormalizer``'s validator with "expected string,
        got NoneType" on every Appointment that lacked an ``end``.
        """
        mappings = [{"source_path": "period.start", "target_field": "start", "datatype": "string"}]
        result = self.extractor.extract({"end": "2024-01-01"}, field_mappings=mappings)
        assert "start" not in result

    def test_field_mappings_unrecognized_source_defaults_to_start(self) -> None:
        mappings = [{"source_path": "weird.path", "target_field": "x", "datatype": "string"}]
        result = self.extractor.extract({"start": "2024-01-01"}, field_mappings=mappings)
        assert result["x"] == "2024-01-01"

    def test_field_mappings_skips_when_missing_target(self) -> None:
        mappings = [{"source_path": "period.start"}]
        result = self.extractor.extract({"start": "2024-01-01"}, field_mappings=mappings)
        assert result == {}

    def test_skips_non_dict_entries(self) -> None:
        result = self.extractor.extract(["not-a-dict", {"start": "2024-01-01"}])
        assert result["start"] == "2024-01-01"

    def test_resource_rooted_synthesizes_period_from_top_level_scalars(self) -> None:
        """
        With ``source: $resource`` in the YAML rule, the extractor
        receives the full FHIR resource. ``Appointment.start`` /
        ``Appointment.end`` are top-level scalars (not a nested
        Period), but the iteration logic treats them as a synthetic
        single-period dict — exactly what the bundled
        ``Appointment.yaml`` ``period`` rule needs.
        """
        resource = {
            "resourceType": "Appointment",
            "id": "appt-1",
            "start": "2024-06-20T09:00:00Z",
            "end": "2024-06-20T10:00:00Z",
        }
        mappings = [
            {"source_path": "start", "target_field": "appointmentPeriod.start", "datatype": "string"},
            {"source_path": "end", "target_field": "appointmentPeriod.end", "datatype": "string"},
        ]
        result = self.extractor.extract(resource, field_mappings=mappings)
        assert result["appointmentPeriod.start"] == "2024-06-20T09:00:00Z"
        assert result["appointmentPeriod.end"] == "2024-06-20T10:00:00Z"

    def test_resource_rooted_omits_missing_end(self) -> None:
        """An Appointment with ``start`` but no ``end`` must not write a None end."""
        resource = {"resourceType": "Appointment", "id": "appt-1", "start": "2024-07-01T08:00:00Z"}
        mappings = [
            {"source_path": "start", "target_field": "appointmentPeriod.start", "datatype": "string"},
            {"source_path": "end", "target_field": "appointmentPeriod.end", "datatype": "string"},
        ]
        result = self.extractor.extract(resource, field_mappings=mappings)
        assert result["appointmentPeriod.start"] == "2024-07-01T08:00:00Z"
        assert "appointmentPeriod.end" not in result


# ---------------------------------------------------------------------------
# QuantityExtractor
# ---------------------------------------------------------------------------


class TestQuantityExtractor:
    """Quantity: single object vs array, optional fields."""

    def setup_method(self) -> None:
        self.extractor = QuantityExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}
        assert self.extractor.extract([]) == {}

    def test_single_quantity_returns_object(self) -> None:
        result = self.extractor.extract(
            {"value": 120, "unit": "mmHg", "system": "http://unitsofmeasure.org", "code": "mm[Hg]"}
        )
        assert result == {
            "value": 120,
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]",
        }

    def test_single_quantity_with_comparator(self) -> None:
        result = self.extractor.extract({"value": 5, "unit": "mg", "comparator": ">"})
        assert result["comparator"] == ">"

    def test_multiple_quantities_returns_array(self) -> None:
        result = self.extractor.extract([
            {"value": 1, "unit": "mg"},
            {"value": 2, "unit": "ml"},
        ])
        assert result == {
            "quantities": [
                {"value": 1, "unit": "mg"},
                {"value": 2, "unit": "ml"},
            ]
        }

    def test_skips_non_dict_entries(self) -> None:
        result = self.extractor.extract(["not-a-dict", {"value": 5, "unit": "mg"}, "junk"])
        assert "quantities" in result
        assert result["quantities"] == [{"value": 5, "unit": "mg"}]

    def test_single_empty_dict_returns_empty(self) -> None:
        assert self.extractor.extract({}) == {}

    def test_multiple_with_only_invalid_returns_empty(self) -> None:
        assert self.extractor.extract(["a", "b", "c"]) == {}


# ---------------------------------------------------------------------------
# ExtensionExtractor
# ---------------------------------------------------------------------------


class TestExtensionExtractor:
    def setup_method(self) -> None:
        self.extractor = ExtensionExtractor()

    def test_empty_input(self) -> None:
        assert self.extractor.extract(None) == {}
        assert self.extractor.extract([]) == {}

    def test_default_extraction_collects_all_value_types(self) -> None:
        extensions = [
            {"url": "http://x/string", "valueString": "hello"},
            {"url": "http://x/int", "valueInteger": 42},
            {"url": "http://x/bool", "valueBoolean": True},
            {"url": "http://x/code", "valueCode": "active"},
            {"url": "http://x/uri", "valueUri": "http://example.com/uri"},
        ]
        result = self.extractor.extract(extensions)
        assert "http://x/string" in result["extensionUrls"]
        assert result["extensionStringValues"] == ["hello"]
        assert result["extensionIntegerValues"] == [42]
        assert result["extensionBooleanValues"] == [True]
        assert result["extensionCodeValues"] == ["active"]
        assert result["extensionsByUrl"]["http://x/string"] == ["hello"]
        assert result["extensionsByUrl"]["http://x/uri"] == ["http://example.com/uri"]

    def test_field_mappings_extension_url_filter(self) -> None:
        extensions = [
            {"url": "http://x/race", "valueString": "asian"},
            {"url": "http://x/ethnicity", "valueString": "non-hispanic"},
        ]
        mappings = [
            {
                "target_field": "race",
                "source_path": "extension.valueString",
                "extension_url": "http://x/race",
            }
        ]
        result = self.extractor.extract(extensions, field_mappings=mappings)
        assert result["race"] == ["asian"]

    @pytest.mark.parametrize(
        "source_path,extracted_field",
        [
            ("extension.url", "urls"),
            ("extension.valueString", "strings"),
            ("extension.valueInteger", "ints"),
            ("extension.valueBoolean", "bools"),
            ("extension.valueCode", "codes"),
            ("extension.valueUri", "uris"),
        ],
    )
    def test_field_mappings_by_source_path(self, source_path: str, extracted_field: str) -> None:
        extensions = [
            {"url": "http://x/u", "valueString": "s"},
            {"url": "http://x/v", "valueInteger": 1},
            {"url": "http://x/w", "valueBoolean": False},
            {"url": "http://x/y", "valueCode": "c"},
            {"url": "http://x/z", "valueUri": "http://uri"},
        ]
        mappings = [{"target_field": extracted_field, "source_path": source_path}]
        result = self.extractor.extract(extensions, field_mappings=mappings)
        assert extracted_field in result

    def test_skips_non_dict_extensions(self) -> None:
        result = self.extractor.extract(["junk", {"url": "u", "valueString": "v"}])
        assert result["extensionStringValues"] == ["v"]

    def test_field_mappings_skips_when_missing_target(self) -> None:
        result = self.extractor.extract(
            [{"url": "u", "valueString": "v"}],
            field_mappings=[{"source_path": "extension.url"}],
        )
        assert result == {}


# ---------------------------------------------------------------------------
# DosageExtractor
# ---------------------------------------------------------------------------


class TestDosageExtractor:
    def setup_method(self) -> None:
        self.extractor = DosageExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}
        assert self.extractor.extract([]) == {}

    def test_default_extraction_full_dosage(self) -> None:
        dosage = {
            "text": "Take twice daily",
            "route": {
                "text": "Oral",
                "coding": [{"system": "http://snomed", "code": "26643006"}],
            },
            "method": {
                "text": "Swallow",
                "coding": [{"system": "http://snomed", "code": "421521009"}],
            },
            "timing": {"event": ["08:00", "20:00"]},
            "doseQuantity": {"value": 500, "unit": "mg"},
            "rateQuantity": {"value": 100, "unit": "mL/h"},
        }
        result = self.extractor.extract(dosage)
        assert result["dosageText"] == ["Take twice daily"]
        assert result["dosageRoute"] == ["Oral"]
        assert result["dosageRouteCodes"] == ["26643006"]
        assert result["dosageMethod"] == ["Swallow"]
        assert result["dosageMethodCodes"] == ["421521009"]
        assert result["dosageTimingEvents"] == ["08:00", "20:00"]
        assert result["dosageDoseValue"] == [500]
        assert result["dosageDoseUnit"] == ["mg"]

    def test_timing_event_scalar(self) -> None:
        result = self.extractor.extract({"timing": {"event": "08:00"}})
        assert result["dosageTimingEvents"] == ["08:00"]

    def test_field_mappings_route_codes(self) -> None:
        dosage = {
            "route": {"coding": [{"system": "http://snomed", "code": "26643006"}]},
        }
        mappings = [
            {"target_field": "route_code", "source_path": "dosage.route.coding.code"},
        ]
        result = self.extractor.extract(dosage, field_mappings=mappings)
        assert result["route_code"] == ["26643006"]

    def test_field_mappings_dose_value(self) -> None:
        dosage = {"doseQuantity": {"value": 100, "unit": "mg"}}
        mappings = [
            {"target_field": "doseValue", "source_path": "dosage.doseQuantity.value"},
            {"target_field": "doseUnit", "source_path": "dosage.doseQuantity.unit"},
        ]
        result = self.extractor.extract(dosage, field_mappings=mappings)
        assert result["doseValue"] == [100]
        assert result["doseUnit"] == ["mg"]

    def test_field_mappings_rate_value(self) -> None:
        dosage = {"rateQuantity": {"value": 50, "unit": "mL/h"}}
        mappings = [
            {"target_field": "rateValue", "source_path": "dosage.rateQuantity.value"},
            {"target_field": "rateUnit", "source_path": "dosage.rateQuantity.unit"},
        ]
        result = self.extractor.extract(dosage, field_mappings=mappings)
        assert result["rateValue"] == [50]
        assert result["rateUnit"] == ["mL/h"]

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"text": "x"}])
        assert result["dosageText"] == ["x"]


# ---------------------------------------------------------------------------
# TimingExtractor
# ---------------------------------------------------------------------------


class TestTimingExtractor:
    def setup_method(self) -> None:
        self.extractor = TimingExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_full_default_extraction(self) -> None:
        timing = {
            "event": ["2024-01-01", "2024-02-01"],
            "repeat": {
                "boundsPeriod": {"start": "2024-01-01", "end": "2024-12-31"},
                "frequency": 3,
                "period": 1,
                "periodUnit": "d",
            },
            "code": {"coding": [{"system": "http://hl7.org/fhir/timing-abbreviation", "code": "BID"}]},
        }
        result = self.extractor.extract(timing)
        assert result["timingEvents"] == ["2024-01-01", "2024-02-01"]
        assert result["timingBoundsStart"] == "2024-01-01"
        assert result["timingBoundsEnd"] == "2024-12-31"
        assert result["timingFrequencies"] == [3]
        assert result["timingPeriods"] == [1]
        assert result["timingPeriodUnits"] == ["d"]
        assert result["timingCodes"] == ["BID"]

    def test_event_scalar(self) -> None:
        result = self.extractor.extract({"event": "2024-01-01"})
        assert result["timingEvents"] == ["2024-01-01"]

    def test_field_mappings_each_aspect(self) -> None:
        timing = {
            "event": ["e1"],
            "repeat": {
                "boundsPeriod": {"start": "s", "end": "e"},
                "frequency": 1,
                "period": 1,
                "periodUnit": "d",
            },
            "code": {"coding": [{"code": "BID"}]},
        }
        mappings = [
            {"target_field": "events", "source_path": "timing.event"},
            {"target_field": "bStart", "source_path": "timing.repeat.boundsPeriod.start"},
            {"target_field": "bEnd", "source_path": "timing.repeat.boundsPeriod.end"},
            {"target_field": "freq", "source_path": "timing.repeat.frequency"},
            {"target_field": "period", "source_path": "timing.repeat.period"},
            {"target_field": "punit", "source_path": "timing.repeat.periodUnit"},
            {"target_field": "codes", "source_path": "timing.code"},
        ]
        result = self.extractor.extract(timing, field_mappings=mappings)
        assert result["events"] == ["e1"]
        assert result["bStart"] == ["s"]
        assert result["bEnd"] == ["e"]
        assert result["freq"] == [1]
        assert result["period"] == [1]
        assert result["punit"] == ["d"]
        assert result["codes"] == ["BID"]

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"event": ["x"]}])
        assert result["timingEvents"] == ["x"]


# ---------------------------------------------------------------------------
# CodingExtractor
# ---------------------------------------------------------------------------


class TestCodingExtractor:
    def setup_method(self) -> None:
        self.extractor = CodingExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction_with_user_selected_and_version(self) -> None:
        coding = {
            "system": "http://snomed",
            "code": "ABC",
            "display": "ABC display",
            "version": "1.0",
            "userSelected": True,
        }
        result = self.extractor.extract(coding)
        assert result["codingCodes"] == ["ABC"]
        assert result["codingSystems"] == ["http://snomed"]
        assert result["codingSystemValues"] == ["http://snomed|ABC"]
        assert result["codingDisplays"] == ["ABC display"]

    def test_default_extraction_code_without_system(self) -> None:
        result = self.extractor.extract({"code": "X"})
        assert result["codingSystemValues"] == ["|X"]

    def test_field_mappings_each_aspect(self) -> None:
        coding = {"system": "S", "code": "C", "display": "D", "version": "v1", "userSelected": True}
        mappings = [
            {"target_field": "codes", "source_path": "coding.code"},
            {"target_field": "systems", "source_path": "coding.system"},
            {"target_field": "systemValue", "source_path": "coding.system|code"},
            {"target_field": "displays", "source_path": "coding.display"},
            {"target_field": "versions", "source_path": "coding.version"},
            {"target_field": "selected", "source_path": "coding.userSelected"},
        ]
        result = self.extractor.extract(coding, field_mappings=mappings)
        assert result["codes"] == ["C"]
        assert result["systems"] == ["S"]
        assert result["systemValue"] == ["S|C"]
        assert result["displays"] == ["D"]
        assert result["versions"] == ["v1"]
        assert result["selected"] == [True]

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"code": "X"}])
        assert result["codingCodes"] == ["X"]


# ---------------------------------------------------------------------------
# AgeDurationExtractor
# ---------------------------------------------------------------------------


class TestAgeDurationExtractor:
    def setup_method(self) -> None:
        self.extractor = AgeDurationExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction_single(self) -> None:
        result = self.extractor.extract(
            {"value": 30, "unit": "years", "system": "http://unitsofmeasure.org", "code": "a"}
        )
        assert result == {
            "value": 30,
            "unit": "years",
            "system": "http://unitsofmeasure.org",
            "code": "a",
        }

    def test_default_extraction_multiple(self) -> None:
        result = self.extractor.extract([
            {"value": 30, "unit": "y"},
            {"value": 6, "unit": "mo"},
        ])
        assert result["value"] == [30, 6]
        assert result["unit"] == ["y", "mo"]

    def test_field_mappings(self) -> None:
        mappings = [
            {"target_field": "v", "source_path": "age.value"},
            {"target_field": "u", "source_path": "age.unit"},
            {"target_field": "s", "source_path": "age.system"},
            {"target_field": "c", "source_path": "age.code"},
        ]
        result = self.extractor.extract(
            {"value": 30, "unit": "y", "system": "S", "code": "a"}, field_mappings=mappings
        )
        assert result == {"v": 30, "u": "y", "s": "S", "c": "a"}

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"value": 1}])
        assert result["value"] == 1


# ---------------------------------------------------------------------------
# MoneyExtractor
# ---------------------------------------------------------------------------


class TestMoneyExtractor:
    def setup_method(self) -> None:
        self.extractor = MoneyExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_single(self) -> None:
        result = self.extractor.extract({"value": 100.50, "currency": "USD"})
        assert result == {"moneyValue": 100.50, "moneyCurrency": "USD"}

    def test_default_multiple(self) -> None:
        result = self.extractor.extract([
            {"value": 100, "currency": "USD"},
            {"value": 200, "currency": "EUR"},
        ])
        assert result["moneyValue"] == [100, 200]
        assert result["moneyCurrency"] == ["USD", "EUR"]

    def test_field_mappings(self) -> None:
        mappings = [
            {"target_field": "v", "source_path": "money.value"},
            {"target_field": "c", "source_path": "money.currency"},
        ]
        result = self.extractor.extract({"value": 50, "currency": "GBP"}, field_mappings=mappings)
        assert result == {"v": 50, "c": "GBP"}

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"value": 5}])
        assert result["moneyValue"] == 5


# ---------------------------------------------------------------------------
# AvailabilityExtractor
# ---------------------------------------------------------------------------


class TestAvailabilityExtractor:
    def setup_method(self) -> None:
        self.extractor = AvailabilityExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_full(self) -> None:
        availability = [
            {
                "daysOfWeek": ["mon", "tue"],
                "allDay": False,
                "availableStartTime": "09:00:00",
                "availableEndTime": "17:00:00",
            },
            {
                "daysOfWeek": "wed",
                "allDay": True,
                "availableStartTime": "00:00:00",
                "availableEndTime": "23:59:59",
            },
        ]
        result = self.extractor.extract(availability)
        assert set(result["availabilityDaysOfWeek"]) == {"mon", "tue", "wed"}
        assert result["availabilityAllDay"] is True
        assert result["availabilityStartTime"] == ["09:00:00", "00:00:00"]
        assert result["availabilityEndTime"] == ["17:00:00", "23:59:59"]

    def test_field_mappings(self) -> None:
        availability = [
            {
                "daysOfWeek": ["mon"],
                "allDay": False,
                "availableStartTime": "09:00",
                "availableEndTime": "17:00",
            }
        ]
        mappings = [
            {"target_field": "days", "source_path": "availability.daysOfWeek"},
            {"target_field": "allDay", "source_path": "availability.allDay"},
            {"target_field": "startTime", "source_path": "availability.availableStartTime"},
            {"target_field": "endTime", "source_path": "availability.availableEndTime"},
        ]
        result = self.extractor.extract(availability, field_mappings=mappings)
        assert result["days"] == ["mon"]
        assert result["allDay"] is False
        assert result["startTime"] == ["09:00"]
        assert result["endTime"] == ["17:00"]

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"daysOfWeek": ["mon"]}])
        assert result["availabilityDaysOfWeek"] == ["mon"]


# ---------------------------------------------------------------------------
# RangeExtractor / RatioExtractor / RatioRangeExtractor
# ---------------------------------------------------------------------------


class TestRangeExtractor:
    def setup_method(self) -> None:
        self.extractor = RangeExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction_single(self) -> None:
        rng = {"low": {"value": 1, "unit": "mg"}, "high": {"value": 10, "unit": "mg"}}
        result = self.extractor.extract(rng)
        assert result == {
            "rangeLowValue": 1,
            "rangeLowUnit": "mg",
            "rangeHighValue": 10,
            "rangeHighUnit": "mg",
        }

    def test_field_mappings(self) -> None:
        rng = {"low": {"value": 1, "unit": "mg"}, "high": {"value": 10, "unit": "mg"}}
        mappings = [
            {"target_field": "lv", "source_path": "range.low.value"},
            {"target_field": "lu", "source_path": "range.low.unit"},
            {"target_field": "hv", "source_path": "range.high.value"},
            {"target_field": "hu", "source_path": "range.high.unit"},
        ]
        result = self.extractor.extract(rng, field_mappings=mappings)
        assert result == {"lv": 1, "lu": "mg", "hv": 10, "hu": "mg"}

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"low": {"value": 1}}])
        assert result["rangeLowValue"] == 1


class TestRatioExtractor:
    def setup_method(self) -> None:
        self.extractor = RatioExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction(self) -> None:
        ratio = {"numerator": {"value": 1, "unit": "g"}, "denominator": {"value": 4, "unit": "L"}}
        result = self.extractor.extract(ratio)
        assert result["ratioNumeratorValue"] == 1
        assert result["ratioDenominatorValue"] == 4
        assert result["ratioValue"] == 0.25

    def test_zero_denominator_skipped(self) -> None:
        ratio = {"numerator": {"value": 1}, "denominator": {"value": 0}}
        result = self.extractor.extract(ratio)
        assert "ratioValue" not in result

    def test_field_mappings(self) -> None:
        ratio = {"numerator": {"value": 5, "unit": "g"}, "denominator": {"value": 2, "unit": "L"}}
        mappings = [
            {"target_field": "numeratorValue", "source_path": "ratio.numerator.value"},
            {"target_field": "numeratorUnit", "source_path": "ratio.numerator.unit"},
            {"target_field": "denominatorValue", "source_path": "ratio.denominator.value"},
            {"target_field": "denominatorUnit", "source_path": "ratio.denominator.unit"},
            {"target_field": "ratioValue", "source_path": "ratio.value"},
        ]
        result = self.extractor.extract(ratio, field_mappings=mappings)
        assert result["numeratorValue"] == 5
        assert result["denominatorValue"] == 2
        assert result["ratioValue"] == 2.5

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk", {"numerator": {"value": 1}}])
        assert result["ratioNumeratorValue"] == 1


class TestRatioRangeExtractor:
    def setup_method(self) -> None:
        self.extractor = RatioRangeExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction(self) -> None:
        rr = {
            "lowNumerator": {"value": 1},
            "lowDenominator": {"value": 4},
            "highNumerator": {"value": 3},
            "highDenominator": {"value": 4},
        }
        result = self.extractor.extract(rr)
        assert result["ratioRangeLowValue"] == 0.25
        assert result["ratioRangeHighValue"] == 0.75

    def test_field_mappings(self) -> None:
        rr = {
            "lowNumerator": {"value": 1},
            "lowDenominator": {"value": 2},
            "highNumerator": {"value": 3},
            "highDenominator": {"value": 4},
        }
        mappings = [
            {"target_field": "lowNumerator", "source_path": "rr.lowNumerator.value"},
            {"target_field": "lowDenominator", "source_path": "rr.lowDenominator.value"},
            {"target_field": "lowRatio", "source_path": "rr.lowRatio"},
            {"target_field": "highNumerator", "source_path": "rr.highNumerator.value"},
            {"target_field": "highDenominator", "source_path": "rr.highDenominator.value"},
            {"target_field": "highRatio", "source_path": "rr.highRatio"},
        ]
        result = self.extractor.extract(rr, field_mappings=mappings)
        assert result["lowNumerator"] == 1
        assert result["lowDenominator"] == 2
        assert result["highNumerator"] == 3
        assert result["highDenominator"] == 4
        assert result["lowRatio"] == 0.5
        assert result["highRatio"] == 0.75

    def test_skips_non_dict(self) -> None:
        result = self.extractor.extract(["junk"])
        assert result == {}


# ---------------------------------------------------------------------------
# ContactPointExtractor
# ---------------------------------------------------------------------------


class TestContactPointExtractor:
    def setup_method(self) -> None:
        self.extractor = ContactPointExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_default_extraction(self) -> None:
        contacts = [
            {"system": "phone", "value": "555-1234", "use": "home"},
            {"system": "email", "value": "test@example.com", "use": "work"},
            {"system": "fax", "value": "555-5678"},
        ]
        result = self.extractor.extract(contacts)
        assert "555-1234" in result["values"]
        assert "phone" in result["systems"]
        assert result["phone"] == ["555-1234"]
        assert result["email"] == ["test@example.com"]
        assert result["fax"] == ["555-5678"]

    def test_field_mappings_phone_only(self) -> None:
        contacts = [
            {"system": "phone", "value": "555-1234"},
            {"system": "email", "value": "x@y.com"},
        ]
        mappings = [
            {"target_field": "phone", "source_path": "telecom.value", "datatype": "array[string]"},
        ]
        result = self.extractor.extract(contacts, field_mappings=mappings)
        assert result["phone"] == ["555-1234"]

    def test_field_mappings_email_lowercase(self) -> None:
        contacts = [{"system": "email", "value": "JOHN@EXAMPLE.COM"}]
        mappings = [
            {
                "target_field": "email",
                "source_path": "telecom.value",
                "datatype": "array[string]",
                "normalize": "lowercase",
            }
        ]
        result = self.extractor.extract(contacts, field_mappings=mappings)
        assert result["email"] == ["john@example.com"]

    def test_field_mappings_scalar_datatype(self) -> None:
        contacts = [{"system": "phone", "value": "555"}]
        mappings = [
            {"target_field": "phone", "source_path": "telecom.value", "datatype": "string"}
        ]
        result = self.extractor.extract(contacts, field_mappings=mappings)
        assert result["phone"] == "555"

    def test_field_mappings_returns_none_when_missing(self) -> None:
        contacts: List[Dict[str, Any]] = []
        # ContactPointExtractor short-circuits on empty contacts list
        result = self.extractor.extract(contacts, field_mappings=[
            {"target_field": "phone", "source_path": "telecom.value", "datatype": "string"}
        ])
        assert result == {}

    def test_field_mappings_systems_path(self) -> None:
        contacts = [{"system": "phone", "value": "1"}, {"system": "email", "value": "x"}]
        mappings = [
            {"target_field": "systems", "source_path": "telecom.system", "datatype": "array[string]"}
        ]
        result = self.extractor.extract(contacts, field_mappings=mappings)
        assert result["systems"] == ["phone", "email"]

    def test_field_mappings_uses_path(self) -> None:
        contacts = [{"system": "phone", "value": "1", "use": "home"}]
        mappings = [
            {"target_field": "uses", "source_path": "telecom.use", "datatype": "array[string]"}
        ]
        result = self.extractor.extract(contacts, field_mappings=mappings)
        assert result["uses"] == ["home"]

    def test_skips_non_dict(self) -> None:
        contacts = ["junk", {"system": "phone", "value": "555"}]
        result = self.extractor.extract(contacts)
        assert result["phone"] == ["555"]


# ---------------------------------------------------------------------------
# CodeableConceptExtractor / IdentifierExtractor / ReferenceExtractor /
# AddressExtractor / HumanNameExtractor — these have decent coverage already.
# Add coverage for the field_mappings driven branches.
# ---------------------------------------------------------------------------


class TestCodeableConceptExtractor:
    def setup_method(self) -> None:
        self.extractor = CodeableConceptExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_extracts_codes_systems_displays(self) -> None:
        cc = {
            "coding": [
                {"system": "http://loinc.org", "code": "8480-6", "display": "Systolic BP"},
                {"system": "http://snomed.info/sct", "code": "271649006"},
            ],
            "text": "Systolic blood pressure",
        }
        mappings = [
            {"source_path": "code.coding[*].code", "target_field": "codes"},
            {"source_path": "code.coding[*].system_code", "target_field": "system_codes"},
            {"source_path": "code.coding[*].display", "target_field": "displays"},
            {"source_path": "code.text", "target_field": "text"},
        ]
        result = self.extractor.extract(cc, field_mappings=mappings)
        assert "8480-6" in result["codes"]
        assert any("|" in s for s in result["system_codes"])

    def test_array_input(self) -> None:
        cc = [
            {"coding": [{"system": "S1", "code": "C1"}]},
            {"coding": [{"system": "S2", "code": "C2"}]},
        ]
        mappings = [
            {"source_path": "category[*].coding[*].code", "target_field": "codes"},
            {"source_path": "category[*].coding[*].system_code", "target_field": "sysCodes"},
        ]
        result = self.extractor.extract(cc, field_mappings=mappings)
        assert "C1" in result["codes"]
        assert "C2" in result["codes"]

    # ------------------------------------------------------------------
    # Regression tests locking in the projection-ordering fixes.
    # ------------------------------------------------------------------

    def test_coding_substring_does_not_misroute_systemCode_target(self) -> None:
        """Source path ``coding[*]`` must not steal a ``*_systemCode`` target.

        The legacy projection's ``"code" in source_path`` test was
        accidentally true for ``coding[*]`` (because "coding" CONTAINS
        "code") and would silently route the rule to the codes list.
        The target_field-first ordering must prevent this.
        """
        cc = {
            "coding": [
                {"system": "http://loinc.org", "code": "8480-6"},
                {"system": "http://snomed.info/sct", "code": "271649006"},
            ]
        }
        mappings = [
            {"source_path": "appointmentType.coding[*].code", "target_field": "appointmentType_codes",
             "datatype": "array[string]"},
            {"source_path": "appointmentType.coding[*]", "target_field": "appointmentType_systemCode",
             "datatype": "array[string]"},
        ]
        result = self.extractor.extract(cc, field_mappings=mappings)
        assert result["appointmentType_codes"] == ["8480-6", "271649006"]
        # systemCode MUST contain "system|code" pairs, NOT plain codes.
        assert all("|" in s for s in result["appointmentType_systemCode"])
        assert "http://loinc.org|8480-6" in result["appointmentType_systemCode"]

    def test_camelcase_and_snakecase_systemCode_both_recognized(self) -> None:
        """Both ``*_systemCode`` (camelCase) and ``system_codes`` (snake) project to system|value pairs."""
        cc = {"coding": [{"system": "http://loinc.org", "code": "X"}]}
        mappings = [
            {"source_path": "code.coding[*]", "target_field": "code_systemCode",
             "datatype": "array[string]"},
            {"source_path": "code.coding[*]", "target_field": "system_codes",
             "datatype": "array[string]"},
        ]
        result = self.extractor.extract(cc, field_mappings=mappings)
        assert result["code_systemCode"] == ["http://loinc.org|X"]
        assert result["system_codes"] == ["http://loinc.org|X"]

    def test_empty_scalar_projection_omits_target_field(self) -> None:
        """An empty scalar projection must not write ``None`` (sparse output).

        A subtle bug rolled back the entire ``code`` rule when one of
        its mappings (e.g. ``code.text`` against a CodeableConcept that
        had no ``text``) projected to an empty list — the legacy
        ``_assign_legacy`` wrote ``None`` and the denormalizer's
        ``datatype: string`` validator rejected it, dropping the
        rule's other mappings (``code_codes``, ``code_systemCode``).
        Sparse output is the contract.
        """
        cc = {"coding": [{"system": "http://loinc.org", "code": "X"}]}
        mappings = [
            {"source_path": "code.text", "target_field": "code_text",
             "datatype": "string"},
            {"source_path": "code.coding[*].code", "target_field": "code_codes",
             "datatype": "array[string]"},
        ]
        result = self.extractor.extract(cc, field_mappings=mappings)
        # Sibling mapping must still populate.
        assert result["code_codes"] == ["X"]
        # Empty scalar projection must be ABSENT, not present-with-None.
        assert "code_text" not in result


class TestIdentifierExtractor:
    def setup_method(self) -> None:
        self.extractor = IdentifierExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_extracts_systemCode_and_values(self) -> None:
        identifiers = [
            {"system": "http://hospital.org/mrn", "value": "MRN-123", "use": "official"},
            {"system": "http://national/ssn", "value": "555-55-5555"},
        ]
        mappings = [
            {"source_path": "identifier[*].value", "target_field": "values"},
            {"source_path": "identifier[*]", "target_field": "systemCode"},
        ]
        result = self.extractor.extract(identifiers, field_mappings=mappings)
        assert "MRN-123" in result["values"]
        assert any("MRN-123" in s for s in result["systemCode"])

    # ------------------------------------------------------------------
    # Regression tests for the target-field-first projection ordering.
    # ------------------------------------------------------------------

    def test_systemCode_target_routes_to_system_value_pairs(self) -> None:
        """Target ``identifier_systemCode`` must produce ``system|value`` pairs."""
        identifiers = [
            {"system": "http://hospital.org", "value": "MRN-123"},
        ]
        mappings = [
            {
                "source_path": "identifier[*]",
                "target_field": "identifier_systemCode",
                "datatype": "array[string]",
            }
        ]
        result = self.extractor.extract(identifiers, field_mappings=mappings)
        assert result["identifier_systemCode"] == ["http://hospital.org|MRN-123"]

    def test_resource_rooted_union_path_for_organization_qualification(self) -> None:
        """Organization's identifier rule walks ``identifier | qualification[*].identifier``."""
        organization = {
            "resourceType": "Organization",
            "id": "org-1",
            "identifier": [
                {"system": "http://example.org/orgs", "value": "ORG-1"},
            ],
            "qualification": [
                {
                    "identifier": [
                        {"system": "http://accred.example.org", "value": "ACC-1"},
                    ]
                }
            ],
        }
        mappings = [
            {
                "source_path": "identifier | qualification[*].identifier",
                "target_field": "identifier_values",
                "datatype": "array[string]",
            },
            {
                "source_path": "identifier | qualification[*].identifier",
                "target_field": "identifier_systemCode",
                "datatype": "array[string]",
            },
        ]
        result = self.extractor.extract(organization, field_mappings=mappings)
        assert sorted(result["identifier_values"]) == ["ACC-1", "ORG-1"]
        assert "http://example.org/orgs|ORG-1" in result["identifier_systemCode"]
        assert "http://accred.example.org|ACC-1" in result["identifier_systemCode"]

    def test_empty_scalar_projection_omits_target_field(self) -> None:
        """Empty projections in IdentifierExtractor must also be sparse."""
        mappings = [
            {
                "source_path": "identifier[*].value",
                "target_field": "single_value",
                "datatype": "string",
            }
        ]
        result = self.extractor.extract([], field_mappings=mappings)
        assert "single_value" not in result


class TestReferenceExtractor:
    def setup_method(self) -> None:
        self.extractor = ReferenceExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_extracts_id_and_type(self) -> None:
        ref = {"reference": "Patient/123", "display": "John"}
        mappings = [
            {"source_path": "subject.reference", "target_field": "patientId", "extractType": "id"},
            {"source_path": "subject.reference", "target_field": "patientType", "extractType": "type"},
        ]
        result = self.extractor.extract(ref, field_mappings=mappings)
        assert result.get("patientId") == "123"
        assert result.get("patientType") == "Patient"

    def test_array_extraction(self) -> None:
        refs = [
            {"reference": "Practitioner/p1"},
            {"reference": "Practitioner/p2"},
        ]
        mappings = [
            {
                "source_path": "performer[*].reference",
                "target_field": "performerId",
                "datatype": "array[string]",
                "extractType": "id",
            }
        ]
        result = self.extractor.extract(refs, field_mappings=mappings)
        assert result["performerId"] == ["p1", "p2"]

    # ------------------------------------------------------------------
    # filterType: pin the type-filtered projection that powers
    # Appointment.participant's `patientId` / `practitionerId` /
    # `locationId` buckets. Before this option existed, every typed
    # bucket was a duplicate of `actorIds` (every participant ID,
    # regardless of resource type).
    # ------------------------------------------------------------------

    def _mixed_participant_actors(self) -> list:
        """Three actors, three different resource types — pre-resolved shape."""
        return [
            {"reference": "Patient/p1"},
            {"reference": "Practitioner/pr9"},
            {"reference": "Location/loc7"},
        ]

    def test_filter_type_keeps_only_matching_resource_type_pre_resolved(self) -> None:
        mappings = [
            {
                "source_path": "participant[*].actor.reference",
                "target_field": "patientId",
                "datatype": "array[string]",
                "extractType": "id",
                "filterType": "Patient",
            },
            {
                "source_path": "participant[*].actor.reference",
                "target_field": "practitionerId",
                "datatype": "array[string]",
                "extractType": "id",
                "filterType": "Practitioner",
            },
            {
                "source_path": "participant[*].actor.reference",
                "target_field": "locationId",
                "datatype": "array[string]",
                "extractType": "id",
                "filterType": "Location",
            },
        ]
        # Pre-resolved shape: caller passes the participant array as
        # the navigation root with `actor.reference` as the per-item
        # path. This is what Appointment.yaml's `participant` rule
        # emits today.
        participants = [
            {"actor": p, "status": "accepted"}
            for p in self._mixed_participant_actors()
        ]
        result = self.extractor.extract(participants, field_mappings=mappings)
        assert result.get("patientId") == ["p1"]
        assert result.get("practitionerId") == ["pr9"]
        assert result.get("locationId") == ["loc7"]

    def test_filter_type_omits_target_when_no_match_pre_resolved(self) -> None:
        # No Patient participant → `patientId` MUST NOT appear in the
        # output (sparse). The previous behavior wrote the full
        # actor-id list into every typed bucket regardless of type;
        # the previous-previous behavior wrote `[]`. Both broke
        # `$exists` coverage queries.
        participants = [
            {"actor": {"reference": "Practitioner/pr1"}},
            {"actor": {"reference": "Location/loc1"}},
        ]
        mappings = [
            {
                "source_path": "participant[*].actor.reference",
                "target_field": "patientId",
                "datatype": "array[string]",
                "extractType": "id",
                "filterType": "Patient",
            }
        ]
        result = self.extractor.extract(participants, field_mappings=mappings)
        assert "patientId" not in result

    def test_filter_type_works_with_resource_rooted_mode(self) -> None:
        # `looks_like_resource` triggers the alternate code path —
        # filterType MUST also work there (otherwise resources that
        # opt into `source: $resource` lose the type guard).
        resource = {
            "resourceType": "Appointment",
            "participant": [
                {"actor": {"reference": "Patient/p1"}},
                {"actor": {"reference": "Practitioner/pr9"}},
            ],
        }
        mappings = [
            {
                "source_path": "participant[*].actor",
                "target_field": "patientId",
                "datatype": "array[string]",
                "extractType": "id",
                "filterType": "Patient",
            }
        ]
        result = self.extractor.extract(resource, field_mappings=mappings)
        assert result.get("patientId") == ["p1"]


class TestAddressExtractor:
    def setup_method(self) -> None:
        self.extractor = AddressExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_extract_full_address(self) -> None:
        addresses = [
            {
                "use": "home",
                "line": ["123 Main St"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "US",
                "text": "123 Main St, Springfield, IL 62701",
            }
        ]
        mappings = [
            {"source_path": "address[*]", "target_field": "addressFull", "datatype": "array[string]"},
            {"source_path": "address[*].city", "target_field": "addressCity", "datatype": "array[string]"},
            {
                "source_path": "address[*].city",
                "target_field": "addressCity_lower",
                "datatype": "array[string]",
                "normalize": "lowercase",
            },
        ]
        result = self.extractor.extract(addresses, field_mappings=mappings)
        assert "Springfield" in result["addressCity"]
        assert "springfield" in result["addressCity_lower"]


class TestHumanNameExtractor:
    def setup_method(self) -> None:
        self.extractor = HumanNameExtractor()

    def test_empty(self) -> None:
        assert self.extractor.extract(None) == {}

    def test_extracts_family_given_full(self) -> None:
        names = [
            {"family": "Smith", "given": ["John", "Q"], "prefix": ["Mr."], "suffix": ["Jr."]},
            {"use": "nickname", "given": ["Johnny"]},
        ]
        mappings = [
            {"source_path": "name[*].family", "target_field": "familyName", "datatype": "string"},
            {
                "source_path": "name[*].family",
                "target_field": "familyName_lower",
                "datatype": "string",
                "normalize": "lowercase",
            },
            {"source_path": "name[*].given", "target_field": "givenNames", "datatype": "array[string]"},
            {"source_path": "name[*]", "target_field": "fullName", "datatype": "array[string]"},
            {
                "source_path": "name[*]",
                "target_field": "fullName_lower",
                "datatype": "array[string]",
                "normalize": "lowercase",
            },
        ]
        result = self.extractor.extract(names, field_mappings=mappings)
        assert "Smith" in (
            result["familyName"] if isinstance(result["familyName"], list) else [result["familyName"]]
        )
        assert "smith" in (
            result["familyName_lower"]
            if isinstance(result["familyName_lower"], list)
            else [result["familyName_lower"]]
        )


# ---------------------------------------------------------------------------
# ResourceDenormalizer extractor registry
# ---------------------------------------------------------------------------


class TestExtractorRegistry:
    """Verify the ResourceDenormalizer wires every extractor we expect."""

    EXPECTED = {
        "HumanNameExtractor",
        "CodeableConceptExtractor",
        "ReferenceExtractor",
        "IdentifierExtractor",
        "ContactPointExtractor",
        "AddressExtractor",
        "QuantityExtractor",
        "PeriodExtractor",
        "TimingExtractor",
        "RangeExtractor",
        "RatioExtractor",
        "RatioRangeExtractor",
        "CodingExtractor",
        "ExtensionExtractor",
        "MoneyExtractor",
        "AgeDurationExtractor",
        "DosageExtractor",
        "AvailabilityExtractor",
        "PhoneticExtractor",
        "TextExtractor",
        "DirectFieldExtractor",
        "CompartmentMembershipExtractor",
    }

    def test_registry_has_all_extractors(self) -> None:
        assert set(ResourceDenormalizer.EXTRACTORS.keys()) == self.EXPECTED

    def test_each_extractor_instantiable_and_callable(self) -> None:
        for name, cls in ResourceDenormalizer.EXTRACTORS.items():
            instance = cls()
            assert hasattr(instance, "extract")
            result = instance.extract(None)
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# DirectFieldExtractor — polymorphic-scalar copy (Patient.deceased[x] etc.)
# ---------------------------------------------------------------------------


class TestDirectFieldExtractor:
    """
    DirectFieldExtractor is the resource-rooted scalar copier introduced
    to fix Patient.deceased[x]: the previous YAML named a non-existent
    extractor, AND used `source: deceased` (which never resolves —
    FHIR uses the `deceasedBoolean` / `deceasedDateTime` polymorphic
    siblings). These tests pin both the basic copy semantics and the
    `transform: presence` shortcut that bridges deceasedDateTime →
    `deceased: true`.
    """

    def setup_method(self) -> None:
        self.extractor = DirectFieldExtractor()

    def test_returns_empty_when_resource_is_not_dict(self) -> None:
        assert self.extractor.extract(None, [{"source_path": "x", "target_field": "y"}]) == {}
        assert self.extractor.extract(["a"], [{"source_path": "x", "target_field": "y"}]) == {}

    def test_returns_empty_with_no_field_mappings(self) -> None:
        assert self.extractor.extract({"deceasedBoolean": True}) == {}

    def test_copies_boolean_polymorphic_variant(self) -> None:
        resource = {"resourceType": "Patient", "deceasedBoolean": True}
        mappings = [{"source_path": "deceasedBoolean", "target_field": "deceased", "datatype": "boolean"}]
        result = self.extractor.extract(resource, mappings)
        assert result == {"deceased": True}

    def test_copies_dateTime_polymorphic_variant_to_distinct_target(self) -> None:
        resource = {"resourceType": "Patient", "deceasedDateTime": "2024-03-15T12:00:00Z"}
        mappings = [
            {"source_path": "deceasedDateTime", "target_field": "deathDate", "datatype": "date"},
        ]
        result = self.extractor.extract(resource, mappings)
        assert result == {"deathDate": "2024-03-15T12:00:00Z"}

    def test_transform_presence_writes_true_for_dateTime_input(self) -> None:
        """deceasedDateTime → deceased=true bridge for FHIR R5 token search param."""
        resource = {"resourceType": "Patient", "deceasedDateTime": "2024-03-15T12:00:00Z"}
        mappings = [
            {
                "source_path": "deceasedDateTime",
                "target_field": "deceased",
                "datatype": "boolean",
                "transform": "presence",
            }
        ]
        result = self.extractor.extract(resource, mappings)
        assert result == {"deceased": True}

    def test_transform_presence_omits_when_path_does_not_resolve(self) -> None:
        """No source value → no write (sparse contract)."""
        resource = {"resourceType": "Patient"}
        mappings = [
            {
                "source_path": "deceasedDateTime",
                "target_field": "deceased",
                "datatype": "boolean",
                "transform": "presence",
            }
        ]
        assert self.extractor.extract(resource, mappings) == {}

    def test_polymorphic_choice_only_writes_present_variant(self) -> None:
        """A Patient with `deceasedDateTime` should NOT have a phantom
        `deceasedBoolean`-driven write — sparse output is the contract."""
        resource = {"resourceType": "Patient", "deceasedDateTime": "2024-03-15T12:00:00Z"}
        mappings = [
            {"source_path": "deceasedBoolean", "target_field": "deceased", "datatype": "boolean"},
            {
                "source_path": "deceasedDateTime",
                "target_field": "deceased",
                "datatype": "boolean",
                "transform": "presence",
            },
            {"source_path": "deceasedDateTime", "target_field": "deathDate", "datatype": "date"},
        ]
        result = self.extractor.extract(resource, mappings)
        assert result == {"deceased": True, "deathDate": "2024-03-15T12:00:00Z"}

    def test_integer_coerces_string_input(self) -> None:
        resource = {"count": "42"}
        mappings = [{"source_path": "count", "target_field": "count", "datatype": "integer"}]
        assert self.extractor.extract(resource, mappings) == {"count": 42}

    def test_decimal_coerces_string_input(self) -> None:
        resource = {"weight": "1.5"}
        mappings = [{"source_path": "weight", "target_field": "weight", "datatype": "decimal"}]
        assert self.extractor.extract(resource, mappings) == {"weight": 1.5}

    def test_boolean_coerces_string_input(self) -> None:
        for raw, expected in [("true", True), ("True", True), ("false", False), ("FALSE", False)]:
            mappings = [{"source_path": "x", "target_field": "x", "datatype": "boolean"}]
            assert self.extractor.extract({"x": raw}, mappings) == {"x": expected}

    def test_boolean_drops_unparseable_string(self) -> None:
        mappings = [{"source_path": "x", "target_field": "x", "datatype": "boolean"}]
        assert self.extractor.extract({"x": "maybe"}, mappings) == {}


# ---------------------------------------------------------------------------
# CodeableConceptExtractor — sparse-array regression
# ---------------------------------------------------------------------------


class TestCodeableConceptExtractorSparseArray:
    """
    Pre-resolved CodeableConceptExtractor used to write `[]` for
    `array[*]` datatypes when the input contained no codings — which
    polluted Appointment._search with empty `reasonCode_codes` /
    `serviceType_codes` arrays whenever the resource used the R5
    CodeableReference shape (`reason[*].reference` only, no `concept`).
    Empty arrays clutter indexes and break `$exists` coverage queries.
    """

    def setup_method(self) -> None:
        self.extractor = CodeableConceptExtractor()

    def test_pre_resolved_array_is_sparse_when_no_codings(self) -> None:
        # CodeableReference list — only `reference`, no `concept`. The
        # extractor in pre-resolved mode tries to flatten each item as
        # a CodeableConcept and finds no codings.
        value = [{"reference": {"reference": "Condition/cond-1"}}]
        mappings = [
            {
                "source_path": "reason[*].concept.coding[*].code",
                "target_field": "reasonCode_codes",
                "datatype": "array[string]",
            }
        ]
        result = self.extractor.extract(value, field_mappings=mappings)
        assert "reasonCode_codes" not in result, (
            f"empty array leaked into output: {result!r}"
        )

    def test_pre_resolved_array_populated_when_codings_present(self) -> None:
        value = [
            {"coding": [{"system": "http://snomed.info/sct", "code": "162673000"}]}
        ]
        mappings = [
            {
                "source_path": "reason[*].coding[*].code",
                "target_field": "reasonCode_codes",
                "datatype": "array[string]",
            }
        ]
        result = self.extractor.extract(value, field_mappings=mappings)
        assert result["reasonCode_codes"] == ["162673000"]


# ---------------------------------------------------------------------------
# PeriodExtractor — array[object] of {start, end} dicts
# ---------------------------------------------------------------------------


class TestPeriodExtractorArrayObject:
    """
    The legacy PeriodExtractor only routed by source_path token suffix
    ('start' / 'end') and produced flat arrays of dateTime strings.
    For Appointment.requestedPeriod (Period[]), this dropped the END
    boundary entirely — breaking date/period range queries. The
    `array[object]` datatype now projects each input Period as a
    sparse `{start, end}` dict so range queries against
    `_search.requestedPeriod.start` / `.end` work after a $unwind.
    """

    def setup_method(self) -> None:
        self.extractor = PeriodExtractor()

    def test_array_object_emits_full_period_dicts(self) -> None:
        periods = [
            {"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"},
            {"start": "2024-08-01T00:00:00Z", "end": "2024-08-15T00:00:00Z"},
        ]
        mappings = [
            {
                "source_path": "requestedPeriod",
                "target_field": "requestedPeriod",
                "datatype": "array[object]",
            }
        ]
        result = self.extractor.extract(periods, field_mappings=mappings)
        assert result == {
            "requestedPeriod": [
                {"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"},
                {"start": "2024-08-01T00:00:00Z", "end": "2024-08-15T00:00:00Z"},
            ]
        }

    def test_array_object_drops_period_with_only_start_or_only_end(self) -> None:
        # Sparse contract: each emitted Period dict only carries the
        # keys that were actually present in the input. A Period
        # missing both is dropped entirely.
        periods = [
            {"start": "2024-06-25T00:00:00Z"},
            {},
            {"end": "2024-08-15T00:00:00Z"},
        ]
        mappings = [
            {
                "source_path": "requestedPeriod",
                "target_field": "requestedPeriod",
                "datatype": "array[object]",
            }
        ]
        result = self.extractor.extract(periods, field_mappings=mappings)
        assert result == {
            "requestedPeriod": [
                {"start": "2024-06-25T00:00:00Z"},
                {"end": "2024-08-15T00:00:00Z"},
            ]
        }

    def test_array_object_omits_target_when_no_periods(self) -> None:
        mappings = [
            {
                "source_path": "requestedPeriod",
                "target_field": "requestedPeriod",
                "datatype": "array[object]",
            }
        ]
        assert self.extractor.extract([], field_mappings=mappings) == {}


# ---------------------------------------------------------------------------
# TimingExtractor — *Bounds fallback to repeat.boundsPeriod
# ---------------------------------------------------------------------------


class TestTimingExtractorBoundsFallback:
    """
    Observation.effectiveTimingBounds was empty whenever an Observation
    used `effectiveTiming.repeat.boundsPeriod` instead of populating
    `effectiveTiming.event[]`. The TimingExtractor now falls back to
    `repeat.boundsPeriod` for any `*Bounds` target whose
    chronological min/max from `event[]` is unavailable.
    """

    def setup_method(self) -> None:
        self.extractor = TimingExtractor()

    def test_bounds_falls_back_to_repeat_bounds_period(self) -> None:
        timing = {
            "repeat": {
                "boundsPeriod": {
                    "start": "2024-06-15T08:00:00Z",
                    "end": "2024-06-15T08:30:00Z",
                }
            }
        }
        mappings = [
            {
                "source_path": "event[*]",
                "target_field": "effectiveTimingBounds",
                "datatype": "object",
            }
        ]
        result = self.extractor.extract(timing, field_mappings=mappings)
        assert result["effectiveTimingBounds"] == {
            "start": "2024-06-15T08:00:00Z",
            "end": "2024-06-15T08:30:00Z",
        }

    def test_bounds_prefers_event_when_present(self) -> None:
        timing = {
            "event": ["2024-06-15T08:00:00Z", "2024-06-15T08:30:00Z"],
            "repeat": {
                "boundsPeriod": {
                    "start": "2030-01-01T00:00:00Z",
                    "end": "2030-12-31T00:00:00Z",
                }
            },
        }
        mappings = [
            {
                "source_path": "event[*]",
                "target_field": "effectiveTimingBounds",
                "datatype": "object",
            }
        ]
        result = self.extractor.extract(timing, field_mappings=mappings)
        # event[] wins — confirmed by the fact that the boundsPeriod
        # values (which we deliberately set far in the future) are
        # NOT what was projected.
        assert result["effectiveTimingBounds"] == {
            "start": "2024-06-15T08:00:00Z",
            "end": "2024-06-15T08:30:00Z",
        }

    def test_bounds_omitted_when_neither_event_nor_repeat_bounds(self) -> None:
        timing = {"repeat": {"frequency": 2}}
        mappings = [
            {
                "source_path": "event[*]",
                "target_field": "effectiveTimingBounds",
                "datatype": "object",
            }
        ]
        result = self.extractor.extract(timing, field_mappings=mappings)
        assert "effectiveTimingBounds" not in result
