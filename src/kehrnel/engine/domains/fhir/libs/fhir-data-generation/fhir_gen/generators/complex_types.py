"""FHIR R5 general-purpose complex datatype generators."""

from __future__ import annotations

from datetime import datetime, timedelta

from ..codes.loader import get_system, random_code
from .primitives import PrimitiveGenerator


class ComplexTypeGenerator:
    """Generates FHIR complex datatypes as JSON-compatible dicts."""

    COMPLEX_TYPES = frozenset({
        "Address", "Age", "Annotation", "Attachment", "CodeableConcept", "Coding",
        "ContactPoint", "Count", "Distance", "Duration", "HumanName", "Identifier",
        "Money", "MoneyQuantity", "Period", "Quantity", "Range", "Ratio", "RatioRange",
        "SampledData", "Signature", "SimpleQuantity", "Timing",
    })

    def __init__(self, prim: PrimitiveGenerator):
        self.p = prim
        self.rng = prim.rng

    def generate(self, type_name: str, **kwargs) -> dict:
        method = getattr(self, f"gen_{type_name}", None)
        if method:
            return method(**kwargs)
        raise ValueError(f"No generator for complex type: {type_name}")

    def gen_Identifier(self, system: str | None = None, value: str | None = None) -> dict:
        type_code = random_code("identifier_types", self.rng)
        return {
            "use": self.rng.choice(["usual", "official", "temp", "secondary"]),
            "type": self.gen_CodeableConcept(
                system=get_system("identifier_types"),
                code=type_code["code"] if type_code else "MR",
                display=type_code["display"] if type_code else "Medical Record Number",
            ),
            "system": system or "http://hospital.example.org/identifiers",
            "value": value or self.p.gen_id(),
        }

    def gen_HumanName(self, use: str | None = None) -> dict:
        first = self.p.faker.first_name()
        last = self.p.faker.last_name()
        use = use or self.rng.choice(["usual", "official", "nickname"])
        prefix = (
            [self.rng.choice(["Mr.", "Mrs.", "Ms.", "Dr.", "Prof."])]
            if self.rng.random() < 0.3
            else []
        )
        return {
            "use": use,
            "family": last,
            "given": [first],
            "text": f"{first} {last}",
            "prefix": prefix,
        }

    def gen_Address(self, country: str | None = None) -> dict:
        f = self.p.faker
        use_code = random_code("address_use", self.rng)
        state = f.state_abbr() if hasattr(f, "state_abbr") else f.state()
        return {
            "use": use_code["code"] if use_code else "home",
            "type": self.rng.choice(["postal", "physical", "both"]),
            "line": [f.street_address()],
            "city": f.city(),
            "state": state,
            "postalCode": f.postcode(),
            "country": country or "US",
            "text": f.address(),
        }

    def gen_ContactPoint(self, system: str | None = None) -> dict:
        system = system or self.rng.choice(["phone", "email", "fax", "url", "sms"])
        use_code = random_code("telecom_use", self.rng)
        value_map = {
            "phone": self.p.faker.phone_number(),
            "email": self.p.faker.email(),
            "fax": self.p.faker.phone_number(),
            "url": self.p.faker.url(),
            "sms": self.p.faker.phone_number(),
        }
        return {
            "system": system,
            "value": value_map.get(system, self.p.faker.phone_number()),
            "use": use_code["code"] if use_code else "home",
            "rank": self.p.gen_positiveInt(max_val=5),
        }

    def gen_CodeableConcept(
        self,
        system: str | None = None,
        code: str | None = None,
        display: str | None = None,
        text: str | None = None,
    ) -> dict:
        if system is None and code is None:
            label = text or display or self.p.faker.sentence(nb_words=2).rstrip(".")
            return {"text": label}
        return {
            "coding": [self.gen_Coding(system=system, code=code, display=display)],
            "text": text or display or self.p.faker.word(),
        }

    def gen_CodeableReference(
        self,
        *,
        concept: dict | None = None,
        reference: dict | None = None,
        system: str | None = None,
        code: str | None = None,
        display: str | None = None,
    ) -> dict:
        """FHIR R5/R6 CodeableReference — concept and/or reference arm."""
        out: dict = {}
        if concept is not None:
            out["concept"] = concept
        elif system is not None or code is not None:
            out["concept"] = self.gen_CodeableConcept(
                system=system, code=code, display=display
            )
        if reference is not None:
            out["reference"] = reference
        return out

    def gen_Coding(
        self,
        system: str | None = None,
        code: str | None = None,
        display: str | None = None,
        version: str | None = None,
    ) -> dict:
        if system is None and code is None:
            return {}
        result: dict = {
            "system": system or "http://snomed.info/sct",
            "code": code or self.p.gen_code(),
            "display": display or self.p.faker.sentence(nb_words=3).rstrip("."),
        }
        if version:
            result["version"] = version
        return result

    def gen_Period(self, start_dt: str | None = None, duration_days: int | None = None) -> dict:
        start = start_dt or self.p.gen_dateTime(min_year=2020, max_year=2024)
        if duration_days is None:
            duration_days = self.rng.randint(1, 365)
        start_obj = self._parse_datetime(start)
        end_obj = start_obj + timedelta(days=duration_days)
        return {
            "start": start,
            "end": end_obj.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }

    def gen_Quantity(
        self,
        value: float | None = None,
        unit: str | None = None,
        system: str = "http://unitsofmeasure.org",
        code: str | None = None,
    ) -> dict:
        u = unit or "mg"
        return {
            "value": value if value is not None else round(self.rng.uniform(0.1, 500.0), 2),
            "unit": u,
            "system": system,
            "code": code or u,
        }

    def gen_SimpleQuantity(self, **kwargs) -> dict:
        q = self.gen_Quantity(**kwargs)
        q.pop("comparator", None)
        return q

    def gen_Range(
        self,
        low_val: float | None = None,
        high_val: float | None = None,
        unit: str = "mg",
    ) -> dict:
        low = low_val if low_val is not None else round(self.rng.uniform(0, 100), 1)
        high = high_val if high_val is not None else round(low + self.rng.uniform(10, 100), 1)
        return {
            "low": self.gen_SimpleQuantity(value=low, unit=unit),
            "high": self.gen_SimpleQuantity(value=high, unit=unit),
        }

    def gen_Ratio(self, unit: str = "mg/mL") -> dict:
        _ = unit
        return {
            "numerator": self.gen_Quantity(value=round(self.rng.uniform(1, 100), 2), unit="mg"),
            "denominator": self.gen_Quantity(value=round(self.rng.uniform(1, 10), 1), unit="mL"),
        }

    def gen_Annotation(self, author_ref: dict | None = None) -> dict:
        result: dict = {"text": self.p.gen_markdown()}
        if author_ref:
            result["authorReference"] = author_ref
        else:
            result["authorString"] = self.p.faker.name()
        result["time"] = self.p.gen_dateTime()
        return result

    def gen_Attachment(self, content_type: str | None = None) -> dict:
        ct = content_type or self.rng.choice(
            ["application/pdf", "image/jpeg", "text/plain"]
        )
        return {
            "contentType": ct,
            "language": "en",
            "data": self.p.gen_base64Binary(byte_count=32),
            "title": self.p.faker.sentence(nb_words=4).rstrip("."),
            "creation": self.p.gen_dateTime(),
        }

    def gen_Money(self, currency: str = "USD") -> dict:
        return {
            "value": round(self.rng.uniform(10.0, 5000.0), 2),
            "currency": currency,
        }

    def gen_Timing(self) -> dict:
        timing = random_code("dosage_timing", self.rng)
        result: dict = {
            "repeat": {
                "frequency": timing.get("frequency", 1) if timing else 1,
                "period": timing.get("period", 1) if timing else 1,
                "periodUnit": timing.get("period_unit", "d") if timing else "d",
                "boundsPeriod": self.gen_Period(),
            }
        }
        if timing:
            result["code"] = self.gen_CodeableConcept(
                system=get_system("dosage_timing"),
                code=timing["code"],
                display=timing.get("display"),
            )
        return result

    def gen_Age(self, value: float | None = None) -> dict:
        return {
            "value": value if value is not None else float(self.rng.randint(1, 100)),
            "unit": "years",
            "system": "http://unitsofmeasure.org",
            "code": "a",
        }

    def gen_Duration(self, value: float | None = None, unit: str = "d") -> dict:
        unit_map = {
            "d": "days",
            "h": "hours",
            "min": "minutes",
            "s": "seconds",
            "wk": "weeks",
            "mo": "months",
            "a": "years",
        }
        return {
            "value": value if value is not None else float(self.rng.randint(1, 30)),
            "unit": unit_map.get(unit, unit),
            "system": "http://unitsofmeasure.org",
            "code": unit,
        }

    def gen_SampledData(self) -> dict:
        count = self.rng.randint(10, 50)
        data = " ".join(str(round(self.rng.uniform(-100, 100), 2)) for _ in range(count))
        return {
            "origin": self.gen_Quantity(value=0.0, unit="uV"),
            "interval": round(self.rng.uniform(0.001, 1.0), 3),
            "intervalUnit": "s",
            "factor": 1.0,
            "lowerLimit": -200.0,
            "upperLimit": 200.0,
            "dimensions": 1,
            "data": data,
        }

    def gen_Count(self, **kwargs) -> dict:
        return self.gen_Quantity(**kwargs)

    def gen_Distance(self, value: float | None = None, unit: str = "km", **_) -> dict:
        return self.gen_Quantity(value=value, unit=unit, code=unit)

    def gen_MoneyQuantity(self, **kwargs) -> dict:
        return self.gen_Quantity(**kwargs)

    def gen_RatioRange(self, **_) -> dict:
        return {"lowRatio": self.gen_Ratio(), "highRatio": self.gen_Ratio()}

    def gen_Signature(self) -> dict:
        practitioner_id = self.p.gen_id()
        return {
            "type": [
                self.gen_Coding(
                    system="http://hl7.org/fhir/signature-type",
                    code="1.2.840.10065.1.12.1.1",
                )
            ],
            "when": self.p.gen_instant(),
            "who": {
                "reference": f"Practitioner/{practitioner_id}",
                "type": "Practitioner",
            },
            "data": self.p.gen_base64Binary(byte_count=16),
        }

    def _parse_datetime(self, dt_str: str) -> datetime:
        if "T" not in dt_str:
            return datetime.fromisoformat(f"{dt_str}T00:00:00+00:00")
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
