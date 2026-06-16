"""FHIR R5 special-purpose and metadata datatype generators."""

from __future__ import annotations

from ..codes.loader import get_system, random_code
from .complex_types import ComplexTypeGenerator

_EXTENSION_VALUE_TYPES = ("string", "boolean", "integer", "decimal", "code", "uri")


class SpecialTypeGenerator(ComplexTypeGenerator):
    """Extends complex generators with Meta, Reference, Extension, Dosage, metadata types."""

    SPECIAL_TYPES = frozenset({
        "Meta", "Narrative", "Reference", "Extension", "Dosage",
        "Availability", "ContactDetail", "Contributor", "DataRequirement",
        "Expression", "ExtendedContactDetail", "MonetaryComponent",
        "ParameterDefinition", "TriggerDefinition", "UsageContext",
        "VirtualServiceDetail", "ElementDefinition",
    })

    def generate(self, type_name: str, **kwargs) -> dict:
        if type_name in self.SPECIAL_TYPES:
            method = getattr(self, f"gen_{type_name}", None)
            if method:
                return method(**kwargs)
        return super().generate(type_name, **kwargs)

    def gen_Meta(self, profile: str | None = None) -> dict:
        """FHIR Meta — resource metadata."""
        result: dict = {
            "versionId": str(self.rng.randint(1, 10)),
            "lastUpdated": self.p.gen_instant(),
            "source": self.p.gen_uri(),
        }
        if profile:
            result["profile"] = [profile]
        return result

    def gen_Narrative(self, status: str = "generated", text: str | None = None) -> dict:
        """FHIR Narrative — human-readable summary."""
        return {
            "status": status,
            "div": text or self.p.gen_xhtml(),
        }

    def gen_Reference(
        self,
        resource_type: str | None = None,
        resource_id: str | None = None,
        display: str | None = None,
    ) -> dict:
        """FHIR Reference — link to another resource."""
        rid = resource_id or self.p.gen_id()
        result: dict = {}
        if resource_type:
            result["reference"] = f"{resource_type}/{rid}"
            result["type"] = resource_type
        else:
            result["reference"] = f"urn:uuid:{rid}"
        if display:
            result["display"] = display
        return result

    def gen_Extension(self, url: str | None = None, value_type: str | None = None) -> dict:
        """FHIR Extension; rotates polymorphic value[x] when value_type is omitted."""
        url = url or f"http://example.org/fhir/StructureDefinition/{self.p.faker.word()}"
        if value_type is None:
            value_type = self.rng.choice(_EXTENSION_VALUE_TYPES)
        value_map = {
            "string": {"valueString": self.p.gen_string()},
            "boolean": {"valueBoolean": self.p.gen_boolean()},
            "integer": {"valueInteger": self.p.gen_integer(0, 100)},
            "decimal": {"valueDecimal": self.p.gen_decimal(0, 10)},
            "code": {"valueCode": self.p.gen_code()},
            "uri": {"valueUri": self.p.gen_uri()},
        }
        ext: dict = {"url": url}
        ext.update(value_map.get(value_type, value_map["string"]))
        return ext

    def gen_Dosage(self, route_code: dict | None = None) -> dict:
        """FHIR Dosage — medication dosage instructions."""
        route = route_code or random_code("dosage_routes", self.rng)
        dose_value = round(self.rng.uniform(50.0, 1000.0), 1)
        return {
            "sequence": self.rng.randint(1, 3),
            "text": f"Take {dose_value}mg by mouth",
            "timing": self.gen_Timing(),
            "route": self.gen_CodeableConcept(
                system=get_system("dosage_routes"),
                code=route["code"] if route else "26643006",
                display=route["display"] if route else "Oral route",
            ),
            "doseAndRate": [
                {
                    "type": self.gen_CodeableConcept(
                        system="http://terminology.hl7.org/CodeSystem/dose-rate-type",
                        code="ordered",
                        display="Ordered",
                    ),
                    "doseQuantity": self.gen_Quantity(
                        value=dose_value,
                        unit="mg",
                        code="mg",
                    ),
                }
            ],
            "maxDosePerPeriod": [self.gen_Ratio()],
        }

    def gen_ContactDetail(self, name: str | None = None) -> dict:
        return {
            "name": name or self.p.faker.name(),
            "telecom": [
                self.gen_ContactPoint(system="email"),
                self.gen_ContactPoint(system="phone"),
            ],
        }

    def gen_UsageContext(self) -> dict:
        return {
            "code": self.gen_Coding(
                system="http://terminology.hl7.org/CodeSystem/usage-context-type",
                code=self.rng.choice(["gender", "age", "focus", "user", "workflow", "task"]),
            ),
            "valueCodeableConcept": self.gen_CodeableConcept(),
        }

    def gen_DataRequirement(self) -> dict:
        return {
            "type": self.rng.choice(
                ["Patient", "Observation", "Condition", "MedicationRequest"]
            ),
            "mustSupport": [self.p.faker.word() for _ in range(self.rng.randint(1, 3))],
        }

    def gen_Expression(self) -> dict:
        return {
            "language": self.rng.choice(
                ["text/fhirpath", "text/cql", "application/x-fhir-query"]
            ),
            "expression": f"Patient.where(id = '{self.p.gen_id()}')",
        }

    def gen_TriggerDefinition(self) -> dict:
        return {
            "type": self.rng.choice(
                [
                    "named-event",
                    "periodic",
                    "data-changed",
                    "data-accessed",
                    "data-access-ended",
                ]
            ),
            "name": self.p.faker.word(),
        }

    def gen_Availability(self) -> dict:
        return {
            "availableTime": [self.gen_Timing()],
            "notAvailableTime": [],
        }

    def gen_Contributor(self) -> dict:
        return {
            "type": self.rng.choice(["author", "editor", "reviewer", "endorser"]),
            "name": self.p.faker.name(),
            "contact": [self.gen_ContactDetail()],
        }

    def gen_ExtendedContactDetail(self) -> dict:
        return {
            "purpose": self.gen_CodeableConcept(),
            "name": [self.gen_HumanName()],
            "telecom": [self.gen_ContactPoint()],
        }

    def gen_MonetaryComponent(self) -> dict:
        return {
            "type": self.rng.choice(
                ["base", "surcharge", "deduction", "discount", "tax", "informational"]
            ),
            "code": self.gen_CodeableConcept(),
            "factor": self.p.gen_decimal(0.8, 1.2),
            "amount": self.gen_Money(),
        }

    def gen_ParameterDefinition(self) -> dict:
        return {
            "name": self.p.faker.word(),
            "use": self.rng.choice(["in", "out"]),
            "type": self.rng.choice(["string", "integer", "boolean", "CodeableConcept"]),
        }

    def gen_VirtualServiceDetail(self) -> dict:
        return {
            "channelType": self.gen_Coding(
                system="http://terminology.hl7.org/CodeSystem/service-mode",
                code=self.rng.choice(["phone", "video", "chat"]),
            ),
        }

    def gen_ElementDefinition(self) -> dict:
        return {
            "path": self.p.faker.word(),
            "min": 0,
            "max": "1",
            "type": [
                {
                    "code": self.rng.choice(
                        ["string", "CodeableConcept", "Reference"]
                    )
                }
            ],
        }
