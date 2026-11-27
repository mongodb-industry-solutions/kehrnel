"""FHIR R5 primitive datatype value generators."""

from __future__ import annotations

import base64
import random as _random
import re
import uuid
from datetime import date, datetime, timedelta

from faker import Faker

from .healthcare_text import (
    clinical_markdown,
    clinical_text,
    clinical_xhtml,
)

# FHIR id: any combination of A-Z a-z 0-9 - . with length 1..64
_ID_PATTERN = re.compile(r"^[A-Za-z0-9\-\.]{1,64}$")


class PrimitiveGenerator:
    """Generates FHIR R5 primitive datatype values."""

    PRIMITIVE_TYPES = frozenset({
        "base64Binary", "boolean", "canonical", "code", "date", "dateTime",
        "decimal", "id", "instant", "integer", "integer64", "markdown", "oid",
        "positiveInt", "string", "time", "unsignedInt", "uri", "url", "uuid",
        "xhtml",
    })

    def __init__(self, seed: int | None = None):
        self._seed = seed
        self.rng = _random.Random(seed)
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)

    def generate(self, type_name: str, **kwargs) -> object:
        """Dispatch to the correct generator by FHIR primitive type name."""
        method = getattr(self, f"gen_{type_name}", self.gen_string)
        return method(**kwargs)

    def _uuid4(self) -> uuid.UUID:
        return uuid.UUID(int=self.rng.getrandbits(128), version=4)

    def gen_id(self, **_) -> str:
        """FHIR id: [A-Za-z0-9\\-\\.]{1,64}"""
        value = str(self._uuid4())
        assert _ID_PATTERN.match(value)
        return value

    def gen_string(
        self,
        max_length: int = 100,
        resource_type: str | None = None,
        field_name: str | None = None,
        **_,
    ) -> str:
        return clinical_text(
            self.rng,
            resource_type=resource_type,
            field_name=field_name,
            max_length=max_length,
        )

    def gen_boolean(self, **_) -> bool:
        return self.rng.choice([True, False])

    def gen_integer(self, min_val: int = -(2**31), max_val: int = 2**31 - 1, **_) -> int:
        lo = max(min_val, -1000)
        hi = min(max_val, 1000)
        return self.rng.randint(lo, hi)

    def gen_integer64(self, **_) -> int:
        return self.rng.randint(-10**9, 10**9)

    def gen_decimal(
        self,
        min_val: float = 0.0,
        max_val: float = 1000.0,
        precision: int = 2,
        **_,
    ) -> float:
        return round(self.rng.uniform(min_val, max_val), precision)

    def gen_unsignedInt(self, max_val: int = 1000, **_) -> int:
        return self.rng.randint(0, max_val)

    def gen_positiveInt(self, max_val: int = 1000, **_) -> int:
        return self.rng.randint(1, max_val)

    def gen_date(self, min_year: int = 1920, max_year: int = 2024, **_) -> str:
        """FHIR date: YYYY-MM-DD (full date for valid clinical records)."""
        start = date(min_year, 1, 1)
        end = date(max_year, 12, 31)
        delta = (end - start).days
        d = start + timedelta(days=self.rng.randint(0, delta))
        return d.strftime("%Y-%m-%d")

    def gen_dateTime(self, min_year: int = 2000, max_year: int = 2024, **_) -> str:
        """FHIR dateTime: YYYY-MM-DDThh:mm:ss+zz:zz"""
        start = datetime(min_year, 1, 1)
        end = datetime(max_year, 12, 31, 23, 59, 59)
        delta = int((end - start).total_seconds())
        dt = start + timedelta(seconds=self.rng.randint(0, max(delta, 0)))
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def gen_instant(self, **_) -> str:
        """FHIR instant: YYYY-MM-DDThh:mm:ss.sssZ"""
        start = datetime(2000, 1, 1)
        end = datetime(2024, 12, 31, 23, 59, 59)
        delta = int((end - start).total_seconds())
        dt = start + timedelta(seconds=self.rng.randint(0, max(delta, 0)))
        ms = self.rng.randint(0, 999)
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"

    def gen_time(self, **_) -> str:
        """FHIR time: hh:mm:ss"""
        h = self.rng.randint(0, 23)
        m = self.rng.randint(0, 59)
        s = self.rng.randint(0, 59)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def gen_uri(self, **_) -> str:
        return f"urn:uuid:{self._uuid4()}"

    def gen_url(self, **_) -> str:
        return self.faker.url()

    def gen_canonical(self, base: str = "http://example.org/fhir", **_) -> str:
        resource = self.rng.choice(["StructureDefinition", "ValueSet", "CodeSystem"])
        name = self.faker.word().lower()
        version = f"{self.rng.randint(1, 3)}.{self.rng.randint(0, 9)}"
        return f"{base}/{resource}/{name}|{version}"

    def gen_code(self, code_set: list[str] | None = None, **_) -> str:
        if code_set:
            return self.rng.choice(code_set)
        return self.faker.lexify("????").lower()

    def gen_oid(self, **_) -> str:
        """OID: urn:oid:x.x.x..."""
        parts = [str(self.rng.randint(1, 9))] + [
            str(self.rng.randint(0, 999)) for _ in range(self.rng.randint(3, 7))
        ]
        return "urn:oid:" + ".".join(parts)

    def gen_uuid(self, **_) -> str:
        return f"urn:uuid:{self._uuid4()}"

    def gen_markdown(
        self,
        resource_type: str | None = None,
        field_name: str | None = "note",
        **_,
    ) -> str:
        return clinical_markdown(
            self.rng, resource_type=resource_type, field_name=field_name
        )

    def gen_base64Binary(self, byte_count: int = 64, **_) -> str:
        data = bytes(self.rng.getrandbits(8) for _ in range(byte_count))
        return base64.b64encode(data).decode()

    def gen_xhtml(
        self,
        resource_type: str | None = None,
        field_name: str | None = "note",
        **_,
    ) -> str:
        return clinical_xhtml(
            self.rng, resource_type=resource_type, field_name=field_name
        )
