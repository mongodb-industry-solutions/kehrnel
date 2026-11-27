from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PACKAGE_ROOT = Path(__file__).parent
LIB_ROOT = PACKAGE_ROOT.parent
_LIB_ENV_FILE = LIB_ROOT / ".env"
SCHEMA_PATH = PACKAGE_ROOT / "schema" / "fhir.schema.v5.json"
V6_SCHEMA_PATH = PACKAGE_ROOT / "schema" / "fhir.schema.v6.json"
CODES_PATH = PACKAGE_ROOT / "hl7_codes" / "healthcare_codes.yaml"
DEFAULT_SCHEMA_VERSION = "R5"


class Settings(BaseSettings):
    # Load .env from the fhir-gen package root only (not host app cwd, e.g. kehrnel).
    model_config = SettingsConfigDict(
        env_prefix="FHIR_GEN_",
        env_file=_LIB_ENV_FILE if _LIB_ENV_FILE.is_file() else None,
        extra="ignore",
    )

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db: str = "fhir_synthetic"
    mongodb_collection_prefix: str = ""
    default_locale: str = "en-US"
    seed: int | None = None
    schema_version: str = DEFAULT_SCHEMA_VERSION
    schema_path: Path | None = None
    codes_path: Path = CODES_PATH
    log_level: str = "INFO"

    @field_validator("schema_version", mode="before")
    @classmethod
    def _coerce_schema_version(cls, value: object) -> str:
        if value is None or value == "":
            return DEFAULT_SCHEMA_VERSION
        return str(value).strip()

    @property
    def resolved_schema_path(self) -> Path:
        from fhir_gen.schema.versions import resolve_schema_path

        return resolve_schema_path(
            schema_version=self.schema_version,
            schema_path=self.schema_path,
        )

    @property
    def fhir_version(self) -> str:
        """R5 or R6 — from ``schema_version`` or explicit ``schema_path``."""
        from fhir_gen.schema.versions import normalize_schema_version

        if self.schema_path is not None:
            name = self.schema_path.name.lower()
            if "v6" in name:
                return "R6"
            if "v5" in name:
                return "R5"
        return normalize_schema_version(self.schema_version)


settings = Settings()
