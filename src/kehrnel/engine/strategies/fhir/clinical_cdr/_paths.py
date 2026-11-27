"""Paths for the fhir.clinical_cdr strategy pack layout."""

from pathlib import Path

PACK_ROOT = Path(__file__).resolve().parent
SPEC_DIR = PACK_ROOT / "specification"
SCRIPTS_DIR = PACK_ROOT / "scripts"
FHIR_LIBS_DIR = PACK_ROOT.parents[2] / "domains" / "fhir" / "libs"
FHIR_GEN_ROOT = FHIR_LIBS_DIR / "fhir-data-generation"
FHIR_MQL_ROOT = FHIR_LIBS_DIR / "fhir-search-to-mql"
