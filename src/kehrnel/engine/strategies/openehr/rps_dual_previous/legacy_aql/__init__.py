"""Legacy AQL→MQL wrappers (vendored transformers)."""
from __future__ import annotations

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from .compile_patient import compile_patient
from .compile_cross_patient import compile_cross_patient
