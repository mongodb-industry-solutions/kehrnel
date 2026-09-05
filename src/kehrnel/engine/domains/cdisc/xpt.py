"""SAS XPORT to Dataset-JSON conversion with optional Define-XML enrichment."""

from __future__ import annotations

import math
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from .define_xml import DefineDocument


def _read_xport(pyreadstat, path: str):
    """Read modern UTF-8 XPT and tolerate common legacy Windows-1252 text."""

    try:
        return pyreadstat.read_xport(path, output_format="dict")
    except UnicodeDecodeError:
        return pyreadstat.read_xport(path, output_format="dict", encoding="WINDOWS-1252")


def _json_value(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    item = getattr(value, "item", None)
    return item() if callable(item) else value


def _resolve_xpt_domain(table_name: Any, values: Dict[str, Any]) -> str:
    """Resolve the CDISC domain without losing supplemental-qualifier identity.

    Some valid XPT producers label every SUPPQUAL member ``SUPP`` even though
    the standard dataset identity is ``SUPP`` plus its one RDOMAIN value (for
    example SUPPMA or SUPPMI). Treating all of those members as one domain
    causes immutable dataset and record identity collisions during package
    ingestion.
    """

    domain = str(table_name or "").strip().upper()
    if domain == "SUPP" and "RDOMAIN" in values:
        related_domains = {
            str(value).strip().upper()
            for value in values["RDOMAIN"]
            if value not in (None, "") and str(value).strip()
        }
        if len(related_domains) == 1:
            return f"SUPP{related_domains.pop()}"
    if not domain and "DOMAIN" in values:
        domain_values = {
            str(value).strip().upper()
            for value in values["DOMAIN"]
            if value not in (None, "") and str(value).strip()
        }
        if len(domain_values) == 1:
            return domain_values.pop()
    return domain


def xpt_to_dataset_json(
    content: bytes,
    *,
    define: Optional[DefineDocument] = None,
    study_oid: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        import pyreadstat
    except ImportError as exc:
        raise RuntimeError("XPT support requires the 'cdisc' extra (pyreadstat)") from exc
    if not isinstance(content, bytes) or not content:
        raise ValueError("XPT content must be non-empty bytes")
    descriptor, path = tempfile.mkstemp(suffix=".xpt")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
        values, metadata = _read_xport(pyreadstat, path)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    names = list(metadata.column_names)
    record_count = len(values[names[0]]) if names else 0
    domain = _resolve_xpt_domain(metadata.table_name, values)
    if not domain:
        raise ValueError("XPT dataset domain/name could not be determined")
    resolved_study_oid = study_oid or (define.study_oid if define else None)
    if not resolved_study_oid and "STUDYID" in values:
        study_values = {str(value) for value in values["STUDYID"] if value not in (None, "")}
        if len(study_values) == 1:
            resolved_study_oid = study_values.pop()
    if not resolved_study_oid:
        raise ValueError("studyOID is required when XPT does not contain one unique STUDYID")

    defined_dataset = define.datasets.get(domain) if define else None
    defined_variables = {variable.name: variable for variable in defined_dataset.variables} if defined_dataset else {}
    columns = []
    labels = dict(zip(metadata.column_names, metadata.column_labels))
    for ordinal, name in enumerate(names, start=1):
        defined = defined_variables.get(name)
        readstat_type = metadata.readstat_variable_types.get(name)
        column = {
            "itemOID": defined.item_oid if defined else f"{domain}.{name}",
            "name": name,
            "label": defined.label if defined else labels.get(name),
            "dataType": defined.data_type if defined else ("string" if readstat_type == "string" else "float"),
        }
        length = defined.length if defined else metadata.variable_storage_width.get(name)
        if length:
            column["length"] = int(length)
        if defined and defined.key_sequence:
            column["keySequence"] = defined.key_sequence
        if defined and (defined.mandatory or defined.role):
            column["origin"] = {
                key: value
                for key, value in {
                    "mandatory": defined.mandatory,
                    "role": defined.role,
                    "source": "Define-XML",
                }.items()
                if value is not None
            }
        columns.append({key: value for key, value in column.items() if value is not None})
    rows = [
        [_json_value(values[name][row_index]) for name in names]
        for row_index in range(record_count)
    ]
    document: Dict[str, Any] = {
        "datasetJSONVersion": "1.1.0",
        "fileOID": f"{resolved_study_oid}.{domain.lower()}",
        "studyOID": resolved_study_oid,
        "metaDataVersionOID": define.metadata_version_oid if define else "UNSPECIFIED",
        "itemGroupOID": defined_dataset.item_group_oid if defined_dataset else domain,
        "records": record_count,
        "name": domain,
        "label": (defined_dataset.label if defined_dataset else metadata.file_label) or domain,
        "columns": columns,
        "rows": rows,
        "sourceSystem": {"name": "pyreadstat", "version": str(pyreadstat.__version__)},
    }
    return document


def dataset_json_to_xpt(document: Dict[str, Any], *, version: int = 5) -> bytes:
    """Encode a Dataset-JSON document as SAS XPORT v5 or v8 bytes."""

    try:
        import pandas as pd
        import pyreadstat
    except ImportError as exc:
        raise RuntimeError("XPT support requires the 'cdisc' extra (pyreadstat)") from exc
    if version not in {5, 8}:
        raise ValueError("XPT file format version must be 5 or 8")
    columns = document.get("columns") or []
    names = [str(column.get("name") or "") for column in columns]
    if not names or any(not name for name in names):
        raise ValueError("Dataset-JSON columns require names")
    if version == 5 and any(len(name.encode("ascii", errors="ignore")) > 8 for name in names):
        raise ValueError("XPT v5 variable names cannot exceed 8 ASCII characters")
    rows = document.get("rows") or []
    frame = pd.DataFrame(rows, columns=names)
    labels = [column.get("label") for column in columns]
    descriptor, path = tempfile.mkstemp(suffix=".xpt")
    os.close(descriptor)
    try:
        pyreadstat.write_xport(
            frame,
            path,
            file_label=str(document.get("label") or document.get("name") or "")[:40],
            column_labels=labels,
            table_name=str(document.get("name") or "DATA")[:8],
            file_format_version=version,
        )
        return Path(path).read_bytes()
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
