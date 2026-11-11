"""Deterministic, side-effect-free execution of the bounded Mapping IR v1."""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from datetime import datetime
from typing import Any, Iterable

from kehrnel.contracts.artifacts import ExecutionState
from kehrnel.contracts.mapping_ir import (
    CdcOperation,
    ErrorBehavior,
    FieldLineage,
    MappingDefinition,
    MappingExecutionMode,
    MappedDocumentRoute,
    MappingRecordError,
    MappingSelector,
    MappingSimulationRequest,
    MappingSimulationResult,
    MappingTransformOperation,
    SelectorDialect,
)
from kehrnel.engine.core.mappings import load_mapping_catalog, mapping_index


class MappingExecutionError(ValueError):
    """A mapping simulation request cannot be safely executed."""


class _RuleError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        source_path: str | None = None,
        rule_id: str | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.source_path = source_path
        self.rule_id = rule_id


_MISSING = object()


def _stable_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_stable_json_bytes(value)).hexdigest()


def _pointer_parts(path: str) -> list[str]:
    if not path.startswith("/"):
        raise _RuleError("INVALID_SELECTOR", "JSON Pointer must start with '/'")
    return [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")]


def _select(document: Any, selector: MappingSelector) -> Any:
    if selector.dialect is SelectorDialect.DOTTED_PATH:
        parts = selector.path.split(".")
    else:
        parts = _pointer_parts(selector.path)
    current = document
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
            continue
        return _MISSING
    return current


def _set_pointer(document: dict[str, Any], pointer: str, value: Any) -> None:
    parts = _pointer_parts(pointer)
    if not parts or parts == [""]:
        raise _RuleError("INVALID_TARGET", "mapping cannot replace the document root")
    current: Any = document
    for index, part in enumerate(parts[:-1]):
        next_is_index = parts[index + 1].isdigit()
        if isinstance(current, dict):
            if part not in current:
                current[part] = [] if next_is_index else {}
            child = current[part]
        elif isinstance(current, list) and part.isdigit():
            position = int(part)
            while len(current) <= position:
                current.append(None)
            if current[position] is None:
                current[position] = [] if next_is_index else {}
            child = current[position]
        else:
            raise _RuleError(
                "TARGET_PATH_CONFLICT",
                f"target path '{pointer}' conflicts with an existing scalar",
            )
        if not isinstance(child, (dict, list)):
            raise _RuleError(
                "TARGET_PATH_CONFLICT",
                f"target path '{pointer}' conflicts with an existing scalar",
            )
        current = child

    leaf = parts[-1]
    if isinstance(current, dict):
        current[leaf] = value
        return
    if isinstance(current, list) and leaf.isdigit():
        position = int(leaf)
        while len(current) <= position:
            current.append(None)
        current[position] = value
        return
    raise _RuleError("INVALID_TARGET", f"cannot write target path '{pointer}'")


def _transform(value: Any, operation: MappingTransformOperation) -> Any:
    if operation is MappingTransformOperation.TO_STRING:
        if isinstance(value, (dict, list)):
            raise ValueError("objects and arrays cannot be converted to string")
        return str(value)
    if operation is MappingTransformOperation.TO_INTEGER:
        if isinstance(value, bool):
            raise ValueError("booleans cannot be converted to integer")
        return int(value)
    if operation is MappingTransformOperation.TO_DECIMAL:
        if isinstance(value, bool):
            raise ValueError("booleans cannot be converted to decimal")
        converted = float(value)
        if converted != converted or converted in (float("inf"), float("-inf")):
            raise ValueError("decimal must be finite")
        return converted
    if operation is MappingTransformOperation.ISO_DATETIME:
        if not isinstance(value, str):
            raise ValueError("ISO datetime transform requires a string")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("ISO datetime must include a UTC offset")
        return parsed.isoformat()
    if operation is MappingTransformOperation.LOWERCASE:
        if not isinstance(value, str):
            raise ValueError("lowercase transform requires a string")
        return value.lower()
    if operation is MappingTransformOperation.UPPERCASE:
        if not isinstance(value, str):
            raise ValueError("uppercase transform requires a string")
        return value.upper()
    raise ValueError(f"unsupported transform operation '{operation}'")


def _idempotency_key(mapping: MappingDefinition, record: dict[str, Any]) -> str:
    values = []
    for selector in mapping.execution.idempotency_selectors:
        value = _select(record, selector)
        if value is _MISSING or value is None:
            raise _RuleError(
                "IDEMPOTENCY_KEY_MISSING",
                f"required idempotency field '{selector.path}' is missing",
                source_path=selector.path,
            )
        values.append(value)
    try:
        return _digest(
            {
                "mappingDigest": mapping.artifact_ref.digest,
                "sourceIdentity": values,
            }
        )
    except (TypeError, ValueError) as exc:
        raise _RuleError(
            "INVALID_IDEMPOTENCY_VALUE",
            "idempotency fields must contain finite JSON values",
        ) from exc


def _identity_string(value: Any, *, code: str, source_path: str) -> str:
    if not isinstance(value, (str, int, float, bool)):
        raise _RuleError(
            code,
            "target identity source must be a scalar JSON value",
            source_path=source_path,
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise _RuleError(
            code,
            "target identity source must be finite",
            source_path=source_path,
        )
    return str(value)


def _target_route(
    mapping: MappingDefinition,
    record: dict[str, Any],
    document_index: int,
) -> MappedDocumentRoute:
    document_id = _select(record, mapping.target.document_id_source)
    if document_id is _MISSING or document_id is None:
        raise _RuleError(
            "TARGET_DOCUMENT_ID_MISSING",
            "target document identity source is missing",
            source_path=mapping.target.document_id_source.path,
        )
    subject_key = None
    if mapping.target.subject_key_source is not None:
        subject_key = _select(record, mapping.target.subject_key_source)
        if subject_key is _MISSING or subject_key is None:
            raise _RuleError(
                "TARGET_SUBJECT_KEY_MISSING",
                "target subject identity source is missing",
                source_path=mapping.target.subject_key_source.path,
            )
    return MappedDocumentRoute(
        document_index=document_index,
        document_id=_identity_string(
            document_id,
            code="INVALID_TARGET_DOCUMENT_ID",
            source_path=mapping.target.document_id_source.path,
        ),
        subject_key=(
            None
            if subject_key is None
            else _identity_string(
                subject_key,
                code="INVALID_TARGET_SUBJECT_KEY",
                source_path=mapping.target.subject_key_source.path,
            )
        ),
    )


def _execute_record(
    mapping: MappingDefinition,
    record: dict[str, Any],
    document_index: int,
) -> tuple[dict[str, Any], str, list[FieldLineage]]:
    idempotency_key = _idempotency_key(mapping, record)
    output: dict[str, Any] = {}
    lineage: list[FieldLineage] = []
    for rule in mapping.rules:
        if rule.source is None:
            value = deepcopy(rule.constant)
            source_kind = "constant"
            source_path = None
        else:
            value = _select(record, rule.source)
            source_kind = "field"
            source_path = rule.source.path
            if value is _MISSING or value is None:
                if not rule.required:
                    continue
                raise _RuleError(
                    "REQUIRED_SOURCE_FIELD_MISSING",
                    f"required source field '{source_path}' is missing",
                    source_path=source_path,
                    rule_id=rule.rule_id,
                )
            if isinstance(value, (dict, list)):
                raise _RuleError(
                    "NON_SCALAR_SOURCE_VALUE",
                    f"mapping v1 source field '{source_path}' must be scalar",
                    source_path=source_path,
                    rule_id=rule.rule_id,
                )
        try:
            for step in rule.transforms:
                value = _transform(value, step.operation)
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise ValueError("mapping output must be a JSON scalar")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("mapping output number must be finite")
            _set_pointer(output, rule.target.path, value)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, _RuleError):
                if exc.rule_id is None:
                    exc.rule_id = rule.rule_id
                raise
            raise _RuleError(
                "TRANSFORM_FAILED",
                f"rule '{rule.rule_id}' failed: {exc}",
                source_path=source_path,
                rule_id=rule.rule_id,
            ) from exc
        lineage.append(
            FieldLineage(
                document_index=document_index,
                rule_id=rule.rule_id,
                target_path=rule.target.path,
                source_path=source_path,
                source_kind=source_kind,
                transform_operations=[step.operation for step in rule.transforms],
                semantic_concept_id=rule.semantic_concept_id,
                terminology_ref=rule.terminology_ref,
            )
        )
    return output, idempotency_key, lineage


def _records(
    mapping: MappingDefinition, request: MappingSimulationRequest
) -> Iterable[tuple[int, dict[str, Any] | None, _RuleError | None]]:
    if request.mode is MappingExecutionMode.BATCH:
        for index, record in enumerate(request.records):
            yield index, record, None
        return

    for index, event in enumerate(request.events):
        if event.operation is CdcOperation.DELETE:
            yield index, None, _RuleError(
                "DELETE_NOT_SUPPORTED",
                "this mapping requires source-state reconciliation for deletes",
            )
            continue
        yield index, event.after, None


def simulate_mapping(
    request: MappingSimulationRequest,
    *,
    catalog=None,
) -> MappingSimulationResult:
    """Execute one exact mapping version without writes or external calls."""

    catalog = catalog or load_mapping_catalog()
    key = (
        request.mapping_ref.id,
        request.mapping_ref.version,
        request.mapping_ref.digest,
    )
    mapping = mapping_index(catalog).get(key)
    if mapping is None:
        raise KeyError(
            f"mapping not found: {request.mapping_ref.id}@{request.mapping_ref.version}"
        )
    if request.mode not in mapping.execution.supported_modes:
        raise MappingExecutionError(
            f"mapping does not support execution mode '{request.mode.value}'"
        )

    documents: list[dict[str, Any]] = []
    document_routes: list[MappedDocumentRoute] = []
    idempotency_keys: list[str] = []
    lineage: list[FieldLineage] = []
    errors: list[MappingRecordError] = []
    for record_index, record, preflight_error in _records(mapping, request):
        try:
            if preflight_error is not None:
                raise preflight_error
            if record is None:
                raise _RuleError("SOURCE_RECORD_MISSING", "source record is missing")
            document, idempotency_key, record_lineage = _execute_record(
                mapping, record, len(documents)
            )
            document_route = _target_route(mapping, record, len(documents))
            documents.append(document)
            document_routes.append(document_route)
            idempotency_keys.append(idempotency_key)
            lineage.extend(record_lineage)
        except _RuleError as exc:
            errors.append(
                MappingRecordError(
                    record_index=record_index,
                    code=exc.code,
                    message=str(exc),
                    rule_id=exc.rule_id,
                    source_path=exc.source_path,
                    dead_letter_contract=mapping.execution.dead_letter_contract,
                )
            )
            if mapping.execution.error_behavior is ErrorBehavior.FAIL_BATCH:
                documents = []
                document_routes = []
                idempotency_keys = []
                lineage = []
                break

    return MappingSimulationResult(
        contract="mapping-simulation-result/v1",
        mapping_ref=mapping.artifact_ref,
        state=ExecutionState.EXECUTED if documents else ExecutionState.FAILED,
        mode=request.mode,
        documents=documents,
        document_routes=document_routes,
        document_idempotency_keys=idempotency_keys,
        lineage=lineage,
        errors=errors,
        output_digest=_digest(documents),
    )
