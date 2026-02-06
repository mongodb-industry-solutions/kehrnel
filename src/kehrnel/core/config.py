"""Config validation helpers."""
from __future__ import annotations

import jsonschema

from kehrnel.core.errors import KehrnelError


def validate_config(schema: dict, config: dict):
    """Validate config against JSON Schema; raise KehrnelError with details on failure."""
    if not schema:
        return
    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.ValidationError as exc:
        path = list(exc.absolute_path)
        details = {"path": path, "schema_error": exc.message}
        raise KehrnelError(code="CONFIG_INVALID", status=400, message="Configuration validation failed", details=details) from exc
