#!/usr/bin/env python3
"""Qualify deployed FHIR/openEHR semantic execution and write a safe receipt."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pydantic import ValidationError

from kehrnel.contracts.semantic_qualification import (
    QualificationHttpResponse,
    SemanticExecutionQualificationSpec,
    run_semantic_execution_qualification,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute a semantic qualification spec against one Kehrnel environment "
            "and write a redacted, digest-bound result artifact."
        )
    )
    parser.add_argument(
        "--spec", required=True, type=Path, help="Qualification spec JSON"
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Result artifact JSON"
    )
    parser.add_argument(
        "--base-url",
        help="Override baseUrl from the spec without modifying the reference file",
    )
    parser.add_argument(
        "--environment-id",
        help="Override environmentId from the spec without modifying the reference file",
    )
    parser.add_argument(
        "--api-key-env",
        default="KEHRNEL_API_KEY",
        help="Environment variable containing the API key (default: KEHRNEL_API_KEY)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-probe timeout in seconds (default: 30, maximum: 300)",
    )
    return parser.parse_args()


def _http_transport(
    url: str,
    body: dict[str, Any],
    headers: Mapping[str, str],
    timeout: float,
) -> QualificationHttpResponse:
    encoded = json.dumps(
        body,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(url, data=encoded, headers=dict(headers), method="POST")
    try:
        # The qualification contract validates the URL scheme and strips
        # credential-bearing, query-bearing, and fragment-bearing targets.
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            return QualificationHttpResponse(
                status=response.status,
                payload=_read_json_object(response.read()),
            )
    except HTTPError as exc:
        return QualificationHttpResponse(
            status=exc.code,
            payload=_read_json_object(exc.read()),
        )
    except (URLError, TimeoutError, socket.timeout) as exc:
        raise RuntimeError("semantic endpoint transport failed") from exc


def _read_json_object(data: bytes) -> dict[str, Any] | None:
    if not data:
        return None
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def main() -> int:
    args = _parse_args()
    try:
        spec = SemanticExecutionQualificationSpec.model_validate_json(
            args.spec.read_text(encoding="utf-8")
        )
        if args.base_url or args.environment_id:
            payload = spec.to_wire()
            if args.base_url:
                payload["baseUrl"] = args.base_url
            if args.environment_id:
                payload["environmentId"] = args.environment_id
            spec = SemanticExecutionQualificationSpec.model_validate(payload)
        api_key = os.getenv(args.api_key_env)
        result = run_semantic_execution_qualification(
            spec,
            _http_transport,
            api_key=api_key,
            timeout_seconds=args.timeout,
        )
    except (OSError, ValidationError, ValueError) as exc:
        print(f"qualification setup failed: {exc}", file=sys.stderr)
        return 2

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result.to_wire(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"semantic qualification {result.status}: {result.artifact_digest} "
        f"({args.output})"
    )
    return 0 if result.status == "qualified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
