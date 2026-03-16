# src/kehrnel/api/strategies/openehr/rps_dual/ingest/routes.py

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import copy
import os
import logging
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.ingest.exceptions_g import FlattenerError
from kehrnel.api.strategies.openehr.rps_dual.ingest.service import IngestionService
from kehrnel.api.strategies.openehr.rps_dual.ingest.repository import IngestionRepository
from kehrnel.api.bridge.app.utils.config_runtime import DEFAULT_MAPPINGS_PATH
from kehrnel.strategy_sdk import StrategyBindings
from kehrnel.strategy_sdk.runtime import StrategyRuntimeError

from kehrnel.api.strategies.openehr.rps_dual.ingest.models import (
    FilePathRequest,
    EhrIdRequest,
    CanonicalCompositionPayload,
    IngestionSuccessResponse
)
from kehrnel.api.strategies.openehr.rps_dual.ingest.api_responses import (
    ingest_from_payload_responses,
    ingest_from_file_responses,
    ingest_from_db_responses,
    ingest_from_body_example
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.remap import remap_fields_for_config

router = APIRouter()
logger = logging.getLogger(__name__)


def _allow_local_file_ingest() -> bool:
    return os.getenv("KEHRNEL_ALLOW_LOCAL_FILE_INPUTS", "false").lower() in ("1", "true", "yes")


def _local_file_inputs_base_dir() -> Path:
    """
    Base directory for any server-side "read local file" features.

    Rationale: if KEHRNEL_ALLOW_LOCAL_FILE_INPUTS=true is enabled, we still want a
    guardrail to prevent reading arbitrary files (e.g. /etc/passwd) via API.
    """
    raw = (os.getenv("KEHRNEL_LOCAL_FILE_INPUTS_BASE_DIR") or "").strip()
    if not raw:
        return Path.cwd().resolve()
    return Path(raw).expanduser().resolve()


def _validate_local_ingest_path(file_path: str) -> str:
    p = Path(file_path)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    base = _local_file_inputs_base_dir()
    try:
        p.relative_to(base)
    except ValueError:
        raise ValueError("Provided path is outside the allowed inputs directory")
    if not p.exists() or not p.is_file():
        raise FileNotFoundError("File not found")
    if p.suffix.lower() != ".json":
        raise ValueError("Only .json files are allowed for file ingest")
    return str(p)


def get_ingestion_service(request: Request) -> IngestionService | None:
    """Dependency to create and provide the IngestionService.

    Returns None if config is not available (preview mode can still work).
    """
    # Use explicit None checks - MongoDB objects don't support bool()
    target_db = getattr(request.app.state, "target_db", None)
    if target_db is None:
        target_db = getattr(request.app.state, "db", None)
    source_db = getattr(request.app.state, "source_db", None)
    if source_db is None:
        source_db = target_db
    config = getattr(request.app.state, "config", None)
    options = getattr(request.app.state, "ingest_options", None) or {}
    flattener = getattr(request.app.state, "flattener", None)

    # If config is None, we can't create a full repository - return None
    # Preview mode will handle this gracefully
    if config is None:
        return None

    repository = IngestionRepository(
        target_db=target_db,
        source_db=source_db,
        config=config,
        options=options,
    )
    return IngestionService(flattener=flattener, repository=repository)


@router.post(
    "/body",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from Request Body",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_payload_responses
)
async def ingest_from_payload(
    payload: CanonicalCompositionPayload = Body(..., examples={ "default": ingest_from_body_example }),
    service: IngestionService | None = Depends(get_ingestion_service),
    request: Request = None,
):
    """
    Receives a canonical OpenEHR composition document in the request body, transforms it into the
    flattened format, and stores it in the target database.

    This is a custom ingestion endpoint, not part of the standard OpenEHR REST API.
    """
    try:
        raw_doc = payload.root
        preview_mode = False
        if request:
            preview_mode = (
                request.query_params.get("preview") == "true"
                or request.headers.get("x-preview-mode") == "true"
                or raw_doc.pop("preview", False) is True
            )

        # For non-preview mode, we need a valid service
        if not preview_mode and service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Ingestion service not configured. Use /api/strategies/openehr/rps_dual/config to set up ingestion configuration."
            )

        # Preview: transform and return docs without persisting
        if preview_mode:
            runtime_config = raw_doc.pop("config", None)
            analytics_paths = (
                (runtime_config or {}).get("analytics_allowed_paths")
                or (runtime_config or {}).get("search", {}).get("allowed_paths")
            ) or []

            try:
                flattener = await get_runtime_flattener(request, runtime_config)
            except Exception as e:
                logger.exception("Preview flattener initialization failed")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to initialize preview transformation."
                )

            try:
                base_doc, search_doc = flattener.transform_composition(raw_doc)
            except Exception as e:
                logger.exception("Preview transformation failed")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to transform payload in preview mode."
                )
            base_doc, search_doc, path_field = remap_fields_for_config(
                base_doc, search_doc, runtime_config
            )

            # Apply analytics allowlist if provided; if empty, drop search_doc
            if analytics_paths:
                search_doc = filter_search_nodes(search_doc, analytics_paths, path_field)
            else:
                search_doc = None

            meta_doc = build_meta(raw_doc, base_doc)

            return JSONResponse(jsonable_encoder({
                "status": "preview",
                "base_doc": base_doc,
                "search_doc": search_doc,
                "meta_doc": meta_doc,
            }))

        runtime_config = raw_doc.pop("config", None)
        analytics_paths = (
            (runtime_config or {}).get("analytics_allowed_paths")
            or (runtime_config or {}).get("search", {}).get("allowed_paths")
        ) or []

        flattener = await get_runtime_flattener(request, runtime_config)

        # Optional: dispatch through strategy runtime when requested
        strategy_id = request.query_params.get("strategy_id") if request else None
        if not strategy_id:
            strategy_id = request.headers.get("x-strategy-id") if request else None
        strategy_protocol = request.query_params.get("strategy_protocol") if request else None
        if not strategy_protocol:
            strategy_protocol = request.headers.get("x-strategy-protocol") if request else None

        if strategy_id and getattr(request.app.state, "strategy_runtime", None):
            sr = request.app.state.strategy_runtime
            bindings = StrategyBindings(extras={"flattener": flattener})
            router = sr.router()
            ctx = sr.context()
            try:
                # Prefer ingest capability if implemented; fallback to transform
                ingest_result = router.dispatch(
                    "ingest",
                    payload=raw_doc,
                    strategy_id=strategy_id,
                    protocol=strategy_protocol,
                    bindings=bindings,
                    context=ctx,
                )
                base_doc = ingest_result.get("base")
                search_doc = ingest_result.get("search")
            except StrategyRuntimeError as exc:
                if "not implemented" not in str(exc).lower():
                    raise
                base_doc, search_doc = router.dispatch(
                    "transform",
                    payload=raw_doc,
                    strategy_id=strategy_id,
                    protocol=strategy_protocol,
                    bindings=bindings,
                    context=ctx,
                )
        else:
            base_doc, search_doc = flattener.transform_composition(raw_doc)
        base_doc, search_doc, path_field = remap_fields_for_config(
            base_doc, search_doc, runtime_config
        )
        if analytics_paths:
            search_doc = filter_search_nodes(search_doc, analytics_paths, path_field)
        else:
            search_doc = None
        meta_doc = build_meta(raw_doc, base_doc)

        # Persist using repository (same as before)
        new_comp_id = await service.repository.insert_flattened_composition_in_transaction(
            base_doc,
            search_doc,
            raw_canonical_doc=raw_doc,
        )

        return IngestionSuccessResponse(
            message="Composition from payload ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception:
        logger.exception("Unexpected error while ingesting payload")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error.")


# Helper: filter search nodes by allowed paths
def filter_search_nodes(search_doc, allowed_paths, path_field="p"):
    if not search_doc:
        return search_doc
    node_key = None
    for candidate in ["sn", "nodes"]:
        if candidate in search_doc:
            node_key = candidate
            break
    if not node_key:
        return search_doc
    nodes = search_doc.get(node_key)
    if not isinstance(nodes, list):
        return search_doc
    allowed = set(allowed_paths)
    filtered = [n for n in nodes if n.get(path_field) in allowed]
    return {**search_doc, node_key: filtered}


# Helper: build minimal meta doc
def build_meta(raw_doc, base_doc):
    ehr_id = raw_doc.get("ehr_id") or base_doc.get("ehr_id")
    template_id = raw_doc.get("template_id") or raw_doc.get("template_name") or base_doc.get("tid") or base_doc.get("template_id")
    comp_id = base_doc.get("_id") or raw_doc.get("_id")
    return {
        "ehr_id": ehr_id,
        "templates": [template_id] if template_id else [],
        "documents": [{
            "id": comp_id,
            "template": template_id,
            "archetype": raw_doc.get("archetype_node_id") or base_doc.get("ap")
        }]
    }


# Default config structure required by CompositionFlattener
DEFAULT_FLATTENER_CONFIG = {
    "target": {
        "codes_collection": "dictionaries",
        "shortcuts_collection": "dictionaries",
        "compositions_collection": "compositions_rps",
        "search_collection": "compositions_search",
    },
    "reverse_paths": True,
    "path_joiner": ".",
    "composition_fields": {
        "nodes": "cn",
        "path": "p",
        "archetype_path": "ap",
        "ehr_id": "ehr_id",
        "comp_id": "cid",
    },
    "search_fields": {
        "nodes": "sn",
        "path": "p",
        "ehr_id": "ehr_id",
        "comp_id": "cid",
    },
    "apply_shortcuts": False,  # Disable shortcuts for preview mode
}

# Helper: create per-request flattener with runtime overrides
async def get_runtime_flattener(request: Request, runtime_config: dict | None):
    # Get base config from existing flattener, or use default if not initialized
    base_flattener = getattr(request.app.state, "flattener", None)
    if base_flattener is not None and hasattr(base_flattener, "config"):
        base_cfg = base_flattener.config
    else:
        # Use default config when no flattener is initialized
        base_cfg = copy.deepcopy(DEFAULT_FLATTENER_CONFIG)
        logger.debug("Preview mode: using default flattener config (no preloaded flattener in app state)")

    merged = copy.deepcopy(base_cfg) if isinstance(base_cfg, dict) else copy.deepcopy(DEFAULT_FLATTENER_CONFIG)
    if runtime_config and isinstance(runtime_config, dict):
        # Deep merge runtime config
        _deep_merge(merged, runtime_config)

    # If composition/search fields passed under "fields" shape, respect them
    if runtime_config and isinstance(runtime_config, dict):
        if "fields" in runtime_config:
            merged.setdefault("composition_fields", {})
            merged.setdefault("search_fields", {})
            merged["composition_fields"].update(runtime_config["fields"].get("composition", {}) or {})
            merged["search_fields"].update(runtime_config["fields"].get("search", {}) or {})
        if "composition_fields" in runtime_config:
            merged.setdefault("composition_fields", {})
            merged["composition_fields"].update(runtime_config.get("composition_fields") or {})
        if "search_fields" in runtime_config:
            merged.setdefault("search_fields", {})
            merged["search_fields"].update(runtime_config.get("search_fields") or {})

    # Ensure target config is always present
    merged.setdefault("target", DEFAULT_FLATTENER_CONFIG["target"])

    # Get database - use explicit None check (MongoDB objects don't support bool())
    db = getattr(request.app.state, "target_db", None)
    if db is None:
        db = getattr(request.app.state, "db", None)

    logger.debug("Preview mode: creating flattener with config keys: %s", list(merged.keys()))

    return await CompositionFlattener.create(
        db=db,
        config=merged,
        mappings_path=DEFAULT_MAPPINGS_PATH,
        # Preview/runtime flattener must work without DB dictionaries.
        coding_opts={"arcodes": {"strategy": "literal"}, "atcodes": {"strategy": "literal"}},
    )


def _deep_merge(target: dict, source: dict):
    """Deep merge source into target in place."""
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value

@router.post(
    "/file",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from a Local File",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_file_responses
)
async def ingest_from_file(
    request_body: FilePathRequest,
    service: IngestionService | None = Depends(get_ingestion_service),
):
    """
    Reads a canonical composition from a JSON file on the server's filesystem,
    transforms it, and stores the flattened version.
    """
    try:
        if not _allow_local_file_ingest():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Local file ingest is disabled. Enable KEHRNEL_ALLOW_LOCAL_FILE_INPUTS=true to use this endpoint.",
            )
        if service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Ingestion service not configured.",
            )
        safe_path = _validate_local_ingest_path(request_body.file_path)
        new_comp_id = await service.ingest_from_local_file(safe_path)
        return IngestionSuccessResponse(
            message=f"Composition from file '{Path(safe_path).name}' ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception:
        logger.exception("Unexpected error while ingesting from local file")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error.")
    

@router.post(
    "/database",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest and Flatten a Canonical Composition from the Source Database",
    response_model=IngestionSuccessResponse,
    responses=ingest_from_db_responses
)
async def ingest_from_db(
    request_body: EhrIdRequest,
    service: IngestionService | None = Depends(get_ingestion_service),
):
    """
    Finds a canonical composition in the source database collection using an `ehr_id`,
    transforms it, and stores the flattened version.
    """
    try:
        if service is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Ingestion service not configured.",
            )
        new_comp_id = await service.ingest_from_database(request_body.ehr_id)
        return IngestionSuccessResponse(
            message=f"Composition for ehr_id '{request_body.ehr_id}' ingested and stored.",
            flattened_composition_id=new_comp_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except FlattenerError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Transformation Error: {e}")
    except Exception:
        logger.exception("Unexpected error while ingesting from source database")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error.")
