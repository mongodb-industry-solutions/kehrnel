"""
Admin/runtime API for strategy discovery and environment operations.
"""
import json
import os
import tempfile
import secrets
import logging
import ipaddress
from pathlib import Path
from fastapi import APIRouter, Request, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, FileResponse
from typing import Any, Dict, List
import yaml
from lxml import etree
from bson import ObjectId

from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.bundle_store import BundleStore
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.common.mapping.mapping_engine import apply_mapping
from kehrnel.engine.common.mapping.handlers.csv_handler import CSVHandler
from kehrnel.engine.common.mapping.handlers.xml_handler import XMLHandler
from kehrnel.engine.common.mapping.utils.expr import evaluate as eval_expr
from kehrnel.engine.domains.openehr.templates.parser import TemplateParser
from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator
from kehrnel.engine.domains.openehr.templates.validator import kehrnelValidator
from kehrnel.api.bridge.app.core.config import settings as bridge_settings
from kehrnel.engine.core.redaction import redact_sensitive

router = APIRouter()
logger = logging.getLogger(__name__)


ALLOWED_DOC_EXTENSIONS = {".xml", ".csv", ".cda", ".json", ".txt", ".hl7"}
ALLOWED_DOC_MIME_TYPES = {
    "application/xml",
    "text/xml",
    "text/csv",
    "application/csv",
    "application/json",
    "text/json",
    "text/plain",
    "application/hl7-v2",
    "application/octet-stream",
}


def _validate_upload_metadata(upload, allowed_exts: set[str], allowed_mimes: set[str]) -> str:
    filename = (getattr(upload, "filename", None) or "").strip()
    if not filename:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="document filename is required")
    if filename != filename.strip():
        raise KehrnelError(code="INVALID_INPUT", status=400, message="Invalid document filename")
    if ".." in filename or "/" in filename or "\\" in filename or "\0" in filename:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="Invalid document filename")

    ext = Path(filename).suffix.lower()
    if ext not in allowed_exts:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="Unsupported document file type")

    content_type = (getattr(upload, "content_type", None) or "").lower()
    if content_type and content_type not in allowed_mimes:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="Unsupported document content type")

    return filename


def _error_response(exc: Exception) -> JSONResponse:
    debug_enabled = os.getenv("KEHRNEL_DEBUG", "false").lower() in ("1", "true", "yes")
    code = "INTERNAL_ERROR"
    status = 500
    details = {}
    if hasattr(exc, "code"):
        code = getattr(exc, "code")
        status = getattr(exc, "status", status)
        details = getattr(exc, "details", {}) or {}
    elif isinstance(exc, KeyError):
        status = 404
        code = "NOT_FOUND"
    elif isinstance(exc, ValueError):
        status = 400
        code = "INVALID_INPUT"
    if debug_enabled or status < 500:
        message = str(exc)
    else:
        message = "Internal server error"
    # Avoid leaking secrets or filesystem internals (absolute paths) via error strings.
    message = redact_sensitive(message) or message
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, "details": details}})


def _json_safe(payload: Any) -> Any:
    """Encode runtime payloads so BSON/ObjectId values do not break API responses."""
    return jsonable_encoder(payload, custom_encoder={ObjectId: str})


def _require_admin_access(request: Request) -> None:
    """Require authenticated admin API key for privileged routes."""
    auth_enabled = os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("1", "true", "yes")
    if not auth_enabled:
        # Auth is globally disabled for this runtime (dev/test mode).
        return

    api_key = (request.headers.get("x-api-key") or "").strip()
    if not api_key:
        raise KehrnelError(code="UNAUTHORIZED", status=401, message="Invalid or missing API key")

    valid_keys = [k.strip() for k in (os.getenv("KEHRNEL_API_KEYS", "") or "").split(",") if k.strip()]
    if not valid_keys or not any(secrets.compare_digest(api_key, k) for k in valid_keys):
        raise KehrnelError(code="UNAUTHORIZED", status=401, message="Invalid or missing API key")

    # Optional stricter admin key separation; fallback to KEHRNEL_API_KEYS when unset.
    admin_keys = [k.strip() for k in (os.getenv("KEHRNEL_ADMIN_API_KEYS", "") or "").split(",") if k.strip()]
    if admin_keys and not any(secrets.compare_digest(api_key, k) for k in admin_keys):
        raise KehrnelError(code="FORBIDDEN", status=403, message="Admin privileges required")


def _parse_api_key_env_scopes() -> dict[str, object]:
    raw = (os.getenv("KEHRNEL_API_KEY_ENV_SCOPES") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.warning("Invalid KEHRNEL_API_KEY_ENV_SCOPES JSON; ignoring")
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _default_env_id() -> str | None:
    return (
        os.getenv("KEHRNEL_DEFAULT_ENV_ID")
        or os.getenv("DEFAULT_ENV_ID")
        or os.getenv("ENV_ID")
        or None
    )


def _env_access_allowed(request: Request, env_id: str) -> bool:
    auth_enabled = os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("1", "true", "yes")
    if not auth_enabled:
        return True

    scopes = _parse_api_key_env_scopes()
    api_key = (request.headers.get("x-api-key") or "").strip()

    if scopes:
        matched_scope = None
        if api_key:
            for key, scope in scopes.items():
                if secrets.compare_digest(api_key, str(key)):
                    matched_scope = scope
                    break
        if matched_scope == "*":
            return True
        if isinstance(matched_scope, list):
            return env_id in {str(v).strip() for v in matched_scope if str(v).strip()}
        return False

    default_env = _default_env_id()
    if default_env:
        return env_id == default_env
    return False


def _require_env_access(request: Request, env_id: str) -> None:
    if not _env_access_allowed(request, env_id):
        raise KehrnelError(
            code="FORBIDDEN",
            status=403,
            message=f"Access to env_id={env_id} is not permitted for this API key.",
        )


def _truthy_env(name: str, default: str = "false") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in ("1", "true", "yes")


def _trusted_proxy_identity_enabled() -> bool:
    return _truthy_env("KEHRNEL_TRUST_PROXY_IDENTITY", "false")


def _trusted_proxy_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """
    Parse trusted proxy CIDRs from KEHRNEL_TRUSTED_PROXY_CIDRS.
    Empty means no proxy source is trusted for identity headers.
    """
    raw = (os.getenv("KEHRNEL_TRUSTED_PROXY_CIDRS") or "").strip()
    if not raw:
        return []
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        try:
            networks.append(ipaddress.ip_network(value, strict=False))
        except ValueError:
            logger.warning("Ignoring invalid trusted proxy CIDR: %s", value)
    return networks


def _is_trusted_proxy_source(request: Request) -> bool:
    """
    Only trust identity headers when request source IP is from configured proxy CIDRs.
    """
    client_host = ((request.client.host if request.client else "") or "").strip()
    if not client_host:
        return False
    try:
        client_ip = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    trusted_networks = _trusted_proxy_networks()
    if not trusted_networks:
        return False
    return any(client_ip in network for network in trusted_networks)


def _request_identity(request: Request) -> dict[str, str | None]:
    """
    Read user/team identity propagated by trusted upstream proxy (HDL backend).
    Headers are ignored unless KEHRNEL_TRUST_PROXY_IDENTITY=true.
    """
    api_key_fingerprint = getattr(request.state, "authenticated_api_key", None)
    user_id = None
    team_id = None
    user_role = None
    if _trusted_proxy_identity_enabled() and _is_trusted_proxy_source(request):
        user_id = (
            request.headers.get("x-user-id")
            or request.headers.get("x-auth-user")
            or request.headers.get("x-forwarded-user")
            or None
        )
        team_id = (
            request.headers.get("x-team-id")
            or request.headers.get("x-org-id")
            or request.headers.get("x-workspace-id")
            or None
        )
        user_role = request.headers.get("x-user-role") or None
        user_id = (user_id or "").strip() or None
        team_id = (team_id or "").strip() or None
        user_role = ((user_role or "").strip().lower() or None)
    elif _trusted_proxy_identity_enabled():
        logger.warning(
            "Ignoring forwarded identity headers from untrusted source ip=%s",
            request.client.host if request.client else None,
        )
    return {
        "api_key_fingerprint": api_key_fingerprint,
        "user_id": user_id,
        "team_id": team_id,
        "user_role": user_role,
    }


def _enforce_synthetic_team_scope(identity: dict[str, str | None], job: dict[str, Any]) -> None:
    """
    Optional second-level isolation: restrict synthetic jobs to caller team.
    Team scope is enforced only when KEHRNEL_SYNTHETIC_ENFORCE_TEAM_SCOPE=true.
    """
    if not _truthy_env("KEHRNEL_SYNTHETIC_ENFORCE_TEAM_SCOPE", "false"):
        return
    if identity.get("user_role") == "admin":
        return
    job_team = (str(job.get("team_id") or "").strip() or None)
    if not job_team:
        return
    caller_team = (identity.get("team_id") or "").strip() or None
    if not caller_team or not secrets.compare_digest(caller_team, job_team):
        raise KehrnelError(
            code="FORBIDDEN",
            status=403,
            message="This job belongs to a different team.",
        )




def _get_max_upload_bytes() -> int:
    try:
        return max(1024 * 1024, int(os.getenv("KEHRNEL_MAX_UPLOAD_BYTES", str(10 * 1024 * 1024))))
    except ValueError:
        return 10 * 1024 * 1024


def _get_max_opt_bytes() -> int:
    try:
        return max(1024 * 1024, int(os.getenv("KEHRNEL_MAX_OPT_BYTES", str(5 * 1024 * 1024))))
    except ValueError:
        return 5 * 1024 * 1024


def _history_summary(rt, env_id: str, domain: str):
    history_entries = rt.registry.list_history(env_id, domain) if hasattr(rt.registry, "list_history") else []
    recent = history_entries[-3:] if history_entries else []

    def _safe_entry(entry):
        activation = (entry or {}).get("activation") or {}
        return {
            "activation_id": activation.get("activation_id"),
            "strategy_id": activation.get("strategy_id"),
            "version": activation.get("version"),
            "manifest_digest": activation.get("manifest_digest"),
            "config_hash": activation.get("config_hash"),
            "timestamp": (entry or {}).get("timestamp"),
            "reason": (entry or {}).get("reason"),
        }

    return {"count": len(history_entries or []), "recent": [_safe_entry(e) for e in recent]}


def _sync_bridge_openehr_settings_from_activation(rt, activation) -> None:
    """
    Keep bridge domain-scoped openEHR API routes aligned with strategy-pack config.
    This is global process state (bridge API is not env-scoped), so the latest activation wins.
    """
    try:
        if os.getenv("KEHRNEL_ENABLE_LEGACY_GLOBAL_SYNC", "false").lower() not in ("1", "true", "yes"):
            return
        # Hard safety guard: bridge global sync is allowed only for a dedicated default env.
        default_env = _default_env_id()
        if not default_env or (activation.env_id or "").strip() != default_env:
            logger.warning(
                "Skipping bridge global sync for env_id=%s (default env is %s)",
                getattr(activation, "env_id", None),
                default_env,
            )
            return

        cfg = activation.config or {}
        collections = cfg.get("collections") or {}

        comp_name = ((collections.get("compositions") or {}).get("name") or "").strip()
        search_name = ((collections.get("search") or {}).get("name") or "").strip()
        ehr_name = ((collections.get("ehr") or {}).get("name") or "").strip()
        contrib_name = ((collections.get("contributions") or {}).get("name") or "").strip()
        if comp_name:
            bridge_settings.COMPOSITIONS_COLL_NAME = comp_name
            bridge_settings.FLAT_COMPOSITIONS_COLL_NAME = comp_name
        if search_name:
            bridge_settings.SEARCH_COMPOSITIONS_COLL_NAME = search_name
            bridge_settings.search_config.search_collection = search_name
        if ehr_name:
            bridge_settings.EHR_COLL_NAME = ehr_name
        if contrib_name:
            bridge_settings.EHR_CONTRIBUTIONS_COLL = contrib_name

        # DB selection priority: bindings(db.name) > strategy config database > unchanged
        db_name = None
        db_bindings = (getattr(activation, "bindings", None) or {}).get("db") or {}
        if isinstance(db_bindings, dict):
            db_name = db_bindings.get("name")
        if not db_name and getattr(activation, "bindings_ref", None) and getattr(rt, "resolver", None):
            try:
                resolved = rt.resolver.resolve(
                    bindings_ref=activation.bindings_ref,
                    env_id=activation.env_id,
                    domain=activation.domain,
                    strategy_id=activation.strategy_id,
                    operation="activate",
                    context={"activation_config": cfg},
                )
                db_name = ((resolved or {}).get("db") or {}).get("name")
            except Exception:
                db_name = None
        if not db_name:
            db_name = cfg.get("database")
        if db_name:
            bridge_settings.MONGODB_DB = db_name
    except Exception:
        # Never break activation because bridge sync failed.
        return


async def _initialize_activation_artifacts(rt, env_id: str, activation) -> Dict[str, Any]:
    """
    Materialize storage/search artifacts and bootstrap dictionaries after an
    activation lifecycle change so activate/upgrade/rollback behave the same.
    """
    domain = (getattr(activation, "domain", None) or "").lower()
    artifact_result = None
    try:
        plan_result = await rt.dispatch(
            env_id,
            "plan",
            {"domain": domain},
        )
        artifact_result = await rt.dispatch(
            env_id,
            "apply",
            {"domain": domain, "plan": plan_result},
        )
    except Exception as exc:
        artifact_result = {
            "ok": False,
            "warning": f"storage artifact initialization failed: {redact_sensitive(str(exc))}",
        }

    dict_result = None
    try:
        apply_shortcuts = bool(
            ((activation.config or {}).get("transform") or {}).get("apply_shortcuts", True)
        )
        bootstrap_cfg = (((activation.config or {}).get("bootstrap") or {}).get("dictionariesOnActivate") or {})
        is_rps_dual = (getattr(activation, "strategy_id", "") or "").strip().lower() == "openehr.rps_dual"
        bootstrap_payload = {}
        if is_rps_dual:
            bootstrap_payload = {
                "codes": bootstrap_cfg.get("codes") or "ensure",
                "shortcuts": bootstrap_cfg.get("shortcuts") or ("seed" if apply_shortcuts else "none"),
            }
        should_bootstrap = (
            is_rps_dual and any(bootstrap_payload.get(key) != "none" for key in ("codes", "shortcuts"))
        ) or (not is_rps_dual and apply_shortcuts)
        if should_bootstrap:
            dict_result = await rt.dispatch(
                env_id,
                "op",
                {"domain": domain, "op": "ensure_dictionaries", "payload": bootstrap_payload},
            )
    except Exception:
        dict_result = {"ok": False, "warning": "dictionary bootstrap failed"}

    return {
        "artifacts": artifact_result,
        "dictionaries": dict_result,
    }


def _load_mapping_payload(mapping_raw: str) -> Dict[str, Any]:
    """Parse mapping payload from YAML first, then JSON fallback."""
    if not mapping_raw or not str(mapping_raw).strip():
        raise KehrnelError(code="INVALID_INPUT", status=400, message="mapping payload is required")
    try:
        parsed = yaml.safe_load(mapping_raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    try:
        parsed_json = json.loads(mapping_raw)
        if isinstance(parsed_json, dict):
            return parsed_json
    except Exception as exc:
        raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Invalid mapping payload: {exc}") from exc
    raise KehrnelError(code="INVALID_INPUT", status=400, message="mapping payload must decode to an object")


def _pick_source_handler(source_path: Path):
    handlers = [CSVHandler(), XMLHandler()]
    for handler in handlers:
        if handler.can_handle(source_path):
            return handler
    raise KehrnelError(
        code="UNSUPPORTED_MEDIA_TYPE",
        status=415,
        message=f"Unsupported source format for '{source_path.name}'",
    )


def _compat_flat_map_from_path_mapping(mapping: Dict[str, Any], source_tree: etree._Element) -> Dict[str, Dict[str, Any]]:
    """
    Fallback converter for path-keyed YAML mappings.
    Supports direct path rules like:
      /a/b/c: "constant: foo"
      /a/b/c:
        xpath: //...
        map: {a: b}
        default: x
        when: "..."
      /a/b/c:
        template: "{{ xpath('//...') }}"
    Ignores dynamic/template append rules and meta sections.
    """
    if not isinstance(mapping, dict):
        return {}

    ns = ((mapping.get("_metadata") or {}).get("namespaces") or {}) if isinstance(mapping.get("_metadata"), dict) else {}
    if not isinstance(ns, dict) or not ns:
        ns = XMLHandler.NS_DEFAULT

    def _xpath(expr: str):
        try:
            res = source_tree.xpath(expr, namespaces=ns)
        except Exception:
            return None
        if not res:
            return None
        if len(res) == 1:
            item = res[0]
            return item.text if isinstance(item, etree._Element) else item
        vals = []
        for item in res:
            vals.append(item.text if isinstance(item, etree._Element) else item)
        return vals

    out: Dict[str, Dict[str, Any]] = {}
    reserved_prefixes = ("_metadata", "_options", "_preprocessing", "_inputs", "_hints", "_gui")

    for raw_path, rule in mapping.items():
        if not isinstance(raw_path, str):
            continue
        if raw_path.startswith(reserved_prefixes):
            continue
        # dynamic append/template keys are not directly representable in flat_map
        if raw_path.startswith('_"') or "{append}" in raw_path or "{i}" in raw_path:
            continue

        path = raw_path.lstrip("/")
        literal = None

        if isinstance(rule, str):
            if rule.startswith("constant:"):
                literal = rule.split("constant:", 1)[1].strip()
            else:
                literal = rule
        elif isinstance(rule, (int, float, bool)):
            literal = rule
        elif isinstance(rule, dict):
            when = rule.get("when")
            if when and not eval_expr(str(when), row={"_xml": True}, vars={"xpath": _xpath}):
                continue

            if "template" in rule:
                try:
                    from kehrnel.engine.common.mapping.utils.jinja_env import env as JINJA
                    rendered = JINJA.from_string(str(rule.get("template") or "")).render({"xpath": _xpath})
                    literal = rendered
                except Exception:
                    literal = None
            elif "xpath" in rule:
                literal = _xpath(str(rule.get("xpath") or ""))
                if isinstance(literal, list):
                    literal = literal[0] if literal else None
            m = rule.get("map")
            if isinstance(m, dict) and literal is not None:
                literal = m.get(str(literal), literal)

            if (literal is None or str(literal).strip() == "") and "default" in rule:
                literal = rule.get("default")

        if literal is None:
            continue
        out[path] = {"literal": literal}

    return out


def _apply_flat_map_safe(gen: kehrnelGenerator, composition: Dict[str, Any], flat_map: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Best-effort application for compatibility maps; skips malformed/non-leaf paths."""
    set_fn = getattr(gen, "set_at_path", None) or getattr(gen, "_set_value_at_path", None)
    if set_fn is None:
        raise KehrnelError(code="INTERNAL_ERROR", status=500, message="Generator has no path setter")

    applied = 0
    skipped = 0
    for path, spec in (flat_map or {}).items():
        if not isinstance(spec, dict) or "literal" not in spec:
            continue
        try:
            set_fn(composition, path, spec["literal"])
            applied += 1
        except Exception:
            skipped += 1
            continue
    return {"applied": applied, "skipped": skipped}


def _transform_document_with_mapping(source_path: Path, mapping: Dict[str, Any], opt_path: Path) -> Dict[str, Any]:
    """
    Build canonical composition from source document using mapping + OPT.
    Uses the same generator/mapping pipeline as kehrnel-map CLI.
    """
    tpl = TemplateParser(opt_path)
    gen = kehrnelGenerator(tpl)
    handler = _pick_source_handler(source_path)
    source_tree = handler.load_source(source_path)
    groups = handler.preprocess_mapping_new(mapping, source_tree)
    if not groups:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="Mapping produced no groups")

    first_group = groups[0]
    flat_map = first_group.get("map") or {}
    used_compat_fallback = False
    if not isinstance(flat_map, dict) or not flat_map:
        # Compatibility fallback for path-keyed mappings used in HDL.
        flat_map = _compat_flat_map_from_path_mapping(mapping, source_tree)
        used_compat_fallback = True
    if not isinstance(flat_map, dict) or not flat_map:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Mapping produced no path rules",
            details={"hint": "Provide mappings.* grammar or path-keyed rules with xpath/template/constant entries."},
        )

    composition = gen.generate_minimal()
    if used_compat_fallback:
        stats = _apply_flat_map_safe(gen, composition, flat_map)
        if stats.get("applied", 0) <= 0:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Mapping rules could not be applied to target template paths",
                details=stats,
            )
    else:
        composition = apply_mapping(gen, flat_map, composition)
    composition = gen._normalize_for_rm(composition)
    composition = gen._prune_incomplete_datavalues(composition)
    if bool(first_group.get("prune_empty")):
        gen._prune_empty(composition)
    return composition


@router.get("/strategies", response_model=Dict[str, List[StrategyManifest]])
async def list_strategies(request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            return {"strategies": []}
        manifests = []
        for m in rt.list_strategies():
            m_dict = m.model_dump()
            # expose pack spec meta (but not full spec body) for clients
            if hasattr(m, "pack_spec") and m.pack_spec:
                m_dict["pack_spec_meta"] = {
                    "has_spec": True,
                    "encoding_profiles": [p.get("id") for p in (m.pack_spec or {}).get("encodingProfiles", []) if isinstance(p, dict)],
                    "stores": [(s or {}).get("role") or (s or {}).get("destinationType") for s in (m.pack_spec or {}).get("storage", {}).get("stores", []) if isinstance(s, dict)],
                }
                m_dict.pop("pack_spec", None)
            manifests.append(m_dict)
        return {"strategies": manifests}
    except Exception as exc:
        return _error_response(exc)


@router.get("/health", include_in_schema=False)
async def health():
    return {"ok": True}


@router.post("/api/transform", include_in_schema=False)
async def api_transform(request: Request):
    """
    Compatibility endpoint for HDL Mapping Studio.

    Expects multipart/form-data:
    - document: uploaded source file (.xml/.csv)
    - mapping_yaml (or mapping): mapping definition (YAML/JSON string)
    - opt_content (or opt): OPT XML content
    - template_id/templateId (optional, informational)
    """
    temp_files: list[Path] = []
    try:
        _require_admin_access(request)
        form = await request.form()
        upload = form.get("document")
        mapping_raw = form.get("mapping_yaml") or form.get("mapping")
        opt_content = form.get("opt_content") or form.get("opt")
        template_id = form.get("template_id") or form.get("templateId")

        if upload is None or not hasattr(upload, "read"):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="document file is required")
        if not opt_content or not str(opt_content).strip():
            raise KehrnelError(code="INVALID_INPUT", status=400, message="opt_content is required")
        if len(str(opt_content).encode("utf-8")) > _get_max_opt_bytes():
            raise KehrnelError(code="PAYLOAD_TOO_LARGE", status=413, message="opt_content exceeds upload limit")

        mapping = _load_mapping_payload(str(mapping_raw or ""))

        filename = _validate_upload_metadata(upload, ALLOWED_DOC_EXTENSIONS, ALLOWED_DOC_MIME_TYPES)
        suffix = Path(filename).suffix or ".xml"
        max_upload_bytes = _get_max_upload_bytes()
        with tempfile.NamedTemporaryFile("wb", suffix=suffix, delete=False) as src_tmp:
            total = 0
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_upload_bytes:
                    raise KehrnelError(code="PAYLOAD_TOO_LARGE", status=413, message="document exceeds upload limit")
                src_tmp.write(chunk)
            src_path = Path(src_tmp.name)
            temp_files.append(src_path)

        with tempfile.NamedTemporaryFile("w", suffix=".opt", encoding="utf-8", delete=False) as opt_tmp:
            opt_tmp.write(str(opt_content))
            opt_path = Path(opt_tmp.name)
            temp_files.append(opt_path)

        composition = _transform_document_with_mapping(src_path, mapping, opt_path)
        if template_id:
            try:
                ad = composition.setdefault("archetype_details", {})
                if isinstance(ad, dict):
                    ad.setdefault("template_id", {"value": str(template_id)})
            except Exception:
                pass
        return JSONResponse(content=composition)
    except Exception as exc:
        return _error_response(exc)
    finally:
        for p in temp_files:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


@router.post("/api/validate-composition", include_in_schema=False)
async def api_validate_composition(request: Request):
    """
    Compatibility endpoint for HDL Mapping Studio composition validation.

    Expects JSON body:
    - composition: canonical composition object
    - opt_content (or opt): OPT XML content
    - template_id/templateId (optional, informational)
    """
    temp_files: list[Path] = []
    try:
        _require_admin_access(request)
        payload = await request.json()
        composition = payload.get("composition")
        opt_content = payload.get("opt_content") or payload.get("opt")

        if not isinstance(composition, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="composition object is required")
        if not opt_content or not str(opt_content).strip():
            raise KehrnelError(code="INVALID_INPUT", status=400, message="opt_content is required")
        if len(str(opt_content).encode("utf-8")) > _get_max_opt_bytes():
            raise KehrnelError(code="PAYLOAD_TOO_LARGE", status=413, message="opt_content exceeds upload limit")

        with tempfile.NamedTemporaryFile("w", suffix=".opt", encoding="utf-8", delete=False) as opt_tmp:
            opt_tmp.write(str(opt_content))
            opt_path = Path(opt_tmp.name)
            temp_files.append(opt_path)

        tpl = TemplateParser(opt_path)
        validator = kehrnelValidator(tpl)
        issues = validator.validate(composition)

        errors = []
        warnings = []
        infos = []
        for issue in issues:
            issue_obj = {
                "path": issue.path,
                "message": issue.message,
                "code": issue.code,
                "expected": issue.expected,
                "found": issue.found,
            }
            severity = (str(issue.severity.value) if hasattr(issue.severity, "value") else str(issue.severity)).lower()
            if severity == "warning":
                warnings.append(issue_obj)
            elif severity == "info":
                infos.append(issue_obj)
            else:
                errors.append(issue_obj)

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "info": infos,
            "summary": {
                "errors": len(errors),
                "warnings": len(warnings),
                "info": len(infos),
                "issues": len(issues),
            },
        }
    except Exception as exc:
        return _error_response(exc)
    finally:
        for p in temp_files:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


@router.get("/agentic", include_in_schema=False)
async def agentic_capabilities():
    """Capability probe endpoint for clients that support agentic features."""
    return {"ok": True, "enabled": False, "features": []}


def _validate_strategy_path(pack_path: str, request: Request) -> Path:
    """Validate that pack_path is within allowed strategy directories."""
    import os
    resolved = Path(pack_path).resolve()
    # Get allowed strategy paths from app state or environment
    allowed_paths = getattr(request.app.state, "allowed_strategy_paths", None)
    if not allowed_paths:
        # Default: only allow paths under the engine strategies directory
        base_strategies = Path(__file__).resolve().parents[3] / "engine" / "strategies"
        allowed_paths = [base_strategies.resolve()]
        # Add paths from environment variable
        extra = os.getenv("KEHRNEL_STRATEGY_PATHS")
        if extra:
            for sep in (":", ","):
                if sep in extra:
                    extra_paths = [p for p in extra.split(sep) if p]
                    break
            else:
                extra_paths = [extra] if extra else []
            for part in extra_paths:
                if part:
                    allowed_paths.append(Path(part).resolve())
    # Check if resolved path is under any allowed path
    for allowed in allowed_paths:
        try:
            resolved.relative_to(allowed)
            return resolved
        except ValueError:
            continue
    raise KehrnelError(
        code="PATH_NOT_ALLOWED",
        status=403,
        message="Strategy path is outside allowed directories"
    )


@router.post("/strategies/load", include_in_schema=False)
async def load_strategy_pack(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        if os.getenv("KEHRNEL_ENABLE_STRATEGY_LOAD", "true").lower() not in ("1", "true", "yes"):
            raise KehrnelError(
                code="FEATURE_DISABLED",
                status=403,
                message="Dynamic strategy loading is disabled",
            )
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise KehrnelError(code="RUNTIME_NOT_INITIALIZED", status=503, message="Strategy runtime not initialized")
        pack_path = body.get("path") or body.get("packPath")
        strategy_id = body.get("strategy_id") or body.get("strategyId")
        if not pack_path:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="path is required")
        # Validate path is within allowed directories
        validated_path = _validate_strategy_path(pack_path, request)
        manifest = load_strategy(strategy_id, str(validated_path))
        rt.register_manifest(manifest)
        return {"ok": True, "strategy": manifest}
    except Exception as exc:
        return _error_response(exc)


@router.get("/endpoints", include_in_schema=False)
async def endpoints_registry(request: Request):
    _require_admin_access(request)
    base = ""
    endpoints = {
        "health": f"{base}/health",
        "strategies": f"{base}/strategies",
        "strategy_detail": f"{base}/strategies/{{strategy_id}}",
        "strategy_spec": f"{base}/strategies/{{strategy_id}}/spec",
        "openehr_domain": f"{base}/api/domains/openehr/...",
        "openehr_rps_dual": f"{base}/api/strategies/openehr/rps_dual/...",
        "strategy_docs": f"{base}/docs/strategies/{{domain}}/{{strategy}}",
        "domain_docs": f"{base}/redoc/domains/{{domain}}",
        "core_docs": f"{base}/docs/core",
        "bundles": f"{base}/bundles",
        "ops": f"{base}/ops",
        "activations": f"{base}/environments/{{env_id}}/activate",
        "capabilities": f"{base}/environments/{{env_id}}/capabilities",
        "run": f"{base}/environments/{{env_id}}/run",
    }
    return {"endpoints": endpoints}


@router.get("/strategies/{strategy_id}/endpoints", include_in_schema=False)
async def strategy_endpoints(strategy_id: str, request: Request):
    _require_admin_access(request)
    base = ""
    if strategy_id == "openehr.rps_dual":
        return {
            "strategy_id": strategy_id,
            "endpoints": {
                "domain_base": f"{base}/api/domains/openehr",
                "ehr": f"{base}/api/domains/openehr/ehr",
                "ehr_status": f"{base}/api/domains/openehr/ehr/{{ehr_id}}/ehr_status",
                "composition": f"{base}/api/domains/openehr/ehr/{{ehr_id}}/composition",
                "contribution": f"{base}/api/domains/openehr/ehr/{{ehr_id}}/contribution",
                "directory": f"{base}/api/domains/openehr/ehr/{{ehr_id}}/directory",
                "template": f"{base}/api/domains/openehr/definition/template/adl1.4",
                "aql": f"{base}/api/domains/openehr/query/aql",
                "strategy_ingest": f"{base}/api/strategies/openehr/rps_dual/ingest",
                "strategy_config": f"{base}/api/strategies/openehr/rps_dual/config",
                "strategy_synthetic": f"{base}/api/strategies/openehr/rps_dual/synthetic",
                "docs": f"{base}/docs/strategies/openehr/rps_dual",
                "domain_docs": f"{base}/redoc/domains/openehr",
            },
        }
    return {
        "strategy_id": strategy_id,
        "endpoints": {
            "activate": f"{base}/environments/{{env_id}}/activate",
            "capabilities": f"{base}/environments/{{env_id}}/capabilities",
            "run": f"{base}/environments/{{env_id}}/run",
            "compile_query": f"{base}/environments/{{env_id}}/compile_query",
            "query": f"{base}/environments/{{env_id}}/query",
            "ops": f"{base}/environments/{{env_id}}/activations/{{domain}}/ops/{{op}}",
        },
    }


@router.get("/strategies/diagnostics", include_in_schema=False)
async def get_strategy_diagnostics(request: Request):
    try:
        _require_admin_access(request)
        diagnostics = getattr(request.app.state, "strategy_diagnostics", None) or []
        return {"strategies": diagnostics}
    except Exception as exc:
        return _error_response(exc)


def _standard_env_operations() -> List[Dict[str, Any]]:
    return [
        {
            "name": "plan",
            "kind": "runtime",
            "summary": "Build execution plan for the active strategy",
            "scope": "environment",
            "route": "POST /environments/{env_id}/plan",
        },
        {
            "name": "apply",
            "kind": "runtime",
            "summary": "Apply previously generated plan",
            "scope": "environment",
            "route": "POST /environments/{env_id}/apply",
        },
        {
            "name": "transform",
            "kind": "runtime",
            "summary": "Transform source payload into strategy output documents",
            "scope": "environment",
            "route": "POST /environments/{env_id}/transform",
        },
        {
            "name": "ingest",
            "kind": "runtime",
            "summary": "Ingest payload using active strategy",
            "scope": "environment",
            "route": "POST /environments/{env_id}/ingest",
        },
        {
            "name": "query",
            "kind": "runtime",
            "summary": "Execute a domain query",
            "scope": "domain",
            "route": "POST /environments/{env_id}/query",
        },
        {
            "name": "compile_query",
            "kind": "runtime",
            "summary": "Compile domain query into execution plan",
            "scope": "domain",
            "route": "POST /environments/{env_id}/compile_query",
        },
    ]


def _serialize_manifest_op(manifest: StrategyManifest, op: Any, include_schemas: bool = True) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "strategy_id": manifest.id,
        "domain": manifest.domain,
        "name": getattr(op, "name", None),
        "kind": getattr(op, "kind", None),
        "summary": getattr(op, "summary", None),
        "scope": "strategy",
        "route": "POST /environments/{env_id}/activations/{domain}/ops/{op}",
    }
    if include_schemas:
        row["input_schema"] = getattr(op, "input_schema", None) or {}
        row["output_schema"] = getattr(op, "output_schema", None) or {}
    return row


@router.get("/ops", include_in_schema=False)
async def list_ops(request: Request):
    try:
        _require_admin_access(request)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        ops = []
        for manifest in rt.list_strategies():
            for op in manifest.ops:
                ops.append(_serialize_manifest_op(manifest, op, include_schemas=False))
        return {"ops": ops}
    except Exception as exc:
        return _error_response(exc)


@router.post("/ops", include_in_schema=False)
async def run_op(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        env_id = body.get("environment") or body.get("env_id")
        domain = body.get("domain")
        op = body.get("op")
        payload = body.get("payload") or {}
        if not env_id or not domain or not op:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="environment, domain, and op are required")
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload, "domain": domain})
        return {"ok": True, "result": result}
    except Exception as exc:
        return _error_response(exc)


@router.get("/environments/{env_id}/capabilities", include_in_schema=False)
async def env_capabilities(env_id: str, request: Request, include_schemas: bool = True):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")

        activations = rt.registry.list_activations(env_id) or {}
        domains: List[Dict[str, Any]] = []
        strategy_ops: List[Dict[str, Any]] = []

        for domain_key, activation in activations.items():
            manifest = rt.registry.get_manifest(activation.strategy_id)
            if not manifest:
                continue
            op_rows = [_serialize_manifest_op(manifest, op, include_schemas=include_schemas) for op in (manifest.ops or [])]
            strategy_ops.extend(op_rows)
            domains.append(
                {
                    "domain": domain_key,
                    "strategy_id": activation.strategy_id,
                    "strategy_version": activation.version,
                    "activation_id": activation.activation_id,
                    "ops": [row.get("name") for row in op_rows if row.get("name")],
                }
            )

        return {
            "env_id": env_id,
            "domains": domains,
            "operations": {
                "standard": _standard_env_operations(),
                "strategy": strategy_ops,
            },
        }
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/run", include_in_schema=False)
async def run_env_op(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")

        operation = str(body.get("operation") or body.get("op") or "").strip()
        if not operation:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="operation is required")

        op_key = operation.lower().replace("-", "_")
        direct_ops = {"plan", "apply", "transform", "ingest", "query", "compile_query"}
        payload = body.get("payload") if isinstance(body.get("payload"), dict) else {}
        payload = dict(payload or {})

        # Lift top-level universal keys into payload if not already provided.
        for key in ("domain", "strategy_id", "strategy", "data_mode", "source", "sink", "query", "aql", "dry_run", "debug", "allow_mismatch"):
            if key in body and key not in payload:
                payload[key] = body.get(key)

        requested_domain = str(payload.get("domain") or body.get("domain") or "").strip().lower()
        strategy_id = str(payload.get("strategy_id") or payload.get("strategy") or body.get("strategy_id") or body.get("strategy") or "").strip()

        dispatch_op = op_key
        dispatch_payload: Dict[str, Any] = payload
        route_scope = "runtime"

        if op_key in direct_ops:
            if op_key in {"query", "compile_query"} and not requested_domain:
                activations = rt.registry.list_activations(env_id) or {}
                if len(activations) == 1:
                    requested_domain = str(next(iter(activations.keys())))
            if op_key in {"query", "compile_query"} and not requested_domain:
                raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required for query and compile_query operations")
            if requested_domain and "domain" not in dispatch_payload:
                dispatch_payload["domain"] = requested_domain
        else:
            if not requested_domain:
                if strategy_id:
                    activation = rt.registry.get_activation_by_strategy(env_id, strategy_id)
                    if activation:
                        requested_domain = str(activation.domain or "").strip().lower()
                if not requested_domain:
                    activations = rt.registry.list_activations(env_id) or {}
                    if len(activations) == 1:
                        requested_domain = str(next(iter(activations.keys())))
            if not requested_domain:
                raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required for strategy operations")
            dispatch_op = "op"
            # Strip routing/envelope keys before passing to the op so that op input
            # schemas with additionalProperties:false don't reject them.
            _routing_keys = {"domain", "strategy_id", "strategy", "data_mode", "allow_mismatch"}
            op_inner_payload = {k: v for k, v in payload.items() if k not in _routing_keys}
            dispatch_payload = {"domain": requested_domain, "op": operation, "payload": op_inner_payload}
            route_scope = "strategy"

        result = await rt.dispatch(env_id, dispatch_op, dispatch_payload)
        return {
            "ok": True,
            "env_id": env_id,
            "operation": operation,
            "dispatch": {
                "scope": route_scope,
                "op": dispatch_op,
                "domain": requested_domain or None,
            },
            "result": result,
        }
    except Exception as exc:
        return _error_response(exc)


@router.get("/bundles", include_in_schema=False)
async def list_bundles(request: Request):
    try:
        _require_admin_access(request)
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        bundles = store.list_bundles() if store else []
        return {"bundles": bundles}
    except Exception as exc:
        return _error_response(exc)


@router.get("/bundles/{bundle_id}", include_in_schema=False)
async def get_bundle(bundle_id: str, request: Request):
    try:
        _require_admin_access(request)
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        bundle = store.get_bundle(bundle_id)
        return {"bundle": bundle}
    except Exception as exc:
        return _error_response(exc)


@router.post("/bundles", include_in_schema=False)
async def import_bundle(request: Request, body: Dict[str, Any] = Body(default_factory=dict), mode: str = "error"):
    try:
        _require_admin_access(request)
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        res = store.save_bundle(body, mode=mode)
        return {"bundle": res}
    except Exception as exc:
        return _error_response(exc)


@router.delete("/bundles/{bundle_id}", include_in_schema=False)
async def delete_bundle(bundle_id: str, request: Request):
    try:
        _require_admin_access(request)
        store: BundleStore = getattr(request.app.state, "bundle_store", None)
        if not store:
            raise KehrnelError(code="BUNDLE_NOT_AVAILABLE", status=503, message="Bundle store not configured")
        store.delete_bundle(bundle_id)
        return {"ok": True}
    except Exception as exc:
        return _error_response(exc)


@router.get("/strategies/{strategy_id}", response_model=StrategyManifest)
async def get_strategy(strategy_id: str, request: Request):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        manifest = rt.registry.get_manifest(strategy_id)
        if not manifest:
            raise KeyError("Strategy not found")
        data = manifest.model_dump()
        if hasattr(manifest, "pack_spec") and manifest.pack_spec:
            data["pack_spec"] = manifest.pack_spec
        return data
    except Exception as exc:
        return _error_response(exc)


@router.get("/api/persistence-strategies/{strategy_id}", include_in_schema=False)
async def get_persistence_strategy_compat(strategy_id: str, request: Request):
    """Compatibility endpoint for HDL strategy fetches."""
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        manifest = rt.registry.get_manifest(strategy_id)
        if not manifest:
            raise KehrnelError(
                code="STRATEGY_NOT_FOUND",
                status=404,
                message=f"Strategy {strategy_id} not found",
            )
        data = manifest.model_dump()
        return {
            "id": data.get("id"),
            "strategy_id": data.get("id"),
            "name": data.get("name"),
            "version": data.get("version"),
            "domain": data.get("domain"),
            "default_config": data.get("default_config") or {},
            "config_schema": data.get("config_schema") or {},
            "manifest": data,
        }
    except Exception as exc:
        return _error_response(exc)


@router.get("/strategies/{strategy_id}/assets/{asset_path:path}")
async def get_strategy_asset(strategy_id: str, asset_path: str, request: Request):
    try:
        asset_dirs = getattr(request.app.state, "strategy_asset_dirs", None) or {}
        base_dir = asset_dirs.get(strategy_id)
        if not base_dir:
            raise KehrnelError(code="STRATEGY_NOT_FOUND", status=404, message=f"Strategy {strategy_id} not found")
        # Explicit path traversal protection
        if ".." in asset_path or asset_path.startswith("/"):
            raise KehrnelError(code="INVALID_PATH", status=400, message="Path traversal not allowed")
        base_path = Path(base_dir).resolve()
        safe_asset = asset_path.lstrip("/")
        # Additional check for null bytes and other dangerous characters
        if "\x00" in safe_asset or "\\" in safe_asset:
            raise KehrnelError(code="INVALID_PATH", status=400, message="Invalid characters in path")
        candidate = (base_path / safe_asset).resolve()
        if base_path not in candidate.parents and candidate != base_path:
            raise KehrnelError(code="ASSET_OUT_OF_BOUNDS", status=400, message="Invalid asset path")
        if not candidate.exists() or not candidate.is_file():
            raise KehrnelError(code="ASSET_NOT_FOUND", status=404, message="Asset not found")
        return FileResponse(str(candidate))
    except Exception as exc:
        return _error_response(exc)


@router.get("/strategies/{strategy_id}/spec")
async def get_strategy_spec(strategy_id: str, request: Request):
    """Return the full spec.json for a strategy pack."""
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        manifest = rt.registry.get_manifest(strategy_id)
        if not manifest:
            raise KeyError(f"Strategy {strategy_id} not found")
        pack_spec = getattr(manifest, "pack_spec", None)
        if not pack_spec:
            raise KehrnelError(
                code="SPEC_NOT_AVAILABLE",
                status=404,
                message=f"Strategy {strategy_id} does not have a spec.json (pack_format must be strategy-pack/v1)",
            )
        return {"strategy_id": strategy_id, "spec": pack_spec}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/extensions/{strategy_id}/{op}", include_in_schema=False)
async def run_extension(env_id: str, strategy_id: str, op: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        activation = rt.registry.get_activation_by_strategy(env_id, strategy_id)
        if not activation:
            raise KeyError(f"No activation for env {env_id}")
        if activation.strategy_id != strategy_id:
            raise ValueError(f"Environment {env_id} active with {activation.strategy_id}, not {strategy_id}")
        result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload or {}, "strategy_id": strategy_id})
        return {"ok": True, "result": result}
    except Exception as exc:
        return _error_response(exc)



def _coerce_request_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _normalize_environment_metadata(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise KehrnelError(code="INVALID_INPUT", status=400, message="metadata must be an object")


def _serialize_environment(rt, env_id: str, env=None) -> Dict[str, Any] | None:
    record = env or rt.get_environment(env_id)
    if not record:
        return None
    activations = rt.registry.list_activations(env_id) or {}
    return {
        "env_id": record.env_id,
        "name": record.name or record.env_id,
        "description": record.description or "",
        "metadata": record.metadata or {},
        "bindings_ref": record.bindings_ref,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "activation_domains": sorted(activations.keys()),
        "activation_count": len(activations),
    }


def _summarize_env_activations(rt, env_id: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    activations = rt.registry.list_activations(env_id)
    summary = {}
    history_summary = {}
    for proto, activation in activations.items():
        manifest = rt.registry.get_manifest(activation.strategy_id)
        summary[proto] = {
            "strategy_id": activation.strategy_id,
            "strategy_version": activation.version,
            "version": activation.version,
            "manifest_digest": activation.manifest_digest,
            "domain": getattr(manifest, "domain", proto),
            "config": activation.config,
            "bindings_meta": activation.bindings_meta,
            "bindings_ref": activation.bindings_ref,
            "activation_id": activation.activation_id,
            "activated_at": activation.activated_at,
            "updated_at": activation.updated_at,
            "config_hash": activation.config_hash,
        }
        history_summary[proto] = _history_summary(rt, env_id, proto)
    return summary, history_summary


@router.get("/environments", include_in_schema=False)
async def list_environments(request: Request):
    try:
        _require_admin_access(request)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        environments = []
        for env_id, env in sorted(rt.list_environments().items(), key=lambda item: item[0]):
            if _env_access_allowed(request, env_id):
                environments.append(_serialize_environment(rt, env_id, env))
        return {"environments": environments}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments", include_in_schema=False)
async def create_environment(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        env_id = body.get("env_id") or body.get("envId") or body.get("id") or body.get("environment")
        if not env_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="env_id is required")
        _require_env_access(request, env_id)
        existing = rt.get_environment(env_id)
        env = rt.upsert_environment(
            env_id,
            name=body.get("name"),
            description=body.get("description"),
            metadata=_normalize_environment_metadata(body.get("metadata")),
            bindings_ref=body.get("bindings_ref") or body.get("bindingsRef"),
        )
        return {"ok": True, "created": existing is None, "environment": _serialize_environment(rt, env_id, env)}
    except Exception as exc:
        return _error_response(exc)


@router.patch("/environments/{env_id}", include_in_schema=False)
async def update_environment(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        existing = rt.get_environment(env_id)
        if not existing:
            raise KehrnelError(code="ENVIRONMENT_NOT_FOUND", status=404, message=f"Environment {env_id} not found")
        metadata = existing.metadata
        if "metadata" in body:
            metadata = _normalize_environment_metadata(body.get("metadata"))
        env = rt.upsert_environment(
            env_id,
            name=body["name"] if "name" in body else existing.name,
            description=body["description"] if "description" in body else existing.description,
            metadata=metadata,
            bindings_ref=(body.get("bindings_ref") if "bindings_ref" in body else body.get("bindingsRef"))
            if ("bindings_ref" in body or "bindingsRef" in body)
            else existing.bindings_ref,
        )
        return {"ok": True, "environment": _serialize_environment(rt, env_id, env)}
    except Exception as exc:
        return _error_response(exc)


@router.delete("/environments/{env_id}", include_in_schema=False)
async def delete_environment(env_id: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        force = _coerce_request_bool(request.query_params.get("force") or request.query_params.get("purge_activations"))
        removed = rt.delete_environment(env_id, remove_activations=force)
        return {"ok": True, "force": force, "environment": {
            "env_id": removed.env_id,
            "name": removed.name or removed.env_id,
            "description": removed.description or "",
            "metadata": removed.metadata or {},
            "bindings_ref": removed.bindings_ref,
            "created_at": removed.created_at,
            "updated_at": removed.updated_at,
        }}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/activate", include_in_schema=False)
async def activate_env(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        strategy_id = body.get("strategy_id") or body.get("strategyId")
        version = body.get("version") or "latest"
        config = body.get("config") or {}
        bindings = body.get("bindings") or {}
        bindings_ref = body.get("bindings_ref") or body.get("bindingsRef")
        domain = (body.get("domain") or "").lower()
        allow_plain = body.get("allow_plaintext_bindings", False)
        force = bool(body.get("force"))
        replace_reason = body.get("reason")
        auth_enabled = os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("1", "true", "yes")
        if not strategy_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="strategy_id is required")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")

        # Security model:
        # - In production (auth enabled): require bindings_ref; do not accept plaintext bindings.
        # - In dev/test (auth disabled): allow plaintext bindings when explicitly requested.
        if auth_enabled:
            if bindings and not bindings_ref:
                raise KehrnelError(
                    code="PLAINTEXT_BINDINGS_FORBIDDEN",
                    status=400,
                    message="bindings payload is not allowed. Use bindings_ref.",
                )
            if allow_plain and not bindings_ref:
                raise KehrnelError(
                    code="PLAINTEXT_BINDINGS_FORBIDDEN",
                    status=400,
                    message="allow_plaintext_bindings is not supported when auth is enabled. Use bindings_ref.",
                )
            if not bindings_ref:
                raise KehrnelError(code="BINDINGS_REF_REQUIRED", status=400, message="bindings_ref is required")
        else:
            if bindings and not allow_plain:
                raise KehrnelError(
                    code="PLAINTEXT_BINDINGS_FORBIDDEN",
                    status=400,
                    message="bindings payload requires allow_plaintext_bindings=true in dev/test mode.",
                )
        from kehrnel.strategy_sdk import StrategyBindings
        activation = await rt.activate(
            env_id,
            strategy_id,
            version,
            config,
            StrategyBindings(**bindings),
            allow_plaintext_bindings=allow_plain,
            domain=domain,
            force=force,
            replace_reason=replace_reason,
            bindings_ref=bindings_ref,
        )
        _sync_bridge_openehr_settings_from_activation(rt, activation)
        initialization = await _initialize_activation_artifacts(rt, env_id, activation)
        return {
            "ok": True,
            "activation": {
                "env_id": activation.env_id,
                "strategy_id": activation.strategy_id,
                "version": activation.version,
                "strategy_version": activation.version,
                "domain": activation.domain,
                "config": activation.config,
                "bindings_meta": activation.bindings_meta,
                "bindings_ref": activation.bindings_ref,
                "config_hash": activation.config_hash,
                "manifest_digest": activation.manifest_digest,
                "activation_id": activation.activation_id,
                "replaced": activation.replaced,
                "previous_activation_id": activation.previous_activation_id,
                "replaced_from": activation.replaced_from,
                "already_active": activation.already_active,
            },
            "initialization": initialization,
        }
    except Exception as exc:
        return _error_response(exc)


@router.put("/environments/strategy", include_in_schema=False)
async def activate_strategy_compat(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    """Compatibility activation endpoint for HDL proxy."""
    env_id = body.get("envId") or body.get("environment") or body.get("env_id")
    if not env_id:
        return _error_response(KehrnelError(code="INVALID_INPUT", status=400, message="envId is required"))
    auth_enabled = os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("1", "true", "yes")
    mapped = {
        "strategy_id": body.get("strategyId") or body.get("strategy_id"),
        "version": body.get("version") or body.get("strategyVersion") or "latest",
        "config": body.get("configOverrides") or body.get("config") or {},
        "bindings": body.get("bindings") or {},
        "bindings_ref": body.get("bindingsRef") or body.get("bindings_ref"),
        "allow_plaintext_bindings": bool(
            body.get("allowPlaintextBindings", False) or body.get("allow_plaintext_bindings", False)
        ),
        "domain": (body.get("domain") or "").lower() or body.get("domain"),
        "force": body.get("force") or False,
        "reason": body.get("reason"),
    }
    # In dev/test mode, compatibility clients often omit bindings fields entirely.
    # If auth is disabled and no bindings_ref is provided, default to plaintext mode.
    if not auth_enabled and not mapped.get("bindings_ref") and not mapped.get("allow_plaintext_bindings"):
        mapped["allow_plaintext_bindings"] = True
    return await activate_env(env_id, request, mapped)


@router.post("/strategies/activate", include_in_schema=False)
async def activate_env_v2(request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    env_id = body.get("environment")
    if not env_id:
        return _error_response(KehrnelError(code="INVALID_INPUT", status=400, message="environment is required"))
    return await activate_env(env_id, request, body)


@router.get("/environments/{env_id}", include_in_schema=False)
async def get_env(env_id: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        env = rt.get_environment(env_id)
        summary, history_summary = _summarize_env_activations(rt, env_id)
        if not env and not summary:
            raise KeyError("Environment not found")
        return {
            "env_id": env_id,
            "environment": _serialize_environment(rt, env_id, env),
            "activations": summary,
            "history": history_summary,
        }
    except Exception as exc:
        return _error_response(exc)


@router.get("/environments/{env_id}/activations", include_in_schema=False)
async def list_env_activations(env_id: str, request: Request):
    return await get_env(env_id, request)


@router.post("/environments/{env_id}/activations/{domain}/upgrade", include_in_schema=False)
async def upgrade_activation(env_id: str, domain: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        new_act = await rt.upgrade_activation(env_id, domain)
        _sync_bridge_openehr_settings_from_activation(rt, new_act)
        initialization = await _initialize_activation_artifacts(rt, env_id, new_act)
        return {
            "ok": True,
            "activation": {
                "activation_id": new_act.activation_id,
                "strategy_id": new_act.strategy_id,
                "version": new_act.version,
                "domain": new_act.domain,
                "manifest_digest": new_act.manifest_digest,
                "config": new_act.config,
                "bindings_meta": new_act.bindings_meta,
                "bindings_ref": new_act.bindings_ref,
            },
            "initialization": initialization,
        }
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/activations/{domain}/rollback", include_in_schema=False)
async def rollback_activation(env_id: str, domain: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        act = await rt.rollback_activation(env_id, domain)
        _sync_bridge_openehr_settings_from_activation(rt, act)
        initialization = await _initialize_activation_artifacts(rt, env_id, act)
        return {
            "ok": True,
            "activation": {
                "activation_id": act.activation_id,
                "strategy_id": act.strategy_id,
                "version": act.version,
                "domain": act.domain,
                "manifest_digest": act.manifest_digest,
                "config": act.config,
                "bindings_meta": act.bindings_meta,
                "bindings_ref": act.bindings_ref,
                "config_hash": act.config_hash,
            },
            "initialization": initialization,
        }
    except Exception as exc:
        return _error_response(exc)


@router.delete("/environments/{env_id}/activations/{domain}", include_in_schema=False)
async def delete_activation(env_id: str, domain: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        act = rt.delete_activation(env_id, domain)
        return {"ok": True, "activation": {"activation_id": act.activation_id, "domain": act.domain}}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/plan", include_in_schema=False)
async def plan_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "plan", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/apply", include_in_schema=False)
async def apply_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "apply", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/transform", include_in_schema=False)
async def transform_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "transform", payload or {})
        return _json_safe({"ok": True, "result": res})
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/ingest", include_in_schema=False)
async def ingest_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        res = await rt.dispatch(env_id, "ingest", payload or {})
        return _json_safe({"ok": True, "result": res})
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/query", include_in_schema=False)
async def query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        domain = (payload or {}).get("domain")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        payload["domain"] = domain
        res = await rt.dispatch(env_id, "query", payload or {})
        # when dispatch returns QueryResult dict or simple, wrap as ok/result if needed
        if isinstance(res, dict) and "ok" in res:
            return res
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/compile_query", include_in_schema=False)
async def compile_query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict), debug: bool = False):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        domain = (payload or {}).get("domain")
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        payload["domain"] = domain
        if debug and isinstance(payload, dict):
            payload["debug"] = True
        res = await rt.dispatch(env_id, "compile_query", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:
        return _error_response(exc)


@router.get("/environments/{env_id}/endpoints", include_in_schema=False)
async def list_env_endpoints(env_id: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        base = str(request.base_url).rstrip("/")
        rt = getattr(request.app.state, "strategy_runtime", None)
        activations = rt.registry.list_activations(env_id) if rt else {}
        domains = []
        for domain_key, activation in activations.items():
            domains.append(
                {
                    "domain": domain_key,
                    "activation_id": activation.activation_id,
                    "strategy_id": activation.strategy_id,
                    "strategy_version": activation.version,
                }
            )
        sample_domain = domains[0]["domain"] if domains else "domain"
        return {
            "env_id": env_id,
            "domains": domains,
            "endpoints": {
                "capabilities": {
                    "url": f"{base}/environments/{env_id}/capabilities",
                    "method": "GET",
                },
                "run": {
                    "url": f"{base}/environments/{env_id}/run",
                    "method": "POST",
                    "required_params": ["operation"],
                    "payload_example": {
                        "operation": "synthetic_generate_batch",
                        "domain": sample_domain,
                        "payload": {"patient_count": 100, "dry_run": True},
                    },
                },
                "compile_query": {
                    "url": f"{base}/environments/{env_id}/compile_query",
                    "method": "POST",
                    "required_params": ["domain"],
                    "payload_example": {"domain": sample_domain, "query": {"scope": "patient", "predicates": [], "select": []}},
                },
                "query": {
                    "url": f"{base}/environments/{env_id}/query",
                    "method": "POST",
                    "required_params": ["domain"],
                    "payload_example": {"domain": sample_domain, "query": {"scope": "patient", "predicates": [], "select": []}},
                },
                "activations": {"url": f"{base}/environments/{env_id}/activations", "method": "GET"},
                "ops": {
                    "url": f"{base}/environments/{env_id}/extensions/{{strategy_id}}/{{op}}",
                    "method": "POST",
                    "required_params": ["strategy_id", "op"],
                    "payload_example": {"payload": {}, "domain": sample_domain},
                },
                "synthetic_jobs_create": {
                    "url": f"{base}/environments/{env_id}/synthetic/jobs",
                    "method": "POST",
                    "required_params": ["domain", "payload"],
                    "payload_example": {
                        "domain": sample_domain,
                        "op": "synthetic_generate_batch",
                        "payload": {
                            "patient_count": 100,
                            "source_collection": "samples",
                            "model_source": {
                                "catalog_collection": "semantic_models",
                                "links_collection": "semantic_links"
                            },
                            "models": [
                                {"model_id": "opt.tumour_summary.v1", "min_per_patient": 1, "max_per_patient": 2}
                            ],
                        },
                    },
                },
                "synthetic_jobs_list": {"url": f"{base}/environments/{env_id}/synthetic/jobs", "method": "GET"},
                "synthetic_job_get": {"url": f"{base}/environments/{env_id}/synthetic/jobs/{{job_id}}", "method": "GET"},
                "synthetic_job_cancel": {
                    "url": f"{base}/environments/{env_id}/synthetic/jobs/{{job_id}}/cancel",
                    "method": "POST",
                },
            },
        }
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/synthetic/jobs")
async def create_synthetic_job(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        identity = _request_identity(request)
        manager = getattr(request.app.state, "synthetic_job_manager", None)
        if not manager:
            raise KehrnelError(code="JOBS_NOT_AVAILABLE", status=503, message="Synthetic job manager not initialized")
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise KehrnelError(code="RUNTIME_NOT_INITIALIZED", status=503, message="Strategy runtime not initialized")
        domain = (body.get("domain") or request.query_params.get("domain") or "").strip().lower()
        if not domain:
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        op = body.get("op") or "synthetic_generate_batch"
        payload = body.get("payload") or {}
        if not isinstance(payload, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="payload must be an object")

        activation = rt.registry.get_activation(env_id, domain)
        if not activation:
            raise KehrnelError(
                code="ACTIVATION_NOT_FOUND",
                status=404,
                message=f"No activation for env {env_id} (domain={domain})",
            )
        if not activation.bindings_ref and not activation.bindings:
            raise KehrnelError(
                code="BINDINGS_REF_REQUIRED",
                status=400,
                message="Activation must include bindings_ref.",
            )
        if activation.bindings_ref and not getattr(rt, "bindings_resolver", None):
            raise KehrnelError(
                code="BINDINGS_RESOLVER_NOT_CONFIGURED",
                status=400,
                message=(
                    "Activation uses bindings_ref but no bindings resolver is configured. "
                    "Set KEHRNEL_BINDINGS_RESOLVER or configure HDL resolver env vars."
                ),
                details={
                    "env_id": env_id,
                    "domain": domain,
                    "strategy_id": activation.strategy_id,
                    "activation_id": activation.activation_id,
                    "bindings_ref": activation.bindings_ref,
                    "required_env": [
                        "KEHRNEL_BINDINGS_RESOLVER",
                        "ENV_SECRETS_KEY",
                        "CORE_MONGODB_URL",
                        "CORE_DATABASE_NAME",
                    ],
                },
            )

        requested_by = (
            identity.get("api_key_fingerprint")
            or request.headers.get("x-api-key")
        )
        activation_now = rt.registry.get_activation(env_id, domain)
        cfg_now = activation_now.config if activation_now else {}
        collections_cfg = cfg_now.get("collections") if isinstance(cfg_now, dict) else {}
        target_collections = {
            "canonical": payload.get("canonical_collection") if payload.get("store_canonical") else None,
            "compositions": ((collections_cfg or {}).get("compositions") or {}).get("name"),
            "search": (
                ((collections_cfg or {}).get("search") or {}).get("name")
                if ((collections_cfg or {}).get("search") or {}).get("enabled", True)
                else None
            ),
        }
        target_database = None
        if activation_now and getattr(activation_now, "bindings", None):
            db_bindings = getattr(activation_now.bindings, "db", None)
            target_database = getattr(db_bindings, "database", None)
        if not target_database and activation_now and activation_now.bindings_ref:
            try:
                from kehrnel.engine.core.bindings_resolver import resolve_bindings as _resolve_bindings_ref
                resolved = await _resolve_bindings_ref(
                    rt.bindings_resolver,
                    bindings_ref=activation_now.bindings_ref,
                    env_id=env_id,
                    domain=activation_now.domain,
                    strategy_id=activation_now.strategy_id,
                    op=op,
                    context={"payload": payload or {}, "activation_config": activation_now.config or {}},
                ) or {}
                target_database = ((resolved.get("db") or {}).get("database") if isinstance(resolved, dict) else None)
            except Exception:
                target_database = None
        model_source = payload.get("model_source") if isinstance(payload.get("model_source"), dict) else {}
        job = await manager.create_job(
            env_id=env_id,
            domain=domain,
            op=op,
            payload=payload,
            metadata={
                "target_database": target_database,
                "target_collections": target_collections,
                "model_source": {
                    "database_name": model_source.get("database_name") or target_database,
                    "catalog_collection": model_source.get("catalog_collection"),
                    "links_collection": model_source.get("links_collection"),
                },
            },
            requested_by=requested_by,
            requester_id=identity.get("user_id"),
            team_id=identity.get("team_id"),
        )
        return JSONResponse(status_code=202, content={"ok": True, "job": job})
    except Exception as exc:
        return _error_response(exc)


@router.get("/environments/{env_id}/synthetic/jobs")
async def list_synthetic_jobs(env_id: str, request: Request, domain: str | None = None, status: str | None = None):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        manager = getattr(request.app.state, "synthetic_job_manager", None)
        if not manager:
            raise KehrnelError(code="JOBS_NOT_AVAILABLE", status=503, message="Synthetic job manager not initialized")
        identity = _request_identity(request)
        jobs = await manager.list_jobs(env_id=env_id, domain=domain)
        if _truthy_env("KEHRNEL_SYNTHETIC_ENFORCE_TEAM_SCOPE", "false"):
            filtered_jobs: list[dict[str, Any]] = []
            for job in jobs:
                try:
                    _enforce_synthetic_team_scope(identity, job)
                    filtered_jobs.append(job)
                except KehrnelError:
                    continue
            jobs = filtered_jobs
        if status:
            jobs = [j for j in jobs if str(j.get("status") or "").lower() == str(status).lower()]
        return {"ok": True, "jobs": jobs}
    except Exception as exc:
        return _error_response(exc)


@router.get("/environments/{env_id}/synthetic/jobs/{job_id}")
async def get_synthetic_job(env_id: str, job_id: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        manager = getattr(request.app.state, "synthetic_job_manager", None)
        if not manager:
            raise KehrnelError(code="JOBS_NOT_AVAILABLE", status=503, message="Synthetic job manager not initialized")
        identity = _request_identity(request)
        job = await manager.get_job(job_id)
        if not job or job.get("env_id") != env_id:
            raise KehrnelError(code="JOB_NOT_FOUND", status=404, message=f"Job {job_id} not found for env {env_id}")
        _enforce_synthetic_team_scope(identity, job)
        return {"ok": True, "job": job}
    except Exception as exc:
        return _error_response(exc)


@router.post("/environments/{env_id}/synthetic/jobs/{job_id}/cancel")
async def cancel_synthetic_job(env_id: str, job_id: str, request: Request):
    try:
        _require_admin_access(request)
        _require_env_access(request, env_id)
        manager = getattr(request.app.state, "synthetic_job_manager", None)
        if not manager:
            raise KehrnelError(code="JOBS_NOT_AVAILABLE", status=503, message="Synthetic job manager not initialized")
        identity = _request_identity(request)
        existing = await manager.get_job(job_id)
        if not existing or existing.get("env_id") != env_id:
            raise KehrnelError(code="JOB_NOT_FOUND", status=404, message=f"Job {job_id} not found for env {env_id}")
        _enforce_synthetic_team_scope(identity, existing)
        job = await manager.cancel_job(job_id)
        return {"ok": True, "job": job}
    except Exception as exc:
        return _error_response(exc)
