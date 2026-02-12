"""
FHIR ContextObjects - Vitals Window

Pipeline:
1) normalize rows/samples into telemetry samples
2) window samples into time buckets
3) build FHIR Observation resources per sample
4) materialize one ContextObject per window (nodes[])
"""
from __future__ import annotations

import csv
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from kehrnel.core.explain import enrich_explain
from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, QueryPlan, QueryResult, StrategyContext, TransformResult


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"

MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


# --- helpers --------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _hash_id(parts: Iterable[str], length: int = 24) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _get_metric_value(sample: Dict[str, Any], key: str) -> Optional[float]:
    for candidate in (key, key.upper(), key.lower(), key.capitalize()):
        if candidate in sample:
            val = sample.get(candidate)
            return None if val is None else float(val)
    return None


def _format_ref(template: str, patient_id: str, device_id: str) -> str:
    return template.replace("{patientId}", patient_id).replace("{deviceId}", device_id)


# --- ContextObject model helpers -------------------------------------------------

def make_observation(
    sample: Dict[str, Any],
    patient_id: str,
    device_id: str,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """Create a compact FHIR Observation with components for HR/PULSE/RESP/SpO2."""
    fhir_cfg = cfg.get("fhir", {})
    code_system = fhir_cfg.get("codeSystem", "http://loinc.org")
    panel = fhir_cfg.get("panel", {})
    panel_code = panel.get("code", "85353-1")
    panel_display = panel.get("display", "Vital signs panel")

    subject_ref = _format_ref(fhir_cfg.get("subjectRefTemplate", "Patient/{patientId}"), patient_id, device_id)
    device_ref = _format_ref(fhir_cfg.get("deviceRefTemplate", "Device/{deviceId}"), patient_id, device_id)

    components: List[Dict[str, Any]] = []

    def comp(metric_key: str, default_code: str, default_display: str, default_unit: Optional[str]) -> None:
        metric_cfg = (fhir_cfg.get("codes", {}) or {}).get(metric_key.lower(), {})
        value = _get_metric_value(sample, metric_key)
        if value is None:
            return
        system = metric_cfg.get("system", code_system)
        code = metric_cfg.get("code", default_code)
        display = metric_cfg.get("display", default_display)
        unit = metric_cfg.get("unit", default_unit)
        ucum_system = metric_cfg.get("ucumSystem", "http://unitsofmeasure.org")
        ucum_code = metric_cfg.get("ucumCode", unit)
        entry: Dict[str, Any] = {
            "code": {"coding": [{"system": system, "code": code, "display": display}]},
            "valueQuantity": {"value": float(value)},
        }
        if unit is not None:
            entry["valueQuantity"].update({"unit": unit, "system": ucum_system, "code": ucum_code})
        components.append(entry)

    comp("hr", "8867-4", "Heart rate", "beats/min")
    comp("pulse", "8889-8", "Pulse rate", "beats/min")
    comp("resp", "9279-1", "Respiratory rate", "breaths/min")
    comp("spo2", "59408-5", "Oxygen saturation in Arterial blood by Pulse oximetry", "%")

    obs_id_strategy = fhir_cfg.get("idStrategy", "deterministic")
    if obs_id_strategy == "uuid":
        obs_id = str(uuid.uuid4())
    else:
        obs_id = _hash_id(["obs", patient_id, device_id, sample.get("ts"), panel_code], length=32)

    return {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Vital Signs",
                    }
                ]
            }
        ],
        "code": {"coding": [{"system": code_system, "code": panel_code, "display": panel_display}]},
        "subject": {"reference": subject_ref},
        "device": {"reference": device_ref},
        "effectiveDateTime": sample.get("ts"),
        "component": components,
        "meta": {"profile": [fhir_cfg.get("observationProfile", "https://example.org/fhir/StructureDefinition/vitals-window")]},
    }


def make_patient_resource(patient_id: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "resourceType": "Patient",
        "id": patient_id,
    }


def make_device_resource(device_id: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "resourceType": "Device",
        "id": device_id,
    }


def make_context_object(
    window: Dict[str, Any],
    patient_id: str,
    device_id: str,
    cfg: Dict[str, Any],
    tenant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create one ContextObject per window; nodes = resource per sample (+ optional Patient/Device)."""
    context_cfg = cfg.get("context", {}) or {}
    schema_cfg = context_cfg.get("schema", {}) or {}
    schema_id = schema_cfg.get("id") or context_cfg.get("schema_id") or "fhir.vitals.window"
    schema_version = schema_cfg.get("version") or context_cfg.get("schema_version") or "1.0.0"

    id_strategy = (cfg.get("fhir", {}) or {}).get("idStrategy", "deterministic")
    if id_strategy == "uuid":
        co_id = str(uuid.uuid4())
    else:
        co_id = f"co:{schema_id}:{patient_id}:{device_id}:{window.get('start')}"

    nodes: List[Dict[str, Any]] = []
    include_patient = bool((cfg.get("fhir", {}) or {}).get("includePatientResource", False))
    include_device = bool((cfg.get("fhir", {}) or {}).get("includeDeviceResource", False))
    path_tpl = (cfg.get("fhir", {}) or {}).get("pathTokenTemplate", "FHIR.Observation|{system}|{code}")
    code_system = (cfg.get("fhir", {}) or {}).get("codeSystem", "http://loinc.org")
    panel_code = (cfg.get("fhir", {}) or {}).get("panel", {}).get("code", "85353-1")

    def path_token(resource_type: str) -> str:
        if resource_type == "Observation":
            return path_tpl.format(system=code_system, code=panel_code)
        return f"FHIR.{resource_type}"

    if include_patient:
        patient = make_patient_resource(patient_id, cfg)
        nodes.append(
            {
                "p": path_token("Patient"),
                "kp": "/patient",
                "li": 0,
                "t": window.get("start"),
                "data": patient,
            }
        )

    if include_device:
        device = make_device_resource(device_id, cfg)
        nodes.append(
            {
                "p": path_token("Device"),
                "kp": "/device",
                "li": len(nodes),
                "t": window.get("start"),
                "data": device,
            }
        )

    for i, sample in enumerate(window.get("samples", [])):
        obs = make_observation(sample, patient_id, device_id, cfg)
        nodes.append(
            {
                "p": path_token("Observation"),
                "kp": f"/samples[{i}]",
                "li": len(nodes),
                "t": sample.get("ts"),
                "data": obs,
            }
        )

    windowing = cfg.get("windowing", {}) or {}
    profile = None
    window_seconds = windowing.get("windowSeconds")
    if window_seconds:
        profile = f"profile.window_{int(window_seconds)}s"
    source = window.get("source", {}) or {}
    doc: Dict[str, Any] = {
        "_id": co_id,
        "kind": "context_object",
        "schema": {"id": schema_id, "version": schema_version, "profile": profile},
        "subjects": [
            {"kind": "patient", "id": patient_id, "system": "FHIR", "ref": f"Patient/{patient_id}"},
            {"kind": "device", "id": device_id, "system": "FHIR", "ref": f"Device/{device_id}"},
        ],
        "interval": {
            "start": window.get("start"),
            "end": window.get("end"),
            "timezone": windowing.get("timezone", "UTC"),
            "windowSeconds": windowing.get("windowSeconds"),
        },
        "nodes": nodes,
        "provenance": {
            "strategy": source.get("strategy"),
            "source": {
                "dataset": source.get("dataset"),
                "files": source.get("files"),
                "source_file": source.get("source_file"),
                "rowCount": len(window.get("samples", [])),
            },
            "run": source.get("run"),
            "ingestedAt": _iso(datetime.now(timezone.utc)),
            "summary": {"metrics": window.get("metrics", ["HR", "PULSE", "RESP", "SpO2"])},
        },
    }
    if tenant_id:
        doc["tenant_id"] = tenant_id
    return doc


# --- Pipeline ops ----------------------------------------------------------------

def op_normalize_vitals_rows(
    csv_path: str,
    start_time_iso: str,
    patient_id: str,
    device_id: str,
) -> List[Dict[str, Any]]:
    """Read BIDMC *_Numerics.csv and normalize to [{ts, HR, PULSE, RESP, SpO2, t_sec}]."""
    samples: List[Dict[str, Any]] = []
    start_dt = _parse_iso(start_time_iso).astimezone(timezone.utc)
    with open(csv_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            t_raw = row.get("Time [s]") or row.get("Time") or row.get("t_sec") or row.get("t")
            if t_raw is None:
                continue
            try:
                t_sec = float(t_raw)
            except ValueError:
                continue
            ts = (start_dt + timedelta(seconds=t_sec)).strftime("%Y-%m-%dT%H:%M:%SZ")
            sample = {
                "t_sec": t_sec,
                "ts": ts,
                "HR": row.get("HR"),
                "PULSE": row.get("PULSE"),
                "RESP": row.get("RESP"),
                "SpO2": row.get("SpO2"),
                "patient_id": patient_id,
                "device_id": device_id,
            }
            samples.append(sample)
    return samples


def op_window_samples(samples: List[Dict[str, Any]], window_seconds: int) -> List[Dict[str, Any]]:
    """Group samples into fixed windows. Returns [{start,end,samples:[...]}]."""
    if not samples:
        return []
    samples_sorted = sorted(samples, key=lambda s: float(s.get("t_sec", 0)))
    windows: List[Dict[str, Any]] = []
    cur = {"start": samples_sorted[0]["ts"], "end": samples_sorted[0]["ts"], "samples": [], "source": {}}
    w0 = float(samples_sorted[0]["t_sec"])
    for s in samples_sorted:
        t_sec = float(s.get("t_sec", 0))
        if (t_sec - w0) >= window_seconds and cur["samples"]:
            cur["end"] = cur["samples"][-1]["ts"]
            windows.append(cur)
            w0 = t_sec
            cur = {"start": s["ts"], "end": s["ts"], "samples": [], "source": {}}
        cur["samples"].append(s)
    if cur["samples"]:
        cur["end"] = cur["samples"][-1]["ts"]
        windows.append(cur)
    return windows


def op_generate_context_objects(
    windows: List[Dict[str, Any]],
    patient_id: str,
    device_id: str,
    cfg: Dict[str, Any],
    tenant_id: Optional[str] = None,
    source: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out = []
    for w in windows:
        if source:
            w["source"] = source
        out.append(make_context_object(w, patient_id, device_id, cfg, tenant_id))
    return out


def _normalize_samples_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    samples = payload.get("samples") or payload.get("rows") or []
    if not isinstance(samples, list):
        return []
    normalized = []
    ts_values: List[datetime] = []
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        t_sec = sample.get("t_sec") or sample.get("t")
        ts = sample.get("ts")
        if t_sec is None and ts:
            ts_values.append(_parse_iso(ts))
        normalized.append({
            "t_sec": float(t_sec) if t_sec is not None else None,
            "ts": ts,
            "HR": sample.get("HR") or sample.get("hr"),
            "PULSE": sample.get("PULSE") or sample.get("pulse"),
            "RESP": sample.get("RESP") or sample.get("resp"),
            "SpO2": sample.get("SpO2") or sample.get("spo2"),
            "patient_id": sample.get("patient_id"),
            "device_id": sample.get("device_id"),
        })
    if ts_values:
        t0 = min(ts_values)
        for sample in normalized:
            if sample.get("t_sec") is None and sample.get("ts"):
                sample["t_sec"] = (_parse_iso(sample["ts"]) - t0).total_seconds()
    for sample in normalized:
        if sample.get("t_sec") is None:
            sample["t_sec"] = 0.0
    return normalized


# --- Strategy implementation -----------------------------------------------------

class FhirVitalsWindowStrategy(StrategyPlugin):
    """FHIR ContextObjects - Vitals Window strategy (runtime compatible)."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.base_path = Path(__file__).parent

    async def validate_config(self, ctx: StrategyContext) -> None:
        return None

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = ctx.config
        collections = cfg.get("collections", {})
        contextobjects = collections.get("contextobjects", {})
        raw_cfg = collections.get("raw", {})

        artifacts = {"collections": [], "indexes": []}

        if contextobjects.get("name"):
            artifacts["collections"].append(contextobjects["name"])

        if raw_cfg.get("enabled") and raw_cfg.get("name"):
            artifacts["collections"].append(raw_cfg["name"])

        for idx in cfg.get("indexes", {}).get("contextobjects", []) or []:
            artifacts["indexes"].append(
                {"collection": contextobjects.get("name"), "keys": idx.get("keys", []), "options": idx.get("options", {})}
            )

        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        index_admin = (ctx.adapters or {}).get("index_admin")
        created = []
        warnings = []

        for coll in plan.artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
            else:
                warnings.append("index_admin adapter missing")
                break

        for idx in plan.artifacts.get("indexes", []):
            if not index_admin or not idx.get("collection"):
                continue
            res = await index_admin.ensure_indexes(
                idx.get("collection"),
                [{"keys": idx.get("keys", []), "options": idx.get("options", {})}],
            )
            warnings.extend(res.get("warnings", []))

        return ApplyResult(created=created, warnings=warnings)

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        cfg = ctx.config
        identity = cfg.get("identity", {})
        window_cfg = cfg.get("windowing", {})
        fhir_cfg = cfg.get("fhir", {})

        patient_id = (
            payload.get("patient_id")
            or (payload.get("subject", {}) or {}).get("patient_id")
            or identity.get("defaultPatientId")
            or "patient-unknown"
        )
        device_id = (
            payload.get("device_id")
            or (payload.get("source", {}) or {}).get("device_id")
            or identity.get("defaultDeviceId")
            or "device-unknown"
        )
        tenant_id = payload.get("tenant_id") or payload.get("tenantId")

        samples: List[Dict[str, Any]] = []
        if payload.get("csv_path") or (payload.get("input", {}) or {}).get("csv_path"):
            csv_path = payload.get("csv_path") or (payload.get("input", {}) or {}).get("csv_path")
            start_time_iso = payload.get("start_time_iso") or payload.get("startTimeIso") or (payload.get("input", {}) or {}).get("start_time_iso")
            if not start_time_iso:
                start_time_iso = fhir_cfg.get("defaultStartTime") or "1970-01-01T00:00:00Z"
            samples = op_normalize_vitals_rows(csv_path, start_time_iso, patient_id, device_id)
        else:
            samples = _normalize_samples_from_payload(payload)
            for s in samples:
                s["patient_id"] = s.get("patient_id") or patient_id
                s["device_id"] = s.get("device_id") or device_id

        if not samples:
            return TransformResult(base={}, meta={"warning": "no_samples"})

        window_seconds = int(window_cfg.get("windowSeconds", 60))
        windows = op_window_samples(samples, window_seconds=window_seconds)

        source = dict(payload.get("source") or {})
        strategy_id = getattr(ctx.manifest, "id", None) or cfg.get("strategy_id")
        strategy_version = getattr(ctx.manifest, "version", None)
        source.setdefault("strategy", {"id": strategy_id, "version": strategy_version})
        if payload.get("csv_path") or (payload.get("input", {}) or {}).get("csv_path"):
            csv_path = payload.get("csv_path") or (payload.get("input", {}) or {}).get("csv_path")
            if csv_path:
                source.setdefault("source_file", Path(csv_path).name)
                source.setdefault("files", [Path(csv_path).name])
        run_id = payload.get("run_id") or payload.get("runId") or str(uuid.uuid4())
        source.setdefault("run", {"runId": run_id, "createdAt": _iso(datetime.now(timezone.utc))})
        docs = op_generate_context_objects(
            windows,
            patient_id=patient_id,
            device_id=device_id,
            cfg=cfg,
            tenant_id=tenant_id,
            source=source,
        )

        base = {"docs": docs}
        if (cfg.get("collections", {}) or {}).get("raw", {}).get("enabled"):
            base["samples"] = samples
        meta = {"windows": len(windows), "samples": len(samples)}
        return TransformResult(base=base, meta=meta)

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        tf = await self.transform(ctx, payload)
        storage = (ctx.adapters or {}).get("storage")
        cfg = ctx.config
        collections = cfg.get("collections", {})
        contextobjects = collections.get("contextobjects", {})
        raw_cfg = collections.get("raw", {})

        inserted: Dict[str, Any] = {}
        skipped = 0

        docs = (tf.base or {}).get("docs") if isinstance(tf.base, dict) else None
        if storage and contextobjects.get("name") and docs:
            for doc in docs:
                existing = await storage.find_one(contextobjects["name"], {"_id": doc.get("_id")})
                if existing:
                    skipped += 1
                    continue
                await storage.insert_one(contextobjects["name"], doc)
            inserted["contextobjects"] = contextobjects.get("name")

        if storage and raw_cfg.get("enabled") and raw_cfg.get("name"):
            raw_samples = (tf.base or {}).get("samples") if isinstance(tf.base, dict) else None
            if not raw_samples:
                raw_samples = payload.get("samples") or payload.get("rows") or []
            if raw_samples:
                await storage.insert_many(raw_cfg["name"], raw_samples)
                inserted["raw"] = raw_cfg["name"]

        return {"inserted": inserted, "skipped": skipped, "meta": tf.meta}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        cfg = ctx.config
        collection = (cfg.get("collections", {}) or {}).get("contextobjects", {}).get("name")

        filters = query.get("filters") or query.get("filter") or {}
        pipeline: List[Dict[str, Any]] = []

        if filters:
            pipeline.append({"$match": filters})
        if query.get("sort"):
            pipeline.append({"$sort": query.get("sort")})
        if query.get("skip"):
            pipeline.append({"$skip": int(query.get("skip"))})
        if query.get("limit"):
            pipeline.append({"$limit": int(query.get("limit"))})

        explain = enrich_explain(
            {"builder": {"chosen": "fhir_contextobjects_vitals"}, "scope": "contextobjects"},
            ctx,
            domain=domain or "fhir",
            engine="mongo",
            scope="contextobjects",
        )

        return QueryPlan(
            engine="mongo",
            plan={"collection": collection, "pipeline": pipeline, "explain": explain},
            explain=explain,
        )

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        storage = (ctx.adapters or {}).get("storage")
        if storage and plan.plan.get("collection"):
            rows = await storage.aggregate(plan.plan["collection"], plan.plan.get("pipeline", []))
        else:
            rows = []
        return QueryResult(engine_used=plan.engine, rows=rows, explain=plan.explain)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise ValueError(f"Strategy op '{op}' not supported")
