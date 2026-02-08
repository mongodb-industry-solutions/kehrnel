# src/kehrnel/api/legacy/internal/api_server.py
"""
Internal Kehrnel API (legacy).
Deprecated: use `uvicorn kehrnel.api.app:app --reload` for the new strategy runtime.
This module remains only for historical reference and should not be used in new setups.
"""

import os
import json
import yaml
import tempfile
from pathlib import Path
from datetime import datetime
from functools import lru_cache
from contextlib import suppress
from typing import Dict, List, Optional, Any
from io import StringIO
import sys

# Ensure repository root is on sys.path for src.* imports when running via CLI entrypoint
BASE_DIR = Path(__file__).resolve().parents[3]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, Query, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from bson import ObjectId
import motor.motor_asyncio

from kehrnel.common.mapping.skeleton import build_skeleton
from kehrnel.api.legacy.app.utils.config_runtime import apply_ingestion_config, DEFAULT_MAPPINGS_PATH
from kehrnel.api.legacy.app.core.config import settings
from kehrnel.api.legacy.v1.ingest.routes import router as ingest_router
from kehrnel.api.legacy.v1.config.routes import router as config_router
from kehrnel.api.legacy.v1.strategy.routes import router as strategy_router
from kehrnel.api.legacy.v1.strategy_dispatcher import router as strategy_dispatcher
from kehrnel.api.legacy.app.strategy_runtime import init_strategy_runtime

load_dotenv()


@lru_cache
def get_identifier() -> "DocumentIdentifier":
    """Lazily build and cache ONE DocumentIdentifier for this process."""
    return DocumentIdentifier(
        patterns=[],
        include_default=False,
        debug=False
    )


# Initialize the components we need
# For now, we'll create placeholder classes if the actual imports fail
try:
    from kehrnel.legacy.core.generator import kehrnelGenerator
    from kehrnel.legacy.core.validator import kehrnelValidator
    from kehrnel.legacy.core.parser import TemplateParser
    from kehrnel.common.mapping.document_identifier import DocumentIdentifier, DocumentPattern
    from kehrnel.common.mapping.mapping_engine import MappingEngine
    from kehrnel.common.mapping.handlers.xml_handler import XMLHandler
    from kehrnel.common.mapping.handlers.csv_handler import CSVHandler
    from kehrnel.common.mapping.utils.macro_expander import expand_macros
    from kehrnel.legacy.cli.map_skeleton import main as _skeleton_cli
except ImportError as e:
    print(f"Warning: Some kehrnel components are not implemented yet: {e}")
    print("Creating placeholder classes for development...")

    class kehrnelGenerator:
        def __init__(self, template_parser):
            self.template_parser = template_parser
            self.handlers = {}

        def register_handler(self, handler):
            pass

        def generate_from_mapping(self, mapping_dict, doc_path):
            return {"_type": "COMPOSITION", "name": {"value": "Generated Composition"}}

    class kehrnelValidator:
        def __init__(self, template_parser):
            self.template_parser = template_parser

        def validate(self, composition):
            return []

    class TemplateParser:
        def __init__(self, template_path):
            self.template_path = template_path

    class MappingEngine:
        pass

    class XMLHandler:
        pass

    class CSVHandler:
        pass

    class HL7v2Handler:
        pass

    try:
        from kehrnel.common.mapping.document_identifier import DocumentIdentifier, DocumentPattern
    except Exception:
        class DocumentPattern:
            def __init__(self, name, handler, required_elements=None, **kwargs):
                self.name = name
                self.handler = handler
                self.required_elements = required_elements or []

        class DocumentIdentifier:
            def __init__(self):
                self.patterns = []

            def identify_document(self, file_path):
                return {
                    "documentType": "unknown_xml",
                    "handler": "xml",
                    "sampleData": {},
                    "structure": {"elements": ["root"]}
                }

            def add_pattern(self, pattern):
                self.patterns.append(pattern)

            def list_patterns(self):
                return [p.name for p in self.patterns]


# Initialize FastAPI app
app = FastAPI(title="Kehrnel Internal API", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Next.js frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["location", "etag", "date", "server", "content-length", "content-type"],
)

# MongoDB connection (shared for mapping + ingestion)
MONGODB_URL = os.getenv("MONGODB_URL") or settings.MONGODB_URI
motor_client = None
db = None
document_identifier = get_identifier()


# Pydantic models (mapping studio)
class MappingDefinition(BaseModel):
    documentType: str
    targetTemplate: str
    content: str
    version: str = "1.0"
    description: Optional[str] = ""
    author: Optional[str] = "OpenEHR Playground User"
    handler: Optional[str] = None
    sampleStructure: Optional[Dict] = None


class ValidationRequest(BaseModel):
    composition: Dict[str, Any]
    templateId: str


class TransformRequest(BaseModel):
    documentContent: str
    mappingContent: str
    templateContent: str
    documentType: str = "xml"


# Helper functions (mapping studio)
async def get_template_by_id(template_id: str) -> Optional[Dict]:
    try:
        template = await db.templates.find_one({"_id": ObjectId(template_id)})
        return template
    except Exception:
        return None


async def save_temp_file(content: bytes, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(content)
        return Path(tmp.name)


async def load_patterns_from_db():
    """Load patterns from MongoDB into the identifier (no patterns.yaml)."""
    identifier = get_identifier()
    count_before = len(identifier.patterns)
    async for pat in db.patterns.find():
        try:
            pattern_data = {
                "name": pat.get("name"),
                "handler": pat.get("handler"),
                "priority": pat.get("priority", 50),
                "required_elements": pat.get("required_elements", []),
                "xpath_patterns": pat.get("xpath_patterns", []),
                "namespaces": pat.get("namespaces", {}),
                "csv_headers": pat.get("csv_headers", []),
                "exclude_elements": pat.get("exclude_elements", [])
            }
            identifier.patterns.append(DocumentPattern(**pattern_data))
        except Exception as e:
            print(f"⚠️  Skipping invalid pattern from DB: {e}")
    identifier.patterns.sort(key=lambda p: getattr(p, "priority", 50), reverse=True)
    print(f"🚀 Loaded {len(identifier.patterns) - count_before} patterns from MongoDB (total {len(identifier.patterns)})")


# Startup/shutdown
@app.on_event("startup")
async def startup_db_client():
    """Connect to Mongo, initialize ingestion (flattener) and mapping patterns."""
    global motor_client, db
    try:
        motor_client = motor.motor_asyncio.AsyncIOMotorClient(
            MONGODB_URL,
            serverSelectionTimeoutMS=5000,
            tls=True if "mongodb+srv" in MONGODB_URL else False,
            tlsAllowInvalidCertificates=True  # dev only
        )
        db = motor_client.openehr_playground
        await motor_client.admin.command("ping")
        print("✅ Connected to MongoDB")
    except Exception as e:
        print(f"❌ Failed to create MongoDB client: {e}")
        db = None
        return

    # Always set minimal db state first (required for all endpoints)
    app.state.config = None
    app.state.flattener = None
    app.state.transformer = None
    app.state.db = db
    app.state.target_db = db
    app.state.source_db = db
    app.state.ingest_options = {}

    # Apply ingestion config if present (same as openehr api)
    config_path = Path("config.json")
    if config_path.exists():
        try:
            with config_path.open() as f:
                cfg = json.load(f)
            await apply_ingestion_config(
                app,
                config=cfg,
                mappings_inline=None,
                use_mappings_file=True,
                mappings_path=str(DEFAULT_MAPPINGS_PATH),
                field_map=None,
                coding_opts=None,
                ingest_options={},
            )
            print("CompositionFlattener initialized (config.json).")
        except Exception as e:
            print(f"❌ Failed to apply ingestion config: {e}")
            print("Continuing with minimal configuration. Use /v1/config to configure at runtime.")
    else:
        print("No config.json found. Waiting for runtime configuration via /v1/config.")

    # Initialize strategy runtime (best-effort)
    try:
        init_strategy_runtime(app, environment=os.getenv("KEHRNEL_ENV", "dev"), tenant=os.getenv("KEHRNEL_TENANT"))
    except Exception as e:
        print(f"⚠️  Strategy runtime init failed: {e}")

    # Ensure collections exist for mapping studio and load patterns
    try:
        collections = await db.list_collection_names()
        for coll in ("mappings", "templates", "patterns", "type_template_associations"):
            if coll not in collections:
                await db.create_collection(coll)
                print(f"✅ Created '{coll}' collection")
        await load_patterns_from_db()
    except Exception as e:
        print(f"❌ MongoDB startup failed: {e}")


@app.on_event("shutdown")
async def shutdown_db_client():
    if motor_client:
        motor_client.close()


# --- Existing mapping studio endpoints below (unchanged) ---
# ... (rest of file with mapping endpoints) ...

# Add ingestion/config routers so Transform tab can use this internal API
app.include_router(ingest_router, prefix="/v1/ingestions", tags=["Ingestion"])
app.include_router(config_router, prefix="/v1/config", tags=["Configuration"])
app.include_router(strategy_router, prefix="/v1", tags=["Strategies"])
app.include_router(strategy_dispatcher, prefix="/v1", tags=["Strategy Capabilities"])


# ---- Mapping Studio endpoints (original functionality) ----
@app.get("/")
async def root():
    """API root endpoint with comprehensive endpoint documentation"""
    return {
        "message": "Kehrnel Mapping Studio API",
        "version": "1.0.0",
        "description": "API for document identification, mapping, and transformation to openEHR format",
        "endpoints": {
            "mappings": {
                "identification": {
                    "POST /api/internal/identify-document": "Identify document type with optional debug mode (?debug=true)",
                    "POST /api/internal/test-pattern": "Test a pattern against a document without saving"
                },
                "patterns": {
                    "GET /api/internal/patterns": "List all active patterns with full details",
                    "POST /api/internal/patterns": "Add or update a pattern",
                    "DELETE /api/internal/patterns/{name}": "Delete a pattern",
                    "POST /api/internal/patterns/import": "Import patterns from JSON file"
                },
                "definitions": {
                    "GET /api/internal/mappings": "Get all mapping definitions",
                    "POST /api/internal/mappings": "Create or update a mapping",
                    "DELETE /api/internal/mappings/{id}": "Delete a mapping",
                    "GET /api/internal/mappings/by-type/{type}": "Get mapping for specific document type",
                    "POST /api/internal/mappings/test": "Test a mapping without saving"
                },
                "associations": {
                    "GET /api/internal/type-template-associations": "Get document type to template associations",
                    "POST /api/internal/type-template-associations": "Save type-template association"
                }
            },
            "transformation": {
                "POST /api/internal/transform": "Transform document using mapping to openEHR composition",
                "POST /api/internal/validate-composition": "Validate composition against template"
            },
            "utilities": {
                "GET /api/internal/handlers": "List available document handlers",
                "GET /healthz": "Health check endpoint"
            }
        },
        "documentation": {
            "openapi": "/docs",
            "redoc": "/redoc"
        }
    }


class IdentificationResult(BaseModel):
    documentType: str
    handler: str
    filename: str
    sampleData: dict
    structure: dict


@app.post("/api/internal/identify-document")
async def identify_document(
    document: UploadFile = File(...),
    debug: bool = Query(False, description="Enable debug mode for pattern matching details")
):
    tmp_path = await save_temp_file(await document.read(), Path(document.filename).suffix)
    try:
        identifier = get_identifier()
        if debug:
            debug_info = {"patternsChecked": [], "matchedPattern": None, "evaluationOrder": []}
            for pattern in identifier.patterns:
                debug_info["evaluationOrder"].append({
                    "name": pattern.name,
                    "priority": pattern.priority,
                    "handler": pattern.handler
                })
                temp_identifier = DocumentIdentifier(patterns=[pattern])
                result = temp_identifier.identify_document(tmp_path)
                pattern_matched = result.get("documentType") == pattern.name
                debug_info["patternsChecked"].append({
                    "name": pattern.name,
                    "matched": pattern_matched,
                    "handler": pattern.handler
                })
                if pattern_matched:
                    debug_info["matchedPattern"] = pattern.name
                    result["debugInfo"] = debug_info
                    result["confidence"] = f"{pattern.priority}/100"
                    result["filename"] = document.filename
                    return result
            result = identifier.identify_document(tmp_path)
            result["debugInfo"] = debug_info
            result["filename"] = document.filename
            return result
        result = identifier.identify_document(tmp_path)
        result["filename"] = document.filename
        return result
    finally:
        with suppress(FileNotFoundError):
            tmp_path.unlink()


@app.get("/api/internal/mappings")
async def get_mappings():
    if db is None:
        raise HTTPException(status_code=503, detail="Database connection not available.")
    mappings = []
    cursor = db.mappings.find()
    async for mapping in cursor:
        mapping["_id"] = str(mapping["_id"])
        mappings.append(mapping)
    return mappings


@app.post("/api/internal/mappings")
async def save_mapping(mapping: MappingDefinition):
    if db is None:
        raise HTTPException(status_code=503, detail="Database connection not available.")
    existing = await db.mappings.find_one({"documentType": mapping.documentType})
    mapping_data = mapping.dict()
    mapping_data["updated"] = datetime.utcnow()
    if existing:
        mapping_data["created"] = existing.get("created", datetime.utcnow())
        await db.mappings.replace_one({"_id": existing["_id"]}, mapping_data)
        mapping_data["_id"] = str(existing["_id"])
    else:
        mapping_data["created"] = datetime.utcnow()
        mapping_data["usageCount"] = 0
        result = await db.mappings.insert_one(mapping_data)
        mapping_data["_id"] = str(result.inserted_id)
    return mapping_data


@app.post("/api/internal/generate-skeleton")
async def generate_skeleton(
    templateContent: str,
    useMacros: bool = True,
    includeHeader: bool = False,
    helpers: bool = True,
):
    tmp = await save_temp_file(
        templateContent.encode("utf-8"),
        ".opt" if "<template" in templateContent[:200].lower() else ".json",
    )
    try:
        data = build_skeleton(tmp, use_macros=useMacros, include_header=includeHeader, include_helpers=helpers)
        import yaml as _yaml
        return {
            "mappingYaml": _yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
            "mappingJson": data,
        }
    finally:
        tmp.unlink(missing_ok=True)


@app.post("/api/internal/transform")
async def transform_document(
    document: UploadFile = File(...),
    mappingId: str = Form(...),
    templateId: str = Form(...)
):
    try:
        mapping = await db.mappings.find_one({"_id": ObjectId(mappingId)})
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        template_doc = await get_template_by_id(templateId)
        if not template_doc:
            raise HTTPException(status_code=404, detail="Template not found")
        doc_content = await document.read()
        doc_path = await save_temp_file(doc_content, Path(document.filename).suffix)
        if "optContent" in template_doc:
            template_content = template_doc["optContent"]
        elif "content" in template_doc:
            template_content = template_doc["content"]
        else:
            template_content = json.dumps(template_doc.get("webTemplate", {}))
        template_path = await save_temp_file(
            template_content.encode("utf-8"),
            ".opt" if "<template" in template_content else ".json"
        )
        try:
            template_parser = TemplateParser(str(template_path))
            generator = kehrnelGenerator(template_parser)
            handler_type = mapping.get("handler", "xml")
            if handler_type == "xml":
                generator.register_handler(XMLHandler())
            elif handler_type == "csv":
                generator.register_handler(CSVHandler())
            elif handler_type == "hl7v2":
                generator.register_handler(HL7v2Handler())
            mapping_dict = json.loads(mapping["content"])
            composition = generator.generate_from_mapping(mapping_dict, doc_path)
            await db.mappings.update_one(
                {"_id": ObjectId(mappingId)},
                {"$inc": {"usageCount": 1}, "$set": {"lastUsed": datetime.utcnow()}}
            )
            return composition
        finally:
            doc_path.unlink(missing_ok=True)
            template_path.unlink(missing_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/internal/validate-composition")
async def validate_composition(request: ValidationRequest):
    try:
        template_doc = await get_template_by_id(request.templateId)
        if not template_doc:
            raise HTTPException(status_code=404, detail="Template not found")
        if "optContent" in template_doc:
            template_content = template_doc["optContent"]
        elif "content" in template_doc:
            template_content = template_doc["content"]
        else:
            template_content = json.dumps(template_doc.get("webTemplate", {}))
        template_path = await save_temp_file(
            template_content.encode("utf-8"),
            ".opt" if "<template" in template_content else ".json"
        )
        try:
            template_parser = TemplateParser(str(template_path))
            validator = kehrnelValidator(template_parser)
            issues = validator.validate(request.composition)
            errors, warnings, info = [], [], []
            for issue in issues:
                issue_data = {
                    "path": getattr(issue, "path", ""),
                    "message": getattr(issue, "message", str(issue)),
                    "severity": getattr(issue, "severity", "error")
                }
                severity = issue_data["severity"]
                if hasattr(severity, "value"):
                    severity = severity.value
                if severity == "error":
                    errors.append(issue_data)
                elif severity == "warning":
                    warnings.append(issue_data)
                else:
                    info.append(issue_data)
            return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings, "info": info}
        finally:
            template_path.unlink(missing_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/internal/mappings/{mapping_id}")
async def delete_mapping(mapping_id: str):
    result = await db.mappings.delete_one({"_id": ObjectId(mapping_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return {"message": "Mapping deleted successfully"}


@app.get("/api/internal/mappings/by-type/{document_type}")
async def get_mapping_by_type(document_type: str):
    mapping = await db.mappings.find_one({"documentType": document_type})
    if mapping:
        mapping["_id"] = str(mapping["_id"])
        return mapping
    return None


@app.post("/api/internal/mappings/test")
async def test_mapping(request: TransformRequest):
    try:
        doc_path = await save_temp_file(
            request.documentContent.encode("utf-8"),
            ".xml" if request.documentType == "xml" else ".csv"
        )
        template_path = await save_temp_file(
            request.templateContent.encode("utf-8"),
            ".opt"
        )
        try:
            template_parser = TemplateParser(str(template_path))
            generator = kehrnelGenerator(template_parser)
            if request.documentType == "xml":
                generator.register_handler(XMLHandler())
            mapping_dict = json.loads(request.mappingContent)
            composition = generator.generate_from_mapping(mapping_dict, doc_path)
            validator = kehrnelValidator(template_parser)
            issues = validator.validate(composition)
            return {
                "success": True,
                "composition": composition,
                "validationIssues": [
                    {
                        "path": getattr(issue, "path", ""),
                        "message": getattr(issue, "message", str(issue)),
                        "severity": getattr(issue, "severity", "error")
                    }
                    for issue in issues
                ]
            }
        finally:
            doc_path.unlink(missing_ok=True)
            template_path.unlink(missing_ok=True)
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/internal/patterns")
async def get_patterns():
    identifier = get_identifier()
    return {
        "patterns": [
            {
                "name": p.name,
                "handler": p.handler,
                "priority": p.priority,
                "required_elements": p.required_elements,
                "xpath_patterns": p.xpath_patterns,
                "namespaces": p.namespaces,
                "csv_headers": p.csv_headers,
                "exclude_elements": p.exclude_elements,
            }
            for p in identifier.patterns
        ],
        "count": len(identifier.patterns)
    }


@app.post("/api/internal/patterns")
async def add_or_update_pattern(pattern: Dict[str, Any]):
    required_fields = ["name", "handler"]
    for field in required_fields:
        if field not in pattern:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
    new_pattern = DocumentPattern(**pattern)
    identifier = get_identifier()
    identifier.patterns = [p for p in identifier.patterns if p.name != new_pattern.name]
    identifier.patterns.append(new_pattern)
    identifier.patterns.sort(key=lambda p: p.priority, reverse=True)
    if db is not None:
        await db.patterns.replace_one({"name": new_pattern.name}, pattern, upsert=True)
    return {
        "message": f"Pattern '{new_pattern.name}' saved successfully",
        "pattern": pattern,
        "total_patterns": len(identifier.patterns)
    }


@app.delete("/api/internal/patterns/{pattern_name}")
async def delete_pattern(pattern_name: str):
    identifier = get_identifier()
    original_count = len(identifier.patterns)
    identifier.patterns = [p for p in identifier.patterns if p.name != pattern_name]
    if len(identifier.patterns) == original_count:
        raise HTTPException(status_code=404, detail=f"Pattern '{pattern_name}' not found")
    if db is not None:
        await db.patterns.delete_one({"name": pattern_name})
    return {
        "message": f"Pattern '{pattern_name}' deleted successfully",
        "remaining_patterns": len(identifier.patterns)
    }


@app.post("/api/internal/test-pattern")
async def test_pattern(document: UploadFile = File(...), patterns: str = Form(...)):
    pattern_data = json.loads(patterns)
    test_pattern = DocumentPattern(**pattern_data)
    tmp_path = await save_temp_file(await document.read(), Path(document.filename).suffix)
    try:
        temp_identifier = DocumentIdentifier(patterns=[test_pattern])
        result = temp_identifier.identify_document(tmp_path)
        matches = result.get("documentType") == test_pattern.name
        return {
            "matches": matches,
            "result": result,
            "details": {
                "documentType": result.get("documentType"),
                "handler": result.get("handler"),
                "pattern_name": test_pattern.name
            }
        }
    finally:
        tmp_path.unlink(missing_ok=True)


@app.post("/api/internal/patterns/import")
async def import_patterns(patterns_file: UploadFile = File(...)):
    content = await patterns_file.read()
    filename = patterns_file.filename.lower()
    if filename.endswith((".yaml", ".yml")):
        patterns_data = yaml.safe_load(content)
    elif filename.endswith(".json"):
        patterns_data = json.loads(content)
    else:
        try:
            patterns_data = json.loads(content)
        except json.JSONDecodeError:
            patterns_data = yaml.safe_load(content)
    if not isinstance(patterns_data, list):
        raise HTTPException(status_code=400, detail="File must contain an array/list of patterns")
    imported_count = 0
    errors = []
    for pattern_data in patterns_data:
        try:
            if "optional_elements" in pattern_data and not pattern_data["optional_elements"]:
                pattern_data.pop("optional_elements", None)
            pattern = DocumentPattern(**pattern_data)
            identifier = get_identifier()
            identifier.patterns = [p for p in identifier.patterns if p.name != pattern.name]
            identifier.patterns.append(pattern)
            if db is not None:
                await db.patterns.replace_one({"name": pattern.name}, pattern_data, upsert=True)
            imported_count += 1
        except Exception as e:
            errors.append({"pattern": pattern_data.get("name", "unknown"), "error": str(e)})
    identifier = get_identifier()
    identifier.patterns.sort(key=lambda p: p.priority, reverse=True)
    return {
        "imported": imported_count,
        "errors": errors,
        "total_patterns": len(identifier.patterns)
    }


@app.get("/api/internal/type-template-associations")
async def get_type_template_associations():
    if db is not None:
        associations = {}
        async for assoc in db.type_template_associations.find():
            associations[assoc["documentType"]] = assoc["templateId"]
        return associations
    return {}


@app.post("/api/internal/type-template-associations")
async def save_type_template_association(data: Dict[str, str]):
    document_type = data.get("documentType")
    template_id = data.get("templateId")
    if not document_type or not template_id:
        raise HTTPException(status_code=400, detail="Both documentType and templateId are required")
    if db is not None:
        await db.type_template_associations.replace_one(
            {"documentType": document_type},
            {"documentType": document_type, "templateId": template_id, "updated": datetime.utcnow()},
            upsert=True
        )
    else:
        raise HTTPException(status_code=503, detail="Database connection not available")
    return {"message": "Association saved successfully"}


@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"status": "ok", "db": db is not None}


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "db": db is not None}


@app.get("/api/internal/handlers")
async def list_handlers():
    return ["xml", "csv", "json", "hl7v2"]


def main():
    sys.stderr.write(
        "[DEPRECATED] src/api/internal/api_server.py is legacy. "
        "Run `uvicorn kehrnel.api.app:app --reload` for the current runtime.\n"
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
