# /src/api/internal/api_server.py
"""FastAPI server for Mapping Studio integration with kehrnel - Internal Tooling API"""

import os
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, Query, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import yaml
import tempfile
from pathlib import Path
from bson import ObjectId
import motor.motor_asyncio
from functools import lru_cache         
from contextlib import suppress
from mapper.skeleton import build_skeleton

from io import StringIO
import sys

load_dotenv()

@lru_cache
def get_identifier() -> "DocumentIdentifier":
    """
    Lazily build and cache ONE DocumentIdentifier for this process.
    Only loads patterns from MongoDB, NOT from patterns.yaml.
    """
    return DocumentIdentifier(
        patterns=[], 
        include_default=False,  # ← Add this parameter!
        debug=False
    )


# Initialize the components we need
# For now, we'll create placeholder classes if the actual imports fail
try:
    from core.generator import kehrnelGenerator
    from core.validator import kehrnelValidator
    from core.parser import TemplateParser
    from mapper.document_identifier import DocumentIdentifier, DocumentPattern
    from mapper.mapping_engine import MappingEngine
    from mapper.handlers.xml_handler import XMLHandler
    from mapper.handlers.csv_handler import CSVHandler
    from mapper.utils.macro_expander import expand_macros
    from cli.map_skeleton import main as _skeleton_cli 

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
            from mapper.document_identifier import DocumentIdentifier, DocumentPattern
    except:
        # If still failing, create minimal versions
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
app = FastAPI(title="Kehrnel Mapping Studio API", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["location", "etag", "date", "server", "content-length", "content-type"],
)

# MongoDB connection
MONGODB_URL = os.getenv("MONGODB_URI") 

try:
    motor_client = motor.motor_asyncio.AsyncIOMotorClient(
        MONGODB_URL,
        serverSelectionTimeoutMS=5000,  # 5 second timeout
        tls=True if "mongodb+srv" in MONGODB_URL else False,  # Enable TLS for Atlas
        tlsAllowInvalidCertificates=True  # For development only
    )
    db = motor_client.openehr_playground
except Exception as e:
    print(f"❌ Failed to create MongoDB client: {e}")
    db = None

# Initialize document identifier
document_identifier = DocumentIdentifier()

# Pydantic models
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

# Helper functions
async def get_template_by_id(template_id: str) -> Optional[Dict]:
    """Retrieve template from database"""
    try:
        template = await db.templates.find_one({"_id": ObjectId(template_id)})
        return template
    except:
        return None

async def save_temp_file(content: bytes, suffix: str) -> Path:
    """Save content to temporary file"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(content)
        return Path(tmp.name)

# API Endpoints
@app.on_event("startup")
async def startup_db_client():
    if db is None:
        print("❌ MongoDB client not initialised")
        return

    try:
        await motor_client.admin.command("ping")
        print("✅ Connected to MongoDB")

        # Ensure collections exist
        collections = await db.list_collection_names()
        for coll in ("mappings", "templates", "patterns", "type_template_associations"):
            if coll not in collections:
                await db.create_collection(coll)
                print(f"✅ Created '{coll}' collection")

        # Load ONLY persisted patterns from MongoDB (not from patterns.yaml)
        identifier = get_identifier()
        count_before = len(identifier.patterns)
        
        # Load patterns from MongoDB only
        async for pat in db.patterns.find():
            try:
                # Ensure we have all required fields with defaults
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
        
        # Re-sort by priority after loading
        identifier.patterns.sort(key=lambda p: p.priority, reverse=True)
        
        print(f"🚀 Loaded {len(identifier.patterns) - count_before} patterns from MongoDB "
              f"(total {len(identifier.patterns)})")

    except Exception as e:
        print(f"❌ MongoDB startup failed: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    if motor_client:
        motor_client.close()

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
    """
    Identify document with optional debug information.
    Returns detailed pattern matching info when debug=true.
    """
    tmp_path = await save_temp_file(await document.read(), Path(document.filename).suffix)
    
    try:
        identifier = get_identifier()
        
        # If debug mode, capture pattern evaluation
        if debug:
            debug_info = {
                "patternsChecked": [],
                "matchedPattern": None,
                "evaluationOrder": []
            }
            
            # Manually check each pattern to capture debug info
            for pattern in identifier.patterns:
                debug_info["evaluationOrder"].append({
                    "name": pattern.name,
                    "priority": pattern.priority,
                    "handler": pattern.handler
                })
                
                # Use a temporary identifier with just this pattern
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
            
            # No pattern matched, use default identification
            result = identifier.identify_document(tmp_path)
            result["debugInfo"] = debug_info
            result["filename"] = document.filename
            return result
        else:
            # Normal identification without debug
            result = identifier.identify_document(tmp_path)
            result["filename"] = document.filename
            return result
            
    finally:
        with suppress(FileNotFoundError):
            tmp_path.unlink()

@app.get("/api/internal/mappings")
async def get_mappings():
    """Retrieve all existing mapping definitions"""
    if db is None:
        raise HTTPException(
            status_code=503, 
            detail="Database connection not available. Please check MongoDB configuration."
        )
    
    try:
        mappings = []
        cursor = db.mappings.find()
        async for mapping in cursor:
            mapping['_id'] = str(mapping['_id'])
            mappings.append(mapping)
        return mappings
    except Exception as e:
        error_msg = str(e)
        if "authentication" in error_msg.lower():
            raise HTTPException(
                status_code=503, 
                detail="MongoDB authentication failed. Please check your credentials."
            )
        elif "ServerSelectionTimeoutError" in error_msg:
            raise HTTPException(
                status_code=503, 
                detail="Cannot connect to MongoDB. Please check if the server is accessible."
            )
        else:
            raise HTTPException(status_code=500, detail=f"Database error: {error_msg}")


@app.post("/api/internal/mappings")
async def save_mapping(mapping: MappingDefinition):
    """Save a new or updated mapping definition"""
    if db is None:
        raise HTTPException(
            status_code=503, 
            detail="Database connection not available. Please check MongoDB configuration."
        )
    
    try:
        # Check if mapping exists
        existing = await db.mappings.find_one({"documentType": mapping.documentType})
        
        mapping_data = mapping.dict()
        mapping_data['updated'] = datetime.utcnow()
        
        if existing:
            # Update existing
            mapping_data['created'] = existing.get('created', datetime.utcnow())
            result = await db.mappings.replace_one(
                {"_id": existing['_id']},
                mapping_data
            )
            mapping_data['_id'] = str(existing['_id'])
        else:
            # Create new
            mapping_data['created'] = datetime.utcnow()
            mapping_data['usageCount'] = 0
            result = await db.mappings.insert_one(mapping_data)
            mapping_data['_id'] = str(result.inserted_id)
        
        return mapping_data
        
    except Exception as e:
        error_msg = str(e)
        if "authentication" in error_msg.lower():
            raise HTTPException(
                status_code=503, 
                detail="MongoDB authentication failed. Please check your credentials."
            )
        else:
            raise HTTPException(status_code=500, detail=f"Database error: {error_msg}")


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
        # Return both YAML and JSON, so GUI can pick either
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
    """Transform document using mapping to openEHR composition"""
    try:
        # Load mapping from database
        mapping = await db.mappings.find_one({"_id": ObjectId(mappingId)})
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        # Load template
        template_doc = await get_template_by_id(templateId)
        if not template_doc:
            raise HTTPException(status_code=404, detail="Template not found")
        
        # Save files temporarily
        doc_content = await document.read()
        doc_path = await save_temp_file(doc_content, Path(document.filename).suffix)
        
        # Get template content (handle different storage formats)
        if 'optContent' in template_doc:
            template_content = template_doc['optContent']
        elif 'content' in template_doc:
            template_content = template_doc['content']
        else:
            # Reconstruct from webTemplate if needed
            template_content = json.dumps(template_doc.get('webTemplate', {}))
        
        template_path = await save_temp_file(
            template_content.encode('utf-8'), 
            '.opt' if '<template' in template_content else '.json'
        )
        
        try:
            # Initialize components
            template_parser = TemplateParser(str(template_path))
            generator = kehrnelGenerator(template_parser)
            
            # Register handler based on document type
            handler_type = mapping.get('handler', 'xml')
            if handler_type == 'xml':
                generator.register_handler(XMLHandler())
            elif handler_type == 'csv':
                generator.register_handler(CSVHandler())
            elif handler_type == 'hl7v2':
                generator.register_handler(HL7v2Handler())
            # Add more handlers as needed
            
            # Parse mapping
            mapping_dict = json.loads(mapping['content'])
            
            # Transform
            composition = generator.generate_from_mapping(mapping_dict, doc_path)
            
            # Update usage statistics
            await db.mappings.update_one(
                {"_id": ObjectId(mappingId)},
                {
                    "$inc": {"usageCount": 1},
                    "$set": {"lastUsed": datetime.utcnow()}
                }
            )
            
            return composition
            
        finally:
            # Cleanup
            doc_path.unlink(missing_ok=True)
            template_path.unlink(missing_ok=True)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/internal/validate-composition")
async def validate_composition(request: ValidationRequest):
    """Validate composition against template"""
    try:
        # Load template
        template_doc = await get_template_by_id(request.templateId)
        if not template_doc:
            raise HTTPException(status_code=404, detail="Template not found")
        
        # Get template content
        if 'optContent' in template_doc:
            template_content = template_doc['optContent']
        elif 'content' in template_doc:
            template_content = template_doc['content']
        else:
            template_content = json.dumps(template_doc.get('webTemplate', {}))
        
        # Save template temporarily
        template_path = await save_temp_file(
            template_content.encode('utf-8'),
            '.opt' if '<template' in template_content else '.json'
        )
        
        try:
            # Initialize validator
            template_parser = TemplateParser(str(template_path))
            validator = kehrnelValidator(template_parser)
            
            # Validate
            issues = validator.validate(request.composition)
            
            # Format results
            errors = []
            warnings = []
            info = []
            
            for issue in issues:
                issue_data = {
                    "path": getattr(issue, 'path', ''),
                    "message": getattr(issue, 'message', str(issue)),
                    "severity": getattr(issue, 'severity', 'error')
                }
                
                severity = issue_data["severity"]
                if hasattr(severity, 'value'):
                    severity = severity.value
                    
                if severity == "error":
                    errors.append(issue_data)
                elif severity == "warning":
                    warnings.append(issue_data)
                else:
                    info.append(issue_data)
            
            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
                "info": info
            }
            
        finally:
            # Cleanup
            template_path.unlink(missing_ok=True)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/internal/mappings/{mapping_id}")
async def delete_mapping(mapping_id: str):
    """Delete a mapping definition"""
    try:
        result = await db.mappings.delete_one({"_id": ObjectId(mapping_id)})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Mapping not found")
        return {"message": "Mapping deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/internal/mappings/by-type/{document_type}")
async def get_mapping_by_type(document_type: str):
    """Get mapping for a specific document type"""
    try:
        mapping = await db.mappings.find_one({"documentType": document_type})
        if mapping:
            mapping['_id'] = str(mapping['_id'])
            return mapping
        return None
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/internal/mappings/test")
async def test_mapping(request: TransformRequest):
    """Test a mapping without saving it"""
    try:
        # Save content temporarily
        doc_path = await save_temp_file(
            request.documentContent.encode('utf-8'),
            '.xml' if request.documentType == 'xml' else '.csv'
        )
        template_path = await save_temp_file(
            request.templateContent.encode('utf-8'),
            '.opt'
        )
        
        try:
            # Initialize components
            template_parser = TemplateParser(str(template_path))
            generator = kehrnelGenerator(template_parser)
            
            # Register handler
            if request.documentType == 'xml':
                generator.register_handler(XMLHandler())
            
            # Parse mapping
            mapping_dict = json.loads(request.mappingContent)
            
            # Transform
            composition = generator.generate_from_mapping(mapping_dict, doc_path)
            
            # Validate
            validator = kehrnelValidator(template_parser)
            issues = validator.validate(composition)
            
            return {
                "success": True,
                "composition": composition,
                "validationIssues": [
                    {
                        "path": getattr(issue, 'path', ''),
                        "message": getattr(issue, 'message', str(issue)),
                        "severity": getattr(issue, 'severity', 'error')
                    }
                    for issue in issues
                ]
            }
            
        finally:
            # Cleanup
            doc_path.unlink(missing_ok=True)
            template_path.unlink(missing_ok=True)
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/api/internal/patterns")
async def get_patterns():
    """Get all active patterns with full details"""
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
    """
    Add or update a pattern.
    - Adds to in-memory singleton immediately
    - Persists to MongoDB for future restarts
    """
    try:
        # Validate pattern structure
        required_fields = ["name", "handler"]
        for field in required_fields:
            if field not in pattern:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Create pattern object
        new_pattern = DocumentPattern(**pattern)
        
        # Add to singleton
        identifier = get_identifier()
        
        # Remove existing pattern with same name if exists
        identifier.patterns = [p for p in identifier.patterns if p.name != new_pattern.name]
        identifier.patterns.append(new_pattern)
        
        # Re-sort by priority
        identifier.patterns.sort(key=lambda p: p.priority, reverse=True)
        
        # Persist to MongoDB
        if db is not None:
            await db.patterns.replace_one(
                {"name": new_pattern.name},
                pattern,
                upsert=True
            )
        
        return {
            "message": f"Pattern '{new_pattern.name}' saved successfully",
            "pattern": pattern,
            "total_patterns": len(identifier.patterns)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/internal/patterns/{pattern_name}")
async def delete_pattern(pattern_name: str):
    """Delete a pattern from both memory and database"""
    try:
        identifier = get_identifier()
        
        # Remove from memory
        original_count = len(identifier.patterns)
        identifier.patterns = [p for p in identifier.patterns if p.name != pattern_name]
        
        if len(identifier.patterns) == original_count:
            raise HTTPException(status_code=404, detail=f"Pattern '{pattern_name}' not found")
        
        # Remove from database
        if db is not None:
            await db.patterns.delete_one({"name": pattern_name})
        
        return {
            "message": f"Pattern '{pattern_name}' deleted successfully",
            "remaining_patterns": len(identifier.patterns)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/internal/test-pattern")
async def test_pattern(
    document: UploadFile = File(...),
    patterns: str = Form(...)
):
    """Test a pattern against a document"""
    try:
        # Parse pattern from JSON string
        pattern_data = json.loads(patterns)
        test_pattern = DocumentPattern(**pattern_data)
        
        # Save document temporarily
        tmp_path = await save_temp_file(await document.read(), Path(document.filename).suffix)
        
        try:
            # Create a temporary identifier with just this pattern
            temp_identifier = DocumentIdentifier(patterns=[test_pattern])
            
            # Test identification
            result = temp_identifier.identify_document(tmp_path)
            
            # Check if it matches
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
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/internal/patterns/import")
async def import_patterns(patterns_file: UploadFile = File(...)):
    """Import multiple patterns from a JSON or YAML file"""
    try:
        content = await patterns_file.read()
        filename = patterns_file.filename.lower()
        
        # Parse based on file extension
        if filename.endswith(('.yaml', '.yml')):
            patterns_data = yaml.safe_load(content)
        elif filename.endswith('.json'):
            patterns_data = json.loads(content)
        else:
            # Try to auto-detect format
            try:
                patterns_data = json.loads(content)
            except json.JSONDecodeError:
                try:
                    patterns_data = yaml.safe_load(content)
                except yaml.YAMLError:
                    raise HTTPException(status_code=400, detail="File must be valid JSON or YAML")
        
        if not isinstance(patterns_data, list):
            raise HTTPException(status_code=400, detail="File must contain an array/list of patterns")
        
        imported_count = 0
        errors = []
        
        for pattern_data in patterns_data:
            try:
                # Handle optional fields that might be None or missing
                if 'optional_elements' in pattern_data and not pattern_data['optional_elements']:
                    pattern_data.pop('optional_elements', None)
                
                pattern = DocumentPattern(**pattern_data)
                
                # Add to singleton
                identifier = get_identifier()
                identifier.patterns = [p for p in identifier.patterns if p.name != pattern.name]
                identifier.patterns.append(pattern)
                
                # Persist to MongoDB
                if db is not None:
                    await db.patterns.replace_one(
                        {"name": pattern.name},
                        pattern_data,
                        upsert=True
                    )
                
                imported_count += 1
            except Exception as e:
                errors.append({
                    "pattern": pattern_data.get("name", "unknown"),
                    "error": str(e)
                })
        
        # Re-sort patterns
        identifier = get_identifier()
        identifier.patterns.sort(key=lambda p: p.priority, reverse=True)
        
        return {
            "imported": imported_count,
            "errors": errors,
            "total_patterns": len(identifier.patterns)
        }
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid file format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Add type-template associations endpoints
@app.get("/api/internal/type-template-associations")
async def get_type_template_associations():
    """Get all document type to template associations"""
    if db is not None:
        associations = {}
        async for assoc in db.type_template_associations.find():
            associations[assoc["documentType"]] = assoc["templateId"]
        return associations
    return {}

@app.post("/api/internal/type-template-associations")
async def save_type_template_association(data: Dict[str, str]):
    """Save a document type to template association"""
    document_type = data.get("documentType")
    template_id = data.get("templateId")
    
    if not document_type or not template_id:
        raise HTTPException(status_code=400, detail="Both documentType and templateId are required")
    
    if db is not None:
        await db.type_template_associations.replace_one(
            {"documentType": document_type},
            {
                "documentType": document_type,
                "templateId": template_id,
                "updated": datetime.utcnow()
            },
            upsert=True
        )
    else:
        raise HTTPException(status_code=503, detail="Database connection not available")
    
    return {"message": "Association saved successfully"}

@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"status": "ok", "db": db is not None}

@app.get("/api/internal/handlers")
async def list_handlers():
    return ["xml", "csv", "json", "hl7v2"]

def main():
    """Main entry point for the API server"""
    import uvicorn
    
    # Get configuration from environment or use defaults
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", 8000))
    reload = os.getenv("API_RELOAD", "true").lower() == "true"
    log_level = os.getenv("API_LOG_LEVEL", "info")
    
    print(f"Starting Kehrnel Mapping Studio API server...")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Reload: {reload}")
    print(f"MongoDB URL: {MONGODB_URL}")
    print(f"API Documentation will be available at: http://{host}:{port}/docs")
    
    uvicorn.run(
        "api.internal.api_server:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level
    )

# Run the server
if __name__ == "__main__":
    main()