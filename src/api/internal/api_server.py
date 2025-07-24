"""FastAPI server for Mapping Studio integration with kehrnel - Internal Tooling API"""

import os
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
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
    from mapper.handlers.hl7v2_handler import HL7v2Handler
except ImportError as e:
    print(f"Warning: Some kehrnel components are not implemented yet: {e}")
    print("Creating placeholder classes for development...")
    
    # Placeholder classes for development
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
    
    # Import DocumentIdentifier and DocumentPattern from the fixed file
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
                    "confidence": 0.5,
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
    allow_origins=["http://localhost:3000"],  # Your Next.js app
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
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
    if db is not None:
        try:
            # Test the connection
            await motor_client.admin.command('ping')
            print("✅ Connected to MongoDB successfully!")
            
            # Ensure required collections exist
            collections = await db.list_collection_names()
            if 'mappings' not in collections:
                await db.create_collection('mappings')
                print("✅ Created 'mappings' collection")
            if 'templates' not in collections:
                await db.create_collection('templates')
                print("✅ Created 'templates' collection")
                
        except Exception as e:
            print(f"❌ MongoDB connection test failed: {e}")
            print("Please check your MONGODB_URL environment variable")
    else:
        print("❌ MongoDB client not initialized")

@app.on_event("shutdown")
async def shutdown_db_client():
    if motor_client:
        motor_client.close()

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Kehrnel Mapping Studio API",
        "version": "1.0.0",
        "endpoints": [
            "/api/internal/identify-document",
            "/api/internal/mappings",
            "/api/internal/transform",
            "/api/internal/validate-composition"
        ]
    }

@app.post("/api/internal/identify-document")
async def identify_document(document: UploadFile = File(...)):
    """Identify document type and handler from uploaded file"""
    try:
        # Save uploaded file temporarily
        content = await document.read()
        file_path = await save_temp_file(content, Path(document.filename).suffix)
        
        try:
            # Use document identifier
            result = document_identifier.identify_document(file_path)
            
            # Add filename to result
            result["filename"] = document.filename
            
            return result
            
        finally:
            # Cleanup
            file_path.unlink(missing_ok=True)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

@app.post("/api/internal/patterns")
async def add_document_pattern(pattern: Dict[str, Any]):
    """Add a new document pattern for identification"""
    try:
        new_pattern = DocumentPattern(
            name=pattern['name'],
            handler=pattern['handler'],
            required_elements=pattern.get('required_elements', []),
            optional_elements=pattern.get('optional_elements', []),
            namespaces=pattern.get('namespaces', {}),
            xpath_patterns=pattern.get('xpath_patterns', []),
            csv_headers=pattern.get('csv_headers', [])
        )
        
        document_identifier.add_pattern(new_pattern)
        
        # Optionally save to database for persistence
        await db.document_patterns.insert_one(pattern)
        
        return {"message": "Pattern added successfully", "patterns": document_identifier.list_patterns()}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/internal/patterns")
async def list_document_patterns():
    """List all registered document patterns"""
    return {
        "patterns": document_identifier.list_patterns(),
        "count": len(document_identifier.patterns)
    }

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