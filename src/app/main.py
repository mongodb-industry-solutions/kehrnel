# src/app/main.py

import os
import json
import json5
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.app.core.database import connect_to_mongo, close_mongo_connection, get_mongodb_ehr_db

from src.api.v1.ehr.routes import router as ehr_router
from src.api.v1.template.routes import router as template_router
from src.api.v1.aql.routes import router as aql_router
from src.api.v1.composition.routes import router as composition_router
from src.api.v1.contribution.routes import router as contribution_router
from src.api.v1.ehr_status.routes import router as ehr_status_router
from src.api.v1.ingest.routes import router as ingest_router
from src.api.v1.synthetic.routes import router as synthetic_router

from src.transform.flattener_g import CompositionFlattener
from src.transform.core import Transformer, load_default_cfg

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup Logic ---
    print("Application startup...")
    
    # 1. Connect to MongoDB using the existing function
    await connect_to_mongo()
    
    # 2. Get a database instance for the flattener
    #    Use the existing get_mongodb_ehr_db() function for consistency
    db = await get_mongodb_ehr_db()

    # 3. Load configuration for the flattener
    #    (Assumes a config.json is in your project root)
    #    TODO: It's better to manage this via a centralized settings module in core/config.py
    with open("config.json") as f:
        config = json.load(f)

    # 4. Initialize the Flattener and store it in app.state
    #    app.state is the recommended place to store objects for the app's lifespan
    mappings_path = os.path.join(
        os.path.dirname(__file__), '..', 'transform', 'config', 'flattener_mappings.jsonc'
    )
    
    app.state.db = db
    app.state.config = config
    app.state.flattener = await CompositionFlattener.create(
        db=db,
        config=config,
        mappings_path=mappings_path
    )
    
    print("CompositionFlattener initialized.")
    print("Initializing Transformer for un-flattening...")
    
    # Load the base config (for shortcuts, etc.)
    transformer_config = load_default_cfg(None)
    
    # Add the MAPPINGS FILE PATH to the transformer's config dictionary
    # The RulesEngine expects a path, not a loaded dictionary.
    transformer_config['mappings'] = mappings_path

    # 3. Now, initialize the Transformer with the complete config
    app.state.transformer = Transformer(cfg=transformer_config, role="primary")
    print("Transformer initialized.")

    yield  # --- Application is now running ---
    
    # --- Shutdown Logic ---
    print("Application shutdown...")
    
    # 1. Flush codes to the database if in primary mode
    if app.state.flattener and app.state.flattener.role == 'primary':
        print("Flushing codes to database...")
        await app.state.flattener.flush_codes_to_db()
    
    # 2. Close the MongoDB connection using your existing function
    await close_mongo_connection()
    print("MongoDB connection closed.")


# --- FastAPI App Initialization ---
app = FastAPI(
    title="OpenEHR MongoDB API",
    version="1.0.0",
    lifespan=lifespan
)

# Your existing CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["location", "etag", "date", "server", "content-length", "content-type"],
)

# --- Include Routers ---
app.include_router(ehr_router, prefix="/v1", tags=["EHR"])
app.include_router(template_router, prefix="/v1", tags=["Template"])
app.include_router(aql_router, prefix="/v1", tags=["AQL"])
app.include_router(composition_router, prefix="/v1", tags=["Compositions"])
app.include_router(contribution_router, prefix="/v1", tags=["Contributions"])
app.include_router(ehr_status_router, prefix="/v1", tags=["EHR_STATUS"])
app.include_router(ingest_router, prefix="/v1/ingestions", tags=["Ingestion"])
app.include_router(synthetic_router, prefix="/v1", tags=["Synthetic Data"])