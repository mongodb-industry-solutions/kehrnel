# src/app/main.py

import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Your existing database connection handlers
from src.app.core.database import connect_to_mongo, close_mongo_connection, get_mongodb_ehr_db

# Your existing routers
from src.api.v1.ehr.routes import router as ehr_router
from src.api.v1.template.routes import router as template_router
from src.api.v1.aql.routes import router as aql_router
# The new ingestion router
from src.api.v1.ingest import routes as ingest_routes

# The new flattener class
from src.transform.flattener_g import CompositionFlattener


# --- Lifespan Manager for Application Startup and Shutdown ---
# This replaces the deprecated on_event("startup") and on_event("shutdown")
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup Logic ---
    print("Application startup...")
    
    # 1. Connect to MongoDB using your existing function
    await connect_to_mongo()
    
    # 2. Get a database instance for the flattener
    #    We use your existing get_mongodb_ehr_db() function for consistency
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
    lifespan=lifespan  # Use the modern lifespan manager
)

# Your existing CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Include Routers ---
# Your existing routers
app.include_router(ehr_router, prefix="/v1", tags=["EHR"])
app.include_router(template_router, prefix="/v1", tags=["Template"])
app.include_router(aql_router, prefix="/v1", tags=["AQL"])

# The new ingestion router
app.include_router(
    ingest_routes.router,
    prefix="/v1/ingest", # Using /v1 prefix for consistency
    tags=["Ingestion"]
)