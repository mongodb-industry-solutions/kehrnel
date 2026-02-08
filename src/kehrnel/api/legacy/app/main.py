# src/kehrnel/api/legacy/app/main.py
# Deprecated legacy API entrypoint. Use `uvicorn kehrnel.api.app:app --reload` for the new runtime.

import os
import json
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from kehrnel.api.legacy.app.core.database import connect_to_mongo, close_mongo_connection, get_mongodb_ehr_db
from kehrnel.api.legacy.app.utils.config_runtime import apply_ingestion_config, DEFAULT_MAPPINGS_PATH

from kehrnel.api.legacy.v1.ehr.routes import router as ehr_router
from kehrnel.api.legacy.v1.template.routes import router as template_router
from kehrnel.api.legacy.v1.aql.routes import router as aql_router
from kehrnel.api.legacy.v1.composition.routes import router as composition_router
from kehrnel.api.legacy.v1.contribution.routes import router as contribution_router
from kehrnel.api.legacy.v1.ehr_status.routes import router as ehr_status_router
from kehrnel.api.legacy.v1.ingest.routes import router as ingest_router
from kehrnel.api.legacy.v1.synthetic.routes import router as synthetic_router
from kehrnel.api.legacy.v1.config.routes import router as config_router
from kehrnel.api.legacy.v1.strategy.routes import router as strategy_router
from kehrnel.api.legacy.app.strategy_runtime import init_strategy_runtime


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup...")

    # 1. Connect to MongoDB
    await connect_to_mongo(tls_allow_invalid_certificates=True)

    # 2. Always set app.state.db first (required for all endpoints)
    app.state.db = await get_mongodb_ehr_db()
    app.state.config = None
    app.state.flattener = None
    app.state.transformer = None

    # 3. Load configuration for the flattener
    #    Default: config.json if present; can be overridden later via /v1/config
    config_path = Path("config.json")
    if config_path.exists():
        try:
            with config_path.open() as f:
                config = json.load(f)
            await apply_ingestion_config(
                app,
                config=config,
                mappings_inline=None,
                use_mappings_file=True,
                mappings_path=None,
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

    # 3. Initialize strategy runtime (non-blocking; falls back silently)
    init_strategy_runtime(app, environment=os.getenv("KEHRNEL_ENV", "dev"), tenant=os.getenv("KEHRNEL_TENANT"))

    yield 
    
    # --- Shutdown Logic ---
    print("Application shutdown...")
    
    # 1. Flush codes to the database if in primary mode
    if hasattr(app.state, 'flattener') and app.state.flattener and app.state.flattener.role == 'primary':
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
app.include_router(config_router, prefix="/v1/config", tags=["Configuration"])
app.include_router(strategy_router, prefix="/v1", tags=["Strategies"])

# Health endpoints (both /health and /healthz for compatibility)
@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}

@app.get("/healthz", include_in_schema=False)
async def healthz():
    """Kubernetes-style health probe endpoint"""
    return {"status": "ok"}
