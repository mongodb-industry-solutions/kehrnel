from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.app.core.database import connect_to_mongo, close_mongo_connection
from src.api.v1.ehr.routes import router as ehr_router
from src.api.v1.template.routes import router as template_router

app = FastAPI(
    title="OpenEHR MongoDB API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    await connect_to_mongo()


@app.on_event("shutdown")
async def shutdown_event():
    await close_mongo_connection()

app.include_router(ehr_router, prefix="/v1")
app.include_router(template_router, prefix="/v1")

# Test with Curl
# curl -v -X POST http://localhost:8000/v1/ehr -H "Content-Type: application/json"