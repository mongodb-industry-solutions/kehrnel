import httpx

import os
# Test harness defaults: do not require auth and avoid relying on external env vars.
# create_app() loads .env.local with override=False, so explicitly set values here.
os.environ.setdefault("KEHRNEL_AUTH_ENABLED", "false")

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from httpx import AsyncClient
import asyncio

from typing import AsyncGenerator
from dotenv import load_dotenv

try:
    # Compatibility v1 app (optional, can be removed in strategy-only runtime)
    from src.app.main import app  # type: ignore
    from src.app.core.database import get_mongodb_ehr_db  # type: ignore
    _LEGACY_V1_AVAILABLE = True
except Exception:
    app = None
    get_mongodb_ehr_db = None
    _LEGACY_V1_AVAILABLE = False

# Database Fixture
# This fixture will create a new, clean database for each test session
# It will drop the database connection after all tests are done

# Load the environment variables
load_dotenv()

MONGO_TEST_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
TEST_DB_NAME = os.getenv("TEST_DB_NAME", "kehrnel_test")

@pytest_asyncio.fixture(scope="function")
async def test_db_client() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """
    Creates a Motor client for the test database that lives for each test function.
    """
    client = AsyncIOMotorClient(MONGO_TEST_URI)
    yield client
    client.close()


# API Client Fixture
# This fixture creates a client that can make requests to your API
# It uses the test_db fixture to override the production database dependency

@pytest_asyncio.fixture(scope="function")
async def client(test_db_client: AsyncIOMotorClient) -> AsyncGenerator[AsyncClient, None]:
    """
    Provides an HTTPX AsyncClient for making the requests to the FastAPI app
    with the database dependency overriden to use the test database
    """
    if not _LEGACY_V1_AVAILABLE:
        pytest.skip("Compatibility src.app v1 API is not present in this repo.")

    # Only run legacy-v1 DB-backed tests when a test Mongo URI is explicitly provided.
    # This avoids failing collection in environments (CI/local) without MongoDB.
    if "MONGODB_URI" not in os.environ:
        pytest.skip("MONGODB_URI not set; skipping legacy v1 MongoDB-backed tests.")

    db = test_db_client[TEST_DB_NAME]

    # --- Pre-test cleanup ---
    for col in ("ehr", "contributions", "compositions", "templates", "_codes", "flatten_compositions"):
        await db[col].delete_many({})

    # Tells the FastAPI app to replace the get_mongodb_ehr_db with the test_db
    def override_get_db():
        return db

    app.dependency_overrides[get_mongodb_ehr_db] = override_get_db
    async with AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c

    # --- Post-test cleanup (teardown) ---
    for col in ("ehr", "contributions", "compositions", "templates", "_codes", "flatten_compositions"):
        await db[col].delete_many({})

    # Clean the override after the session
    app.dependency_overrides.clear()
