import httpx
print(f"HTTPretty version being used by pytest: {httpx.__version__}")
print(f"HTTPretty file location: {httpx.__file__}")

import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from httpx import AsyncClient
import asyncio

# Importing the app from FastAPI main file
from src.app.main import app
from src.app.core.database import get_mongodb_ehr_db
from typing import AsyncGenerator
from dotenv import load_dotenv
import os

# Database Fixture
# This fixture will create a new, clean database for each test session
# It will drop the database connection after all tests are done

# Load the environment variables
load_dotenv()

MONGO_TEST_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
TEST_DB_NAME = os.getenv("TEST_DB_NAME")

@pytest_asyncio.fixture(scope="function")
async def test_db_client() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """
    Creates a Motor client for the test database that lives for each test function.
    """
    client = AsyncIOMotorClient(MONGO_TEST_URI)
    yield client
    client.close()


@pytest_asyncio.fixture(scope="function")
async def test_db(test_db_client: AsyncIOMotorClient) -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    """
    Provides a clean test database for each test function,
    cleaning collections before and after each test.
    """
    db = test_db_client[TEST_DB_NAME]
    
    # --- Pre-test cleanup ---
    # Ensure collections are empty before tests start
    await db["ehr"].delete_many({})
    await db["contributions"].delete_many({})
    await db["compositions"].delete_many({})
    await db["templates"].delete_many({})
    await db["_codes"].delete_many({})
    await db["flatten_compositions"].delete_many({})

    yield db

    # --- Post-test cleanup (teardown) ---
    await db["ehr"].delete_many({})
    await db["contributions"].delete_many({})
    await db["compositions"].delete_many({})
    await db["templates"].delete_many({})
    await db["_codes"].delete_many({})
    await db["flatten_compositions"].delete_many({})


# API Client Fixture
# This fixture creates a client that can make requests to your API
# It uses the test_db fixture to override the production database dependency

@pytest_asyncio.fixture(scope="function")
async def client(test_db: AsyncIOMotorDatabase) -> AsyncGenerator[AsyncClient, None]:
    """
    Provides an HTTPX AsyncClient for making the requests to the FastAPI app
    with the database dependency overriden to use the test database
    """
    # Tells the FastAPI app to replace the get_mongodb_ehr_db with the test_db
    def override_get_db():
        return test_db
    
    app.dependency_overrides[get_mongodb_ehr_db] = override_get_db
    async with AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c

    # Clean the override after the session
    app.dependency_overrides.clear()