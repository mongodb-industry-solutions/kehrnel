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

@pytest_asyncio.fixture(scope="session")
def event_loop():
    """
    Creates an instance of the default event loop for the test session.
    Necessary for pytest-asyncio
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def test_db(test_db_client: AsyncIOMotorClient) -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    """
    Provies a clean test database for the entire test session
    dropping the test database at the end
    """
    db = test_db_client[TEST_DB_NAME]
    yield db
    # Drop the database after all tests in the session are complete
    await test_db_client.drop_database(TEST_DB_NAME)


# API Client Fixture
# This fixture creates a client that can make requests to your API
# It uses the test_db fixture to override the production database dependency

@pytest_asyncio.fixture(scope="session")
async def client(test_db: AsyncIOMotorDatabase) -> AsyncGenerator[AsyncClient, None]:
    """
    Provides an HTTPX AsyncClient for making the requests to the FastAPI app
    with the database dependency overriden to use the test database
    """
    # Tells the FastAPI app to replace the get_mongodb_ehr_db with the test_db
    def override_get_db():
        return test_db
    
    app.dependency_overrides[get_mongodb_ehr_db] = override_get_db
    async with AsyncClient(app=app, base_url="http://test") as c:
        yield c

    # Clean the override after the session
    app.dependency_overrides.clear()




