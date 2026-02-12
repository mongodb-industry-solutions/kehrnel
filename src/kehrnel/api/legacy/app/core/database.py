import logging
from typing import Optional

import certifi
from fastapi import HTTPException, Request
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure

from kehrnel.api.legacy.app.core.config import settings
from kehrnel.api.legacy.app.core.config_manager import initialize_config_manager, close_config_manager
from kehrnel.core.bindings_resolver import resolve_bindings as resolve_bindings_ref

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    client: AsyncIOMotorClient = None
    clients_by_uri: dict[str, AsyncIOMotorClient] = {}

db = Database()

def _extract_env_id(request: Request) -> Optional[str]:
    env_id = (
        request.headers.get("x-active-env")
        or request.headers.get("x-env-id")
        or request.headers.get("x-environment-id")
        or request.query_params.get("env_id")
        or request.query_params.get("environment")
        or request.path_params.get("env_id")
    )
    return (env_id or "").strip() or None


def _get_client_for_uri(uri: str) -> AsyncIOMotorClient:
    client = db.clients_by_uri.get(uri)
    if client is None:
        client = AsyncIOMotorClient(uri, tlsCAFile=certifi.where())
        db.clients_by_uri[uri] = client
    return client


async def _resolve_customer_db_from_activation(
    request: Request,
    domain: str = "openehr",
) -> AsyncIOMotorDatabase:
    env_id = _extract_env_id(request)
    if not env_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active environment. Provide x-active-env (or env_id query param).",
        )

    runtime = getattr(request.app.state, "strategy_runtime", None)
    if runtime is None:
        raise HTTPException(
            status_code=503,
            detail="Strategy runtime is not available.",
        )

    activation = runtime.registry.get_activation(env_id, domain) or runtime.registry.get_activation(env_id)
    if activation is None:
        raise HTTPException(
            status_code=404,
            detail=f"No activation found for env_id={env_id} and domain={domain}.",
        )
    if not activation.bindings_ref:
        raise HTTPException(
            status_code=409,
            detail=(
                "Activation has no bindings_ref. Re-activate environment with bindings_ref "
                "to resolve customer database connection."
            ),
        )
    if runtime.bindings_resolver is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Bindings resolver is not configured. Set KEHRNEL_BINDINGS_RESOLVER "
                "or HDL resolver environment variables."
            ),
        )

    resolved = await resolve_bindings_ref(
        runtime.bindings_resolver,
        bindings_ref=activation.bindings_ref,
        env_id=env_id,
        domain=domain,
        strategy_id=activation.strategy_id,
        op=f"http:{request.method.lower()}:{request.url.path}",
        context={"activation_config": activation.config or {}},
    )
    db_cfg = (resolved or {}).get("db") if isinstance(resolved, dict) else None
    provider = (db_cfg or {}).get("provider")
    uri = (db_cfg or {}).get("uri")
    database = (db_cfg or {}).get("database")
    if provider != "mongodb" or not uri or not database:
        raise HTTPException(
            status_code=502,
            detail="Bindings resolver returned invalid MongoDB binding (provider/uri/database required).",
        )

    client = _get_client_for_uri(str(uri))
    return client[str(database)]


async def get_mongodb_ehr_db(request: Request = None) -> AsyncIOMotorDatabase:
    # Request-aware path: resolve customer DB from active env binding contract.
    if request is not None:
        return await _resolve_customer_db_from_activation(request, domain="openehr")

    # Fallback for non-request internal utility calls.
    if db.client is None:
        db.client = AsyncIOMotorClient(settings.MONGODB_URI, tlsCAFile=certifi.where())
    return db.client[settings.MONGODB_DB]


async def get_mongodb_database(database_name: str, request: Request = None) -> AsyncIOMotorDatabase:
    """Get a specific database by name."""
    if request is not None:
        base_db = await _resolve_customer_db_from_activation(request, domain="openehr")
        return base_db.client[database_name]
    if db.client is None:
        db.client = AsyncIOMotorClient(settings.MONGODB_URI, tlsCAFile=certifi.where())
    return db.client[database_name]

async def connect_to_mongo(tls_allow_invalid_certificates: bool = False):
    logger.info("Connecting to MongoDB")
    try:
        db.client = AsyncIOMotorClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS = 5000,
            tlsAllowInvalidCertificates=tls_allow_invalid_certificates
        )

        # It sends an ismaster command which let us know if the connections was established correctly
        await db.client.admin.command('ismaster')
        logger.info("Successfully connected to MongoDB!")
        
        # Initialize configuration manager if dynamic config is enabled
        if settings.USE_DYNAMIC_CONFIG:
            logger.info("Initializing configuration manager...")
            await initialize_config_manager(
                config_db_uri=settings.config_db_uri,
                config_db_name=settings.CONFIG_DB_NAME,
                config_collection_name=settings.CONFIG_COLLECTION_NAME,
                cache_ttl_minutes=settings.CONFIG_CACHE_TTL_MINUTES
            )
            logger.info("Configuration manager initialized successfully")
        else:
            logger.info("Dynamic configuration disabled, using static configuration")
            
    except ConnectionFailure as e:
        logger.critical(f"Fatal error: Could not connect to MongoDB. Application will shut down. Error: {e}")

async def close_mongo_connection():
    if db.client:
        logger.info("Closing MongoDB connection")
        db.client.close()
        logger.info("MongoDB connection closed")
    for uri, client in list(db.clients_by_uri.items()):
        try:
            client.close()
        except Exception:
            pass
        finally:
            db.clients_by_uri.pop(uri, None)
    
    # Close configuration manager
    if settings.USE_DYNAMIC_CONFIG:
        await close_config_manager()
        logger.info("Configuration manager connection closed")
