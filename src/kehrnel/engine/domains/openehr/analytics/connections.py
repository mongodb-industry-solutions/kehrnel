"""Credential loading is separate from portable rules and conversion code."""

from pathlib import Path
import os
from .decoder import IBMDecoder, IBMFields
from .converter import Converter, SourceFields
from .rules import RuleSet
from .store import MongoProjectionStore


def read_environment(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        key, sep, value = text.partition("=")
        if not sep or not key.startswith("ANALYTICS_"):
            raise ValueError("Use ANALYTICS_ environment settings only")
        values[key] = value.strip().strip('"').strip("'")
    values.update({k: v for k, v in os.environ.items() if k.startswith("ANALYTICS_")})
    return values


def connect(config, *, target=True, settings=None):
    settings = settings or {}
    from .setup import worker_environment
    config = worker_environment(config, settings)
    from pymongo import MongoClient

    upload = settings.get("source", {}).get("mode") == "upload"
    required = [] if upload else [
        "ANALYTICS_SOURCE_URI",
        "ANALYTICS_SOURCE_DATABASE",
        "ANALYTICS_SOURCE_COLLECTION",
    ]
    if target:
        required += [
            "ANALYTICS_TARGET_URI",
            "ANALYTICS_TARGET_DATABASE",
            "ANALYTICS_SOURCE_NAMESPACE",
        ]
    if any(not config.get(k) for k in required):
        raise ValueError(
            "Fill the required ANALYTICS_SOURCE / TARGET settings in the local environment file"
        )
    if (
        target
        and not upload
        and config["ANALYTICS_SOURCE_DATABASE"] == config["ANALYTICS_TARGET_DATABASE"]
    ):
        raise ValueError("Use a separate analytics database")
    if settings.get(
        "source_namespace", config.get("ANALYTICS_SOURCE_NAMESPACE")
    ) != config.get("ANALYTICS_SOURCE_NAMESPACE"):
        raise ValueError("Configuration and environment source namespaces differ")
    if target and settings.get("database") and settings["database"] != config["ANALYTICS_TARGET_DATABASE"]:
        raise ValueError("Configuration and environment target databases differ")
    if (settings.get("collection_prefix") and config.get("ANALYTICS_COLLECTION_PREFIX")
            and settings["collection_prefix"] != config["ANALYTICS_COLLECTION_PREFIX"]):
        raise ValueError("Configuration and environment collection prefixes differ")
    source_client = source = None
    if not upload:
        source_client = MongoClient(
            config["ANALYTICS_SOURCE_URI"],
            uuidRepresentation="standard",
            tz_aware=True,
            serverSelectionTimeoutMS=10000,
            appname="kehrnel-analytics-source",
        )
        source = source_client[config["ANALYTICS_SOURCE_DATABASE"]][
            config["ANALYTICS_SOURCE_COLLECTION"]
        ]
    target_client = None
    store = None
    if target:
        if not upload and config["ANALYTICS_SOURCE_DATABASE"] == config["ANALYTICS_TARGET_DATABASE"]:
            raise ValueError("Use a separate analytics database")
        target_client = MongoClient(
            config["ANALYTICS_TARGET_URI"],
            uuidRepresentation="standard",
            tz_aware=True,
            serverSelectionTimeoutMS=10000,
            appname="kehrnel-analytics-target",
        )
        store = MongoProjectionStore(
            target_client[config["ANALYTICS_TARGET_DATABASE"]],
            source_namespace=config["ANALYTICS_SOURCE_NAMESPACE"],
            prefix=settings.get(
                "collection_prefix",
                config.get("ANALYTICS_COLLECTION_PREFIX", "analytics_"),
            ),
            max_bytes=settings.get("max_transaction_bytes", 33554432),
            max_facts=settings.get("max_transaction_facts", 50000),
            rollup_partitions=settings.get("rollup_partitions", 64),
        )
    return source_client, target_client, source, store


def create_converter(config, source_collection, rule_values, source_profile):
    source_format = source_profile.get("format", "canonical")
    if source_format not in ("canonical", "ibm"):
        raise ValueError("Choose canonical or ibm source format")
    decoder = None
    if source_format == "ibm":
        from .profiles import load_ibm_decoder

        decoder = load_ibm_decoder(source_collection.database if source_collection is not None else None, config, source_profile)
    return Converter(
        RuleSet.compile(rule_values),
        source_namespace=config["ANALYTICS_SOURCE_NAMESPACE"],
        fields=SourceFields(**source_profile.get("fields", {})),
        decoder=decoder,
    )
