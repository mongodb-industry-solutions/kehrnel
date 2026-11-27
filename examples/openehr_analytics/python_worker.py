"""Python integration functions for batches and individual compositions.

Pass an iterable of dictionary job specifications to process_partition().
Each Python worker opens its own connections and reuses one converter. Resolve
credentials in the worker through your secret provider, outside rule/job JSON.
Scheduling and the choice of execution platform belong to the caller.
"""

from kehrnel.engine.domains.openehr.analytics import Backfill, run_backfill
from kehrnel.engine.domains.openehr.analytics.connections import (
    connect,
    create_converter,
)
from kehrnel.engine.domains.openehr.analytics.projection import utc
from kehrnel.engine.domains.openehr.analytics.setup import worker_environment


def process_partition(job_specs, environment, config):
    """environment is resolved locally in the Python worker, not stored in rule JSON."""
    if config.get("source", {}).get("mode", "upload") != "mongodb":
        raise ValueError("process_partition requires a MongoDB source; use process_composition for uploaded documents")
    environment = worker_environment(environment, config)
    source_client, target_client, source, store = connect(environment, settings=config)
    try:
        converter = create_converter(
            environment, source, config["rules"], config["source"]
        )
        # The deployment's prepare step creates indexes/collections once, before
        # launching workers. Each worker only reads source and writes projections.
        for spec in job_specs:
            # Copy each job so date normalization does not modify caller input.
            spec = dict(spec)
            spec["accepted_at"] = utc(spec["accepted_at"])
            for name in ("committed_from", "committed_until"):
                if spec.get(name):
                    spec[name] = utc(spec[name])
            run_backfill(source, store, converter, Backfill(**spec))
    finally:
        for client in (source_client, target_client):
            if client is not None:
                client.close()


def process_composition(document, accepted_at, converter, store):
    """The identical converter handles one new versioned composition."""
    return store.write(converter.convert(document, accepted_at=accepted_at))
