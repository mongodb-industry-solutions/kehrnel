"""Load domain-owned source samples and preview them through the native SDK.

This local example reads small registered sample packages. It never connects
to MongoDB, downloads pixels or accepts a filesystem path in an HTTP request.
"""
from __future__ import annotations

import argparse
import asyncio
import json

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.dicom.sample_data import describe_sample, load_sample
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.strategy_sdk import StrategyBindings, StrategyHandle


async def preview(args):
    options = {"root": args.samples_dir} if args.samples_dir else {}
    metadata = describe_sample(args.dataset, args.version, **options)
    source = load_sample(args.dataset, args.version, variant=args.variant,
                         expected_digest=metadata["reference"]["digest"], **options)
    ctx = StrategyContext(environment_id="dicom-domain-sample-sdk", config={})
    strategy = DICOMTieredStoreStrategy()
    handle = StrategyHandle(strategy.manifest, strategy)
    await handle.activate(ctx.config, StrategyBindings(), ctx)
    catalog = await handle.dispatch("run_op", ctx, "dicom_rule_catalog", {})
    result = await handle.dispatch("run_op", ctx, "dicom_rule_preview", {
        **source, "profile": catalog["profiles"][0],
    })
    return {"sourceReference": metadata["reference"], "variant": args.variant,
            "containsPixels": metadata["containsPixels"], "counts": result["counts"],
            "generationId": result["generationId"], "persisted": result["persisted"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="dicom.synthetic-imaging")
    parser.add_argument("--version", default="1.0.0")
    parser.add_argument("--variant", default="mixed")
    parser.add_argument("--samples-dir", help="Local domain sample root containing catalog.json")
    print(json.dumps(asyncio.run(preview(parser.parse_args())), indent=2))
