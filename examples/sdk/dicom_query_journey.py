"""DICOM SDK example consumed by the same contracts as HTTP and CLI.

Run compilation without a database:
    PYTHONPATH=src python examples/sdk/dicom_query_journey.py

For execution, bind a StrategyContext with storage/index_admin adapters and
an artifact adapter for bundle mode, then await run_journey(ctx, seed=True).
Use a fresh demonstration database; the seed writes synthetic metadata.
"""
from __future__ import annotations

import asyncio
import json

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store.catalog import EXAMPLES
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.strategy_sdk import StrategyBindings, StrategyHandle


async def handle_for(ctx):
    strategy = DICOMTieredStoreStrategy()
    handle = StrategyHandle(strategy.manifest, strategy)
    await handle.activate(ctx.config, StrategyBindings(), ctx)
    return handle


async def run_journey(ctx, *, seed=False):
    handle = await handle_for(ctx)
    catalog = await handle.dispatch('run_op', ctx, 'dicom_query_catalog', {})
    if seed:
        await handle.dispatch('apply', ctx, await handle.dispatch('plan', ctx))
        await handle.dispatch('run_op', ctx, 'dicom_generate_synthetic', EXAMPLES['dataset']['payload'])
        if next(resource for resource in catalog['resources'] if resource['id'] == 'research')['enabled']:
            request = EXAMPLES['researchBuildExample']['payload']
            cursor = None
            while True:
                page = await handle.dispatch('run_op', ctx, 'dicom_rebuild_projections', {
                    **request, **({'cursor': cursor} if cursor else {})})
                cursor = page['page']['nextCursor']
                if not cursor:
                    break
    results = {}
    for example in catalog['examples']:
        if not example['enabled']:
            continue
        plan = await handle.dispatch('compile_query', ctx, 'dicom', example['query'])
        result = await handle.dispatch('execute_query', ctx, plan)
        results[example['id']] = {'rows': result.rows, 'explain': result.explain}
    headers = await handle.dispatch('run_op', ctx, EXAMPLES['headerExample']['operation'], EXAMPLES['headerExample']['payload'])
    return {'catalog': catalog, 'queries': results, 'headers': headers}


async def main():
    ctx = StrategyContext(environment_id='dicom-sdk-example', config={'projections': {'enabled': True}})
    handle = await handle_for(ctx)
    catalog = await handle.dispatch('run_op', ctx, 'dicom_query_catalog', {})
    compiled = []
    for example in catalog['examples']:
        if example['enabled']:
            plan = await handle.dispatch('compile_query', ctx, 'dicom', example['query'])
            compiled.append({'id': example['id'], 'resource': plan.explain['resource'], 'grain': plan.explain['resultGrain']})
    print(json.dumps({'mode': 'compile_only', 'strategyVersion': catalog['strategyVersion'], 'queries': compiled}, indent=2))


if __name__ == '__main__':
    asyncio.run(main())
