"""Run the same bounded rule journey exposed through HTTP and CLI.

Without adapters this previews synthetic metadata and compiles queries.
With storage/index_admin adapters, await journey(ctx, persist=True).
Use a disposable demo database; this is not a historical migration worker.
"""
from __future__ import annotations

import asyncio
import json

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.strategy_sdk import StrategyBindings, StrategyHandle


async def journey(ctx, *, persist=False):
    strategy = DICOMTieredStoreStrategy()
    handle = StrategyHandle(strategy.manifest, strategy)
    await handle.activate(ctx.config, StrategyBindings(), ctx)
    async def operation(name, payload):
        return await handle.dispatch('run_op', ctx, name, payload)
    catalog = await operation('dicom_rule_catalog', {})
    data = await operation('dicom_rule_demo', {})
    profile = data['profile']
    generation = data['preview']
    reception = None
    if persist:
        await operation('dicom_rule_apply_indexes', {'profile': profile})
        reception = await operation('dicom_rule_receive', {**data['source'], 'profile': profile})
        generation = await operation('dicom_rule_process', {'batchId': reception['batchId']})
    query = {'text': 'Find CT series where rows = 1024 and photometric = MONOCHROME2',
             'group': 'rendering', 'limit': 50, 'allowPartial': True}
    if persist:
        result = await operation('dicom_rule_query', {**query, 'profile': profile,
                                                      'generationId': generation['generationId']})
    else:
        result = (await operation('dicom_rule_demo', {'profile': profile, 'query': query}))['queryResult']
    return {'catalog': catalog, 'source': data['source'], 'reception': reception,
            'generation': generation, 'query': result}


if __name__ == '__main__':
    result = asyncio.run(journey(StrategyContext(environment_id='dicom-rule-sdk', config={})))
    print(json.dumps({'profileCount': len(result['catalog']['profiles']),
                     'generationId': result['generation']['generationId'],
                     'persisted': result['generation']['persisted'],
                     'matchingSeries': len(result['query']['rows']),
                     'semantics': result['query']['semantics']}, indent=2))
