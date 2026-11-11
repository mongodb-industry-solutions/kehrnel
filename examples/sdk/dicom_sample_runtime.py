"""Run the same persisted sample journey used by the public imaging showcase.

Supply an existing application StrategyContext with MongoDB storage and index
administration adapters. No connection credentials or database are chosen here.
"""
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.strategy_sdk import StrategyBindings, StrategyHandle

async def run_sample_journey(ctx, *, dataset_id='dicom.synthetic-imaging', variant='mixed'):
    strategy=DICOMTieredStoreStrategy()
    handle=StrategyHandle(strategy.manifest,strategy)
    await handle.activate(ctx.config, StrategyBindings(), ctx)
    receipt=await handle.dispatch('run_op',ctx,'dicom_sample_import',{
        'datasetId':dataset_id,'version':'1.0.0','variant':variant})
    return await handle.dispatch('run_op',ctx,'dicom_sample_query',{
        'receiptIds':[receipt['receipt']['_id']], 'where':[{'field':'rows','op':'>','value':0}], 'limit':50})
