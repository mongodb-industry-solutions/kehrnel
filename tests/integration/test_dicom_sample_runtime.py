"""Domain samples execute through the same scoped native operations as applications."""
import pytest
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy
from kehrnel.engine.core.errors import KehrnelError
from test_dicom_rule_ingestion import ctx

@pytest.mark.asyncio
async def test_sample_import_replay_query_and_scope(ctx):
    strategy = DICOMTieredStoreStrategy()
    payload = {'datasetId':'dicom.synthetic-imaging','version':'1.0.0','variant':'mixed'}
    imported = await strategy.run_op(ctx,'dicom_sample_import',payload)
    replay = await strategy.run_op(ctx,'dicom_sample_import',payload)
    assert imported['receipt']['_id'] == replay['receipt']['_id']
    status = await strategy.run_op(ctx,'dicom_sample_status',{k:v for k,v in payload.items() if k!='variant'})
    assert len(status['receipts']) == 1
    request = {'receiptIds':[imported['receipt']['_id']], 'text':'Find CT series where rows > 0'}
    result = await strategy.run_op(ctx,'dicom_sample_query',request)
    assert result['executed'] and result['engine']=='MongoDB' and result['rows']
    assert result['evidence'][0]['pipeline'][0]['$match']
    ctx.config['tenant_id']='someone_else'
    with pytest.raises(KehrnelError): await strategy.run_op(ctx,'dicom_sample_query',request)

@pytest.mark.asyncio
async def test_public_sample_import_and_actual_query(ctx):
    ctx.config['allowed_centers']=['PUBLIC_LIDC_IDRI','PUBLIC_PROSTATEX','PUBLIC_CBIS_DDSM','PUBLIC_LUNG_PET_CT_DX']
    strategy = DICOMTieredStoreStrategy()
    receipts=[]
    for variant in ['ct','mr','mg','linked-ct-pet']:
        imported=await strategy.run_op(ctx,'dicom_sample_import',{'datasetId':'dicom.public-imaging','version':'1.0.0','variant':variant})
        receipts.append(imported['receipt']['_id'])
    result=await strategy.run_op(ctx,'dicom_sample_query',{'receiptIds':receipts})
    assert result['seriesCount']==5
    assert {row['_kehrnel']['modality'] for row in result['rows']}=={'CT','MR','MG','PT'}
    filtered=await strategy.run_op(ctx,'dicom_sample_query',{'receiptIds':receipts,'modality':'MR'})
    assert filtered['seriesCount']==1

@pytest.mark.asyncio
async def test_python_sdk_sample_journey(ctx):
    import importlib.util
    from pathlib import Path
    path = Path(__file__).parents[2] / 'examples/sdk/dicom_sample_runtime.py'
    spec = importlib.util.spec_from_file_location('dicom_sample_sdk_example', path)
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    result = await example.run_sample_journey(ctx)
    assert result['executed'] and result['engine'] == 'MongoDB'
    assert result['seriesCount'] == 2
