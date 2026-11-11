"""Use the exact operation boundary called by the connected DICOM workspace."""
import asyncio
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.dicom.tiered_store.strategy import DICOMTieredStoreStrategy

async def main():
    context = StrategyContext(environment_id="dicom-local-preview", config={})
    plugin = DICOMTieredStoreStrategy()
    await plugin.validate_config(context)
    catalog = await plugin.run_op(context, "dicom_metadata_catalog", {})
    result = await plugin.run_op(context, "dicom_metadata_preview", {
        "scope": {"modalities": ["SR"]}, "fields": ["manufacturer"],
        "includeFacts": True, "destination": "shared"})
    print(result["documentNative"])
    query = await plugin.run_op(context, "dicom_metadata_query", {
        "kind": "searchCombined", "modality": "CT", "manufacturer": "SIEMENS",
        "quantityField": "sliceThickness", "maximum": 2, "execute": False})
    print(query["pipelineNative"])

if __name__ == "__main__":
    asyncio.run(main())
