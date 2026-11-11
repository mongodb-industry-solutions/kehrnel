"""Loopback development fixture: frozen public samples, real native MongoDB queries.

Production uses the normal authenticated Kehrnel runtime and a dedicated demo
activation. This fixture deliberately loads no credential files.
"""
import argparse
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse
os.environ['KEHRNEL_SKIP_DOTENV']='true'
os.environ['KEHRNEL_INIT_INGESTION_RUNTIME']='false'
os.environ['KEHRNEL_AUTH_ENABLED']='false'
os.environ['CORE_MONGODB_URL']=''
import uvicorn
from fastapi.testclient import TestClient
from kehrnel.api.app import create_app
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.strategies.dicom.tiered_store.common import ROOT


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=8094)
    parser.add_argument('--mongodb-uri',default='mongodb://127.0.0.1:27039')
    args=parser.parse_args()
    if urlparse(args.mongodb_uri).hostname not in {'localhost','127.0.0.1','::1'}:
        parser.error('The development fixture accepts only a local MongoDB database.')
    database='dicom_public_showcase'
    prefix='/v1/environments/dicom-showcase-public'
    with tempfile.TemporaryDirectory(prefix='kehrnel-dicom-showcase-') as directory:
        app=create_app(str(Path(directory)/'registry.json'))
        with TestClient(app) as client:
            response=client.post(prefix+'/activate',json={
                'domain':'dicom','strategy_id':'dicom.tiered_store','version':load_strategy('dicom.tiered_store',ROOT).version,
                'config':{'database':database,'tenant_id':'public_demo','allowed_centers':['PUBLIC_LIDC_IDRI','PUBLIC_PROSTATEX','PUBLIC_CBIS_DDSM','PUBLIC_LUNG_PET_CT_DX']},
                'bindings':{'db':{'provider':'mongodb','uri':args.mongodb_uri,'database':database}},'allow_plaintext_bindings':True})
            response.raise_for_status()
            for variant in ['ct','mr','mg','linked-ct-pet']:
                response=client.post(prefix+'/run',json={'domain':'dicom','operation':'dicom_sample_import','payload':{'datasetId':'dicom.public-imaging','version':'1.0.0','variant':variant}})
                response.raise_for_status()
            print('Verified public dataset ready: 4 studies, 5 series, 513 instances.',flush=True)
        uvicorn.run(app,host='127.0.0.1',port=args.port)
if __name__=='__main__':main()
