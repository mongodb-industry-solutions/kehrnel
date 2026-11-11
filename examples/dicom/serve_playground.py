"""Serve only a locally activated synthetic DICOM playground environment.

Run from the Kehrnel checkout with PYTHONPATH=src. Uses a temporary registry,
no customer database, no credentials and no pixel files. Intended for local
Healthcare Data Lab development, not a production public runtime deployment.
"""
from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import uvicorn
from fastapi.testclient import TestClient

# This process is a loopback-only synthetic fixture, isolated from local
# credential discovery and unrelated MongoDB bootstrap/recovery services.
os.environ['KEHRNEL_SKIP_DOTENV'] = 'true'
os.environ['KEHRNEL_INIT_INGESTION_RUNTIME'] = 'false'
os.environ['KEHRNEL_AUTH_ENABLED'] = 'false'
os.environ['CORE_MONGODB_URL'] = ''

from kehrnel.api.app import create_app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8092)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='kehrnel-dicom-playground-') as directory:
        app = create_app(str(Path(directory) / 'registry.json'))
        with TestClient(app) as client:
            response = client.post('/v1/environments/dicom-playground-public/activate', json={
                'domain': 'dicom', 'strategy_id': 'dicom.tiered_store', 'version': '0.4.0',
                'config': {'database': 'dicom_public_synthetic'},
                'bindings': {}, 'allow_plaintext_bindings': True,
            })
            response.raise_for_status()
        uvicorn.run(app, host='127.0.0.1', port=args.port)


if __name__ == '__main__':
    main()
