# kehrnel/persistence/fs.py

import json
from pathlib import Path
from typing import Iterable, Dict, Any

def write_jsonl(path: str, docs: Iterable[Dict[str,Any]]):
    p = Path(path)
    with p.open("w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, default=str))
            f.write("\n")

def read_jsonl(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)