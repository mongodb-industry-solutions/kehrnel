"""Fetch a revision- and checksum-pinned public CDISC example data set."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
from pathlib import Path


CATALOG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "cdisc"
    / "sdr"
    / "examples"
    / "catalog.json"
)


def load_example(example_id: str) -> dict:
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    for example in catalog["examples"]:
        if example["id"] == example_id:
            return example
    choices = ", ".join(item["id"] for item in catalog["examples"])
    raise ValueError(f"unknown example {example_id!r}; choose one of: {choices}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("example_id")
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    example = load_example(args.example_id)
    args.destination.mkdir(parents=True, exist_ok=True)
    for item in example["files"]:
        with urllib.request.urlopen(item["url"], timeout=60) as response:
            content = response.read()
        actual = hashlib.sha256(content).hexdigest()
        if actual != item["sha256"]:
            raise RuntimeError(f"checksum mismatch for {item['name']}: {actual}")
        target = args.destination / item["name"]
        target.write_bytes(content)
        print(f"{item['name']}\t{len(content)}\tsha256:{actual}")


if __name__ == "__main__":
    main()
