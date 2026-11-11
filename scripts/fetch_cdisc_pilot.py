"""Fetch checksum-pinned public CDISC Pilot DM/Define artifacts for integration testing.

The upstream terms require attribution and prohibit misleading use or alteration.
This script downloads the original files without modification. See:
https://github.com/cdisc-org/sdtm-adam-pilot-project/blob/master/README.md
"""

from __future__ import annotations

import argparse
import hashlib
import urllib.request
from pathlib import Path


BASE = "https://raw.githubusercontent.com/cdisc-org/sdtm-adam-pilot-project/master/updated-pilot-submission-package/900172/m5/datasets/cdiscpilot01/tabulations/sdtm"
FILES = {
    "dm.json": "785e2ccbdfec91c8a5c667b74b9f01ae99952fb8679d9c56053437916c404cf2",
    "dm.xpt": "7327baea97fd532d02385248da0c7240402e770099507e2c3a88e2ac706c02a6",
    "define.xml": "fbb065b8bb72d609e1a12670940e0d2ef11e6757cbbcd56bf9d0ba55ce1fc76b",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    args.destination.mkdir(parents=True, exist_ok=True)
    for name, expected in FILES.items():
        with urllib.request.urlopen(f"{BASE}/{name}", timeout=30) as response:
            content = response.read()
        actual = hashlib.sha256(content).hexdigest()
        if actual != expected:
            raise RuntimeError(f"checksum mismatch for {name}: {actual}")
        (args.destination / name).write_bytes(content)
        print(f"{name}\t{len(content)}\tsha256:{actual}")


if __name__ == "__main__":
    main()
