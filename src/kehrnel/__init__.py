"""kehrnel package root."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("kehrnel-core")
except PackageNotFoundError:
    __version__ = "0.0.0+local"
