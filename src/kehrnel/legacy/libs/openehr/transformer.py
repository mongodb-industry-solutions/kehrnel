from __future__ import annotations

from kehrnel.legacy.transform.core import Transformer, load_default_cfg


def get_transformer(cfg_path=None, *, role: str = "primary") -> Transformer:
    """
    Build a Transformer using the default config (or an override path).
    """
    cfg = load_default_cfg(cfg_path)
    return Transformer(cfg=cfg, role=role)
