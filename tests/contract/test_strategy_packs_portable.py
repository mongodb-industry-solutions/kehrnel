import json
import importlib
from pathlib import Path

from kehrnel.api.app import create_app, validate_strategy_pack


def test_strategy_packs_are_portable_and_self_contained():
    app = create_app()
    diagnostics = app.state.strategy_diagnostics
    valid_diags = [d for d in diagnostics if d.get("is_valid")]
    assert valid_diags, "expected at least one valid strategy pack"
    for diag in valid_diags:
        manifest_path = Path(diag["paths"]["manifest_path"])
        base_dir = manifest_path.parent
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        errors = validate_strategy_pack(manifest_data, base_dir)
        assert not errors, f"strategy pack failed validation: {errors}"
        entrypoint = manifest_data.get("entrypoint")
        mod_path = entrypoint.split(":")[0] if entrypoint else None
        if mod_path:
            importlib.import_module(mod_path)
