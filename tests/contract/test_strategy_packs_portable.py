import json
import importlib
from pathlib import Path

from kehrnel.api.app import create_app, validate_strategy_pack


def test_strategy_packs_are_portable_and_self_contained(tmp_path):
    app = create_app(str(tmp_path / "registry.json"))
    diagnostics = app.state.strategy_diagnostics
    builtin_root = (Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies").resolve()
    builtin_diags = [
        item
        for item in diagnostics
        if Path(item["paths"]["manifest_path"]).resolve().is_relative_to(builtin_root)
    ]
    invalid_diags = [item for item in builtin_diags if not item.get("is_valid")]
    assert invalid_diags == []
    valid_diags = [item for item in builtin_diags if item.get("is_valid")]
    assert len(valid_diags) == 8
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
