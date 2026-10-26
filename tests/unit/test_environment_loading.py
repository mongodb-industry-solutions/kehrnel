from __future__ import annotations

import importlib


app_module = importlib.import_module("kehrnel.api.app")
cli_module = importlib.import_module("kehrnel.cli.unified")
bridge_config = importlib.import_module("kehrnel.api.bridge.app.core.config")


def test_local_environment_loading_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("KEHRNEL_SKIP_DOTENV", "true")

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("dotenv discovery must be disabled in an isolated process")

    monkeypatch.setattr(app_module, "find_dotenv", fail_if_called)
    monkeypatch.setattr(app_module, "load_dotenv", fail_if_called)

    app_module._load_local_environment()


def test_local_environment_loading_remains_available_for_local_development(monkeypatch) -> None:
    monkeypatch.delenv("KEHRNEL_SKIP_DOTENV", raising=False)
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        app_module,
        "find_dotenv",
        lambda filename, *, usecwd: "/tmp/example.env.local",
    )

    def record_load(path: str, *, override: bool) -> None:
        observed.update(path=path, override=override)

    monkeypatch.setattr(app_module, "load_dotenv", record_load)

    app_module._load_local_environment()

    assert observed == {"path": "/tmp/example.env.local", "override": False}


def test_cli_dotenv_loading_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("KEHRNEL_SKIP_DOTENV", "true")

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("CLI dotenv discovery must be disabled in an isolated process")

    monkeypatch.setattr(cli_module, "find_dotenv", fail_if_called)
    monkeypatch.setattr(cli_module, "load_dotenv", fail_if_called)

    cli_module._load_local_environment()


def test_bridge_settings_disable_env_file_in_test_process() -> None:
    assert bridge_config.Settings.model_config.get("env_file") is None
