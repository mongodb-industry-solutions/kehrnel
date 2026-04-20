from typer.testing import CliRunner

import kehrnel.cli.unified as unified


runner = CliRunner()


def test_env_list_cli_renders_response(monkeypatch):
    monkeypatch.setattr(unified, "_http_json", lambda method, url, api_key=None, payload=None: (200, {"environments": [{"env_id": "env-cli"}]}))

    result = runner.invoke(unified.app, ["core", "env", "list", "--runtime-url", "http://localhost:8080"])

    assert result.exit_code == 0
    assert '"env_id": "env-cli"' in result.stdout
