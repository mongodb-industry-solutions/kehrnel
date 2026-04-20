import os
import subprocess
import sys
from pathlib import Path


def test_api_help_does_not_trigger_app_side_effects():
    repo_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONWARNINGS"] = "default"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from kehrnel.api.app import main; main(['--help'])",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert "Run kehrnel API server." in result.stdout
    assert "Documentation mounted at /guide" not in result.stderr
    assert "FastAPIDeprecationWarning" not in result.stderr
