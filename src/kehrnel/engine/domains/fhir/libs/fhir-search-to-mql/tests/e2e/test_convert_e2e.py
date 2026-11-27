"""E2E convert commands from CLI_COMMANDS (no MongoDB)."""

from __future__ import annotations

import pytest

from fhir_search_to_mql import cli

from .cli_scenarios_mql import CONVERT_SMOKE

pytestmark = pytest.mark.e2e


class TestConvertSmoke:
    @pytest.mark.parametrize("step", CONVERT_SMOKE, ids=lambda s: s[0])
    def test_convert_exits_zero(self, step: tuple[str, str, tuple[str, ...]]) -> None:
        resource, query, extra = step
        rc = cli.main(["convert", resource, query, *extra])
        assert rc == cli.EXIT_OK
