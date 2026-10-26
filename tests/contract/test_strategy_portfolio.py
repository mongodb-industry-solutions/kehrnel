from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.strategy_portfolio import (
    StrategyDeliveryTrack,
    StrategyPortfolio,
)
from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy_manifest,
)
from kehrnel.engine.core.strategy_portfolio import (
    StrategyPortfolioError,
    assert_portfolio_covers,
    load_strategy_portfolio,
)


STRATEGY_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
)


def _discovered_strategy_ids() -> set[str]:
    return {
        load_strategy_manifest(path).id
        for path in iter_strategy_manifest_paths(STRATEGY_ROOT)
    }


def test_portfolio_partitions_every_discoverable_strategy():
    portfolio = load_strategy_portfolio()
    discovered = _discovered_strategy_ids()

    assert_portfolio_covers(portfolio, discovered)
    assert portfolio.release_blocking_ids == (
        "openehr.rps_dual",
        "openehr.rps_dual_ibm",
        "fhir.resource_store",
        "cdisc.sdr",
    )
    assert portfolio.preview_ids == (
        "contextobjects.rps",
        "genomics.variant_first",
        "snomedct.mongodb",
        "x12.co_single",
    )


def test_portfolio_rejects_missing_or_unknown_strategies():
    portfolio = load_strategy_portfolio()

    with pytest.raises(StrategyPortfolioError, match="missing=.*fhir.resource_store"):
        assert_portfolio_covers(
            portfolio,
            (_discovered_strategy_ids() - {"fhir.resource_store"})
            | {"unknown.strategy"},
        )


def test_portfolio_contract_rejects_non_blocking_supported_strategy():
    with pytest.raises(ValueError, match="releaseBlocking"):
        StrategyPortfolio.model_validate(
            {
                "contract": "strategy-portfolio/v1",
                "entries": [
                    {
                        "strategyId": "openehr.rps_dual",
                        "track": StrategyDeliveryTrack.PRIMARY.value,
                        "priority": 10,
                        "releaseBlocking": False,
                        "statement": "invalid",
                        "catalog": {
                            "role": "target_strategy",
                            "provenance": "mongodb_curated",
                            "paths": [
                                {
                                    "industry": "healthcare",
                                    "segments": ["data-standards", "openehr"],
                                }
                            ],
                        },
                    }
                ],
            }
        )


def test_portfolio_is_exposed_through_versioned_api(tmp_path):
    response = TestClient(create_app(str(tmp_path / "registry.json"))).get(
        "/v1/strategy-portfolio"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["contract"] == "strategy-portfolio/v1"
    by_id = {entry["strategyId"]: entry for entry in payload["entries"]}
    assert by_id["openehr.rps_dual"]["track"] == "primary"
    assert by_id["openehr.rps_dual"]["releaseBlocking"] is True
    assert by_id["contextobjects.rps"]["track"] == "preview"
    assert by_id["contextobjects.rps"]["releaseBlocking"] is False
