"""Load and validate Kehrnel's product-level strategy delivery policy."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from pydantic import ValidationError

from kehrnel.contracts.strategy_portfolio import StrategyPortfolio


DEFAULT_STRATEGY_PORTFOLIO_PATH = (
    Path(__file__).resolve().parents[1] / "strategies" / "portfolio.json"
)


class StrategyPortfolioError(ValueError):
    """The portfolio is malformed or inconsistent with installed strategies."""


def load_strategy_portfolio(
    path: str | Path = DEFAULT_STRATEGY_PORTFOLIO_PATH,
) -> StrategyPortfolio:
    portfolio_path = Path(path)
    try:
        payload = json.loads(portfolio_path.read_text(encoding="utf-8"))
        return StrategyPortfolio.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise StrategyPortfolioError(
            f"invalid strategy portfolio at {portfolio_path}: {exc}"
        ) from exc


def assert_portfolio_covers(
    portfolio: StrategyPortfolio,
    discovered_strategy_ids: Iterable[str],
) -> None:
    discovered = set(discovered_strategy_ids)
    declared = {entry.strategy_id for entry in portfolio.entries}
    missing = sorted(discovered - declared)
    unknown = sorted(declared - discovered)
    if missing or unknown:
        raise StrategyPortfolioError(
            "strategy portfolio must partition the discoverable strategy set; "
            f"missing={missing}, unknown={unknown}"
        )
