"""Product-level delivery policy for the strategy portfolio.

This contract is intentionally separate from strategy manifests. A strategy
describes what the package can do; the portfolio declares which packages must
block a Kehrnel release and which are discovery-only previews.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Literal

from pydantic import Field, model_validator

from .artifacts import CatalogMetadata, ContractModel


class StrategyDeliveryTrack(str, Enum):
    PRIMARY = "primary"
    SUPPORTED = "supported"
    PREVIEW = "preview"


class StrategyPortfolioEntry(ContractModel):
    strategy_id: str = Field(min_length=1, pattern=r"^\S+$")
    track: StrategyDeliveryTrack
    priority: int = Field(ge=1)
    release_blocking: bool
    statement: str = Field(min_length=1)
    catalog: CatalogMetadata

    @model_validator(mode="after")
    def release_gate_matches_track(self) -> "StrategyPortfolioEntry":
        expected = self.track in {
            StrategyDeliveryTrack.PRIMARY,
            StrategyDeliveryTrack.SUPPORTED,
        }
        if self.release_blocking != expected:
            raise ValueError(
                "releaseBlocking must be true for primary/supported strategies "
                "and false for preview strategies"
            )
        return self


class StrategyPortfolio(ContractModel):
    contract: Literal["strategy-portfolio/v1"]
    entries: List[StrategyPortfolioEntry] = Field(min_length=1)

    @model_validator(mode="after")
    def entries_are_unique(self) -> "StrategyPortfolio":
        strategy_ids = [entry.strategy_id for entry in self.entries]
        if len(strategy_ids) != len(set(strategy_ids)):
            raise ValueError("strategy portfolio contains duplicate strategyId values")
        priorities = [entry.priority for entry in self.entries]
        if len(priorities) != len(set(priorities)):
            raise ValueError("strategy portfolio contains duplicate priorities")
        return self

    def entry_for(self, strategy_id: str) -> StrategyPortfolioEntry | None:
        return next(
            (entry for entry in self.entries if entry.strategy_id == strategy_id),
            None,
        )

    @property
    def release_blocking_ids(self) -> tuple[str, ...]:
        return tuple(
            entry.strategy_id
            for entry in sorted(self.entries, key=lambda item: item.priority)
            if entry.release_blocking
        )

    @property
    def preview_ids(self) -> tuple[str, ...]:
        return tuple(
            entry.strategy_id
            for entry in sorted(self.entries, key=lambda item: item.priority)
            if entry.track is StrategyDeliveryTrack.PREVIEW
        )
