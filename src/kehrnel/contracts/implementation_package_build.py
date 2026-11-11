"""Request contract for deterministic implementation-package creation."""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, model_validator

from .architecture_foundation import (
    ArchitectureResolutionRequest,
    DeliveryLevel,
    RuntimeDependency,
)
from .artifacts import ArtifactRef, ContractModel


class ImplementationPackageBuildRequest(ContractModel):
    """Build one immutable package from an exact architecture resolution input."""

    contract: Literal["implementation-package-build-request/v1"]
    resolution_request: ArchitectureResolutionRequest
    expected_resolved_plan_ref: ArtifactRef
    delivery_level: DeliveryLevel = DeliveryLevel.CUSTOMER_EVALUATION
    lifecycle_owner: Optional[str] = Field(default=None, min_length=1)
    runtime_dependencies: List[RuntimeDependency] = Field(default_factory=list)

    @model_validator(mode="after")
    def request_is_coherent(self) -> "ImplementationPackageBuildRequest":
        if self.expected_resolved_plan_ref.kind != "resolved-architecture-plan":
            raise ValueError(
                "expectedResolvedPlanRef.kind must be 'resolved-architecture-plan'"
            )
        if (
            self.delivery_level is DeliveryLevel.PRODUCTION_IMPLEMENTATION
            and not self.lifecycle_owner
        ):
            raise ValueError(
                "production implementation package requests require lifecycleOwner"
            )
        dependency_ids = [item.dependency_id for item in self.runtime_dependencies]
        if len(dependency_ids) != len(set(dependency_ids)):
            raise ValueError("runtime dependencyId values must be unique")
        return self


__all__ = ["ImplementationPackageBuildRequest"]
