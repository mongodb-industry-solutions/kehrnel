"""Governed semantic-resolution orchestration shared by industry strategies."""

from .cohort_resolution import (
    CohortResolutionOrchestrator,
    ResolutionPolicy,
    ResolutionRegistry,
    StructuralResolver,
    TerminologyCandidateProvider,
)
from .terminology import GatewayTerminologyCandidateProvider, TerminologyGateway
from .known_strategies import (
    FhirConditionCohortStructuralResolver,
    OpenEhrOptCohortStructuralResolver,
    OpenEhrRoleBinding,
)

__all__ = [
    "CohortResolutionOrchestrator",
    "ResolutionPolicy",
    "ResolutionRegistry",
    "StructuralResolver",
    "TerminologyCandidateProvider",
    "GatewayTerminologyCandidateProvider",
    "TerminologyGateway",
    "FhirConditionCohortStructuralResolver",
    "OpenEhrOptCohortStructuralResolver",
    "OpenEhrRoleBinding",
]
