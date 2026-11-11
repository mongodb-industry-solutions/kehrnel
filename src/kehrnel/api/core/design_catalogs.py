"""Request-local exposure of integrity-checked installed design metadata.

The loader stays a pure library API. Only this boundary applies installation
policy; profile route hints never establish package authority or readiness.
"""
import os
from types import MappingProxyType

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from kehrnel.api.common.delegated_principal import request_authority
from kehrnel.contracts.source_architecture import SourceProfileCatalog
from kehrnel.contracts.architecture_foundation import ArchitectureResolutionInputCatalog
from kehrnel.engine.core.architecture_catalog import (
    ArchitectureInputCatalogError, load_architecture_resolution_context,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.source_profiles import (
    SourceProfileCatalogError, load_source_profile_catalog,
)
from kehrnel.engine.core.strategy_access import PackageRef

router = APIRouter()
PRIVATE_HEADERS = {
    "Cache-Control": "private, no-store",
    "Vary": "X-API-Key, X-HDL-Principal-Assertion",
}

# Reviewed full profile pin -> required installed package identity. This is
# server-owned qualification metadata, independent of public strategyIds hints.
PROFILE_PACKAGE_REQUIREMENTS = MappingProxyType({
    ('source-profile', 'exchange.openehr-json.rps-dual-design', '0.1.0', 'sha256:0f114ef59dc24eb964cf7eb862645eae53cdea44fa6f93c5e2ba12781eaf8061'): (PackageRef(
        "openehr.rps_dual", "0.2.0",
        "sha256:aafd9228abf73a516bb7d72f5c38ef510e981916067977359723e079e5580233",
    ),),
})


def authorized_source_profile_catalog(catalog, *, registry, policy, authority):
    """Filter whole profiles against current exact identities and read policy."""
    profiles = []
    for profile in catalog.profiles:
        ref = profile.artifact_ref
        required = PROFILE_PACKAGE_REQUIREMENTS.get((ref.kind, ref.id, ref.version, ref.digest))
        if required and all(policy.allowed(registry, package, authority, "strategy:read") for package in required):
            profiles.append(profile)
    if not profiles:
        raise KehrnelError("SOURCE_PROFILE_CATALOG_UNAVAILABLE", 503, "Source profile catalog unavailable")
    return SourceProfileCatalog(contract="source-profile-catalog/v1", profiles=profiles)


@router.get("/source-profiles", response_model=SourceProfileCatalog, response_model_exclude_none=True)
async def source_profiles(request: Request, response: Response):
    """Expose eligible design profiles without compilation or activation."""
    response.headers.update(PRIVATE_HEADERS)
    try:
        catalog = load_source_profile_catalog()
        return authorized_source_profile_catalog(
            catalog, registry=request.app.state.strategy_runtime.registry,
            policy=request.app.state.strategy_access_policy, authority=request_authority(request),
        )
    except (SourceProfileCatalogError, KehrnelError):
        # Never expose filesystem paths, private IDs, profile counts or policy causes.
        return JSONResponse(status_code=503, headers=PRIVATE_HEADERS, content={
            "error": {"code": "SOURCE_PROFILE_CATALOG_UNAVAILABLE", "message": "Source profile catalog unavailable", "details": {}},
        })


def authorized_architecture_input_catalog(records, *, registry, policy, authority):
    """Project complete eligible owner records; publication never shares them."""
    targets, bindings = {}, {}
    for record in records:
        if record.owner != authority.owner or "strategy:read" not in authority.actions:
            continue
        required = set(record.required_package_refs)
        associations = [PROFILE_PACKAGE_REQUIREMENTS.get((r.kind, r.id, r.version, r.digest)) for r in record.source_profile_refs]
        if not required <= authority.refs or not all(association and set(association) <= required for association in associations):
            continue
        if not all(policy.allowed(registry, package, authority, "strategy:read") for package in required):
            continue
        for target in record.inputs.target_capability_profiles:
            targets[target.artifact_ref] = target
        for binding in record.inputs.semantic_execution_bindings:
            bindings[binding.artifact_ref] = binding
    if not targets:
        raise ArchitectureInputCatalogError("Architecture inputs unavailable")
    return ArchitectureResolutionInputCatalog(
        contract="architecture-resolution-input-catalog/v1",
        target_capability_profiles=list(targets.values()),
        semantic_execution_bindings=list(bindings.values()),
    )


@router.get("/architecture-resolution-inputs", response_model=ArchitectureResolutionInputCatalog, response_model_exclude_none=True)
async def architecture_resolution_inputs(request: Request, response: Response):
    """Read operator design-reference inputs without compilation or activation."""
    response.headers.update(PRIVATE_HEADERS)
    try:
        runtime = request.app.state.strategy_runtime
        records = load_architecture_resolution_context(
            os.getenv("KEHRNEL_ARCHITECTURE_INPUTS_FILE"),
            forbidden_roots=(*request.app.state.architecture_input_package_roots,
                             *(m.pack_root for m in runtime.registry.manifests.values() if m.pack_root)),
        )
        return authorized_architecture_input_catalog(
            records, registry=runtime.registry, policy=request.app.state.strategy_access_policy,
            authority=request_authority(request),
        )
    except (ArchitectureInputCatalogError, KehrnelError):
        return JSONResponse(status_code=503, headers=PRIVATE_HEADERS, content={
            "error": {"code": "ARCHITECTURE_INPUT_CATALOG_UNAVAILABLE", "message": "Architecture inputs unavailable", "details": {}},
        })
