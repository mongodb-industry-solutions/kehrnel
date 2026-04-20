import uuid
import logging
from copy import deepcopy
from datetime import datetime, timezone
from dateutil.parser import isoparse
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional, Dict, Any, List
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.ingest.unflattener import CompositionUnflattener
from kehrnel.engine.strategies.openehr.rps_dual.ingest.exceptions_g import FlattenerError
from kehrnel.api.bridge.app.core.config_models import CompositionCollectionNames

from kehrnel.api.domains.openehr.composition.repository import (
    find_composition_by_uid,
    find_compositions_by_uids,
    find_flattened_composition_by_uid,
    find_latest_composition_by_object_id,
    find_first_composition_by_object_id,
    insert_composition_contribution_and_update_ehr,
    insert_compositions_contributions_and_update_ehr,
    add_deletion_contribution_and_update_ehr,
    add_bulk_deletion_contributions_and_update_ehr,
)

from kehrnel.api.common.models import (
    RevisionHistory,
    RevisionHistoryItem,
    OriginalVersionResponse,
    ObjectVersionID,
    HierObjectID,
    ObjectRef,
    DvDateTime
)
from kehrnel.api.domains.openehr.composition.models import (
    BulkCompositionCreateFailure,
    BulkCompositionCreateResult,
    BulkCompositionCreateSuccess,
    BulkCompositionDeleteFailure,
    BulkCompositionDeleteResult,
    Composition,
    CompositionCreate,
    CompositionSummary,
    VersionedComposition,
)

from kehrnel.api.domains.openehr.ehr.service import retrieve_ehr_by_id
from kehrnel.api.domains.openehr.ehr.repository import find_ehr_by_id

from kehrnel.api.domains.openehr.contribution.repository import (
    find_deletion_contribution_for_version, 
    find_deletion_contributions_for_versions,
    find_latest_contribution_by_vo_uid,
    find_contributions_for_versioned_object
)

from kehrnel.api.bridge.app.core.models import Contribution, AuditDetails

logger = logging.getLogger(__name__)


def _materialize_versioned_composition_payload(
    payload: Dict[str, Any],
    version_uid: str,
) -> Dict[str, Any]:
    """
    Return a canonical composition payload whose in-body uid matches the
    server-assigned version UID used for persistence and versioning.
    """
    composition = deepcopy(payload or {})
    composition["_type"] = composition.get("_type") or "COMPOSITION"
    composition["uid"] = {
        "_type": "OBJECT_VERSION_ID",
        "value": version_uid,
    }
    return composition


async def _maybe_flush_generated_codes(flattener: CompositionFlattener) -> None:
    flush_codes = getattr(flattener, "flush_codes_to_db", None)
    if not callable(flush_codes):
        return
    maybe_coro = flush_codes()
    if hasattr(maybe_coro, "__await__"):
        await maybe_coro


def _extract_composition_name(payload: Dict[str, Any]) -> str:
    return str((((payload or {}).get("name") or {}).get("value")) or "").strip()


async def _prepare_new_composition_artifacts(
    ehr_id: str,
    composition_create: CompositionCreate,
    flattener: CompositionFlattener,
    composition_uid: str,
    composition_version: str,
    time_committed: datetime,
    committer_name: str,
    change_type: str = "creation",
) -> Dict[str, Any]:
    composition_data = _materialize_versioned_composition_payload(
        composition_create.content,
        composition_uid,
    )

    new_composition_for_db = Composition(
        uid=composition_uid,
        time_created=time_committed,
        data=composition_data,
    )

    raw_doc_for_flattener = {
        "_id": new_composition_for_db.uid,
        "ehr_id": ehr_id,
        "composition_version": composition_version,
        "time_committed": time_committed,
        "time_created": time_committed,
        "canonicalJSON": new_composition_for_db.data,
    }

    try:
        flattened_base_doc, flattened_search_doc = flattener.transform_composition(raw_doc_for_flattener)
    except FlattenerError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Composition could not be processed: {exc}",
        ) from exc

    contribution = Contribution(
        ehr_id=ehr_id,
        audit=AuditDetails(
            system_id="my-openehr-server",
            committer_name=committer_name,
            time_committed=time_committed,
            change_type=change_type,
        ),
        versions=[{
            "_type": "COMPOSITION",
            "uid": {"value": composition_uid, "_type": "OBJECT_VERSION_ID"},
            "template_id": composition_create.template_id,
        }],
    )

    return {
        "composition": new_composition_for_db,
        "contribution": contribution,
        "flattened_base_doc": flattened_base_doc,
        "flattened_search_doc": flattened_search_doc,
    }


async def add_composition(
    ehr_id: str,
    composition_create: CompositionCreate,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    flattener: CompositionFlattener,
    merge_search_docs: bool = False,
    committer_name: str = "System"
) -> Composition:
    """
    Handles the business logic of adding a new, client-provided canonical
    Composition to an existing EHR, and also creating its flattened version.

    It involves:

    1. Validating that the target EHR exists.
    2. Assigning a system-managed version UID to the composition.
    3. Creating a Contribution to audit the change.
    4. Preparing and transforming the composition into a flattened format.
    5. Calling the repository to perform an atomic update of all related documents.

    Args:
        ehr_id: The ID of the EHR to which the composition will be added.
        composition_create: The data for the new composition from the request.
        db: The database session
        flattener: The initialized CompositionFlattener instance.
        committer_name: The name of the committer for the audit trail.

    Returns:
        The newly created and persisted Composition object

    Raises:
        HTTPException: 404 if EHR not found, 422 on transformation error, 500 on database error.
    """

    # Validate that the EHR Exists
    ehr = await find_ehr_by_id(ehr_id, db)
    if not ehr:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = f"EHR with id '{ehr_id}' not found."
        )
    
    time_created = datetime.now(timezone.utc)
    composition_object_id = str(uuid.uuid4())
    composition_uid = f"{composition_object_id}::my-openehr-server::1"
    artifacts = await _prepare_new_composition_artifacts(
        ehr_id=ehr_id,
        composition_create=composition_create,
        flattener=flattener,
        composition_uid=composition_uid,
        composition_version="1",
        time_committed=time_created,
        committer_name=committer_name,
    )
    new_composition_for_db = artifacts["composition"]
    contribution = artifacts["contribution"]
    flattened_base_doc = artifacts["flattened_base_doc"]
    flattened_search_doc = artifacts["flattened_search_doc"]

    # Pass the repository for atomic insertion and update
    try:
        await insert_composition_contribution_and_update_ehr(
            ehr_id = ehr_id,
            composition_doc = new_composition_for_db.model_dump(by_alias = True),
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db,
            config = config,
            flattened_base_doc = flattened_base_doc,
            flattened_search_doc = flattened_search_doc,
            merge_search_docs=merge_search_docs
        )
    except PyMongoError as e:
        # The repository re-raises the error, we catch it here to give a user-friendly response
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not create Composition due to a database error: {e}"
        )

    await _maybe_flush_generated_codes(flattener)
    
    # Return the created composition object
    return new_composition_for_db


async def bulk_add_compositions(
    ehr_id: str,
    items: List[Any],
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    flattener: CompositionFlattener,
    merge_search_docs: bool = False,
    committer_name: str = "System",
) -> BulkCompositionCreateResult:
    """
    Sandbox helper that batches composition creation without changing the
    canonical single-composition create endpoint.
    """
    requested_items = list(items or [])
    if not requested_items:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one composition is required.",
        )

    ehr = await find_ehr_by_id(ehr_id, db)
    if not ehr:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found.",
        )

    time_committed = datetime.now(timezone.utc)
    composition_docs: List[Dict[str, Any]] = []
    contribution_docs: List[Dict[str, Any]] = []
    flattened_base_docs: List[Dict[str, Any]] = []
    flattened_search_docs: List[Dict[str, Any]] = []
    created: List[BulkCompositionCreateSuccess] = []
    failed: List[BulkCompositionCreateFailure] = []

    for index, item in enumerate(requested_items):
        raw_composition = item.composition if hasattr(item, "composition") else (item or {}).get("composition")
        try:
            composition_create = CompositionCreate.model_validate(raw_composition)
            composition_object_id = str(uuid.uuid4())
            composition_uid = f"{composition_object_id}::my-openehr-server::1"
            artifacts = await _prepare_new_composition_artifacts(
                ehr_id=ehr_id,
                composition_create=composition_create,
                flattener=flattener,
                composition_uid=composition_uid,
                composition_version="1",
                time_committed=time_committed,
                committer_name=committer_name,
            )
        except HTTPException as exc:
            failed.append(BulkCompositionCreateFailure(
                index=index,
                message=str(exc.detail),
            ))
            continue
        except Exception as exc:
            failed.append(BulkCompositionCreateFailure(
                index=index,
                message=str(exc),
            ))
            continue

        new_composition = artifacts["composition"]
        composition_docs.append(new_composition.model_dump(by_alias=True))
        contribution_docs.append(artifacts["contribution"].model_dump(by_alias=True))
        flattened_base_docs.append(artifacts["flattened_base_doc"])
        flattened_search_docs.append(artifacts["flattened_search_doc"])
        created.append(BulkCompositionCreateSuccess(
            index=index,
            uid=new_composition.uid,
            name=_extract_composition_name(new_composition.data),
            templateId=composition_create.template_id,
        ))

    if composition_docs:
        try:
            await insert_compositions_contributions_and_update_ehr(
                ehr_id=ehr_id,
                composition_docs=composition_docs,
                contribution_docs=contribution_docs,
                db=db,
                config=config,
                flattened_base_docs=flattened_base_docs,
                flattened_search_docs=flattened_search_docs,
                merge_search_docs=merge_search_docs,
            )
        except PyMongoError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not create Compositions due to a database error: {exc}",
            ) from exc

        await _maybe_flush_generated_codes(flattener)

    return BulkCompositionCreateResult(
        ehr_id=ehr_id,
        createdCount=len(created),
        created=created,
        failed=failed,
        committedAt=time_committed if created else None,
    )


async def retrieve_composition(
    ehr_id: str,
    uid_based_id: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames
) -> Composition:
    """
    Retrieves a version of a Composition based on a UID.

    This function handles two cases as per openEHR spec:
    1. If `uid_based_id` is a full version UID (e.g., "id::server::1"), it fetches that specific version.
    2. If `uid_based_id` is a base object UID (e.g., "id"), it fetches the LATEST version of that composition.

    It also validates that the composition belongs to the specified EHR.

    Args:
        ehr_id: The ID of the parent EHR.
        uid_based_id: The unique ID of the composition (can be version-specific or not).
        db: The database session.

    Returns:
        The validated Composition Pydantic model.

    Raises:
        HTTPException: 404 if the EHR or Composition is not found.
    """

    # Validate that the EHR exists
    ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not ehr_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found"
        )
    
    composition_doc = None
    # Case 1: A specific version is requested (contains '::')
    if "::" in uid_based_id:
        composition_doc = await find_composition_by_uid(uid_based_id, db, config)
    else:
        # Latest version of a base object is requested
        composition_doc = await find_latest_composition_by_object_id(uid_based_id, db, config)

    if not composition_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Composition with id '{uid_based_id}' not found in EHR '{ehr_id}'"
        )
    
    # Extract the base object UID from the found document's full version UID (_id)
    try:
        versioned_object_uid_from_doc = composition_doc["_id"].split("::")[0]
    except IndexError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Malformed composition UID found in the database."
        )

    # Ensure the found composition is actually linked to the specified EHR.
    composition_refs = ehr_doc.get("compositions", [])

    is_linked = any(
        ref.get("id", {}).get("value", "").startswith(versioned_object_uid_from_doc)
        for ref in composition_refs
    )

    if not is_linked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Composition with id '{uid_based_id}' not found in EHR '{ehr_id}'"
        )
    
    return Composition.model_validate(composition_doc)


async def list_composition_summaries(
    ehr_id: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
) -> List[CompositionSummary]:
    """
    Return lightweight composition summaries for the current EHR view.

    This intentionally avoids unflattening and avoids the generic AQL pipeline.
    We read the EHR's current composition references and project only the
    summary fields needed by the sandbox list.
    """
    ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not ehr_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"EHR with id '{ehr_id}' not found",
        )

    ordered_uids: List[str] = []
    seen: set[str] = set()
    for ref in ehr_doc.get("compositions", []) or []:
        uid = (((ref or {}).get("id") or {}).get("value") or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        ordered_uids.append(uid)

    if not ordered_uids:
        return []

    projection = {
        "_id": 1,
        "data.name.value": 1,
        "data.archetype_details.template_id.value": 1,
    }
    docs = await db[config.compositions].find(
        {"_id": {"$in": ordered_uids}},
        projection,
    ).to_list(length=len(ordered_uids))

    by_uid: Dict[str, Dict[str, Any]] = {str(doc.get("_id")): doc for doc in docs if doc.get("_id")}
    summaries: List[CompositionSummary] = []
    for uid in ordered_uids:
        doc = by_uid.get(uid)
        if not doc:
            continue
        data = doc.get("data") or {}
        name = ((data.get("name") or {}).get("value") or "").strip()
        template_id = (
            (((data.get("archetype_details") or {}).get("template_id") or {}).get("value") or "").strip()
        )
        summaries.append(
            CompositionSummary(
                uid=uid,
                name=name,
                templateId=template_id,
            )
        )

    return summaries


async def retrieve_and_unflatten_composition(
    ehr_id: str,
    uid_based_id: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    unflattener: CompositionUnflattener
) -> Composition:
    """
    Retrieves a flattened composition, reconstructs it into canonical JSON,
    and returns it. This service replicates the logic of `retrieve_composition`
    but uses the flattened document as the source of truth for the content.

    Args:
        ehr_id: The ID of the parent EHR.
        uid_based_id: The unique ID of the composition (version-specific or latest).
        db: The database session.
        unflattener: The CompositionUnflattener instance for un-flattening.

    Returns:
        A Composition Pydantic model containing the reconstructed data.

    Raises:
        HTTPException: 404 if resources are not found, 500 for internal errors.
    """
    
    # 1. Validate that the EHR Exists
    ehr_doc = await find_ehr_by_id(ehr_id, db)
    if not ehr_doc:
        raise HTTPException(status_code=404, detail=f"EHR with id '{ehr_id}' not found")

    # 2. Determine the specific version UID to fetch
    canonical_doc = None
    if "::" in uid_based_id:
        # A specific version is requested, we need to fetch its metadata
        canonical_doc = await find_composition_by_uid(uid_based_id, db, config)
    else:
        # The latest version is requested, find it from the canonical collection
        canonical_doc = await find_latest_composition_by_object_id(uid_based_id, db, config)

    if not canonical_doc:
        raise HTTPException(
            status_code=404,
            detail=f"Composition with id '{uid_based_id}' not found in EHR '{ehr_id}'"
        )

    composition_version_uid = canonical_doc["_id"]

    # 3. Security Check: Ensure the composition belongs to the specified EHR
    try:
        versioned_object_uid = composition_version_uid.split("::")[0]
    except IndexError:
        raise HTTPException(status_code=500, detail="Malformed UID in database.")

    if not any(
        ref.get("id", {}).get("value", "").startswith(versioned_object_uid)
        for ref in ehr_doc.get("compositions", [])
    ):
        raise HTTPException(
            status_code=404,
            detail=f"Composition '{uid_based_id}' not found in EHR '{ehr_id}'"
        )
    
    # 4. Fetch the flattened document using the determined version UID
    flattened_doc = await find_flattened_composition_by_uid(composition_version_uid, db, config)

    if not flattened_doc:
        # This case indicates data inconsistency, which should be rare with transactions
        raise HTTPException(
            status_code=500,
            detail=f"Inconsistent data: Canonical composition '{composition_version_uid}' exists but its flattened version is missing."
        )
    
    # 5. Un-flatten the document to reconstruct the canonical JSON
    try:
        reconstructed_data = unflattener.unflatten(flattened_doc)
    except Exception:
        logger.exception("Failed to reverse flattened composition uid=%s", composition_version_uid)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reconstruct composition '{composition_version_uid}'."
        )
    

    # 7. Return the data in the same Pydantic model as the original function
    return Composition(
        uid=composition_version_uid,
        time_created=canonical_doc["time_created"],
        data=reconstructed_data
    )


async def update_composition(
    ehr_id: str,
    preceding_version_uid: str,
    if_match: str,
    new_composition_data: CompositionCreate,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    flattener: CompositionFlattener,
    merge_search_docs: bool = False,
    committer_name: str = "System"
) -> Composition:
    """
    Updates a composition by creating a new version.

    Args:
        ehr_id: The ID of the parent EHR
        preceding_version_uid: The UID of the composition version to be replaced
        if_match: ETag for optimistic locking, must match preceding_version_uid
        new_composition_data: The new canonical composition data from the request
        db: The database session
        committer_name: The name of the committer for the audit trail

    Returns:
        The newly created composition object (the new version)
    """

    # Concurrency and consistency check
    expected_uid = if_match.strip('"')
    if expected_uid != preceding_version_uid:
        raise HTTPException(
            status_code = status.HTTP_400_BAD_REQUEST,
            detail = f"The If-Match header ('{expected_uid}') does not match the preceding_version_uid in the URL ('{preceding_version_uid}')."
        )
    
    # Fetch the composition being updated to ensure it exists
    existing_composition = await retrieve_composition(
        ehr_id = ehr_id,
        uid_based_id = preceding_version_uid,
        db = db,
        config = config
    )

    if not existing_composition:
        raise HTTPException(
            status_code = status.HTTP_404_NOT_FOUND,
            detail = "EHR with id '{ehr_id}' and composition not '{versioned_object_uid}' found"
        )

    # Create the new version UID
    try:
        object_id, system_id, version_str = preceding_version_uid.split('::')
        new_version = int(version_str) + 1
        new_uid = f"{object_id}::{system_id}::{new_version}"
    except (ValueError, IndexError):
        raise HTTPException(status_code=500, detail="Could not parse the existing version UID to create a new version.")
    
    # Prepare the new versioned objects
    time_committed = datetime.now(timezone.utc)

    # Create the new Composition object for the database
    normalized_composition_data = _materialize_versioned_composition_payload(
        new_composition_data.content,
        new_uid,
    )

    new_composition_for_db = Composition(
        uid = new_uid,
        time_created = time_committed,
        data = normalized_composition_data
    )

    raw_doc_for_flattener = {
        "_id": new_composition_for_db.uid,
        "ehr_id": ehr_id,
        "composition_version": str(new_version),
        "time_committed": time_committed,
        "time_created": time_committed,
        "canonicalJSON": new_composition_for_db.data
    }

    try:
        flattened_base_doc, flattened_search_doc = flattener.transform_composition(raw_doc_for_flattener)
    except FlattenerError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Composition could not be processed: {e}"
        )

    # Create the Contribution for this modification
    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = committer_name,
            time_committed = time_committed,
            change_type = "modification"
        ),
        versions = [{
            "_type": "COMPOSITION",
            "uid": {"value": new_uid, "_type": "OBJECT_VERSION_ID"},
            "template_id": new_composition_data.template_id
        }]
    )

    # Atomically inser the new documents and update the EHR. Reuse the same repository function to do so
    try:
        await insert_composition_contribution_and_update_ehr(
            ehr_id = ehr_id,
            composition_doc = new_composition_for_db.model_dump(by_alias = True),
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db,
            config = config,
            flattened_base_doc = flattened_base_doc,
            flattened_search_doc = flattened_search_doc,
            merge_search_docs = merge_search_docs,
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not update Composition due to a database error: {e}"
        )

    await _maybe_flush_generated_codes(flattener)
    
    return new_composition_for_db


async def delete_composition_by_preceding_uid(
    ehr_id: str,
    preceding_version_uid: str,
    if_match: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    committer_name: str = "System"
) -> Dict[str, Any]:
    """
    Handles the logical deletion of a composition version.

    This doesn't delete the record. Instead it creates a new "deleted" contribution that points to the version being deleted

    Args:
        ehr_id: The ID of the parent EHR.
        preceding_version_uid: The UID of the composition version to be "deleted".
        if_match: The ETag for optimistic locking, must match preceding_version_uid.
        db: The database session
        committer_name: The name of the committer for the audit trail

    Returns:
        A dictionary containing the UID of the new deletion audit entry and its creation time
    """
    # Cncurrency and consistency check
    expected_uid = if_match.strip('"')
    if expected_uid != preceding_version_uid:
        raise HTTPException(
            status_code = status.HTTP_412_PRECONDITION_FAILED,
            detail = f"The If-Match header ('{expected_uid}') doesn't match the preceding_version_uid in the URL ('{preceding_version_uid}')"
        )
    
    # Fetch the EHR and composition to ensure they exist and are linked
    ehr = await retrieve_ehr_by_id(ehr_id, db)

    # This function already raises 404 if the composition is not found or not linked to the EHR.
    composition_to_delete = await retrieve_composition(
        ehr_id = ehr_id,
        uid_based_id = preceding_version_uid,
        db = db,
        config = config
    )

    # Verify this version hans't already been deleted
    existing_deletion = await find_deletion_contribution_for_version(preceding_version_uid, db)
    if existing_deletion:
        raise HTTPException(
            status_code = status.HTTP_409_CONFLICT,
            detail = f"Version '{preceding_version_uid}' has already been deleted."
        )
    
    # Create the new version UID for the audit entry
    try:
        object_id, system_id, version_str = preceding_version_uid.split('::')
        new_version = int(version_str) + 1
        new_audit_uid = f"{object_id}::{system_id}::{new_version}"
    except (ValueError, IndexError):
        raise HTTPException(
            status_code = 500,
            detail = "Could not parse the existing version UID to create a new version for the deletion audit."
        )
    
    # Prepare the new "deleted" contribution
    time_committed = datetime.now(timezone.utc)

    # The 'versions' field in the contribution audit now records the deletion
    # It points to the version that was deleted
    audit_version_data = {
        "_type": "DELETED",
        "uid": {"value": new_audit_uid, "_type": "OBJECT_VERSION_ID"},
        "preceding_version_uid": preceding_version_uid
    }

    contribution = Contribution(
        ehr_id = ehr_id,
        audit = AuditDetails(
            system_id = "my-openehr-server",
            committer_name = committer_name,
            time_committed = time_committed,
            change_type = "deleted"
        ),
        versions = [audit_version_data]
    )

    # Pass to the repository fo atomic update
    try:
        await add_deletion_contribution_and_update_ehr(
            ehr_id = ehr_id,
            preceding_version_uid = preceding_version_uid,
            contribution_doc = contribution.model_dump(by_alias = True),
            db = db,
            config = config
        )
    except PyMongoError as e:
        raise HTTPException(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail = f"Could not delete Composition due to a database error: {e}"
        )
    
    # Return data needed for response headers
    return {
        "new_audit_uid": new_audit_uid,
        "time_committed": time_committed,
        "versioned_object_locator": f"{object_id}::{system_id}"
    }


async def bulk_delete_compositions(
    ehr_id: str,
    preceding_version_uids: List[str],
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    committer_name: str = "System"
) -> BulkCompositionDeleteResult:
    """
    Sandbox helper that batches logical composition deletions without changing
    the canonical single-composition delete endpoint.
    """
    requested_uids: List[str] = []
    seen: set[str] = set()
    for uid in preceding_version_uids or []:
        value = str(uid or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        requested_uids.append(value)

    if not requested_uids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one composition uid is required."
        )

    ehr = await retrieve_ehr_by_id(ehr_id, db)
    linked_uids = {
        getattr(getattr(ref, "id", None), "value", None)
        for ref in (ehr.compositions or [])
        if getattr(getattr(ref, "id", None), "value", None)
    }

    failures: List[BulkCompositionDeleteFailure] = []
    linked_requested_uids: List[str] = []
    for uid in requested_uids:
        if uid not in linked_uids:
            failures.append(BulkCompositionDeleteFailure(
                uid=uid,
                message=f"Composition with id '{uid}' not found in EHR '{ehr_id}'"
            ))
            continue
        linked_requested_uids.append(uid)

    composition_docs = await find_compositions_by_uids(linked_requested_uids, db, config)
    found_uids = {str(doc.get("_id")) for doc in composition_docs if doc.get("_id")}

    existing_deletions = await find_deletion_contributions_for_versions(linked_requested_uids, db)
    deleted_uids = {
        version.get("preceding_version_uid")
        for doc in existing_deletions
        for version in (doc.get("versions") or [])
        if version.get("preceding_version_uid")
    }

    valid_uids: List[str] = []
    for uid in linked_requested_uids:
        if uid not in found_uids:
            failures.append(BulkCompositionDeleteFailure(
                uid=uid,
                message=f"Composition with id '{uid}' was not found in the composition store."
            ))
            continue
        if uid in deleted_uids:
            failures.append(BulkCompositionDeleteFailure(
                uid=uid,
                message=f"Version '{uid}' has already been deleted."
            ))
            continue
        valid_uids.append(uid)

    contribution_docs: List[Dict[str, Any]] = []
    audit_uids: List[str] = []
    executable_uids: List[str] = []
    time_committed = datetime.now(timezone.utc)

    for uid in valid_uids:
        try:
            object_id, system_id, version_str = uid.split("::")
            new_version = int(version_str) + 1
            new_audit_uid = f"{object_id}::{system_id}::{new_version}"
        except (ValueError, IndexError):
            failures.append(BulkCompositionDeleteFailure(
                uid=uid,
                message="Could not parse the existing version UID to create a deletion audit."
            ))
            continue

        audit_version_data = {
            "_type": "DELETED",
            "uid": {"value": new_audit_uid, "_type": "OBJECT_VERSION_ID"},
            "preceding_version_uid": uid
        }

        contribution = Contribution(
            ehr_id=ehr_id,
            audit=AuditDetails(
                system_id="my-openehr-server",
                committer_name=committer_name,
                time_committed=time_committed,
                change_type="deleted"
            ),
            versions=[audit_version_data]
        )
        contribution_docs.append(contribution.model_dump(by_alias=True))
        audit_uids.append(new_audit_uid)
        executable_uids.append(uid)

    if executable_uids:
        try:
            await add_bulk_deletion_contributions_and_update_ehr(
                ehr_id=ehr_id,
                preceding_version_uids=executable_uids,
                contribution_docs=contribution_docs,
                db=db,
                config=config,
            )
        except PyMongoError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not delete compositions due to a database error: {e}"
            )

    return BulkCompositionDeleteResult(
        ehr_id=ehr_id,
        deletedCount=len(executable_uids),
        deletedUids=executable_uids,
        auditUids=audit_uids,
        failed=failures,
        committedAt=time_committed if executable_uids else None,
    )


async def retrieve_revision_history(
    ehr_id: str,
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames
) -> RevisionHistory:
    """
    Retrieves the revision history for a versioned composition.

    Args:
        ehr_id: The ID of the parent EHR
        versioned_object_uid: The base ID of the composition
        db: The database session

    Returns:
        A RevisionHistory object containing all audit entries for the composition

    Raises:
        HTTPException 404 if the EHR or composition is not found
    """

    # Validate that the EHR exists and contains the composition to prevent data leakage
    # Reuse the logic from retrieve_composition by fetching the latest version

    await retrieve_composition(ehr_id=ehr_id, uid_based_id=versioned_object_uid, db=db, config=config)

    # Fetch all the relevant contribution from the repository
    contribution_docs = await find_contributions_for_versioned_object(versioned_object_uid, db)

    if not contribution_docs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No revision history found for composition '{versioned_object_uid}' in EHR '{ehr_id}'"
        )
    
    # Map the contribution data to the RevisionHistoryItem model
    history_items = []
    for contrib_doc in contribution_docs:
        # Find the specific version entry within the contribution that matches the composition
        matching_version = next(
            (v for v in contrib_doc.get("versions", []) 
             if v.get("uid", {}).get("value", "").startswith(versioned_object_uid)),
            None
        )

        if matching_version:
            item = RevisionHistoryItem(
                versionId=ObjectVersionID.model_validate(matching_version["uid"]),
                audit=AuditDetails.model_validate(contrib_doc["audit"])
            )
            history_items.append(item)
    return RevisionHistory(items=history_items)


async def retrieve_versioned_composition(
    ehr_id: str, 
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames
) -> VersionedComposition:
    """
    Retrieves metadata about a VERSIONED_COMPOSITION

    This function validates that the composition belongs to the EHR, 
    then finds the creation time of its version to construc the response

    Args:
        ehr_id: The ID of the parent EHR
        versioned_object_uid: The base ID of the composition
        db: The database session

    Returns:
        A VersionedComposition object

    Raises:
        HTTPException 404 if the EHR or Composition is not found.
    """

    # Validate that the composition exists within this EHR
    # Reuse the retrieve_composition, which already performs the check
    # If the check fails, it will raise a 404, which is the correct behavior

    await retrieve_composition(ehr_id=ehr_id, uid_based_id=versioned_object_uid, db=db, config=config)

    # Fetch the first version of the composition to get its creation time
    first_composition_doc = await find_first_composition_by_object_id(versioned_object_uid, db, config)

    if not first_composition_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Composition with id '{versioned_object_uid}' not found in EHR '{ehr_id}'"
        )
    
    # Construct the VersionedComposition response object
    versioned_composition_response = VersionedComposition(
        uid=HierObjectID(value=versioned_object_uid),
        ownerId=ObjectRef(
            id=HierObjectID(value=ehr_id),
            type="EHR"
        ),
        timeCreated=DvDateTime(value=first_composition_doc["time_created"])
    )

    return versioned_composition_response


async def retrieve_composition_version(
    ehr_id: str,
    versioned_object_uid: str,
    db: AsyncIOMotorDatabase,
    config: CompositionCollectionNames,
    version_at_time: Optional[str] = None
) -> OriginalVersionResponse:
    """
    Retrieves a specific version of a composition.

    If version_at_time is provided, it finds the version extant at that time.
    Otherwise, it finds the latest version.

    Args:
        ehr_id: The ID of the parent EHR
        versioned_object_uid: The base ID of the composition

    Returns:
        An OriginalVersionResponse object containing the version data and audit

    Raises:
        HTTPException: If the resource or version is not found, or if the timestamp is invalid
    """

    # Validate that the EHR exists and contains this versioned composition.
    # It will raise 404 if not found, which is the correct behavior

    await retrieve_composition(ehr_id=ehr_id, uid_based_id=versioned_object_uid, db=db, config=config)

    at_time_datetime: Optional[datetime] = None
    if version_at_time:
        try:
            at_time_datetime = isoparse(version_at_time)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid 'version_at_time' format: {version_at_time}"
            )

    contribution_doc = await find_latest_contribution_by_vo_uid(
        versioned_object_uid=versioned_object_uid,
        db=db,
        timestamp=at_time_datetime
    )

    if not contribution_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No version of composition '{versioned_object_uid}' found at the specified time."
        )
    
    
    # If the found contribution is a deletion marker, no version was extant.
    if contribution_doc["audit"]["change_type"] == "deleted":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Composition '{versioned_object_uid}' was deleted at or before the specified time."
        )
    
    # Extract composition version info from the contribution's 'versions' array.
    version_info = next(
        (v for v in contribution_doc.get("versions", [])
         if v.get("uid", {}).get("value", "").startswith(versioned_object_uid)),
        None
    )
    
    if not version_info:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Inconsistent data: Contribution found but version link is missing."
        )
    
    composition_version_uid = version_info["uid"]["value"]
    preceding_uid_val = version_info.get("preceding_version_uid")

    # Fetch the actual composition data document.
    composition_doc = await find_composition_by_uid(composition_version_uid, db, config)
    if not composition_doc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Inconsistent data: Version referenced in contribution not found."
        )
    
    response = OriginalVersionResponse(
        uid=ObjectVersionID.model_validate(version_info["uid"]),
        preceding_version_uid=ObjectVersionID(value=preceding_uid_val) if preceding_uid_val else None,
        data=composition_doc["data"],
        commit_audit=AuditDetails.model_validate(contribution_doc["audit"]),
        contribution=ObjectRef(
            id=HierObjectID(value=contribution_doc["_id"]),
            type="CONTRIBUTION"
        )
    )

    return response
