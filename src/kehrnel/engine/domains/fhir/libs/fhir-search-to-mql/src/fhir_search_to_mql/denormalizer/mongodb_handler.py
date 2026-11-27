"""
MongoDB handler for FHIR resource denormalization.

Provides utilities for reading, writing, and processing FHIR resources
from MongoDB collections. The handler is intentionally agnostic of the
:class:`ResourceDenormalizer` — callers pass in a ``processor``
callable so the same routines work for any denormalization strategy.

Top-level denormalized buckets
-------------------------------
Two top-level keys are written by the denormalizer and are jointly
managed by this handler:

* ``_search`` — flat, indexable projections of FHIR fields used by the
  query converter (``familyName_lower``, ``codeSystem_code``, …).
* ``_compartments`` — precomputed compartment membership (e.g.
  ``_compartments.Patient = ["patient-123", ...]``) used by the
  Patient-compartment fast path. See
  :class:`fhir_search_to_mql.compartments.compartment_resolver.CompartmentResolver`.

Both buckets MUST be kept in sync — historically only ``_search`` was
written/cleared, which silently broke compartment fast-path queries
after a re-denormalization. All bulk mutators below now write/unset
``_compartments`` symmetrically.
"""

import inspect
import sys
from typing import Any, Dict, List, Optional, Callable, Tuple
from fhir_search_to_mql.core.exceptions import DenormalizationError


def _log(msg: str) -> None:
    """
    Emit a progress / warning message to stderr.

    Bulk operations historically printed progress on stdout, which
    silently corrupted JSON output when the same Python process also
    emits structured data on stdout (e.g. the ``fhir-mql`` CLI). All
    informational logging is routed to stderr so stdout stays clean.
    """
    print(msg, file=sys.stderr)


def _processor_supports_warnings(processor: Callable) -> bool:
    """
    Detect whether ``processor`` accepts a ``warnings=`` kwarg.

    The bundled :class:`ResourceDenormalizer.denormalize` does, but
    ``processor`` is intentionally typed as a generic callable so
    host applications can pass their own transforms. We probe the
    signature once per bulk run and pass the collector only when
    supported — preserving backward compatibility with processors
    that take a single positional ``resource`` argument.
    """
    try:
        sig = inspect.signature(processor)
    except (ValueError, TypeError):
        return False
    for param in sig.parameters.values():
        if param.name == "warnings":
            return True
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            return True
    return False


def _format_completion_line(total: int, stats: Dict[str, int]) -> str:
    """
    Render the bulk-operation completion line for stderr.

    Always shows the per-document tally; appends a parenthetical
    summary of per-field failures only when there were any. This
    keeps the happy-path output unchanged while making partial
    success visible in the same line operators were already
    watching.
    """
    base = (
        f"Completed: {stats['processed']}/{total} documents processed, "
        f"{stats['updated']} updated, {stats['failed']} failed"
    )
    if stats.get("field_failures"):
        base += (
            f" ({stats['field_failures']} field-level warnings on "
            f"{stats['documents_with_field_failures']} document(s))"
        )
    return base


def _make_stats() -> Dict[str, int]:
    """
    Build the canonical stats dict for bulk-mutation methods.

    Tracking distinction:

    * ``processed`` / ``updated`` / ``failed`` — per-DOCUMENT
      counters. ``failed`` increments when the processor raised.
    * ``field_failures`` — total per-FIELD rule failures across
      every document (e.g. an extractor typo that silently broke
      one rule on every doc would surface here as
      ``field_failures == processed``).
    * ``documents_with_field_failures`` — count of DOCUMENTS with
      at least one per-field failure. This is the metric to alert
      on when triaging a noisy log; ``field_failures`` alone can
      look alarming due to multiplication.
    """
    return {
        "processed": 0,
        "updated": 0,
        "failed": 0,
        "field_failures": 0,
        "documents_with_field_failures": 0,
    }


def _denormalized_bucket_set(processed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the ``$set`` payload for the denormalized buckets.

    Always includes ``_search`` (defaulting to ``{}``). Includes
    ``_compartments`` only when the processor produced one — that
    keeps the field optional (Observation/Appointment configs can
    legitimately produce no compartments) without ever writing
    ``None``.
    """
    payload: Dict[str, Any] = {'_search': processed.get('_search', {})}
    compartments = processed.get('_compartments')
    if compartments is not None:
        payload['_compartments'] = compartments
    return payload


class MongoDBHandler:
    """Handle MongoDB operations for FHIR resources."""
    
    @staticmethod
    def read_resources(
        collection,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Read FHIR resources from a MongoDB collection.
        
        Args:
            collection: pymongo collection object
            query: MongoDB query filter (default: {} - all documents)
            projection: Fields to include/exclude (default: None - all fields)
            limit: Maximum number of documents to return
            
        Returns:
            List of FHIR resources
            
        Raises:
            DenormalizationError: If MongoDB operation fails
        """
        if query is None:
            query = {}
        
        try:
            cursor = collection.find(query, projection)
            
            if limit:
                cursor = cursor.limit(limit)
            
            return list(cursor)
            
        except Exception as e:
            raise DenormalizationError(f"MongoDB read failed: {str(e)}")
    
    @staticmethod
    def write_resources(
        collection,
        resources: List[Dict[str, Any]],
        ordered: bool = False
    ) -> int:
        """
        Write FHIR resources to a MongoDB collection.
        
        Args:
            collection: pymongo collection object
            resources: List of FHIR resources to insert
            ordered: If True, stop on first error; if False, continue (default: False)
            
        Returns:
            Number of documents inserted
            
        Raises:
            DenormalizationError: If MongoDB operation fails
        """
        if not resources:
            return 0
        
        try:
            result = collection.insert_many(resources, ordered=ordered)
            return len(result.inserted_ids)
            
        except Exception as e:
            raise DenormalizationError(f"MongoDB write failed: {str(e)}")
    
    @staticmethod
    def update_search_fields(
        collection,
        query: Optional[Dict[str, Any]] = None,
        processor: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Update _search fields for resources in a MongoDB collection.
        
        Args:
            collection: pymongo collection object
            query: MongoDB query filter (default: {} - all documents)
            processor: Function to process each resource and return _search fields
            batch_size: Number of documents to process per batch
            
        Returns:
            Dictionary with statistics (processed, updated, failed)
            
        Raises:
            DenormalizationError: If MongoDB operation fails
        """
        if query is None:
            query = {}
        
        if not processor:
            raise DenormalizationError("processor function is required")

        try:
            stats = _make_stats()
            supports_warnings = _processor_supports_warnings(processor)

            total = collection.count_documents(query)

            cursor = collection.find(query).batch_size(batch_size)

            for resource in cursor:
                try:
                    # Per-document warnings collector. The processor
                    # appends a string for every per-FIELD rule that
                    # failed; we tally these into the bulk stats so
                    # the final report is honest about partial
                    # success (the historical "0 failed" message
                    # masked silently broken rules).
                    doc_warnings: List[str] = []
                    if supports_warnings:
                        processed = processor(resource, warnings=doc_warnings)
                    else:
                        processed = processor(resource)

                    # Update in database. We MUST also persist
                    # _compartments so the Patient compartment
                    # fast-path stays consistent after a re-run.
                    result = collection.update_one(
                        {'_id': resource['_id']},
                        {'$set': _denormalized_bucket_set(processed)}
                    )

                    stats['processed'] += 1
                    if result.modified_count > 0:
                        stats['updated'] += 1
                    if doc_warnings:
                        stats['field_failures'] += len(doc_warnings)
                        stats['documents_with_field_failures'] += 1

                    if stats['processed'] % batch_size == 0:
                        _log(f"Progress: {stats['processed']}/{total} documents processed")

                except Exception as e:
                    stats['failed'] += 1
                    _log(f"Warning: Failed to process document {resource.get('_id')}: {str(e)}")
                    continue

            _log(_format_completion_line(total, stats))

            return stats

        except Exception as e:
            raise DenormalizationError(f"MongoDB update failed: {str(e)}")
    
    @staticmethod
    def batch_process(
        collection,
        query: Optional[Dict[str, Any]] = None,
        processor: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        batch_size: int = 100,
        update_in_place: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Process resources in batches.
        
        Args:
            collection: pymongo collection object
            query: MongoDB query filter (default: {} - all documents)
            processor: Function to process each resource
            batch_size: Number of documents to process per batch
            update_in_place: If True, update documents in DB; if False, return processed docs
            
        Returns:
            List of processed resources (empty if update_in_place=True)
            
        Raises:
            DenormalizationError: If MongoDB operation fails
        """
        if query is None:
            query = {}
        
        if not processor:
            raise DenormalizationError("processor function is required")

        try:
            results = [] if not update_in_place else None
            stats = _make_stats()
            supports_warnings = _processor_supports_warnings(processor)
            total = collection.count_documents(query)

            cursor = collection.find(query).batch_size(batch_size)

            for resource in cursor:
                try:
                    doc_warnings: List[str] = []
                    if supports_warnings:
                        processed_resource = processor(
                            resource, warnings=doc_warnings
                        )
                    else:
                        processed_resource = processor(resource)

                    if update_in_place:
                        # Update in database — persist BOTH _search
                        # and _compartments (when present) so the
                        # compartment fast-path stays in sync.
                        collection.update_one(
                            {'_id': resource['_id']},
                            {'$set': _denormalized_bucket_set(processed_resource)}
                        )
                        stats['updated'] += 1
                    else:
                        results.append(processed_resource)

                    stats['processed'] += 1
                    if doc_warnings:
                        stats['field_failures'] += len(doc_warnings)
                        stats['documents_with_field_failures'] += 1

                    if stats['processed'] % batch_size == 0:
                        _log(
                            f"Progress: {stats['processed']}/{total} "
                            f"documents processed"
                        )

                except Exception as e:
                    stats['failed'] += 1
                    _log(
                        f"Warning: Failed to process document "
                        f"{resource.get('_id')}: {str(e)}"
                    )
                    continue

            _log(_format_completion_line(total, stats))

            return results if not update_in_place else []

        except Exception as e:
            raise DenormalizationError(f"MongoDB batch processing failed: {str(e)}")
    
    @staticmethod
    def get_collection_stats(collection, resource_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about a MongoDB collection.
        
        Args:
            collection: pymongo collection object
            resource_type: Optional filter by resource type
            
        Returns:
            Dictionary with statistics (total_count, with_search, without_search, resource_types)
        """
        try:
            query = {}
            if resource_type:
                query['resourceType'] = resource_type
            
            total_count = collection.count_documents(query)
            
            # Count documents with _search field
            search_query = query.copy()
            search_query['_search'] = {'$exists': True}
            with_search = collection.count_documents(search_query)
            
            without_search = total_count - with_search
            
            # Get resource type counts
            resource_types = {}
            if not resource_type:
                pipeline = [
                    {'$group': {'_id': '$resourceType', 'count': {'$sum': 1}}}
                ]
                for doc in collection.aggregate(pipeline):
                    resource_types[doc['_id']] = doc['count']
            else:
                resource_types[resource_type] = total_count
            
            return {
                'total_count': total_count,
                'with_search': with_search,
                'without_search': without_search,
                'resource_types': resource_types
            }
            
        except Exception as e:
            return {
                'total_count': 0,
                'with_search': 0,
                'without_search': 0,
                'resource_types': {},
                'error': str(e)
            }
    
    @staticmethod
    def ensure_indexes(collection, indexes: List[Dict[str, Any]]) -> List[str]:
        """
        Ensure indexes exist on a collection.

        Accepts the three index-spec ``fields`` shapes that appear in
        the bundled YAML configs and elsewhere:

        1. A single-field shorthand string: ``fields: "birthDate"``.
        2. A mapping (single or compound):
           ``fields: {birthDate: 1, gender: 1}``.
        3. A list of single-key mappings (preserves declaration
           order, the canonical YAML form used by all bundled configs):
           ``fields: [{"_search.familyName_lower": 1}, {"birthDate": 1}]``.

        Args:
            collection: pymongo collection object.
            indexes: List of index specifications from a resource's
                YAML config (the ``indexes:`` block).

        Returns:
            List of created index names (one per spec).

        Raises:
            DenormalizationError: If index creation fails.
        """
        try:
            created = []

            for index_spec in indexes:
                fields = index_spec.get('fields', {})
                options = dict(index_spec.get('options') or {})

                index_fields: Any
                if isinstance(fields, str):
                    # Shorthand: a single field name. pymongo accepts
                    # this as-is, no tuple-conversion needed.
                    index_fields = fields
                elif isinstance(fields, dict):
                    index_fields = [(k, v) for k, v in fields.items()]
                elif isinstance(fields, list):
                    # Could be a list of dicts (canonical YAML form),
                    # a list of (field, direction) tuples, or a list
                    # of bare field-name strings. Normalize them all
                    # to (field, direction) tuples so pymongo gets a
                    # consistent shape.
                    normalized: List[Any] = []
                    for entry in fields:
                        if isinstance(entry, dict):
                            normalized.extend(
                                (k, v) for k, v in entry.items()
                            )
                        elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                            normalized.append(tuple(entry))
                        elif isinstance(entry, str):
                            normalized.append((entry, 1))
                        else:
                            raise DenormalizationError(
                                f"Unsupported index field entry: {entry!r}"
                            )
                    index_fields = normalized
                else:
                    raise DenormalizationError(
                        f"Unsupported 'fields' shape: {type(fields).__name__}"
                    )

                index_name = collection.create_index(index_fields, **options)
                created.append(index_name)

            return created

        except DenormalizationError:
            raise
        except Exception as e:
            raise DenormalizationError(f"Index creation failed: {str(e)}")
    
    @staticmethod
    def remove_search_fields(
        collection,
        query: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Remove ``_search`` AND ``_compartments`` fields from resources.

        Both buckets are written together by the denormalizer, so they
        are also cleared together — leaving stale ``_compartments``
        data after a "reset" would silently corrupt subsequent
        compartment fast-path queries.

        Args:
            collection: pymongo collection object.
            query: MongoDB query filter (default: ``{}`` — all
                documents). The handler narrows it to documents that
                have at least one of the two buckets to avoid no-op
                writes.

        Returns:
            Number of documents modified.

        Raises:
            DenormalizationError: If the MongoDB operation fails.
        """
        if query is None:
            query = {}

        try:
            # Narrow to docs that have either bucket — touching
            # everything else would just inflate write traffic.
            scoped: Dict[str, Any] = dict(query)
            scoped['$or'] = [
                {'_search': {'$exists': True}},
                {'_compartments': {'$exists': True}},
            ]

            result = collection.update_many(
                scoped,
                {'$unset': {'_search': '', '_compartments': ''}},
            )

            return result.modified_count

        except Exception as e:
            raise DenormalizationError(f"MongoDB operation failed: {str(e)}")
    
    @staticmethod
    def copy_collection(
        source_collection,
        target_collection,
        query: Optional[Dict[str, Any]] = None,
        processor: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        batch_size: int = 1000
    ) -> int:
        """
        Copy documents from one collection to another with optional processing.
        
        Args:
            source_collection: Source pymongo collection object
            target_collection: Target pymongo collection object
            query: MongoDB query filter (default: {} - all documents)
            processor: Optional function to process each document
            batch_size: Number of documents to process per batch
            
        Returns:
            Number of documents copied
            
        Raises:
            DenormalizationError: If operation fails
        """
        if query is None:
            query = {}
        
        try:
            copied = 0
            batch = []
            
            cursor = source_collection.find(query).batch_size(batch_size)
            
            for doc in cursor:
                # Remove _id to avoid duplicate key errors
                doc_copy = doc.copy()
                if '_id' in doc_copy:
                    del doc_copy['_id']
                
                # Process if processor provided
                if processor:
                    doc_copy = processor(doc_copy)
                
                batch.append(doc_copy)
                
                # Insert batch when it reaches batch_size
                if len(batch) >= batch_size:
                    target_collection.insert_many(batch, ordered=False)
                    copied += len(batch)
                    batch = []
                    _log(f"Copied: {copied} documents")
            
            # Insert remaining documents
            if batch:
                target_collection.insert_many(batch, ordered=False)
                copied += len(batch)
            
            _log(f"Completed: {copied} documents copied")
            return copied
            
        except Exception as e:
            raise DenormalizationError(f"Collection copy failed: {str(e)}")
