"""Deterministic semantic catalog compilation for openEHR OPT 1.4 templates.

The catalog is the governed boundary between an OPT and any later natural
language or agentic query planner.  It deliberately contains no fuzzy matching
or model output: every field, label, binding, and path is derived from the OPT.

Two path forms are exposed for each field:

* ``composition_aql_path`` is relative to a COMPOSITION variable (for example
  ``/content[openEHR-EHR-SECTION.foo.v1]/items[at0001]``).
* ``absolute_aql_path`` prefixes that path with the root archetype predicate,
  matching the path form used by OPT annotations.

``archetype_relative_aql_path`` is additionally scoped to the field's owning
archetype.  This is important because node identifiers such as ``at0001`` are
only unique inside an archetype, not inside a template.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Iterator, Literal, Mapping, Optional, Sequence
from xml.etree import ElementTree as ET

from pydantic import Field, ValidationError, model_validator

from kehrnel.contracts.artifacts import ContractModel, Sha256Digest


CATALOG_CONTRACT = "openehr-opt-semantic-catalog/v1"
MAX_OPT_BYTES = 16 * 1024 * 1024
MAX_OPT_XML_ELEMENTS = 250_000
MAX_OPT_XML_DEPTH = 256
MAX_CATALOG_FIELDS = 50_000

_OPT_NS = "http://schemas.openehr.org/v1"
_XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
_NS = {"opt": _OPT_NS, "xsi": _XSI_NS}
_XSI_TYPE = f"{{{_XSI_NS}}}type"


class OptSemanticCatalogError(ValueError):
    """Raised when an OPT cannot be compiled into a trustworthy catalog."""


class Multiplicity(ContractModel):
    """A lower/upper interval; ``upper=None`` means unbounded."""

    lower: int = Field(ge=0)
    upper: Optional[int] = Field(default=None, ge=0)
    upper_unbounded: bool = False

    @model_validator(mode="after")
    def interval_is_valid(self) -> "Multiplicity":
        if self.upper_unbounded and self.upper is not None:
            raise ValueError("an unbounded interval cannot declare an upper value")
        if not self.upper_unbounded and self.upper is None:
            raise ValueError("a bounded interval must declare an upper value")
        if self.upper is not None and self.upper < self.lower:
            raise ValueError("upper must be greater than or equal to lower")
        return self

    @property
    def repeated(self) -> bool:
        return self.upper_unbounded or bool(self.upper and self.upper > 1)


class TermText(ContractModel):
    language: str = Field(min_length=1)
    text: str = Field(min_length=1)
    description: Optional[str] = None
    comment: Optional[str] = None


class TerminologyBinding(ContractModel):
    terminology: str = Field(min_length=1)
    code: str = Field(min_length=1)
    raw_code: str = Field(min_length=1)


class QueryPath(ContractModel):
    """A scalar facet below the field's ELEMENT path."""

    facet: str = Field(min_length=1)
    aql_path_suffix: str = Field(pattern=r"^/")


class CodingConstraint(ContractModel):
    """An allowed-code constraint on an ELEMENT value.

    This is intentionally separate from ``term_bindings``: a term binding says
    what an archetype node means, while this contract constrains which coded
    values may be stored in that node.
    """

    constraint_kind: Literal["terminology", "code_list", "value_set", "mixed"]
    terminology_id: Optional[str] = None
    allowed_codes: list[str] = Field(default_factory=list)
    value_set_uri: Optional[str] = None


class OrdinalOption(ContractModel):
    """One permitted ``DV_ORDINAL`` value and its coded symbol."""

    ordinal: int
    terminology_id: str = Field(min_length=1)
    code: str = Field(min_length=1)


class ValueVariant(ContractModel):
    rm_type: str = Field(min_length=1)
    aql_path_suffix: Literal["/value"] = "/value"
    query_paths: list[QueryPath]
    coding_constraints: list[CodingConstraint] = Field(default_factory=list)
    ordinal_options: list[OrdinalOption] = Field(default_factory=list)


class PathCardinality(ContractModel):
    """Multiplicity at one object segment of the AQL path."""

    composition_aql_path: str
    attribute_name: str = Field(min_length=1)
    node_occurrences: Multiplicity
    attribute_existence: Multiplicity
    attribute_cardinality: Optional[Multiplicity] = None
    ordered: Optional[bool] = None
    unique: Optional[bool] = None


class ArchetypeReference(ContractModel):
    scope_id: str = Field(pattern=r"^openehr-scope:sha256:[0-9a-f]{64}$")
    archetype_id: str = Field(min_length=1)
    rm_type: str = Field(min_length=1)
    node_id: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    absolute_aql_path: str = Field(min_length=1)
    composition_aql_path: str
    parent_scope_id: Optional[str] = None
    occurrences: Multiplicity


class FieldAnnotation(ContractModel):
    key: str = Field(min_length=1)
    value: str


class CorrelationAnchor(ContractModel):
    """Nearest container within which predicates must remain correlated."""

    rm_type: str = Field(min_length=1)
    node_id: Optional[str] = None
    archetype_id: Optional[str] = None
    absolute_aql_path: str = Field(min_length=1)
    composition_aql_path: str
    reason: Literal[
        "nearest_repeating_archetype",
        "nearest_repeating_event",
        "nearest_repeating_node",
        "containing_archetype",
    ]


class QueryableField(ContractModel):
    field_id: str = Field(pattern=r"^openehr-field:sha256:[0-9a-f]{64}$")
    semantic_key: str = Field(
        pattern=r"^openehr-semantic-key:sha256:[0-9a-f]{64}$"
    )
    node_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    description: Optional[str] = None
    comment: Optional[str] = None
    rm_type: Literal["ELEMENT"] = "ELEMENT"
    containing_archetype: ArchetypeReference
    absolute_aql_path: str = Field(min_length=1)
    composition_aql_path: str = Field(pattern=r"^/")
    archetype_relative_aql_path: str = Field(pattern=r"^/")
    value_variants: list[ValueVariant]
    term_bindings: list[TerminologyBinding]
    localized_terms: list[TermText]
    annotations: list[FieldAnnotation]
    occurrences: Multiplicity
    path_cardinalities: list[PathCardinality]
    containment: list[ArchetypeReference]
    correlation_anchor: CorrelationAnchor


class TemplateMetadata(ContractModel):
    template_id: str = Field(min_length=1)
    uid: Optional[str] = None
    concept: Optional[str] = None
    language: Optional[str] = None
    lifecycle_state: Optional[str] = None
    semantic_version: Optional[str] = None
    root_archetype_id: str = Field(min_length=1)
    source_digest: Sha256Digest


class OpenEhrOptSemanticCatalog(ContractModel):
    contract: Literal["openehr-opt-semantic-catalog/v1"] = CATALOG_CONTRACT
    template: TemplateMetadata
    archetypes: list[ArchetypeReference]
    fields: list[QueryableField]
    catalog_digest: Sha256Digest

    @model_validator(mode="after")
    def identities_and_order_are_stable(self) -> "OpenEhrOptSemanticCatalog":
        scope_ids = [item.scope_id for item in self.archetypes]
        if len(scope_ids) != len(set(scope_ids)):
            raise ValueError("archetype scope ids must be unique")
        field_ids = [item.field_id for item in self.fields]
        if len(field_ids) != len(set(field_ids)):
            raise ValueError("field ids must be unique")
        if self.archetypes != sorted(
            self.archetypes,
            key=lambda item: (item.absolute_aql_path, item.scope_id),
        ):
            raise ValueError("archetypes must be sorted by absolute AQL path")
        if self.fields != sorted(
            self.fields,
            key=lambda item: (item.absolute_aql_path, item.field_id),
        ):
            raise ValueError("fields must be sorted by absolute AQL path")
        known_scopes = set(scope_ids)
        if any(field.containing_archetype.scope_id not in known_scopes for field in self.fields):
            raise ValueError("each field must reference an archetype in the catalog")
        return self

    def verify_digest(self) -> bool:
        return self.catalog_digest == _catalog_digest(self)


@dataclass(frozen=True, slots=True)
class _Term:
    text: str
    description: Optional[str]
    comment: Optional[str]


@dataclass(frozen=True, slots=True)
class _ObjectFrame:
    rm_type: str
    node_id: Optional[str]
    archetype_id: Optional[str]
    absolute_aql_path: str
    composition_aql_path: str
    occurrences: Multiplicity
    parent_attribute_cardinality: Optional[Multiplicity]

    @property
    def repeated(self) -> bool:
        return self.occurrences.repeated or bool(
            self.parent_attribute_cardinality
            and self.parent_attribute_cardinality.repeated
        )


@dataclass(frozen=True, slots=True)
class _Scope:
    reference: ArchetypeReference
    terms: Mapping[str, _Term]
    bindings: Mapping[str, tuple[TerminologyBinding, ...]]
    composition_aql_path: str


class OptSemanticCatalogCompiler:
    """Compile one OPT document into a deterministic, versioned semantic view."""

    def __init__(self, xml_bytes: bytes):
        if not xml_bytes:
            raise OptSemanticCatalogError("OPT content is empty")
        if len(xml_bytes) > MAX_OPT_BYTES:
            raise OptSemanticCatalogError(
                f"OPT exceeds the {MAX_OPT_BYTES}-byte parsing limit"
            )
        # XML declarations may legitimately select UTF-16, where each ASCII
        # character is separated by a NUL byte.  Normalize those bytes before
        # scanning so a differently encoded DTD/entity declaration cannot
        # bypass the fail-closed parser policy.
        upper_xml = xml_bytes.replace(b"\x00", b"").upper()
        if b"<!DOCTYPE" in upper_xml or b"<!ENTITY" in upper_xml:
            raise OptSemanticCatalogError(
                "OPT XML document type and entity declarations are not allowed"
            )
        self._xml_bytes = xml_bytes
        self._source_digest = _sha256_digest(xml_bytes)
        try:
            self._root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            raise OptSemanticCatalogError(f"Invalid OPT XML: {exc}") from exc
        if _local_name(self._root.tag) != "template":
            raise OptSemanticCatalogError("OPT root element must be <template>")
        _validate_xml_shape(self._root)

        self._translations = _parse_translations(self._root)
        self._annotations = _parse_annotations(self._root)
        self._archetypes: list[ArchetypeReference] = []
        self._fields: list[QueryableField] = []

    @classmethod
    def from_path(cls, opt_path: Path | str) -> "OptSemanticCatalogCompiler":
        path = Path(opt_path)
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise OptSemanticCatalogError(f"Cannot read OPT '{path}': {exc}") from exc
        if size > MAX_OPT_BYTES:
            raise OptSemanticCatalogError(
                f"OPT exceeds the {MAX_OPT_BYTES}-byte parsing limit"
            )
        try:
            return cls(path.read_bytes())
        except OSError as exc:
            raise OptSemanticCatalogError(f"Cannot read OPT '{path}': {exc}") from exc

    @classmethod
    def from_xml(cls, xml: str | bytes) -> "OptSemanticCatalogCompiler":
        return cls(xml.encode("utf-8") if isinstance(xml, str) else bytes(xml))

    def compile(self) -> OpenEhrOptSemanticCatalog:
        # Compiler instances are deliberately reusable; never let a previous
        # compilation leak duplicate entries into the next artifact.
        self._archetypes.clear()
        self._fields.clear()
        template_id = _text(self._root, "opt:template_id/opt:value")
        definition = self._root.find("opt:definition", _NS)
        if definition is None:
            raise OptSemanticCatalogError("OPT does not contain a definition")
        root_archetype_id = _text(definition, "opt:archetype_id/opt:value")
        if not template_id:
            raise OptSemanticCatalogError("OPT does not declare template_id")
        if not root_archetype_id:
            raise OptSemanticCatalogError(
                "OPT definition does not declare a root archetype_id"
            )

        metadata = TemplateMetadata(
            template_id=template_id,
            uid=_none_if_empty(_text(self._root, "opt:uid/opt:value")),
            concept=_none_if_empty(_text(self._root, "opt:concept")),
            language=_none_if_empty(_text(self._root, "opt:language/opt:code_string")),
            lifecycle_state=_none_if_empty(
                _text(self._root, "opt:description/opt:lifecycle_state")
            ),
            semantic_version=_none_if_empty(
                _other_detail(self._root, "sem_ver")
            ),
            root_archetype_id=root_archetype_id,
            source_digest=self._source_digest,
        )

        root_occurrences = _parse_interval(
            definition.find("opt:occurrences", _NS), default_lower=1, default_upper=1
        )
        root_frame = _ObjectFrame(
            rm_type=_text(definition, "opt:rm_type_name") or "COMPOSITION",
            node_id=_none_if_empty(_text(definition, "opt:node_id")),
            archetype_id=root_archetype_id,
            absolute_aql_path=f"[{root_archetype_id}]",
            composition_aql_path="",
            occurrences=root_occurrences,
            parent_attribute_cardinality=None,
        )
        self._walk_object(
            definition,
            template_id=template_id,
            root_archetype_id=root_archetype_id,
            frame=root_frame,
            current_scope=None,
            containment=(),
            frames=(),
            path_cardinalities=(),
        )

        archetypes = sorted(
            self._archetypes,
            key=lambda item: (item.absolute_aql_path, item.scope_id),
        )
        fields = sorted(
            self._fields,
            key=lambda item: (item.absolute_aql_path, item.field_id),
        )
        draft = OpenEhrOptSemanticCatalog(
            template=metadata,
            archetypes=archetypes,
            fields=fields,
            catalog_digest="sha256:" + "0" * 64,
        )
        return draft.model_copy(update={"catalog_digest": _catalog_digest(draft)})

    def _walk_object(
        self,
        node: ET.Element,
        *,
        template_id: str,
        root_archetype_id: str,
        frame: _ObjectFrame,
        current_scope: Optional[_Scope],
        containment: tuple[ArchetypeReference, ...],
        frames: tuple[_ObjectFrame, ...],
        path_cardinalities: tuple[PathCardinality, ...],
    ) -> None:
        direct_archetype_id = _none_if_empty(
            _text(node, "opt:archetype_id/opt:value")
        )
        if direct_archetype_id:
            terms = _parse_direct_terms(node)
            bindings = _parse_direct_bindings(node)
            root_term = terms.get(frame.node_id or "") or terms.get("at0000")
            scope_id = _stable_id(
                "openehr-scope",
                template_id,
                direct_archetype_id,
                frame.absolute_aql_path,
            )
            reference = ArchetypeReference(
                scope_id=scope_id,
                archetype_id=direct_archetype_id,
                rm_type=frame.rm_type,
                node_id=frame.node_id,
                label=root_term.text if root_term else None,
                description=root_term.description if root_term else None,
                absolute_aql_path=frame.absolute_aql_path,
                composition_aql_path=frame.composition_aql_path,
                parent_scope_id=(
                    current_scope.reference.scope_id if current_scope else None
                ),
                occurrences=frame.occurrences,
            )
            self._archetypes.append(reference)
            current_scope = _Scope(
                reference=reference,
                terms=terms,
                bindings=bindings,
                composition_aql_path=frame.composition_aql_path,
            )
            containment = (*containment, reference)

        next_frames = (*frames, frame)

        if frame.rm_type == "ELEMENT" and current_scope is not None and frame.node_id:
            if len(self._fields) >= MAX_CATALOG_FIELDS:
                raise OptSemanticCatalogError(
                    "OPT exceeds the "
                    f"{MAX_CATALOG_FIELDS}-field semantic catalog limit"
                )
            term = current_scope.terms.get(frame.node_id)
            relative_path = _relative_aql_path(
                frame.composition_aql_path,
                current_scope.composition_aql_path,
            )
            value_variants = _value_variants(node)
            normalized_path = _normalize_opt_path(frame.absolute_aql_path)
            annotation_values = self._annotations.get(normalized_path)
            if annotation_values is None:
                annotation_values = self._annotations.get(
                    _strip_name_predicates(normalized_path), ()
                )
            self._fields.append(
                QueryableField(
                    field_id=_stable_id(
                        "openehr-field",
                        self._source_digest,
                        _semantic_key(
                            template_id=template_id,
                            archetype_id=current_scope.reference.archetype_id,
                            absolute_aql_path=frame.absolute_aql_path,
                            value_variants=value_variants,
                        ),
                    ),
                    semantic_key=_semantic_key(
                        template_id=template_id,
                        archetype_id=current_scope.reference.archetype_id,
                        absolute_aql_path=frame.absolute_aql_path,
                        value_variants=value_variants,
                    ),
                    node_id=frame.node_id,
                    label=term.text if term else frame.node_id,
                    description=term.description if term else None,
                    comment=term.comment if term else None,
                    containing_archetype=current_scope.reference,
                    absolute_aql_path=frame.absolute_aql_path,
                    composition_aql_path=frame.composition_aql_path,
                    archetype_relative_aql_path=relative_path,
                    value_variants=value_variants,
                    term_bindings=list(
                        current_scope.bindings.get(frame.node_id, ())
                    ),
                    localized_terms=list(
                        self._translations.get(
                            (current_scope.reference.archetype_id, frame.node_id),
                            (),
                        )
                    ),
                    annotations=list(annotation_values),
                    occurrences=frame.occurrences,
                    path_cardinalities=list(path_cardinalities),
                    containment=list(containment),
                    correlation_anchor=_correlation_anchor(
                        next_frames,
                        current_scope.reference,
                    ),
                )
            )

        for attribute in node.findall("opt:attributes", _NS):
            attribute_name = _text(attribute, "opt:rm_attribute_name")
            if not attribute_name:
                continue
            existence = _parse_interval(
                attribute.find("opt:existence", _NS),
                default_lower=0,
                default_upper=1,
            )
            cardinality_element = attribute.find("opt:cardinality", _NS)
            attribute_cardinality = None
            ordered: Optional[bool] = None
            unique: Optional[bool] = None
            if cardinality_element is not None:
                attribute_cardinality = _parse_interval(
                    cardinality_element.find("opt:interval", _NS),
                    default_lower=0,
                    default_upper=None,
                    default_unbounded=True,
                )
                ordered = _parse_bool(
                    _text(cardinality_element, "opt:is_ordered"), default=False
                )
                unique = _parse_bool(
                    _text(cardinality_element, "opt:is_unique"), default=False
                )

            children = attribute.findall("opt:children", _NS)
            predicates = [
                _none_if_empty(_text(child, "opt:archetype_id/opt:value"))
                or _none_if_empty(_text(child, "opt:node_id"))
                for child in children
            ]
            predicate_counts = Counter(
                predicate for predicate in predicates if predicate
            )
            duplicate_predicates = {
                predicate
                for predicate, count in predicate_counts.items()
                if count > 1
            }
            for child, predicate in zip(children, predicates):
                child_rm_type = _text(child, "opt:rm_type_name")
                if not child_rm_type:
                    continue
                child_node_id = _none_if_empty(_text(child, "opt:node_id"))
                child_archetype_id = _none_if_empty(
                    _text(child, "opt:archetype_id/opt:value")
                )
                segment = f"/{attribute_name}"
                if predicate:
                    qualifier = ""
                    if predicate in duplicate_predicates:
                        constrained_name = _fixed_name(child)
                        if not constrained_name:
                            raise OptSemanticCatalogError(
                                "OPT contains indistinguishable sibling constraints for "
                                f"'{attribute_name}[{predicate}]'"
                            )
                        escaped_name = constrained_name.replace("'", "''")
                        qualifier = f" and name/value='{escaped_name}'"
                    segment += f"[{predicate}{qualifier}]"
                composition_path = frame.composition_aql_path + segment
                absolute_path = f"[{root_archetype_id}]{composition_path}"
                child_occurrences = _parse_interval(
                    child.find("opt:occurrences", _NS),
                    default_lower=0,
                    default_upper=1,
                )
                child_frame = _ObjectFrame(
                    rm_type=child_rm_type,
                    node_id=child_node_id,
                    archetype_id=child_archetype_id or frame.archetype_id,
                    absolute_aql_path=absolute_path,
                    composition_aql_path=composition_path,
                    occurrences=child_occurrences,
                    parent_attribute_cardinality=attribute_cardinality,
                )
                multiplicity = PathCardinality(
                    composition_aql_path=composition_path,
                    attribute_name=attribute_name,
                    node_occurrences=child_occurrences,
                    attribute_existence=existence,
                    attribute_cardinality=attribute_cardinality,
                    ordered=ordered,
                    unique=unique,
                )
                self._walk_object(
                    child,
                    template_id=template_id,
                    root_archetype_id=root_archetype_id,
                    frame=child_frame,
                    current_scope=current_scope,
                    containment=containment,
                    frames=next_frames,
                    path_cardinalities=(*path_cardinalities, multiplicity),
                )


def _validate_xml_shape(root: ET.Element) -> None:
    """Reject resource-hostile XML before semantic extraction begins.

    ``ElementTree`` has already bounded the input by bytes at this point.  This
    iterative walk additionally caps structural amplification while using
    memory proportional to XML depth rather than document width.
    """

    element_count = 1
    stack: list[tuple[Iterator[ET.Element], int]] = [(iter(root), 1)]
    while stack:
        children, parent_depth = stack[-1]
        try:
            child = next(children)
        except StopIteration:
            stack.pop()
            continue

        element_count += 1
        if element_count > MAX_OPT_XML_ELEMENTS:
            raise OptSemanticCatalogError(
                "OPT exceeds the "
                f"{MAX_OPT_XML_ELEMENTS}-element XML traversal limit"
            )

        child_depth = parent_depth + 1
        if child_depth > MAX_OPT_XML_DEPTH:
            raise OptSemanticCatalogError(
                f"OPT exceeds the {MAX_OPT_XML_DEPTH}-level XML depth limit"
            )
        stack.append((iter(child), child_depth))


def compile_opt_semantic_catalog(
    opt_path: Path | str,
) -> OpenEhrOptSemanticCatalog:
    """Compile an OPT file without introducing any inferred semantics."""

    try:
        return OptSemanticCatalogCompiler.from_path(opt_path).compile()
    except OptSemanticCatalogError:
        raise
    except ValidationError as exc:
        raise OptSemanticCatalogError(
            f"OPT semantic constraints are invalid: {exc}"
        ) from exc


def compile_opt_semantic_catalog_xml(
    xml: str | bytes,
) -> OpenEhrOptSemanticCatalog:
    """Compile raw OPT XML without introducing any inferred semantics."""

    try:
        return OptSemanticCatalogCompiler.from_xml(xml).compile()
    except OptSemanticCatalogError:
        raise
    except ValidationError as exc:
        raise OptSemanticCatalogError(
            f"OPT semantic constraints are invalid: {exc}"
        ) from exc


def _parse_direct_terms(node: ET.Element) -> dict[str, _Term]:
    terms: dict[str, _Term] = {}
    for definition in node.findall("opt:term_definitions", _NS):
        code = (definition.get("code") or "").strip()
        if not code:
            continue
        values = _items_by_id(definition)
        text = values.get("text", "").strip()
        if not text:
            continue
        terms[code] = _Term(
            text=text,
            description=_none_if_empty(values.get("description", "")),
            comment=_none_if_empty(values.get("comment", "")),
        )
    return terms


def _parse_direct_bindings(
    node: ET.Element,
) -> dict[str, tuple[TerminologyBinding, ...]]:
    by_code: dict[str, list[TerminologyBinding]] = {}
    for group in node.findall("opt:term_bindings", _NS):
        group_terminology = (group.get("terminology") or "").strip()
        for item in group.findall("opt:items", _NS):
            local_code = (item.get("code") or "").strip()
            if not local_code:
                continue
            for value in item.findall("opt:value", _NS):
                terminology = (
                    _text(value, "opt:terminology_id/opt:value")
                    or group_terminology
                )
                raw_code = _text(value, "opt:code_string")
                if not terminology or not raw_code:
                    continue
                external_code = _external_code(raw_code)
                by_code.setdefault(local_code, []).append(
                    TerminologyBinding(
                        terminology=terminology,
                        code=external_code,
                        raw_code=raw_code,
                    )
                )
    return {
        code: tuple(
            sorted(
                {
                    (item.terminology, item.code, item.raw_code): item
                    for item in values
                }.values(),
                key=lambda item: (item.terminology, item.code, item.raw_code),
            )
        )
        for code, values in by_code.items()
    }


def _parse_translations(
    root: ET.Element,
) -> dict[tuple[str, str], tuple[TermText, ...]]:
    translated: dict[tuple[str, str], list[TermText]] = {}
    parents = [
        *root.findall("opt:ontology", _NS),
        *root.findall("opt:component_ontologies", _NS),
    ]
    for parent in parents:
        archetype_id = (parent.get("archetype_id") or "").strip()
        if not archetype_id:
            continue
        for language_group in parent.findall("opt:term_definitions", _NS):
            language = (language_group.get("language") or "").strip()
            if not language:
                continue
            for item in language_group.findall("opt:items", _NS):
                code = (item.get("code") or "").strip()
                values = _items_by_id(item)
                label = values.get("text", "").strip()
                if not code or not label:
                    continue
                translated.setdefault((archetype_id, code), []).append(
                    TermText(
                        language=language,
                        text=label,
                        description=_none_if_empty(values.get("description", "")),
                        comment=_none_if_empty(values.get("comment", "")),
                    )
                )
    return {
        key: tuple(
            sorted(
                {
                    (item.language, item.text, item.description, item.comment): item
                    for item in values
                }.values(),
                key=lambda item: (item.language, item.text),
            )
        )
        for key, values in translated.items()
    }


def _parse_annotations(
    root: ET.Element,
) -> dict[str, tuple[FieldAnnotation, ...]]:
    annotations: dict[str, list[FieldAnnotation]] = {}
    for annotation in root.findall("opt:annotations", _NS):
        path = _normalize_opt_path((annotation.get("path") or "").strip())
        if not path:
            continue
        parsed_items: list[FieldAnnotation] = []
        for item in annotation.findall("opt:items", _NS):
            key = (item.get("id") or "").strip()
            if not key:
                continue
            parsed_items.append(
                FieldAnnotation(key=key, value=(item.text or "").strip())
            )
        annotations.setdefault(path, []).extend(parsed_items)
        name_free_path = _strip_name_predicates(path)
        if name_free_path != path:
            annotations.setdefault(name_free_path, []).extend(parsed_items)
    return {
        path: tuple(
            sorted(
                {(item.key, item.value): item for item in values}.values(),
                key=lambda item: (item.key, item.value),
            )
        )
        for path, values in annotations.items()
    }


def _value_variants(element: ET.Element) -> list[ValueVariant]:
    rm_types: set[str] = set()
    coding_constraints_by_rm_type: dict[str, list[CodingConstraint]] = {}
    ordinal_options_by_rm_type: dict[str, list[OrdinalOption]] = {}
    for attribute in element.findall("opt:attributes", _NS):
        if _text(attribute, "opt:rm_attribute_name") != "value":
            continue
        for child in attribute.findall("opt:children", _NS):
            rm_type = _text(child, "opt:rm_type_name")
            if rm_type:
                rm_types.add(rm_type)
                if rm_type.upper() == "DV_CODED_TEXT":
                    coding_constraints_by_rm_type.setdefault(rm_type, []).extend(
                        _coding_constraints(child)
                    )
                elif rm_type.upper() == "DV_ORDINAL":
                    ordinal_options_by_rm_type.setdefault(rm_type, []).extend(
                        _ordinal_options(child)
                    )
    return [
        ValueVariant(
            rm_type=rm_type,
            query_paths=_query_paths_for(rm_type),
            coding_constraints=sorted(
                {
                    (
                        item.constraint_kind,
                        item.terminology_id,
                        tuple(item.allowed_codes),
                        item.value_set_uri,
                    ): item
                    for item in coding_constraints_by_rm_type.get(rm_type, [])
                }.values(),
                key=lambda item: (
                    item.constraint_kind,
                    item.terminology_id or "",
                    tuple(item.allowed_codes),
                    item.value_set_uri or "",
                ),
            ),
            ordinal_options=_normalize_ordinal_options(
                ordinal_options_by_rm_type.get(rm_type, [])
            ),
        )
        for rm_type in sorted(rm_types)
    ]


def _coding_constraints(value_node: ET.Element) -> list[CodingConstraint]:
    constraints: list[CodingConstraint] = []
    for attribute in value_node.findall(".//opt:attributes", _NS):
        if _text(attribute, "opt:rm_attribute_name") != "defining_code":
            continue
        for child in attribute.findall("opt:children", _NS):
            xsi_type = (child.get(_XSI_TYPE) or "").strip()
            if xsi_type not in {"C_CODE_PHRASE", "C_CODE_REFERENCE"}:
                continue
            terminology_id = _none_if_empty(
                _text(child, "opt:terminology_id/opt:value")
            )
            allowed_codes = sorted(
                {
                    (item.text or "").strip()
                    for item in child.findall("opt:code_list", _NS)
                    if (item.text or "").strip()
                }
            )
            value_set_uri = _none_if_empty(
                _text(child, "opt:referenceSetUri")
                or _text(child, "opt:reference_set_uri")
            )
            if not terminology_id and not allowed_codes and not value_set_uri:
                continue
            if allowed_codes and value_set_uri:
                kind = "mixed"
            elif allowed_codes:
                kind = "code_list"
            elif value_set_uri:
                kind = "value_set"
            else:
                kind = "terminology"
            constraints.append(
                CodingConstraint(
                    constraint_kind=kind,
                    terminology_id=terminology_id,
                    allowed_codes=allowed_codes,
                    value_set_uri=value_set_uri,
                )
            )
    return constraints


def _ordinal_options(value_node: ET.Element) -> list[OrdinalOption]:
    options: list[OrdinalOption] = []
    for item in value_node.findall("opt:list", _NS):
        raw_ordinal = _text(item, "opt:value")
        terminology_id = _text(
            item,
            "opt:symbol/opt:defining_code/opt:terminology_id/opt:value",
        )
        code = _text(item, "opt:symbol/opt:defining_code/opt:code_string")
        if not raw_ordinal or not terminology_id or not code:
            raise OptSemanticCatalogError(
                "Malformed DV_ORDINAL option: value, terminology_id, and "
                "code_string are required"
            )
        ordinal = _parse_required_int(raw_ordinal, label="DV_ORDINAL value")
        options.append(
            OrdinalOption(
                ordinal=ordinal,
                terminology_id=terminology_id,
                code=code,
            )
        )
    return _normalize_ordinal_options(options)


def _normalize_ordinal_options(
    options: Sequence[OrdinalOption],
) -> list[OrdinalOption]:
    """Deduplicate exact options and reject an ambiguous ordinal scale."""

    by_ordinal: dict[int, OrdinalOption] = {}
    by_symbol: dict[tuple[str, str], OrdinalOption] = {}
    for option in options:
        symbol = (option.terminology_id, option.code)
        existing_ordinal = by_ordinal.get(option.ordinal)
        if existing_ordinal is not None:
            if existing_ordinal != option:
                raise OptSemanticCatalogError(
                    f"Conflicting DV_ORDINAL value {option.ordinal!r}"
                )
            continue
        if symbol in by_symbol:
            raise OptSemanticCatalogError(
                "Conflicting DV_ORDINAL coded symbol "
                f"{option.terminology_id!r}::{option.code!r}"
            )
        by_ordinal[option.ordinal] = option
        by_symbol[symbol] = option
    return sorted(
        by_ordinal.values(),
        key=lambda item: (item.ordinal, item.terminology_id, item.code),
    )


def _query_paths_for(rm_type: str) -> list[QueryPath]:
    normalized = rm_type.upper()
    if normalized == "DV_CODED_TEXT":
        values = (
            ("display", "/value/value"),
            ("terminology", "/value/defining_code/terminology_id/value"),
            ("code", "/value/defining_code/code_string"),
        )
    elif normalized == "DV_QUANTITY":
        values = (("magnitude", "/value/magnitude"), ("units", "/value/units"))
    elif normalized == "DV_COUNT":
        values = (("magnitude", "/value/magnitude"),)
    elif normalized == "DV_ORDINAL":
        values = (
            ("ordinal", "/value/value"),
            ("symbol", "/value/symbol/value"),
            (
                "terminology",
                "/value/symbol/defining_code/terminology_id/value",
            ),
            ("code", "/value/symbol/defining_code/code_string"),
        )
    elif normalized == "DV_PROPORTION":
        values = (
            ("numerator", "/value/numerator"),
            ("denominator", "/value/denominator"),
            ("type", "/value/type"),
        )
    elif normalized.startswith("DV_INTERVAL"):
        values = (("lower", "/value/lower"), ("upper", "/value/upper"))
    elif normalized in {"DV_IDENTIFIER"}:
        values = (
            ("id", "/value/id"),
            ("issuer", "/value/issuer"),
            ("assigner", "/value/assigner"),
            ("type", "/value/type"),
        )
    else:
        values = (("value", "/value/value"),)
    return [QueryPath(facet=facet, aql_path_suffix=suffix) for facet, suffix in values]


def _correlation_anchor(
    frames: Sequence[_ObjectFrame],
    containing_archetype: ArchetypeReference,
) -> CorrelationAnchor:
    for frame in reversed(frames[:-1]):
        if not frame.repeated:
            continue
        if frame.archetype_id and frame.absolute_aql_path == containing_archetype.absolute_aql_path:
            reason = "nearest_repeating_archetype"
        elif frame.rm_type in {"EVENT", "POINT_EVENT", "INTERVAL_EVENT"}:
            reason = "nearest_repeating_event"
        else:
            reason = "nearest_repeating_node"
        return CorrelationAnchor(
            rm_type=frame.rm_type,
            node_id=frame.node_id,
            archetype_id=frame.archetype_id,
            absolute_aql_path=frame.absolute_aql_path,
            composition_aql_path=frame.composition_aql_path,
            reason=reason,
        )
    return CorrelationAnchor(
        rm_type=containing_archetype.rm_type,
        node_id=containing_archetype.node_id,
        archetype_id=containing_archetype.archetype_id,
        absolute_aql_path=containing_archetype.absolute_aql_path,
        composition_aql_path=containing_archetype.composition_aql_path,
        reason="containing_archetype",
    )


def _parse_interval(
    element: Optional[ET.Element],
    *,
    default_lower: int,
    default_upper: Optional[int],
    default_unbounded: bool = False,
) -> Multiplicity:
    if element is None:
        return Multiplicity(
            lower=default_lower,
            upper=None if default_unbounded else default_upper,
            upper_unbounded=default_unbounded,
        )
    lower = _parse_int(_text(element, "opt:lower"), default_lower)
    unbounded = _parse_bool(
        _text(element, "opt:upper_unbounded"), default=default_unbounded
    )
    upper = None if unbounded else _parse_int(
        _text(element, "opt:upper"),
        default_upper if default_upper is not None else lower,
    )
    return Multiplicity(lower=lower, upper=upper, upper_unbounded=unbounded)


def _items_by_id(element: ET.Element) -> dict[str, str]:
    values: dict[str, str] = {}
    for item in element.findall("opt:items", _NS):
        key = (item.get("id") or "").strip()
        if key:
            values[key] = "".join(item.itertext()).strip()
    return values


def _other_detail(root: ET.Element, detail_id: str) -> str:
    for item in root.findall("opt:description/opt:other_details", _NS):
        if item.get("id") == detail_id:
            return (item.text or "").strip()
    return ""


def _relative_aql_path(path: str, scope_path: str) -> str:
    if not scope_path:
        return path or "/"
    if path == scope_path:
        return "/"
    if not path.startswith(scope_path + "/"):
        raise OptSemanticCatalogError(
            f"Field path '{path}' is outside archetype scope '{scope_path}'"
        )
    return path[len(scope_path) :]


_COMMA_NAME_PREDICATE = re.compile(
    r"\[([^\],\s]+)\s*,\s*'((?:[^']|'')*)'\]"
)
_AND_NAME_PREDICATE = re.compile(
    r"\[([^\],\s]+)\s+and\s+name/value\s*=\s*'((?:[^']|'')*)'\]"
)


def _normalize_opt_path(path: str) -> str:
    compact = "".join(path.splitlines()).strip()
    compact = _COMMA_NAME_PREDICATE.sub(
        lambda match: f"[{match.group(1)} and name/value='{match.group(2)}']",
        compact,
    )
    return compact


def _strip_name_predicates(path: str) -> str:
    return _AND_NAME_PREDICATE.sub(r"[\1]", path)


def _fixed_name(node: ET.Element) -> Optional[str]:
    """Return an explicit RM name constraint, never a generated term label."""

    for attribute in node.findall("opt:attributes", _NS):
        if _text(attribute, "opt:rm_attribute_name") != "name":
            continue
        for value in attribute.findall(".//opt:list", _NS):
            fixed = (value.text or "").strip()
            if fixed:
                return fixed
    return None


def _external_code(raw_code: str) -> str:
    match = re.fullmatch(r"\[[^:\]]+::([^\]]+)\]", raw_code.strip())
    return match.group(1) if match else raw_code.strip()


def _stable_id(prefix: str, *parts: str) -> str:
    canonical = json.dumps(
        list(parts), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(canonical).hexdigest()}"


def _semantic_key(
    *,
    template_id: str,
    archetype_id: str,
    absolute_aql_path: str,
    value_variants: Sequence[ValueVariant],
) -> str:
    """Return a migration key stable across non-structural OPT revisions."""

    return _stable_id(
        "openehr-semantic-key",
        template_id,
        archetype_id,
        absolute_aql_path,
        ",".join(variant.rm_type for variant in value_variants),
    )


def _catalog_digest(catalog: OpenEhrOptSemanticCatalog) -> str:
    payload = catalog.model_dump(
        mode="json",
        by_alias=True,
        exclude={"catalog_digest"},
        exclude_none=True,
    )
    canonical = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return _sha256_digest(canonical)


def _sha256_digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _text(element: ET.Element, path: str) -> str:
    return (element.findtext(path, default="", namespaces=_NS) or "").strip()


def _none_if_empty(value: str) -> Optional[str]:
    stripped = value.strip()
    return stripped or None


def _parse_bool(value: str, *, default: bool) -> bool:
    if not value:
        return default
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise OptSemanticCatalogError(
        f"Invalid OPT boolean value {value!r}; expected 'true' or 'false'"
    )


def _parse_int(value: str, default: int) -> int:
    if value is None or not str(value).strip():
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise OptSemanticCatalogError(
            f"Invalid OPT integer value {value!r}"
        )


def _parse_required_int(value: str, *, label: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise OptSemanticCatalogError(
            f"Invalid OPT integer for {label}: {value!r}"
        )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


__all__ = [
    "CATALOG_CONTRACT",
    "ArchetypeReference",
    "CodingConstraint",
    "CorrelationAnchor",
    "FieldAnnotation",
    "Multiplicity",
    "OpenEhrOptSemanticCatalog",
    "OptSemanticCatalogCompiler",
    "OptSemanticCatalogError",
    "OrdinalOption",
    "PathCardinality",
    "QueryableField",
    "QueryPath",
    "TemplateMetadata",
    "TermText",
    "TerminologyBinding",
    "ValueVariant",
    "compile_opt_semantic_catalog",
    "compile_opt_semantic_catalog_xml",
]
