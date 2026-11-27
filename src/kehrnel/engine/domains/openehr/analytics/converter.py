"""One pure conversion path for partition workers, replay and incoming compositions."""

from dataclasses import dataclass, field
from datetime import timedelta
from hashlib import sha256
from copy import deepcopy
from .decoder import IBMDecoder
from .projection import Catalog, project, utc
from .storage_shape import compact
from .source import SourceError, digest, validate_archetype_identities


@dataclass(frozen=True)
class SourceFields:
    composition_id: str = "_id"
    patient_id: str = "ehr_id"
    template: str = "template_name"
    template_id: str = "template_id"
    version: str = "composition_version"
    committed_at: str = "composition_date"
    provider: str = "provider_id"
    canonical: str = "canonicalJSON"


@dataclass
class Projection:
    composition_id: str
    source_version: int
    source_fingerprint: str
    rule_revision: str | None
    rule_epoch: int
    schema_version: int
    decoder_revision: str
    status: str
    template: str
    activity: dict | None
    facts: list[dict]
    source_ref: dict | None
    accepted_at: object
    diagnostics: list[dict] = field(default_factory=list)
    coverage: dict | None = None

    def documents(self):
        return {
            "activity": self.activity,
            "facts": self.facts,
            "source_ref": self.source_ref,
            "state": self.state(),
            "source_coverage": self.coverage or {"status": "not_checked", "reason": "Rule has no OPT structure inventory"},
        }

    def state(self):
        return {
            "_id": self.composition_id,
            "source_version": self.source_version,
            "source_fingerprint": self.source_fingerprint,
            "rule_revision": self.rule_revision,
            "rule_epoch": self.rule_epoch,
            "schema_version": self.schema_version,
            "decoder_revision": self.decoder_revision,
            "status": self.status,
            "template": self.template,
            "accepted_at": self.accepted_at,
            "fact_count": len(self.facts),
            "diagnostics": self.diagnostics,
        }


class Converter:
    def __init__(self, rules, *, source_namespace, fields=None, decoder=None):
        if not isinstance(source_namespace, str) or not source_namespace:
            raise ValueError("A stable source namespace is required")
        self.rules = rules
        self.namespace = source_namespace
        self.fields = fields or SourceFields()
        self.decoder = decoder
        self.empty = Catalog({"variables": [], "templates": []})

    @staticmethod
    def fingerprint(document):
        # PyMongo's C encoder avoids JSON date/UUID coercion and a second canonical serialization.
        from bson import BSON
        from bson.codec_options import CodecOptions
        from bson.binary import UuidRepresentation

        return sha256(
            BSON.encode(
                document,
                codec_options=CodecOptions(
                    uuid_representation=UuidRepresentation.STANDARD
                ),
            )
        ).hexdigest()

    def convert(self, document, *, accepted_at, rule_revision=None, deleted=False):
        f = self.fields
        for key in (
            f.composition_id,
            f.patient_id,
            f.template,
            f.version,
            f.committed_at,
        ):
            if document.get(key) is None:
                raise SourceError("Source envelope is missing " + key)
        raw_version = document[f.version]
        if isinstance(raw_version, bool):
            raise SourceError("Source version must be a positive integer")
        version = str(raw_version).split("::")[-1]
        if not version.isdigit() or int(version) < 1:
            raise SourceError("Source version must be explicit and ordered")
        version = int(version)
        template = document[f.template]
        if not isinstance(template, str) or not template:
            raise SourceError("Template identifier must be an exact non-empty string")
        identifier = source_id(document[f.composition_id])
        accepted = utc(accepted_at)
        rule = self.rules.select(template, accepted, rule_revision)
        if rule:
            expected = rule.catalog.templates[template].get("source_template_ids")
            if (
                expected
                and source_id(document.get(f.template_id, template)) not in expected
            ):
                raise SourceError("Source template UUID is not approved by this rule")
        fingerprint = self.fingerprint(document)
        decoder_revision = (
            self.decoder.revision if self.decoder else "canonical-openEHR/v1"
        )
        if deleted:
            if not rule:
                raise SourceError("A deletion must pin a known rule revision")
            return Projection(
                identifier,
                version,
                fingerprint,
                rule.revision,
                rule.epoch,
                1,
                decoder_revision,
                "deleted",
                template,
                None,
                [],
                None,
                accepted,
                coverage={"status": "not_checked", "reason": "Source deletion has no clinical payload"},
            )
        canonical = (
            self.decoder.decode(document) if self.decoder else document.get(f.canonical)
        )
        if not isinstance(canonical, dict) or canonical.get("_type") != "COMPOSITION":
            raise SourceError("Expected a canonical COMPOSITION")
        policy = rule.source_metadata_policy if rule else None
        collector = rule.source_coverage.collector() if rule and rule.source_coverage else None
        metadata_exceptions = validate_archetype_identities(canonical, policy, collector)
        coverage = collector.result() if collector else None
        incomplete = bool(coverage and coverage["status"] != "covered")
        source = {
            "_id": identifier,
            "ehr_id": source_id(document[f.patient_id]),
            "template": template,
            "template_id": source_id(document.get(f.template_id, template)),
            "creation_date": document[f.committed_at],
            "version": str(version),
            "revision": version,
            "provider_id": document.get(f.provider, "unknown"),
            "synthetic": bool(document.get("synthetic", False)),
        }
        c = rule.catalog if rule else self.empty
        composition = project(
            source,
            canonical,
            c,
            decoder_revision,
            allow_activity_only=rule is None,
            source_fingerprint=fingerprint,
            suppress_facts=incomplete,
        )
        if incomplete:
            composition["lineage"]["clinical_mapping"] = "needs_rule_extension"
        output, refs = compact([composition])
        activity = output[0]
        facts = activity.pop("facts")
        ref = refs[0]
        ref.update(
            source_namespace=self.namespace,
            schema_version=1,
            rule_revision=rule.revision if rule else None,
            rule_epoch=rule.epoch if rule else 0,
        )
        diagnostics = [] if rule else [{"code": "TEMPLATE_RULES_MISSING", "template": template}]
        if incomplete:
            ref["source_coverage"] = coverage
            diagnostics.append({"code": "SOURCE_COVERAGE_INCOMPLETE", "count": coverage["issue_count"],
                                "source_ref": identifier})
        if metadata_exceptions:
            ref["source_metadata_compatibility"] = policy.evidence(metadata_exceptions)
            diagnostics.append({"code": "SOURCE_METADATA_COMPATIBILITY", "count": len(metadata_exceptions),
                                "source_ref": identifier})
        if rule and rule.retention["mode"] == "delete_facts":
            for fact in facts:
                if rule.retention["clock"] == "clinical_time":
                    if fact["context"]["time_basis"] in (
                        "composition_start",
                        "source_committed",
                    ):
                        raise SourceError(
                            "Clinical-time TTL cannot use a fallback timestamp"
                        )
                    clock = fact["context"]["event_at"]
                else:
                    clock = utc(source["creation_date"])
                fact["expires_at"] = clock + timedelta(days=rule.retention["days"])
        return Projection(
            identifier,
            version,
            fingerprint,
            rule.revision if rule else None,
            rule.epoch if rule else 0,
            1,
            decoder_revision,
            "projected" if rule and not incomplete else "needs_rules",
            template,
            activity,
            facts,
            ref,
            accepted,
            diagnostics,
            coverage,
        )

    def iter_convert(self, documents, *, accepted_at, rule_revision=None):
        """Streaming iterator: one composition's output is resident at a time."""
        for document in documents:
            yield self.convert(
                document, accepted_at=accepted_at, rule_revision=rule_revision
            )


def source_id(value):
    """UUID and BSON subtype-4 representations must produce the same clinical ID."""
    from bson.binary import Binary, UUID_SUBTYPE, UuidRepresentation

    if isinstance(value, Binary):
        if value.subtype != UUID_SUBTYPE:
            raise SourceError(
                "Configure a UUID representation; legacy binary IDs cannot be guessed"
            )
        value = value.as_uuid(UuidRepresentation.STANDARD)
    if not isinstance(value, (str, __import__("uuid").UUID)):
        from bson import ObjectId

        if not isinstance(value, ObjectId):
            raise SourceError("Source identifiers must be strings, UUIDs or ObjectIds")
    if not str(value):
        raise SourceError("Source identifiers must be non-empty")
    return str(value)
