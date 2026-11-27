"""Narrow, OPT-bound handling of copied COMPOSITION metadata on OBSERVATIONs.

The source is never repaired. The rule permits analytical path selection from a
confirmed node identity while retaining the conflicting declaration in lineage.
"""
from dataclasses import dataclass
import re
from .source import resolve, segments, selector_parts
from .model_config import require

POLICY = "copied_composition_metadata/v1"


def policy_from_semantic(semantic):
    """Only the exact OPT compiler supplies the eligible roots; no guessed paths."""
    return {
        "policy": POLICY,
        "opt_digest": semantic.template.source_digest,
        "composition_archetype_id": semantic.template.root_archetype_id,
        "observations": [
            {"path": a.composition_aql_path, "archetype_id": a.archetype_id}
            for a in semantic.archetypes if a.rm_type == "OBSERVATION"
        ],
    }


@dataclass(frozen=True)
class IdentityPolicy:
    template: str
    opt_digest: str
    composition_archetype_id: str
    observations: tuple

    @classmethod
    def compile(cls, value, template):
        if value is None:
            return None
        require(isinstance(value, dict) and set(value) == {
            "policy", "opt_digest", "composition_archetype_id", "observations"
        }, "Invalid source metadata compatibility policy")
        require(value["policy"] == POLICY, "Unsupported source metadata policy")
        require(isinstance(value["opt_digest"], str) and
                re.fullmatch(r"sha256:[0-9a-f]{64}", value["opt_digest"]),
                "Source metadata policy requires an exact OPT digest")
        root = value["composition_archetype_id"]
        require(isinstance(root, str) and root.startswith("openEHR-EHR-COMPOSITION."),
                "Source metadata policy requires a COMPOSITION archetype")
        observations = value["observations"]
        require(isinstance(observations, list), "Declare the OPT observation roots")
        paths, compiled = set(), []
        for item in observations:
            require(isinstance(item, dict) and set(item) == {"path", "archetype_id"},
                    "An eligible observation requires its exact OPT path and archetype")
            path, archetype = item["path"], item["archetype_id"]
            require(isinstance(path, str) and path.startswith("/") and path not in paths,
                    "Observation paths must be unique and absolute within the composition")
            require(isinstance(archetype, str) and archetype.startswith("openEHR-EHR-OBSERVATION."),
                    "Only OBSERVATION roots are supported by this policy")
            parts = segments(path)
            require(parts and parts[-1][1] and selector_parts(parts[-1][1])[0] == archetype,
                    "The observation selector must identify the declared archetype")
            paths.add(path)
            compiled.append((path, archetype))
        return cls(template, value["opt_digest"], root, tuple(compiled))

    def matching_path(self, root, node, cache):
        """Resolve only on a mismatch, once per affected archetype per composition."""
        details = root.get("archetype_details", {})
        if not isinstance(details, dict):
            return None
        archetype = details.get("archetype_id", {})
        template = details.get("template_id", {})
        if not isinstance(archetype, dict) or not isinstance(template, dict):
            return None
        if (root.get("_type") != "COMPOSITION"
                or root.get("archetype_node_id") != self.composition_archetype_id
                or archetype.get("value") != self.composition_archetype_id
                or template.get("value") != self.template
                or node.get("_type") != "OBSERVATION"
                or node.get("archetype_details") != details):
            return None
        identifier = node.get("archetype_node_id")
        if identifier not in cache:
            matches = {}
            for path, expected in self.observations:
                if expected != identifier:
                    continue
                for value, identity in resolve(root, path):
                    matches.setdefault(id(value), []).append((path, identity))
            cache[identifier] = matches
        paths = cache[identifier].get(id(node), [])
        return paths[0] if len(paths) == 1 else None

    def evidence(self, exceptions):
        return {"policy": POLICY, "opt_digest": self.opt_digest,
                "source_modified": False, "exceptions": exceptions}


def upgrade_rule(rule, opt_xml, *, epoch, effective_at):
    """Add OPT evidence to a new epoch without replacing selected fields or conversions.

    This builds a candidate only. Callers still preview, review and publish the
    candidate using the normal configuration lifecycle.
    """
    from copy import deepcopy
    from kehrnel.engine.domains.openehr.templates.semantic_catalog import OptSemanticCatalogCompiler
    from .rules import RuleRevision, RuleSet
    previous = RuleRevision.compile(rule)
    require(type(epoch) is int and epoch > previous.epoch,
            "Compatibility changes require a new rule epoch")
    semantic = OptSemanticCatalogCompiler(opt_xml.encode() if isinstance(opt_xml, str) else opt_xml).compile()
    require(semantic.template.template_id == previous.template,
            "The OPT must match the rule's exact template identifier")
    value = deepcopy(previous.document())
    value.pop("_id", None)
    value.pop("revision", None)
    value.update(epoch=epoch, effective_at=effective_at)
    value["catalog"]["templates"][0]["source_metadata_policy"] = policy_from_semantic(semantic)
    candidate = RuleRevision.compile(value)
    RuleSet([previous, candidate])
    return candidate.document()
