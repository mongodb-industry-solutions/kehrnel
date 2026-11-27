"""Version-bound review acknowledgements; these are not approval credentials."""
from .model_config import require
from .source import digest


def review_requirements(result, projection, config):
    context = {
        "proposal_revision": result["proposal"]["revision"],
        "rule_revision": result["rule"]["revision"],
        "rules_revision": result["rules_revision"],
        "source_namespace": config["source_namespace"],
        "source_profile_revision": digest(config["source"]),
        "preview_digest": digest(projection.documents()),
    }
    return {
        "revision": digest(context), "context": context,
        "required_topics": result["proposal"]["review_required"],
        "approval": False, "persisted": False,
    }


def acknowledge(requirements, receipt):
    require(isinstance(receipt, dict) and not set(receipt) - {"revision", "topics"},
            "Acknowledge this preview revision and its topics")
    require(receipt.get("revision") == requirements["revision"],
            "Review is stale; validate and preview the current draft and sample again")
    topics = receipt.get("topics")
    require(isinstance(topics, list) and all(isinstance(t, str) for t in topics)
            and len(topics) == len(set(topics))
            and set(topics) == set(requirements["required_topics"]),
            "Explicitly acknowledge every required review topic")
    return {**requirements, "acknowledged_topics": sorted(topics), "complete": True}
