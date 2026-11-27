"""Avoid duplicating template paths and full source lineage in every fact."""

from copy import deepcopy
from .source import digest


def compact(compositions):
    output, refs = [], []
    for composition in compositions:
        doc = deepcopy(composition)
        ref = {"_id": doc["_id"], **doc["lineage"]}
        refs.append(ref)
        doc["lineage"] = {
            "source_ref": doc["_id"],
            "clinical_mapping": ref["clinical_mapping"],
        }
        if doc.get("quality") == {}:
            doc.pop("quality")
        for fact in doc["facts"]:
            lineage = fact["lineage"]
            fact["lineage"] = {
                "source_ref": doc["_id"],
                "occurrence_id": lineage["occurrence_id"],
                "anchor_key": digest(
                    {"anchor": lineage["anchor_path"], "kind": fact["context"]["kind"]}
                )[:24],
                "instance_chain": lineage["instance_chain"],
            }
            # Present values already carry their presence in BSON. The immutable
            # variable registry declares target units and types. Retain missing,
            # null-flavour and conversion evidence, rather than identical metadata
            # repeated beside every ordinary present scalar.
            quality = {}
            for key, q in fact["quality"].items():
                if q.get("state") != "present":
                    quality[key] = q
                elif (
                    q.get("source_unit") and q["source_unit"] != q.get("unit")
                ) or q.get("conversion"):
                    quality[key] = {
                        "state": "converted",
                        "source_unit": q.get("source_unit"),
                        "source_rm_type": q.get("rm_type"),
                    }
            if quality:
                fact["quality"] = quality
            else:
                fact.pop("quality", None)
        output.append(doc)
    return output, refs
