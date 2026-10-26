import pytest

from kehrnel.engine.strategies.cdisc.sdr.analysis.service import compile_analysis


def _analysis(**overrides):
    value = {
        "version": "cdisc-analysis/v1",
        "scope": {"studies": ["SEND-01"], "snapshots": "published"},
        "from": {"profile": "send", "domains": ["mi"]},
        "where": {"and": [{"path": "facets.finding", "op": "eq", "value": "NECROSIS"}]},
        "groupBy": ["facets.treatmentGroup"],
        "metrics": [
            {"name": "records", "op": "count"},
            {"name": "subjects", "op": "countDistinct", "path": "facets.subjectId"},
        ],
        "orderBy": [{"field": "records", "direction": "desc"}],
        "limit": 25,
    }
    value.update(overrides)
    return value


def test_compile_analysis_injects_tenant_and_published_snapshot_constraints():
    plan = compile_analysis(
        _analysis(), tenant_id="tenant-a", collection="records", snapshot_collection="snapshots"
    )

    assert plan["pipeline"][0] == {"$match": {"$and": [
        {"tenantId": "tenant-a"},
        {"studyId": {"$in": ["SEND-01"]}},
        {"profile": "send"},
        {"domain": {"$in": ["MI"]}},
        {"facets.finding": "NECROSIS"},
    ]}}
    assert plan["pipeline"][1]["$lookup"]["from"] == "snapshots"
    assert plan["pipeline"][2] == {"$match": {"__snapshot.state": "published"}}
    assert plan["pipeline"][3]["$group"]["records"] == {"$sum": 1}
    assert plan["pipeline"][3]["$group"]["__distinct_subjects"] == {"$addToSet": "$facets.subjectId"}
    assert plan["pipeline"][4]["$project"]["subjects"] == {"$size": {"$filter": {
        "input": "$__distinct_subjects", "as": "value", "cond": {"$ne": ["$$value", None]},
    }}}
    assert plan["governance"]["clientMongoOperatorsAccepted"] is False


def test_compile_analysis_rejects_unsafe_paths_and_undeclared_sort_metrics():
    with pytest.raises(ValueError, match="not allowed"):
        compile_analysis(
            _analysis(groupBy=["$where"]),
            tenant_id="tenant-a", collection="records", snapshot_collection="snapshots",
        )
    with pytest.raises(ValueError, match="declared metrics"):
        compile_analysis(
            _analysis(orderBy=[{"field": "notDeclared"}]),
            tenant_id="tenant-a", collection="records", snapshot_collection="snapshots",
        )


def test_compile_analysis_rejects_operator_injection_in_values():
    with pytest.raises(ValueError, match="operator keys"):
        compile_analysis(
            _analysis(where={"and": [{"path": "facets.finding", "op": "eq", "value": {"$ne": None}}]}),
            tenant_id="tenant-a", collection="records", snapshot_collection="snapshots",
        )
