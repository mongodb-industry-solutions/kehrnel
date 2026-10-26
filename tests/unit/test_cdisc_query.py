import pytest

from kehrnel.engine.domains.cdisc.query import compile_study_query


def _query():
    return {
        "version": "cdisc-query/v1",
        "scope": {"studies": ["STUDY-001"], "snapshots": "published"},
        "from": {"profile": "sdtm", "domains": ["dm"]},
        "where": {
            "and": [
                {"path": "facets.subjectId", "op": "eq", "value": "STUDY-001-001"},
                {"path": "data.SEX", "op": "in", "value": ["F", "M"]},
            ]
        },
        "select": ["studyId", "data.USUBJID", "data.SEX"],
        "orderBy": [{"path": "data.USUBJID", "direction": "asc"}],
        "page": {"limit": 25},
    }


def test_query_compiler_injects_tenant_and_atomic_publication_marker():
    plan = compile_study_query(
        _query(),
        tenant_id="tenant-a",
        collection="cdisc_records",
        snapshot_collection="cdisc_snapshots",
    )

    assert plan["pipeline"][0]["$match"]["$and"][0] == {"tenantId": "tenant-a"}
    assert plan["pipeline"][1]["$lookup"]["from"] == "cdisc_snapshots"
    assert plan["pipeline"][2] == {"$match": {"__snapshot.state": "published"}}
    assert plan["pipeline"][-1] == {"$limit": 26}
    assert plan["scope"] == "study_subject"
    assert plan["governance"]["tenantInjected"] is True


def test_query_compiler_rejects_unknown_paths_and_mongodb_operator_injection():
    query = _query()
    query["where"]["and"][0]["path"] = "$where"
    with pytest.raises(ValueError, match="not allowed"):
        compile_study_query(
            query,
            tenant_id="tenant-a",
            collection="records",
            snapshot_collection="snapshots",
        )

    query = _query()
    query["where"]["and"][0]["value"] = {"$ne": "blocked"}
    with pytest.raises(ValueError, match="operator keys"):
        compile_study_query(
            query,
            tenant_id="tenant-a",
            collection="records",
            snapshot_collection="snapshots",
        )


def test_query_compiler_caps_page_size_in_schema():
    query = _query()
    query["page"]["limit"] = 1001
    with pytest.raises(ValueError):
        compile_study_query(
            query,
            tenant_id="tenant-a",
            collection="records",
            snapshot_collection="snapshots",
        )


def test_query_compiler_accepts_opaque_cursor_and_rejects_unsafe_configured_path():
    query = _query()
    from kehrnel.engine.domains.cdisc.query import encode_page_token

    query["page"]["token"] = encode_page_token(25)
    plan = compile_study_query(query, tenant_id="tenant-a", collection="records", snapshot_collection="snapshots")
    assert {"$skip": 25} in plan["pipeline"]

    query = _query()
    query["where"]["and"][0]["path"] = "$where"
    with pytest.raises(ValueError, match="not allowed"):
        compile_study_query(
            query,
            tenant_id="tenant-a",
            collection="records",
            snapshot_collection="snapshots",
            extra_allowed_paths={"$where"},
        )
