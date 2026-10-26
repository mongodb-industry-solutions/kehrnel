import pytest

from kehrnel.engine.strategies.cdisc.sdr.assistant import AssistantService


class Analysis:
    async def run(self, _ctx, payload):
        return {
            "ok": True,
            "version": payload["version"],
            "rows": [{"groupValues": ["HIGH"], "records": 3, "subjects": 2}],
            "columns": {"groupBy": payload["groupBy"], "metrics": payload["metrics"]},
        }


class Query:
    async def search(self, _ctx, _payload):
        return {"rows": [{"datasetId": "d1", "recordKey": "r1", "domain": "MI"}]}


class Repository:
    async def snapshot_summary(self, _ctx, _payload):
        return {
            "snapshot": {"_id": "tenant:S:v1", "state": "published", "standardsPackageId": "send-3.1"},
            "summary": {"datasetCount": 2, "recordCount": 10},
        }

    async def list_validation_runs(self, _ctx, _payload):
        return {"items": []}


class Validation:
    async def get_run(self, _ctx, _payload):
        raise AssertionError("not expected")


class Lineage:
    async def inspect(self, _ctx, _payload):
        return {"datasets": [], "artifacts": []}


def _service():
    return AssistantService(
        analysis=Analysis(), query=Query(), repository=Repository(),
        validation=Validation(), lineage=Lineage(),
    )


@pytest.mark.asyncio
async def test_assistant_runs_governed_grouped_analysis_and_cites_evidence():
    result = await _service().ask(None, {
        "question": "How many lesions by dose group?",
        "studyId": "S", "snapshotId": "v1", "profile": "send", "domains": ["MI"],
    })

    assert result["intent"] == "analysis"
    assert result["data"]["rows"][0]["subjects"] == 2
    assert result["data"]["columns"]["groupBy"] == ["facets.treatmentGroup"]
    assert result["citations"][0]["studyId"] == "S"
    assert result["citations"][0]["snapshotId"] == "v1"
    assert result["toolCalls"] == [{"name": "cdisc_run_analysis", "readOnly": True}]
    assert result["guardrails"] == {
        "readOnly": True, "mayPublish": False, "mayWaive": False,
        "maySupersede": False, "regulatoryComplianceClaimed": False,
    }


@pytest.mark.asyncio
async def test_assistant_maps_plain_language_to_a_governed_send_dimension():
    result = await _service().ask(None, {
        "question": "How many findings are there by severity?",
        "studyId": "S", "snapshotId": "v1", "profile": "send", "domains": ["MI"],
    })

    assert result["data"]["columns"]["groupBy"] == ["facets.severity"]


@pytest.mark.asyncio
async def test_assistant_summary_is_evidence_cited():
    result = await _service().ask(None, {
        "question": "Summarize this snapshot", "studyId": "S", "snapshotId": "v1",
    })

    assert result["intent"] == "summary"
    assert "2 datasets" in result["answer"]
    assert result["citations"][0]["snapshotRef"] == "tenant:S:v1"
