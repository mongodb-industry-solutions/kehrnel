"""Governed assistant orchestration without autonomous mutations."""

from __future__ import annotations

from typing import Any, Dict, Iterable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext


class AssistantService:
    """Route a bounded question to read-only CDISC services and cite every answer."""

    def __init__(self, *, analysis, query, repository, validation, lineage):
        self.analysis = analysis
        self.query = query
        self.repository = repository
        self.validation = validation
        self.lineage = lineage

    @staticmethod
    def _intent(question: str, requested: str | None) -> str:
        if requested in {"summary", "validation", "lineage", "search", "analysis"}:
            return requested
        text = question.lower()
        if any(term in text for term in ("incidence", "count", "how many", "by severity", "by group", "by dose")):
            return "analysis"
        if any(term in text for term in ("validation", "finding", "error", "warning", "waiver")):
            return "validation"
        if any(term in text for term in ("lineage", "source", "artifact", "provenance")):
            return "lineage"
        if any(term in text for term in ("search", "find", "mention", "contains", "similar")):
            return "search"
        return "summary"

    @staticmethod
    def _citations(kind: str, values: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
        return [{"kind": kind, **{k: v for k, v in value.items() if v not in (None, "", [])}} for value in values]

    @staticmethod
    def _analysis_group_by(question: str) -> str:
        """Map plain SEND wording to the small governed dimension allowlist."""
        text = question.lower()
        if "severity" in text:
            return "facets.severity"
        if "specimen" in text or "organ" in text:
            return "facets.specimen"
        if "treatment" in text or "dose" in text or "group" in text:
            return "facets.treatmentGroup"
        if "finding" in text or "lesion" in text:
            return "facets.finding"
        return "facets.treatmentGroup"

    async def ask(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        question = str(payload.get("question") or "").strip()
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not question or not study_id or not snapshot_id:
            raise KehrnelError(
                code="INVALID_INPUT", status=400,
                message="question, studyId, and snapshotId are required",
            )
        intent = self._intent(question, payload.get("intent"))
        base_evidence = {"studyId": study_id, "snapshotId": snapshot_id}

        if intent == "summary":
            result = await self.repository.snapshot_summary(
                ctx, {"studyId": study_id, "snapshotId": snapshot_id}
            )
            summary = result.get("summary") or {}
            snapshot = result.get("snapshot") or {}
            answer = (
                f"Snapshot {snapshot_id} contains {summary.get('datasetCount', 0)} datasets and "
                f"{summary.get('recordCount', 0)} records; its state is {snapshot.get('state', 'unknown')}."
            )
            citations = self._citations("snapshot", [{
                **base_evidence,
                "snapshotRef": snapshot.get("_id"),
                "standardsPackageId": snapshot.get("standardsPackageId"),
            }])
            tool = "cdisc_snapshot_summary"
        elif intent == "validation":
            page = await self.repository.list_validation_runs(
                ctx, {"studyId": study_id, "snapshotId": snapshot_id, "pageSize": 1}
            )
            latest = (page.get("items") or [None])[0]
            if latest:
                result = await self.validation.get_run(ctx, {"runId": latest["runId"], "limit": 200})
                run = result.get("run") or {}
                findings = result.get("findings") or []
                answer = (
                    f"Latest validation is {run.get('status', 'unknown')} with "
                    f"{len(findings)} findings ({(run.get('summary') or {}).get('waived', 0)} waived)."
                )
                citations = self._citations("validation", [{
                    **base_evidence, "runId": run.get("runId"),
                    "ruleIds": sorted({item.get("ruleId") for item in findings if item.get("ruleId")}),
                }])
            else:
                result = {"run": None, "findings": []}
                answer = "No validation run exists for this snapshot."
                citations = self._citations("snapshot", [base_evidence])
            tool = "cdisc_get_validation_run"
        elif intent == "lineage":
            result = await self.lineage.inspect(ctx, {"studyId": study_id, "snapshotId": snapshot_id})
            datasets = result.get("datasets") or []
            artifacts = result.get("artifacts") or []
            answer = (
                f"The snapshot traces to {len(datasets)} datasets and {len(artifacts)} retained artifacts."
            )
            citations = self._citations("lineage", [{
                **base_evidence,
                "datasetIds": [item.get("datasetId") or item.get("_id") for item in datasets],
                "artifactIds": [item.get("artifactId") or item.get("_id") for item in artifacts],
            }])
            tool = "cdisc_get_lineage"
        elif intent == "analysis":
            request = payload.get("analysis") if isinstance(payload.get("analysis"), dict) else {
                "version": "cdisc-analysis/v1",
                "scope": {"studies": [study_id], "snapshots": [snapshot_id]},
                "from": {
                    "profile": str(payload.get("profile") or "send").lower(),
                    "domains": [str(value).upper() for value in (payload.get("domains") or ["MI"])],
                },
                "where": {"and": []},
                "groupBy": [self._analysis_group_by(question)],
                "metrics": [
                    {"name": "records", "op": "count"},
                    {"name": "subjects", "op": "countDistinct", "path": "facets.subjectId"},
                ],
                "orderBy": [{"field": "records", "direction": "desc"}],
                "limit": 100,
            }
            result = await self.analysis.run(ctx, request)
            answer = f"The governed analysis returned {len(result.get('rows') or [])} grouped result rows."
            citations = self._citations("analysis", [{
                **base_evidence,
                "analysisVersion": result.get("version"),
                "groupBy": (result.get("columns") or {}).get("groupBy"),
                "metrics": (result.get("columns") or {}).get("metrics"),
            }])
            tool = "cdisc_run_analysis"
        else:
            search_payload = {
                "q": question,
                "mode": str(payload.get("mode") or "hybrid"),
                "studyIds": [study_id],
                "snapshotIds": [snapshot_id],
                "domains": [str(value).upper() for value in (payload.get("domains") or [])],
                "limit": min(max(int(payload.get("limit") or 10), 1), 50),
            }
            if payload.get("profile"):
                search_payload["profile"] = str(payload["profile"]).lower()
            result = await self.query.search(ctx, search_payload)
            rows = result.get("rows") or []
            answer = f"Found {len(rows)} governed records matching the question."
            citations = self._citations("record", [
                {
                    "studyId": row.get("studyId") or study_id,
                    "snapshotId": row.get("snapshotId") or snapshot_id,
                    "datasetId": row.get("datasetId"),
                    "recordKey": row.get("recordKey") or row.get("_id"),
                    "domain": row.get("domain"),
                }
                for row in rows
            ])
            tool = "cdisc_semantic_search"

        return {
            "ok": True,
            "intent": intent,
            "answer": answer,
            "citations": citations,
            "toolCalls": [{"name": tool, "readOnly": True}],
            "data": result,
            "guardrails": {
                "readOnly": True,
                "mayPublish": False,
                "mayWaive": False,
                "maySupersede": False,
                "regulatoryComplianceClaimed": False,
            },
        }
