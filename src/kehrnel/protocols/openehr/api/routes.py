"""openEHR protocol API routes scaffold."""
from fastapi import APIRouter, Body, HTTPException, Request
from kehrnel.protocols.openehr.aql.parse import parse_aql

router = APIRouter()


@router.post("/v1/openehr/query", include_in_schema=False)
async def query(env_id: str, request: Request, body: dict = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    aql = body.get("aql") or ""
    ir = parse_aql(aql)
    res = await rt.dispatch(env_id, "query", {"protocol": "openehr", "query": ir.to_dict()})
    return {"ok": True, "result": res}
