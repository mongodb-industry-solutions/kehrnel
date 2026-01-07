"""AQL parsing to normalized IR (minimal stub)."""
from __future__ import annotations

from .ir import AqlQueryIR, AqlPredicate


def parse_aql(aql_text: str) -> AqlQueryIR:
    """
    Minimal stub parser:
    - if "ehr_id" present, mark scope=patient
    - else scope=cross_patient
    - predicates: naive capture of ehr_id equality if present
    - select: naive capture of 'select <path> as <alias>'
    """
    text = aql_text.lower()
    scope = "cross_patient"
    predicates = []
    select = []
    if "ehr_id" in text:
        scope = "patient"
        # naive extraction: look for ehr_id = 'value'
        if "=" in text:
            try:
                part = text.split("ehr_id", 1)[1]
                val = part.split("=")[1].split()[0].strip(" '\"")
                predicates.append(AqlPredicate(path="ehr_id", op="eq", value=val))
            except Exception:
                pass
    # very small select parsing
    if "select" in text and "from" in text:
        try:
            sel_part = text.split("select", 1)[1].split("from", 1)[0]
            # support comma separated aliases path as alias
            for chunk in sel_part.split(","):
                c = chunk.strip()
                if " as " in c:
                    path, alias = c.split(" as ", 1)
                    select.append({"path": path.strip(), "alias": alias.strip()})
        except Exception:
            pass
    limit = None
    offset = None
    sort = None
    if "limit" in text:
        try:
            limit = int(text.split("limit", 1)[1].split()[0])
        except Exception:
            pass
    if "offset" in text:
        try:
            offset = int(text.split("offset", 1)[1].split()[0])
        except Exception:
            pass
    if "order by" in text:
        try:
            order_part = text.split("order by", 1)[1].split()[0:2]
            if order_part:
                field = order_part[0]
                direction = -1 if len(order_part) > 1 and order_part[1].startswith("desc") else 1
                sort = {field: direction}
        except Exception:
            pass
    return AqlQueryIR(scope=scope, predicates=predicates, select=select, limit=limit, offset=offset, sort=sort)
