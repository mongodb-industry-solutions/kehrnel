
# x12_contextobjects_min.py
# Minimal copy of the two main functions from the notebook for reuse.
from typing import List, Dict, Any, Optional, Tuple
import re

def detect_separators(edi_text: str):
    isa_line = None
    for seg in edi_text.split("~"):
        if seg.startswith("ISA"):
            isa_line = seg; break
    if not isa_line: return "*","^",":"
    element = isa_line[3]; parts = isa_line.split(element)
    repetition = parts[11] if len(parts)>11 else ">"; component = isa_line[-1]
    return element, repetition, component

def parse_segments(edi_text: str):
    elem, rep, comp = detect_separators(edi_text)
    raw = [s for s in edi_text.replace("\n","").split("~") if s]
    return [seg.split(elem) for seg in raw]

def infer_837_variant(gs08: str) -> str:
    u = (gs08 or "").upper()
    if "X222" in u: return "837P"
    if "X224" in u: return "837D"
    if "X223" in u: return "837I"
    return "837"

def x12_to_lossless_transactions(edi_text: str, tenant_id: str="demo") -> List[Dict[str,Any]]:
    segs = parse_segments(edi_text)
    isa = next((s for s in segs if s[0]=="ISA"), None)
    gs  = next((s for s in segs if s[0]=="GS"), None)
    tx_docs = []; i=0
    while i < len(segs):
        if segs[i][0] == "ST":
            st = segs[i]; j=i+1
            while j < len(segs) and segs[j][0] != "SE": j+=1
            se = segs[j] if j < len(segs) else None
            bht = next((s for s in segs[i:j] if s[0]=="BHT"), None)
            envelope = {}; 
            if isa: envelope["isa"] = isa
            if gs:  envelope["gs"]  = gs
            envelope["st"] = st
            if bht: envelope["bht"] = bht
            control = {"isa13": isa[13] if isa and len(isa)>13 else None,
                       "gs06":  gs[6]   if gs  and len(gs)>6  else None,
                       "st02":  st[2]   if len(st)>2 else None}
            gs08 = gs[8] if gs and len(gs)>8 else (st[3] if len(st)>3 else "")
            tx_type = infer_837_variant(gs08) if st[1]=="837" else st[1]
            tx_segments = [{"tag": segs[k][0], "els": segs[k]} for k in range(i, (j+1 if se else i+1))]
            tx_docs.append({"tenantId": tenant_id, "type": tx_type, "control": control, "envelope": envelope, "segments": tx_segments})
            i = j+1 if se else i+1
        else:
            i+=1
    if not tx_docs:
        gs08 = gs[8] if gs and len(gs)>8 else ""
        tx_docs.append({"tenantId":tenant_id, "type": infer_837_variant(gs08),
                        "control": {"isa13": isa[13] if isa and len(isa)>13 else None,
                                    "gs06": gs[6] if gs and len(gs)>6 else None,
                                    "st02": None},
                        "envelope": {k:v for k,v in (("isa",isa),("gs",gs)) if v},
                        "segments": [{"tag": s[0], "els": s} for s in segs]})
    return tx_docs

def map_lossless_to_contextobject(tx: Dict[str,Any]) -> Dict[str,Any]:
    # For brevity, use the notebook implementation in your environment.
    return {"claim": {"claimType": tx.get("type")}, "context_nodes": []}
