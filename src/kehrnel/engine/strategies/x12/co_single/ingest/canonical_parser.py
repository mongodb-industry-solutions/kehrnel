from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def detect_separators(edi_text: str) -> Tuple[str, str, str, str]:
    element = "*"
    repetition = "^"
    component = ":"
    segment_term = "~"

    isa_idx = edi_text.find("ISA")
    if isa_idx == -1:
        return element, repetition, component, segment_term

    element = edi_text[isa_idx + 3]
    tail = edi_text[isa_idx:]

    parts = tail.split(element)
    if len(parts) >= 17:
        isa_segment = element.join(parts[:17])
        if len(tail) > len(isa_segment):
            segment_term = tail[len(isa_segment)]
        isa_parts = isa_segment.split(element)
        if len(isa_parts) > 11 and isa_parts[11]:
            repetition = isa_parts[11]
        if len(isa_parts) > 16 and isa_parts[16]:
            component = isa_parts[16]
    return element, repetition, component, segment_term


def split_segments(edi_text: str, segment_term: str) -> List[str]:
    clean = edi_text.replace("\r", "").replace("\n", "")
    return [s for s in clean.split(segment_term) if s != ""]


def _infer_transaction_type(st: Optional[List[str]], gs: Optional[List[str]]) -> Optional[str]:
    if not st:
        return None
    st01 = st[1] if len(st) > 1 else None
    impl = None
    if gs and len(gs) > 8:
        impl = gs[8]
    if len(st) > 3 and st[3]:
        impl = st[3]

    if st01 == "837":
        if impl and "005010X224" in impl:
            return "837D"
        if impl and "005010X222" in impl:
            return "837I"
        if impl and "005010X221" in impl:
            return "837P"
        return "837"
    return st01


def parse_x12_to_transaction(
    edi_text: str,
    tenant_id: Optional[str] = None,
    raw_name: Optional[str] = None,
    received_at: Optional[str] = None,
) -> Dict[str, Any]:
    elem, rep, comp, seg_term = detect_separators(edi_text)
    segments_raw = split_segments(edi_text, seg_term)

    segments: List[Dict[str, Any]] = []
    isa_vals = gs_vals = st_vals = bht_vals = None
    for idx, seg in enumerate(segments_raw):
        els = seg.split(elem)
        if not els:
            continue
        tag = els[0]
        if tag == "ISA":
            isa_vals = els
        elif tag == "GS":
            gs_vals = els
        elif tag == "ST":
            st_vals = els
        elif tag == "BHT":
            bht_vals = els
        segments.append(
            {
                "tag": tag,
                "els": els,
                "pos": {"i": idx},
                "raw": seg,
            }
        )

    control = {
        "isa13": isa_vals[13] if isa_vals and len(isa_vals) > 13 else None,
        "gs06": gs_vals[6] if gs_vals and len(gs_vals) > 6 else None,
        "st02": st_vals[2] if st_vals and len(st_vals) > 2 else None,
    }

    envelope = {}
    if isa_vals:
        envelope["isa"] = isa_vals
    if gs_vals:
        envelope["gs"] = gs_vals
    if st_vals:
        envelope["st"] = st_vals
    if bht_vals:
        envelope["bht"] = bht_vals

    tran_type = _infer_transaction_type(st_vals, gs_vals)
    if received_at is None:
        received_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    return {
        "tenantId": tenant_id,
        "type": tran_type,
        "receivedAt": received_at,
        "separators": {
            "element": elem,
            "component": comp,
            "repetition": rep,
            "segment": seg_term,
        },
        "control": control,
        "envelope": envelope,
        "segments": segments,
        "rawPtr": raw_name,
    }
