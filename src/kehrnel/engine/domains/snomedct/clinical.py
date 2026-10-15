"""Deterministic clinical mention extraction, context, and grounding review.

The strategy deliberately separates three concerns:

1. **Extraction** — find the clinical phrases a note mentions.
2. **Context** — read negation, family history, temporality, and plan cues that
   change what a mention asserts.
3. **Grounding** — hand each phrase to the term sidecar and score the returned
   candidates against the phrase and its context.

None of these steps invents terminology: candidate generation always comes from
the canonical release. An LLM may be used for step 1 (and, in
``llm-ground``, for adjudicating between already-retrieved candidates) but it is
never the code authority.
"""

from __future__ import annotations

import re
from typing import Any

from .model import normalize_text

# ---------------------------------------------------------------------------
# Lexicons
# ---------------------------------------------------------------------------

STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "with", "without",
    "de", "del", "la", "las", "el", "los", "of", "on", "to", "from", "at", "by",
    "into", "under", "en", "y", "o", "sin", "con", "para", "por",
}

NEGATION_PATTERNS = [
    r"\bno evidence of\b", r"\bno\b", r"\bnot\b", r"\bwithout\b", r"\bdenies\b",
    r"\bdenied\b", r"\bnegative for\b", r"\babsence of\b", r"\bfree of\b",
    r"\bruled out\b", r"\brule[ds]? out\b", r"\bexcluded\b", r"\bexclude[ds]?\b",
    r"\bsin evidencia de\b", r"\bsin\b", r"\bniega\b", r"\bdescartad[oa]s?\b",
    r"\bexcluid[oa]s?\b",
]

HISTORY_PATTERNS = [
    r"\bhistory of\b", r"\bhx of\b", r"\bpast medical history\b",
    r"\bantecedentes\b", r"\bprevious\b", r"\bprior\b",
]

FAMILY_PATTERNS = [
    r"\bfamily history\b", r"\bfamily hx\b", r"\bmother with\b", r"\bfather with\b",
    r"\bmadre con\b", r"\bpadre con\b", r"\bantecedentes familiares\b",
]

PLAN_PATTERNS = [
    r"\bplan\b", r"\bcontinue\b", r"\brepeat\b", r"\bfollow[\s-]?up\b",
    r"\bmonitor\b", r"\bstart\b", r"\bschedule\b", r"\bcontinuar\b", r"\brepetir\b",
    r"\bseguimiento\b", r"\bcontrol\b",
]

HYPOTHETICAL_PATTERNS = [
    r"\bsuspected\b", r"\bpossible\b", r"\bprobable\b", r"\brule out\b",
    r"\bsospecha\b", r"\bposible\b", r"\bprobable\b",
]

RELATION_SIGNAL_DEFS = [
    ("due-to", [r"\bdue to\b", r"\bsecondary to\b", r"\bdebido a\b", r"\bsecundaria a\b"]),
    ("associated-with", [r"\bassociated with\b", r"\bwith\b", r"\bcon\b"]),
    ("caused-by", [r"\bcaused by\b", r"\bresulting from\b"]),
]

LOW_VALUE_ANCHOR_TOKENS = {
    "patient", "patients", "assessment", "current", "review", "clinical", "note",
    "notes", "summary", "mother", "father", "today", "active", "prior", "previous",
    "continue", "repeat", "monitor", "schedule", "follow", "followup", "follow-up",
    "history", "family", "plan",
    "paciente", "pacientes", "madre", "padre", "hoy", "activo", "activa", "previo",
    "previa", "continuar", "repetir", "seguimiento", "control", "familia", "familiar",
    "familiares", "antecedentes", "resumen", "evaluacion", "revision", "nota", "notas",
    "clinico", "clinica", "actual",
}

WORKFLOW_TERM_PATTERNS = [
    r"\bfollow[\s-]?up\b", r"\breview\b", r"\bevaluation\b", r"\bscreening\b",
    r"\bmonitoring\b", r"\bcheck\b",
]

CANDIDATE_RELATION_PATTERNS = [
    r"\bdue to\b", r"\bassociated with\b", r"\bwith\b", r"\bwithout\b",
    r"\bfollowing\b", r"\bsecondary to\b", r"\bcaused by\b", r"\bresulting from\b",
]

LATERALITY_TOKENS = {"left", "right", "bilateral", "unilateral"}

POPULATION_CONTEXT_TOKENS = {
    "prematurity", "premature", "neonatal", "neonate", "maternal", "pregnancy",
    "congenital",
}

STATUS_QUALIFIER_TOKENS = {
    "uncontrolled", "controlled", "poorly", "well", "stable", "unstable", "severe",
    "mild", "moderate", "advanced",
}

NEUTRAL_CANDIDATE_EXTRA_TOKENS = {"mellitus", "type", "finding", "disorder", "disease"}

ANCHOR_BREAK_TOKENS = {token for token in STOPWORDS if token not in {"with", "without", "con", "sin"}}

MAX_PHRASE_WINDOWS_PER_SEGMENT = 2
DEFAULT_MAX_PHRASES = 20

ASSERTIONS = ("present", "absent", "suspected", "planned", "historical", "family-history")
CONTEXT_TYPES = ("current", "negated", "plan", "hypothetical", "history", "family-history")

_IGNORECASE = re.IGNORECASE
_COMPILED = {
    "negation": [re.compile(pattern, _IGNORECASE) for pattern in NEGATION_PATTERNS],
    "history": [re.compile(pattern, _IGNORECASE) for pattern in HISTORY_PATTERNS],
    "family": [re.compile(pattern, _IGNORECASE) for pattern in FAMILY_PATTERNS],
    "plan": [re.compile(pattern, _IGNORECASE) for pattern in PLAN_PATTERNS],
    "hypothetical": [re.compile(pattern, _IGNORECASE) for pattern in HYPOTHETICAL_PATTERNS],
    "relation": [(value, [re.compile(pattern, _IGNORECASE) for pattern in patterns]) for value, patterns in RELATION_SIGNAL_DEFS],
}
_WORKFLOW_RE = [re.compile(pattern, _IGNORECASE) for pattern in WORKFLOW_TERM_PATTERNS]
_RELATION_TERM_RE = [re.compile(pattern, _IGNORECASE) for pattern in CANDIDATE_RELATION_PATTERNS]
_SECTION_RE = re.compile(r"(?:^|[\n\r]+|(?<=[.!?])\s+)([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ /()_-]{0,40}):[ \t]*", re.MULTILINE)
_SENTENCE_RE = re.compile(r"[^\n.!?]+(?:[.!?]+)?")
_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)


def _matches(text: str, patterns: list[re.Pattern[str]]) -> re.Match[str] | None:
    """Return the earliest match of any pattern, or ``None``."""
    earliest: re.Match[str] | None = None
    for pattern in patterns:
        match = pattern.search(text)
        if match and (earliest is None or match.start() < earliest.start()):
            earliest = match
    return earliest


# ---------------------------------------------------------------------------
# Text segmentation
# ---------------------------------------------------------------------------


def build_section_ranges(text: str) -> list[dict[str, Any]]:
    """Split a note into labelled sections, falling back to one ``Note`` block."""
    normalized = str(text or "").replace("\r\n", "\n")
    matches = list(_SECTION_RE.finditer(normalized))
    if not matches:
        return [{"title": "Note", "slug": "note", "start": 0, "contentStart": 0, "end": len(normalized), "text": normalized}]

    sections: list[dict[str, Any]] = []
    first_start = matches[0].start()
    if first_start > 0:
        sections.append({"title": "Note", "slug": "note", "start": 0, "contentStart": 0, "end": first_start, "text": normalized[:first_start].strip()})

    for index, match in enumerate(matches):
        title = str(match.group(1) or "Section").strip()
        start = match.start()
        content_start = match.end()
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        sections.append(
            {
                "title": title,
                "slug": normalize_text(title).replace(" ", "-") or f"section-{index + 1}",
                "start": start,
                "contentStart": content_start,
                "end": next_start,
                "text": normalized[content_start:next_start].strip(),
            }
        )
    return [section for section in sections if section["end"] > section["start"]]


def build_sentence_windows(text: str, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten sections into sentence-level windows with absolute offsets."""
    windows: list[dict[str, Any]] = []
    for section in sections:
        segment = str(text or "")[section["contentStart"]:section["end"]]
        matches = list(_SENTENCE_RE.finditer(segment))
        if not matches and segment.strip():
            leading = len(segment) - len(segment.lstrip())
            sentence = segment.strip()
            start = section["contentStart"] + leading
            windows.append({"section": section["title"], "sectionSlug": section["slug"], "text": sentence, "start": start, "end": start + len(sentence)})
            continue
        for match in matches:
            raw = match.group(0)
            leading = len(raw) - len(raw.lstrip())
            sentence = raw.strip()
            if not sentence:
                continue
            start = section["contentStart"] + match.start() + leading
            windows.append({"section": section["title"], "sectionSlug": section["slug"], "text": sentence, "start": start, "end": start + len(sentence)})
    return windows


def tokenize_with_offsets(text: str, base_offset: int = 0) -> list[dict[str, Any]]:
    """Tokenize preserving absolute character offsets (for evidence spans)."""
    return [
        {
            "value": match.group(0),
            "normalized": normalize_text(match.group(0)),
            "start": base_offset + match.start(),
            "end": base_offset + match.end(),
        }
        for match in _TOKEN_RE.finditer(str(text or ""))
    ]


# ---------------------------------------------------------------------------
# Context signals
# ---------------------------------------------------------------------------


def _build_signal(
    *,
    cue_type: str,
    value: str,
    source: str,
    text: str | None,
    section_title: str | None,
    sentence_start: int = 0,
    start: int | None = None,
    end: int | None = None,
) -> dict[str, Any]:
    signal_text = str(text or section_title or "").strip()
    return {
        "cueType": cue_type,
        "value": value,
        "source": source,
        "text": signal_text or None,
        "start": sentence_start + start if start is not None else None,
        "end": sentence_start + end if end is not None else None,
    }


def extract_evidence_signals(section: str, sentence_text: str, sentence_start: int = 0) -> list[dict[str, Any]]:
    """Detect the clinical cue words that scope a mention's assertion."""
    section_title = str(section or "")
    sentence = str(sentence_text or "")
    signals: list[dict[str, Any]] = []

    family = _matches(sentence, _COMPILED["family"])
    history = _matches(sentence, _COMPILED["history"])
    plan = _matches(sentence, _COMPILED["plan"])
    hypothetical = _matches(sentence, _COMPILED["hypothetical"])
    negation = _matches(sentence, _COMPILED["negation"])

    if _matches(section_title, _COMPILED["family"]) and not family:
        signals.append(_build_signal(cue_type="family-history", value="family-history", source="section", text=None, section_title=section_title))
    if family:
        signals.append(_build_signal(cue_type="family-history", value="family-history", source="sentence", text=family.group(0), section_title=None, sentence_start=sentence_start, start=family.start(), end=family.end()))

    if _matches(section_title, [re.compile(r"\bhistory\b", _IGNORECASE), re.compile(r"\bantecedentes\b", _IGNORECASE)]) and not history:
        signals.append(_build_signal(cue_type="history", value="historical", source="section", text=None, section_title=section_title))
    if history:
        signals.append(_build_signal(cue_type="history", value="historical", source="sentence", text=history.group(0), section_title=None, sentence_start=sentence_start, start=history.start(), end=history.end()))

    if _matches(section_title, [re.compile(r"\bplan\b", _IGNORECASE), re.compile(r"\bseguimiento\b", _IGNORECASE)]) and not plan:
        signals.append(_build_signal(cue_type="plan", value="planned", source="section", text=None, section_title=section_title))
    if plan:
        signals.append(_build_signal(cue_type="plan", value="planned", source="sentence", text=plan.group(0), section_title=None, sentence_start=sentence_start, start=plan.start(), end=plan.end()))

    if negation:
        signals.append(_build_signal(cue_type="negation", value="absent", source="sentence", text=negation.group(0), section_title=None, sentence_start=sentence_start, start=negation.start(), end=negation.end()))
    if hypothetical:
        signals.append(_build_signal(cue_type="certainty", value="suspected", source="sentence", text=hypothetical.group(0), section_title=None, sentence_start=sentence_start, start=hypothetical.start(), end=hypothetical.end()))

    for value, patterns in _COMPILED["relation"]:
        match = _matches(sentence, patterns)
        if not match:
            continue
        signals.append(_build_signal(cue_type="relation", value=value, source="sentence", text=match.group(0), section_title=None, sentence_start=sentence_start, start=match.start(), end=match.end()))

    return signals


def derive_context_from_signals(signals: list[dict[str, Any]]) -> dict[str, str]:
    """Collapse cue signals into the ``(assertion, contextType, experiencer)`` triple."""
    items = signals or []
    cue_types = {signal.get("cueType") for signal in items}
    if "family-history" in cue_types:
        return {"assertion": "family-history", "contextType": "family-history", "experiencer": "family"}
    if "negation" in cue_types:
        return {"assertion": "absent", "contextType": "negated", "experiencer": "patient"}
    if "plan" in cue_types:
        return {"assertion": "planned", "contextType": "plan", "experiencer": "patient"}
    if "certainty" in cue_types:
        return {"assertion": "suspected", "contextType": "hypothetical", "experiencer": "patient"}
    if "history" in cue_types:
        return {"assertion": "historical", "contextType": "history", "experiencer": "patient"}
    return {"assertion": "present", "contextType": "current", "experiencer": "patient"}


def llm_mention_context(mention: dict[str, Any]) -> dict[str, str]:
    """Map an LLM mention's context fields onto the deterministic triple."""
    assertion = str(mention.get("assertion") or "present").lower()
    subject = str(mention.get("subject") or "patient").lower()
    temporality = str(mention.get("temporality") or "current").lower()
    if assertion == "absent":
        return {"assertion": "absent", "contextType": "negated", "experiencer": "patient"}
    if subject == "family":
        return {"assertion": "family-history", "contextType": "family-history", "experiencer": "family"}
    if assertion == "planned":
        return {"assertion": "planned", "contextType": "plan", "experiencer": "patient"}
    if assertion == "suspected":
        return {"assertion": "suspected", "contextType": "hypothetical", "experiencer": "patient"}
    if temporality == "historical":
        return {"assertion": "historical", "contextType": "history", "experiencer": "patient"}
    return {"assertion": "present", "contextType": "current", "experiencer": "patient"}


# ---------------------------------------------------------------------------
# Phrase candidates
# ---------------------------------------------------------------------------


def score_phrase_candidate(normalized_phrase: str, token_count: int, context_type: str) -> float:
    """Rank how likely a phrase is to be a real clinical anchor."""
    tokens = [token for token in normalize_text(normalized_phrase).split(" ") if token]
    long_tokens = sum(1 for token in tokens if len(token) >= 5)
    low_value = sum(1 for token in tokens if token in LOW_VALUE_ANCHOR_TOKENS)
    digit_tokens = sum(1 for token in tokens if token.isdigit())

    if 2 <= token_count <= 4:
        score = 24.0
    elif token_count == 1:
        score = 10.0
    else:
        score = max(16 - (token_count - 4) * 3, 0)

    score += long_tokens * 6
    score += 2 if digit_tokens else 0
    score -= low_value * 10

    if context_type == "current":
        score += 4
    if context_type in {"history", "negated"}:
        score += 2
    if context_type == "plan":
        score -= 4
    if context_type == "family-history":
        score -= 30

    if len(tokens) == 1 and tokens[0] and len(tokens[0]) < 6:
        score -= 8
    if tokens and all(token in LOW_VALUE_ANCHOR_TOKENS for token in tokens):
        score -= 40
    return score


def _token_overlaps_signal(token: dict[str, Any], signals: list[dict[str, Any]]) -> bool:
    start = token.get("start")
    end = token.get("end")
    if start is None or end is None:
        return False
    for signal in signals or []:
        signal_start = signal.get("start")
        signal_end = signal.get("end")
        if signal_start is None or signal_end is None:
            continue
        if start < signal_end and end > signal_start:
            return True
    return False


def _build_sentence_segments(sentence: dict[str, Any], signals: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Cut a sentence into clinical-term runs, breaking on cue words and stopwords."""
    tokens = [token for token in tokenize_with_offsets(sentence["text"], sentence["start"]) if token["normalized"]]
    segments: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for token in tokens:
        normalized = token["normalized"]
        if _token_overlaps_signal(token, signals) or normalized in ANCHOR_BREAK_TOKENS or normalized in LOW_VALUE_ANCHOR_TOKENS:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(token)
    if current:
        segments.append(current)
    return [segment for segment in segments if segment]


def _build_segment_windows(segment: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Pick the highest-scoring n-gram windows inside one segment."""
    if not segment:
        return []
    windows: list[tuple[float, list[dict[str, Any]]]] = []
    max_window = min(len(segment), 4)
    for size in range(max_window, 0, -1):
        for index in range(0, len(segment) - size + 1):
            tokens = segment[index:index + size]
            normalized = normalize_text(" ".join(token["value"] for token in tokens))
            if not normalized:
                continue
            score = score_phrase_candidate(normalized, len(tokens), "current")
            score += 3 if index + size == len(segment) else 0
            score -= index
            windows.append((score, tokens))
    windows.sort(key=lambda item: (-item[0], -len(item[1])))
    return [tokens for _, tokens in windows[:MAX_PHRASE_WINDOWS_PER_SEGMENT]]


def _push_phrase_entry(
    bucket: list[dict[str, Any]],
    seen: set[str],
    *,
    text: str,
    start: int,
    end: int,
    token_count: int,
    sentence: dict[str, Any],
    signals: list[dict[str, Any]],
    context: dict[str, str],
) -> bool:
    phrase = str(text or "")[start:end].strip()
    normalized = normalize_text(phrase)
    if not phrase or len(normalized) < 3:
        return False
    if token_count == 1 and len(normalized) < 5:
        return False
    key = f"{start}:{end}:{normalized}"
    if key in seen:
        return False
    seen.add(key)
    bucket.append(
        {
            "phrase": phrase,
            "normalizedPhrase": normalized,
            "start": start,
            "end": end,
            "tokenCount": token_count,
            "section": sentence["section"],
            "sectionSlug": sentence["sectionSlug"] or normalize_text(sentence["section"]).replace(" ", "-") or "note",
            "sentence": sentence["text"],
            "sentenceStart": sentence["start"],
            "sentenceEnd": sentence["end"],
            "evidenceSignals": signals,
            "anchorRole": "clinical-term",
            "candidateScore": score_phrase_candidate(normalized, token_count, context["contextType"]),
            **context,
        }
    )
    return True


def extract_candidate_phrases(text: str, max_phrases: int = DEFAULT_MAX_PHRASES) -> dict[str, Any]:
    """Deterministic clinical phrase extraction with per-phrase context."""
    sections = build_section_ranges(text)
    sentences = build_sentence_windows(text, sections)
    phrases: list[dict[str, Any]] = []
    seen: set[str] = set()

    for sentence in sentences:
        signals = extract_evidence_signals(sentence["section"], sentence["text"], sentence["start"])
        context = derive_context_from_signals(signals)
        for segment in _build_sentence_segments(sentence, signals):
            windows = [segment] if len(segment) <= 4 else _build_segment_windows(segment)
            for window in windows:
                if not window:
                    continue
                _push_phrase_entry(
                    phrases,
                    seen,
                    text=text,
                    start=window[0]["start"],
                    end=window[-1]["end"],
                    token_count=len(window),
                    sentence=sentence,
                    signals=signals,
                    context=context,
                )

    phrases.sort(key=lambda entry: (-entry["candidateScore"], -entry["tokenCount"], entry["start"]))
    return {"sections": sections, "phrases": phrases[:max_phrases]}


def build_llm_phrase_entries(text: str, mentions: list[dict[str, Any]], max_phrases: int = DEFAULT_MAX_PHRASES) -> list[dict[str, Any]]:
    """Turn LLM-extracted mentions into the same phrase-entry shape."""
    lower_text = str(text or "").lower()
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for mention in (mentions or [])[:max_phrases]:
        if not isinstance(mention, dict):
            continue
        phrase = str(mention.get("phrase") or "").strip()
        normalized = normalize_text(phrase)
        if not normalized:
            continue
        dedupe_key = f"{normalized}|{mention.get('subject') or 'patient'}|{mention.get('assertion') or 'present'}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        needle = str(mention.get("verbatim") or phrase).lower()
        start = lower_text.find(needle) if needle else -1
        if start < 0:
            start = lower_text.find(normalized)
        span_length = len(mention.get("verbatim") or phrase)
        safe_start = start if start >= 0 else 0
        end = safe_start + span_length
        line_start = str(text or "").rfind("\n", 0, start) + 1 if start >= 0 else 0
        line_end = str(text or "").find("\n", start) if start >= 0 else -1
        if line_end < 0:
            line_end = len(str(text or ""))

        context = llm_mention_context(mention)
        token_count = len([token for token in normalized.split(" ") if token])
        entries.append(
            {
                "phrase": phrase,
                "normalizedPhrase": normalized,
                "start": safe_start,
                "end": end,
                "tokenCount": token_count,
                "section": "note",
                "sectionSlug": "note",
                "sentence": str(text or "")[line_start:line_end] or phrase,
                "sentenceStart": line_start,
                "sentenceEnd": line_end,
                "evidenceSignals": _synth_llm_signals(context, safe_start, end),
                "anchorRole": "clinical-term",
                "candidateScore": score_phrase_candidate(normalized, token_count, context["contextType"]),
                "verbatim": mention.get("verbatim") or phrase,
                "llmSection": mention.get("section") or "other",
                **context,
            }
        )
    return entries


def _synth_llm_signals(context: dict[str, str], start: int, end: int) -> list[dict[str, Any]]:
    """Synthesize the cue signals an LLM context implies, so evidence graphs match."""
    mapping = {
        "negated": ("negation", "absent", "negated"),
        "family-history": ("family-history", "family", "family history"),
        "history": ("history", "historical", "history"),
        "plan": ("plan", "planned", "plan"),
        "hypothetical": ("certainty", "suspected", "suspected"),
    }
    entry = mapping.get(context.get("contextType", ""))
    if not entry:
        return []
    cue_type, value, text = entry
    return [{"cueType": cue_type, "value": value, "text": text, "start": start, "end": end, "source": "llm"}]


# ---------------------------------------------------------------------------
# Candidate scoring
# ---------------------------------------------------------------------------


def infer_match_mode(candidate: dict[str, Any], normalized_phrase: str) -> str:
    normalized_term = normalize_text(candidate.get("term"))
    if not normalized_term:
        return "lexical"
    if normalized_term == normalized_phrase:
        return "exact-span"
    if normalized_phrase in normalized_term or normalized_term in normalized_phrase:
        return "lexical-contains"
    return "lexical-broad"


def summarize_candidate(candidate: dict[str, Any], normalized_phrase: str) -> dict[str, Any]:
    """Normalize a sidecar search row into the grounding candidate contract."""
    if not candidate:
        return {}
    score = candidate.get("score")
    return {
        "conceptId": str(candidate.get("conceptId") or ""),
        "term": candidate.get("term"),
        "fsn": candidate.get("fsn"),
        "semanticTag": candidate.get("semanticTag"),
        "languageCode": candidate.get("languageCode"),
        "ancestorIds": [str(value) for value in (candidate.get("ancestorIds") or [])],
        "active": candidate.get("conceptActive", True) is not False,
        "score": float(score) if isinstance(score, (int, float)) else 0.0,
        "matchedBy": infer_match_mode(candidate, normalized_phrase),
        "matchedText": candidate.get("matchedText") or candidate.get("term"),
        "matchTier": candidate.get("matchTier"),
    }


def analyze_grounding_candidate(entry: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    """Score how well one retrieved concept fits a phrase *in its context*."""
    normalized_phrase = normalize_text(entry.get("phrase"))
    normalized_term = normalize_text(candidate.get("term"))
    normalized_matched = normalize_text(candidate.get("matchedText") or candidate.get("term"))
    normalized_tag = normalize_text(candidate.get("semanticTag"))
    query_tokens = [token for token in normalized_phrase.split(" ") if token]
    query_token_set = set(query_tokens)
    candidate_tokens = [token for token in normalized_term.split(" ") if token]
    informative_extra = [
        token
        for token in candidate_tokens
        if token not in query_token_set
        and token not in STOPWORDS
        and len(token) > 3
        and token not in NEUTRAL_CANDIDATE_EXTRA_TOKENS
    ]
    laterality = [token for token in informative_extra if token in LATERALITY_TOKENS]
    population = [token for token in informative_extra if token in POPULATION_CONTEXT_TOKENS]
    status_qualifier = [token for token in informative_extra if token in STATUS_QUALIFIER_TOKENS]
    workflow_like = any(pattern.search(normalized_term) for pattern in _WORKFLOW_RE)
    relation_in_term = any(pattern.search(normalized_term) for pattern in _RELATION_TERM_RE)
    relation_cue_present = any(signal.get("cueType") == "relation" for signal in entry.get("evidenceSignals") or [])
    exact_term_match = normalized_term == normalized_phrase
    exact_matched_text = normalized_matched == normalized_phrase
    exact_match = exact_term_match or exact_matched_text
    match_tier = candidate.get("matchTier")
    match_tier = int(match_tier) if isinstance(match_tier, (int, float)) else 99

    contextual_mismatch = workflow_like and entry.get("contextType") != "plan"
    laterality_mismatch = bool(laterality) and not all(token in query_token_set for token in laterality)
    population_mismatch = bool(population) and not all(token in query_token_set for token in population)
    over_specific = entry.get("tokenCount") == 1 and bool(informative_extra) and not exact_term_match

    grounding_score = 0.0
    grounding_score += max(50 - match_tier * 8, -22)
    grounding_score += 34 if exact_term_match else 16 if exact_matched_text else 8 if normalized_phrase in normalized_term else 0
    grounding_score += 5 if candidate.get("preferred") or candidate.get("isPreferred") else 0
    grounding_score += min(float(candidate.get("termRank") or 0), 60) / 10
    grounding_score += min(float(candidate.get("score") or 0), 500) / 50

    if normalized_tag in {"disorder", "finding"}:
        grounding_score += 4
    elif normalized_tag:
        grounding_score -= 6

    if exact_term_match:
        penalty_per_token = 0
    elif exact_matched_text:
        penalty_per_token = 14 if entry.get("tokenCount") == 1 else 6
    else:
        penalty_per_token = 12 if entry.get("tokenCount") == 1 else 4.5
    grounding_score -= len(informative_extra) * penalty_per_token

    if contextual_mismatch:
        grounding_score -= 18
    if relation_cue_present and relation_in_term:
        grounding_score -= 16
    elif relation_in_term and not exact_match:
        grounding_score -= 6
    if laterality_mismatch:
        grounding_score -= 14 if entry.get("contextType") == "negated" else 9
    if population_mismatch:
        grounding_score -= 14
    if status_qualifier:
        grounding_score -= 18
    if entry.get("tokenCount") == 1 and informative_extra and not exact_term_match:
        grounding_score -= 14
    if entry.get("contextType") == "negated" and normalized_tag in {"disorder", "finding"}:
        grounding_score += 2

    return {
        "groundingScore": round(grounding_score, 4),
        "flags": {
            "contextualMismatch": contextual_mismatch,
            "relationDuplicated": relation_cue_present and relation_in_term,
            "lateralityMismatch": laterality_mismatch,
            "populationMismatch": population_mismatch,
            "statusQualifierMismatch": bool(status_qualifier),
            "overSpecific": over_specific,
        },
    }


def rerank_grounding_matches(entry: dict[str, Any], matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply the contextual grounding score, keeping retrieval order as tie-break."""
    ranked = []
    for index, candidate in enumerate(matches or []):
        analysis = analyze_grounding_candidate(entry, candidate)
        ranked.append({**candidate, "_groundingIndex": index, "_groundingScore": analysis["groundingScore"], "_groundingFlags": analysis["flags"]})
    ranked.sort(
        key=lambda row: (
            -float(row.get("_groundingScore") or 0),
            int(row.get("matchTier") if isinstance(row.get("matchTier"), (int, float)) else 99),
            0 if row.get("preferred") or row.get("isPreferred") else 1,
            -float(row.get("termRank") or 0),
            -float(row.get("score") or 0),
            int(row.get("_groundingIndex") or 0),
        )
    )
    return ranked


def _clamp01(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def build_review_outcome(entry: dict[str, Any], matches: list[dict[str, Any]]) -> dict[str, Any]:
    """Decide accepted / review / abstain for one grounded phrase."""
    ranked = rerank_grounding_matches(entry, matches)
    normalized_phrase = normalize_text(entry.get("phrase"))
    top = ranked[0] if ranked else None
    top_candidate = summarize_candidate(top, normalized_phrase) if top else None
    alternatives = [summarize_candidate(candidate, normalized_phrase) for candidate in ranked[1:4]]

    if not top_candidate:
        return {
            **entry,
            "status": "abstain",
            "evidenceType": "context",
            "confidence": 0.0,
            "confidencePct": 0,
            "marginToNext": 0.0,
            "decisionReasons": ["low-value-anchor"] if entry.get("strategy") == "skipped-anchor" else ["no-candidate"],
            "topCandidate": None,
            "alternatives": [],
        }

    second_score = float(ranked[1].get("_groundingScore") or 0) if len(ranked) > 1 else 0.0
    top_score = float(top.get("_groundingScore") or 0)
    normalized_top = normalize_text(top_candidate["term"])
    normalized_matched = normalize_text(top_candidate["matchedText"])
    match_tier = top_candidate.get("matchTier")
    match_tier = int(match_tier) if isinstance(match_tier, (int, float)) else 99
    exact_preferred = normalized_phrase == normalized_top
    exact_synonym = bool(normalized_matched) and normalized_matched == normalized_phrase
    flags = top.get("_groundingFlags") or {}
    compound_descendant = any(
        marker in normalized_top
        for marker in (
            f"due to {normalized_phrase}",
            f"associated with {normalized_phrase}",
            f"following {normalized_phrase}",
            f"secondary to {normalized_phrase}",
            f"with {normalized_phrase}",
        )
    )
    exact_match = normalized_phrase == normalized_top or (bool(normalized_matched) and normalized_matched == normalized_phrase)
    contains_match = (
        exact_match
        or (bool(normalized_matched) and (normalized_phrase in normalized_matched or normalized_matched in normalized_phrase))
        or normalized_phrase in normalized_top
        or normalized_top in normalized_phrase
    )
    margin = max(top_score - second_score, 0.0)
    confidence = 0.34
    confidence += min(float(top_candidate.get("score") or 0) * 0.22, 0.24)
    confidence += min(margin / 25, 0.22)
    confidence += 0.22 if exact_preferred else 0.12 if exact_synonym else 0.08 if contains_match else 0
    confidence += 0.18 if match_tier <= 2 else 0.1 if match_tier <= 4 else 0.04 if match_tier <= 6 else -0.08
    confidence -= 0.08 if entry.get("tokenCount") == 1 and not exact_match else 0
    confidence -= 0.0 if top_candidate["active"] else 0.3
    confidence -= 0.22 if flags.get("contextualMismatch") else 0
    confidence -= 0.16 if flags.get("relationDuplicated") else 0
    confidence -= 0.12 if flags.get("lateralityMismatch") else 0
    confidence -= 0.14 if flags.get("populationMismatch") else 0
    confidence -= 0.16 if flags.get("statusQualifierMismatch") else 0
    confidence -= (0.32 if entry.get("tokenCount") == 1 else 0.18) if flags.get("overSpecific") else 0

    reasons: list[str] = []
    if not top_candidate["active"]:
        reasons.append("inactive-candidate")
    if entry.get("tokenCount") == 1 and not exact_match:
        reasons.append("short-generic-span")
    if margin < 0.05 and alternatives:
        reasons.append("ambiguous-top-candidate")
    if not exact_match and not contains_match:
        reasons.append("lexical-mismatch")
    if match_tier >= 7:
        reasons.append("broad-lexical-match")
    if flags.get("contextualMismatch"):
        reasons.append("workflow-term-not-clinical-anchor")
    if flags.get("lateralityMismatch"):
        reasons.append("unattributed-laterality")
    if flags.get("populationMismatch"):
        reasons.append("population-mismatch")
    if flags.get("statusQualifierMismatch"):
        reasons.append("unattributed-clinical-qualifier")
    if flags.get("overSpecific"):
        reasons.append("over-specific-candidate")
    if compound_descendant or flags.get("relationDuplicated"):
        reasons.append("compound-descendant-match")

    confidence = _clamp01(confidence)
    clean = (
        not compound_descendant
        and not flags.get("contextualMismatch")
        and not flags.get("relationDuplicated")
        and not flags.get("lateralityMismatch")
        and not flags.get("populationMismatch")
        and not flags.get("statusQualifierMismatch")
    )

    status = "review"
    if not top_candidate["active"] or confidence < 0.42:
        status = "abstain"
    elif (exact_match or match_tier <= 2) and confidence >= 0.68 and margin >= 0.03 and (entry.get("tokenCount", 0) > 1 or exact_preferred) and clean and not flags.get("overSpecific"):
        status = "accepted"
    elif match_tier <= 4 and confidence >= 0.64 and margin >= 0.05 and entry.get("tokenCount", 0) >= 2 and clean:
        status = "accepted"

    if status == "accepted":
        reasons.append("exact-clinical-match" if exact_match else "high-confidence-match")
    elif status == "review" and not reasons:
        reasons.append("manual-review-required")
    elif status == "abstain" and not reasons:
        reasons.append("low-confidence")

    return {
        **entry,
        "status": status,
        "evidenceType": "direct" if status == "accepted" else "inferred",
        "confidence": confidence,
        "confidencePct": round(confidence * 100),
        "marginToNext": round(margin, 4),
        "decisionReasons": reasons,
        "topCandidate": top_candidate,
        "alternatives": alternatives,
    }


# ---------------------------------------------------------------------------
# Evidence graph
# ---------------------------------------------------------------------------


def build_evidence_graph(
    *,
    document_id: str,
    language_code: str,
    release_id: str,
    sections: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Assemble sections, sentences, spans, cues, mentions, and relations."""
    section_nodes = [
        {
            "sectionId": f"sec-{section.get('slug') or f'section-{index + 1}'}",
            "title": section.get("title"),
            "slug": section.get("slug"),
            "start": section.get("start"),
            "end": section.get("end"),
        }
        for index, section in enumerate(sections or [])
    ]
    section_by_slug = {str(node["slug"] or ""): node["sectionId"] for node in section_nodes}
    section_by_title = {str(node["title"] or ""): node["sectionId"] for node in section_nodes}
    sentence_nodes: dict[str, dict[str, Any]] = {}
    span_nodes: dict[str, dict[str, Any]] = {}
    cue_nodes: dict[str, dict[str, Any]] = {}
    mentions: list[dict[str, Any]] = []
    mentions_by_sentence: dict[str, list[dict[str, Any]]] = {}

    for index, entry in enumerate(results or []):
        span_start = entry.get("start")
        span_end = entry.get("end")
        anchor_text = str(entry.get("phrase") or "").strip()
        sentence_id = f"sent-{entry.get('sectionSlug') or 'note'}-{entry.get('sentenceStart', span_start if span_start is not None else index)}"
        section_id = section_by_slug.get(str(entry.get("sectionSlug") or "")) or section_by_title.get(str(entry.get("section") or "")) or "sec-note"
        mention_id = f"m-{index + 1}-{span_start if span_start is not None else index}-{span_end if span_end is not None else index}"
        anchor_span = {
            "spanId": f"sp-anchor-{mention_id}",
            "start": span_start,
            "end": span_end,
            "text": anchor_text,
            "normalizedText": entry.get("normalizedPhrase") or normalize_text(anchor_text),
            "spanRole": entry.get("anchorRole") or "clinical-term",
            "sentenceId": sentence_id,
            "sectionId": section_id,
        }
        candidate_concepts = [
            _graph_candidate(entry.get("topCandidate")),
            *[_graph_candidate(candidate) for candidate in entry.get("alternatives") or []],
        ]
        mention = {
            "mentionId": mention_id,
            "spanId": anchor_span["spanId"],
            "anchorSpan": anchor_span,
            "text": anchor_text,
            "sentenceId": sentence_id,
            "sectionId": section_id,
            "sectionTitle": entry.get("section") or "Note",
            "sentenceText": entry.get("sentence") or anchor_text,
            "candidateConcepts": [candidate for candidate in candidate_concepts if candidate],
            "appliedCueIds": [],
            "relationIds": [],
            "derivedAssertion": entry.get("assertion") or "present",
            "derivedExperiencer": entry.get("experiencer") or "patient",
            "derivedContextType": entry.get("contextType") or "current",
            "review": {
                "status": entry.get("status") or "review",
                "reasons": list(entry.get("decisionReasons") or []),
                "confidencePct": int(entry.get("confidencePct") or 0),
                "evidenceType": entry.get("evidenceType") or "inferred",
            },
            "groundedNodeId": f"gn-{mention_id}",
            "groundedConcept": _graph_candidate(entry.get("topCandidate")),
        }
        span_nodes[anchor_span["spanId"]] = anchor_span
        mentions.append(mention)
        sentence_nodes.setdefault(
            sentence_id,
            {
                "sentenceId": sentence_id,
                "sectionId": section_id,
                "sectionTitle": entry.get("section") or "Note",
                "text": entry.get("sentence") or anchor_text,
                "start": entry.get("sentenceStart", span_start),
                "end": entry.get("sentenceEnd", span_end),
            },
        )
        mentions_by_sentence.setdefault(sentence_id, []).append(mention)

        for signal in entry.get("evidenceSignals") or []:
            cue_start = signal.get("start")
            cue_end = signal.get("end")
            cue_key = "|".join(
                str(part)
                for part in (
                    sentence_id,
                    signal.get("cueType") or "cue",
                    signal.get("value") or "",
                    signal.get("source") or "",
                    cue_start if cue_start is not None else "na",
                    cue_end if cue_end is not None else "na",
                    str(signal.get("text") or "").strip(),
                )
            )
            if cue_key not in cue_nodes:
                cue_id = f"cue-{len(cue_nodes) + 1}"
                cue_span_id = f"sp-cue-{cue_id}"
                cue_nodes[cue_key] = {
                    "cueId": cue_id,
                    "cueType": signal.get("cueType") or "cue",
                    "value": signal.get("value") or signal.get("cueType") or "cue",
                    "text": signal.get("text"),
                    "source": signal.get("source") or "sentence",
                    "spanId": cue_span_id,
                    "span": {
                        "spanId": cue_span_id,
                        "start": cue_start,
                        "end": cue_end,
                        "text": signal.get("text"),
                        "spanRole": f"{signal.get('cueType') or 'cue'}-cue",
                        "sentenceId": sentence_id,
                        "sectionId": section_id,
                    },
                    "sentenceId": sentence_id,
                    "sectionId": section_id,
                    "scope": {"sentenceId": sentence_id, "targetMentionIds": []},
                }
                span_nodes[cue_span_id] = cue_nodes[cue_key]["span"]
            cue = cue_nodes[cue_key]
            if mention_id not in cue["scope"]["targetMentionIds"]:
                cue["scope"]["targetMentionIds"].append(mention_id)
            if cue["cueId"] not in mention["appliedCueIds"]:
                mention["appliedCueIds"].append(cue["cueId"])

    relations: list[dict[str, Any]] = []
    seen_relations: set[str] = set()
    for cue in [node for node in cue_nodes.values() if node["cueType"] == "relation"]:
        sentence_mentions = sorted(
            mentions_by_sentence.get(cue["sentenceId"], []),
            key=lambda item: (item["anchorSpan"].get("start") or 0, item["anchorSpan"].get("end") or 0),
        )
        if len(sentence_mentions) < 2:
            continue
        cue_start = cue["span"].get("start")
        cue_end = cue["span"].get("end")
        left = next((m for m in reversed(sentence_mentions) if cue_start is not None and (m["anchorSpan"].get("end") or 0) <= cue_start), None)
        right = next((m for m in sentence_mentions if cue_end is not None and (m["anchorSpan"].get("start") or 0) >= cue_end), None)
        source_mention = left or sentence_mentions[0]
        target_mention = right or next((m for m in sentence_mentions if m["mentionId"] != source_mention["mentionId"]), None)
        if not source_mention or not target_mention or source_mention["mentionId"] == target_mention["mentionId"]:
            continue
        relation_key = f"{cue['cueId']}|{source_mention['mentionId']}|{target_mention['mentionId']}|{cue['value']}"
        if relation_key in seen_relations:
            continue
        seen_relations.add(relation_key)
        relation_id = f"rel-{len(relations) + 1}"
        relations.append(
            {
                "relationId": relation_id,
                "sourceMentionId": source_mention["mentionId"],
                "targetMentionId": target_mention["mentionId"],
                "relationType": cue["value"] or "associated-with",
                "cueId": cue["cueId"],
                "cueSpanId": cue["spanId"],
                "text": cue.get("text"),
                "sentenceId": cue["sentenceId"],
                "sectionId": cue["sectionId"],
                "confidence": 0.88 if left and right else 0.72,
            }
        )
        for mention in (source_mention, target_mention):
            if relation_id not in mention["relationIds"]:
                mention["relationIds"].append(relation_id)
            if mention["mentionId"] not in cue["scope"]["targetMentionIds"]:
                cue["scope"]["targetMentionIds"].append(mention["mentionId"])

    grounded_nodes = [
        {
            "nodeId": mention["groundedNodeId"],
            "mentionId": mention["mentionId"],
            "conceptId": (mention["groundedConcept"] or {}).get("conceptId"),
            "term": (mention["groundedConcept"] or {}).get("term"),
            "semanticTag": (mention["groundedConcept"] or {}).get("semanticTag"),
            "assertion": mention["derivedAssertion"],
            "experiencer": mention["derivedExperiencer"],
            "contextType": mention["derivedContextType"],
            "evidence": {
                "anchorSpanId": mention["spanId"],
                "cueIds": mention["appliedCueIds"],
                "relationIds": mention["relationIds"],
                "sentenceId": mention["sentenceId"],
                "sectionId": mention["sectionId"],
            },
            "review": mention["review"],
        }
        for mention in mentions
    ]

    return {
        "document": {"documentId": document_id, "languageCode": language_code, "releaseId": release_id},
        "sections": section_nodes,
        "sentences": list(sentence_nodes.values()),
        "spans": list(span_nodes.values()),
        "cueObjects": list(cue_nodes.values()),
        "mentions": mentions,
        "relations": relations,
        "groundedNodes": grounded_nodes,
    }


def _graph_candidate(candidate: dict[str, Any] | None) -> dict[str, Any] | None:
    if not candidate:
        return None
    return {
        "conceptId": candidate.get("conceptId") or None,
        "term": candidate.get("term") or None,
        "semanticTag": candidate.get("semanticTag") or None,
        "matchedBy": candidate.get("matchedBy") or None,
        "matchedText": candidate.get("matchedText") or None,
        "score": candidate.get("score") if isinstance(candidate.get("score"), (int, float)) else None,
    }


# ---------------------------------------------------------------------------
# Principal diagnosis
# ---------------------------------------------------------------------------


def _section_weight(section: Any) -> float:
    label = str(section or "").strip().lower()
    if not label:
        return 0.0
    if re.search(r"(assessment|diagnos|impression|problem list|problemas|diagnostico|diagnòstic)", label):
        return 22.0
    if re.search(r"(chief complaint|motivo|hpi|present illness|consulta)", label):
        return 12.0
    if re.search(r"(exam|explor|findings|resultados)", label):
        return 6.0
    if re.search(r"(history|antecedentes)", label):
        return -8.0
    if re.search(r"(family|familiares)", label):
        return -18.0
    if re.search(r"(plan|follow|seguimiento|treatment|tratamiento|medication)", label):
        return -12.0
    return 2.0


def _assertion_weight(assertion: Any) -> float:
    value = str(assertion or "").lower()
    return {
        "present": 20.0,
        "suspected": 8.0,
        "historical": -10.0,
        "planned": -18.0,
        "absent": -30.0,
        "family-history": -36.0,
    }.get(value, 0.0)


def _evidence_weight(evidence_type: Any, decision: Any) -> float:
    evidence = str(evidence_type or "").lower()
    resolved = str(decision or "").lower()
    score = 0.0
    score += {"accepted": 16.0, "review": 4.0, "abstain": -12.0, "rejected": -22.0}.get(resolved, 0.0)
    score += {"direct": 10.0, "inferred": 5.0, "context": -10.0}.get(evidence, 0.0)
    return score


def _semantic_tag_weight(semantic_tag: Any, target_context: Any) -> float:
    tag = str(semantic_tag or "").lower()
    context = str(target_context or "").lower()
    if not tag:
        return 0.0
    if "condition" in context:
        if "disorder" in tag:
            return 18.0
        if "finding" in tag:
            return 8.0
        if "event" in tag:
            return 4.0
        if "morphologic abnormality" in tag:
            return -8.0
        if "body structure" in tag:
            return -18.0
        if "procedure" in tag:
            return -24.0
        if "regime" in tag or "therapy" in tag:
            return -24.0
        if "substance" in tag or "product" in tag:
            return -30.0
    if "observation" in context:
        if "finding" in tag:
            return 14.0
        if "observable entity" in tag:
            return 12.0
        if "procedure" in tag:
            return -10.0
    if "procedure" in context:
        if "procedure" in tag:
            return 18.0
        if "regime" in tag or "therapy" in tag:
            return 10.0
        if "disorder" in tag:
            return -12.0
    return 0.0


def compute_primary_diagnosis_candidates(
    entries: list[dict[str, Any]],
    *,
    target_context: str = "",
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Aggregate accepted mentions into ranked principal-diagnosis candidates."""
    buckets: dict[str, dict[str, Any]] = {}
    for entry in entries or []:
        candidate = entry.get("topCandidate")
        concept_id = str((candidate or {}).get("conceptId") or "").strip()
        if not concept_id:
            continue
        if candidate.get("active") is False:
            continue
        if str(entry.get("experiencer") or "patient").lower() != "patient":
            continue

        score = (
            float(entry.get("confidencePct") or 0) * 0.55
            + _section_weight(entry.get("section"))
            + _assertion_weight(entry.get("assertion"))
            + _evidence_weight(entry.get("evidenceType"), entry.get("status"))
            + _semantic_tag_weight(candidate.get("semanticTag"), target_context)
        )
        if score <= 0:
            continue

        bucket = buckets.setdefault(
            concept_id,
            {
                "conceptId": concept_id,
                "term": candidate.get("term") or concept_id,
                "semanticTag": candidate.get("semanticTag"),
                "score": 0.0,
                "confidencePct": 0,
                "supportingSpanCount": 0,
                "supportingSpans": [],
                "matchedTerms": [],
                "_sections": set(),
                "_evidenceTypes": set(),
                "_assertions": set(),
                "_rationale": set(),
            },
        )
        bucket["score"] += score
        bucket["confidencePct"] = max(bucket["confidencePct"], int(entry.get("confidencePct") or 0))
        bucket["supportingSpanCount"] += 1
        bucket["_sections"].add(str(entry.get("section") or "Note"))
        bucket["_evidenceTypes"].add(str(entry.get("evidenceType") or "inferred"))
        bucket["_assertions"].add(str(entry.get("assertion") or "present"))

        span_text = str(entry.get("phrase") or "").strip()
        if span_text and span_text not in bucket["supportingSpans"] and len(bucket["supportingSpans"]) < 5:
            bucket["supportingSpans"].append(span_text)
        matched = str((candidate or {}).get("matchedText") or "").strip()
        if matched and matched not in bucket["matchedTerms"] and len(bucket["matchedTerms"]) < 5:
            bucket["matchedTerms"].append(matched)

        if _section_weight(entry.get("section")) > 0:
            bucket["_rationale"].add(f"section:{entry.get('section') or 'Note'}")
        if _assertion_weight(entry.get("assertion")) > 0:
            bucket["_rationale"].add(f"assertion:{entry.get('assertion') or 'present'}")
        if _semantic_tag_weight(candidate.get("semanticTag"), target_context) > 0:
            bucket["_rationale"].add(f"semanticTag:{candidate.get('semanticTag')}")

    results = []
    for bucket in buckets.values():
        results.append(
            {
                **{key: value for key, value in bucket.items() if not key.startswith("_")},
                "score": round(bucket["score"], 2),
                "supportingSections": sorted(value for value in bucket["_sections"] if value),
                "evidenceTypes": sorted(value for value in bucket["_evidenceTypes"] if value),
                "assertions": sorted(value for value in bucket["_assertions"] if value),
                "rationale": sorted(value for value in bucket["_rationale"] if value),
            }
        )
    results.sort(
        key=lambda item: (
            -float(item["score"] or 0),
            -int(item["supportingSpanCount"] or 0),
            -int(item["confidencePct"] or 0),
            str(item["term"] or ""),
        )
    )
    return results[: max(int(limit or 1), 1)]


def should_ground_phrase_for_target(entry: dict[str, Any], target_context: str | None) -> bool:
    """Prune plan/family mentions only when the caller asked for a target context."""
    normalized_target = str(target_context or "").strip().lower()
    if not normalized_target:
        return True
    context_type = str(entry.get("contextType") or "").strip().lower()
    if context_type == "plan" and normalized_target != "procedure.code":
        return False
    if context_type == "family-history":
        return False
    return True


def should_skip_phrase_lookup(entry: dict[str, Any]) -> bool:
    return float(entry.get("candidateScore") or 0) < 4


# ---------------------------------------------------------------------------
# LLM extraction prompt
# ---------------------------------------------------------------------------

VALID_ASSERTIONS = {"present", "absent", "suspected", "planned"}
VALID_SUBJECTS = {"patient", "family"}
VALID_TEMPORALITIES = {"current", "historical"}
VALID_SECTIONS = {
    "chief_complaint", "history", "active", "procedures", "medications",
    "family_history", "plan", "other",
}

EXTRACTION_SYSTEM_PROMPT = (
    "You are a clinical mention extractor. You return the clinical phrases a note "
    "mentions together with their context, and you never return SNOMED CT codes: "
    "a terminology server is the code authority, not you."
)


def build_extraction_prompt(text: str, language_code: str | None = None) -> str:
    """Prompt asking only for mentions and their context, never for codes."""
    return "\n".join(
        [
            f"Extract the clinical mentions from this note (language: {language_code or 'en'}).",
            "",
            "For each mention return:",
            '- "phrase": the clinical term, normalized to how a clinician would write it',
            '- "verbatim": the exact substring from the note',
            '- "assertion": one of present, absent, suspected, planned',
            '- "subject": patient or family',
            '- "temporality": current or historical',
            '- "section": one of chief_complaint, history, active, procedures, medications, family_history, plan, other',
            "",
            "Rules:",
            "- Never return a SNOMED CT code, and never guess one.",
            "- Keep negated, family-history, and planned mentions, marked with the right context.",
            "- Do not invent text that is not in the note.",
            "",
            'Respond with ONLY this JSON object: {"mentions":[...]}',
            "",
            "Note:",
            str(text or ""),
        ]
    )


def parse_llm_mentions(parsed: Any) -> list[dict[str, Any]]:
    """Validate and de-duplicate the mentions an LLM returned."""
    raw = parsed.get("mentions") if isinstance(parsed, dict) else None
    if not isinstance(raw, list):
        return []
    seen: set[str] = set()
    mentions: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        phrase = str(item.get("phrase") or "").strip()
        if not phrase:
            continue
        assertion = item.get("assertion") if item.get("assertion") in VALID_ASSERTIONS else "present"
        subject = item.get("subject") if item.get("subject") in VALID_SUBJECTS else "patient"
        key = f"{phrase.lower()}|{subject}|{assertion}"
        if key in seen:
            continue
        seen.add(key)
        mentions.append(
            {
                "phrase": phrase,
                "verbatim": str(item.get("verbatim") or phrase).strip(),
                "assertion": assertion,
                "subject": subject,
                "temporality": item.get("temporality") if item.get("temporality") in VALID_TEMPORALITIES else "current",
                "section": item.get("section") if item.get("section") in VALID_SECTIONS else "other",
            }
        )
    return mentions


def build_llm_ground_prompt(mentions: list[dict[str, Any]], language_code: str | None = None) -> str:
    """Prompt asking the model to choose only among already-retrieved candidates."""
    lines = []
    for mention in mentions:
        candidates = "\n".join(
            f"    - {candidate.get('conceptId')}: {candidate.get('term')}"
            + (f" ({candidate.get('semanticTag')})" if candidate.get("semanticTag") else "")
            for candidate in (mention.get("candidates") or [])
        )
        context = ", ".join(
            part
            for part in (
                f"assertion={mention.get('assertion')}" if mention.get("assertion") else "",
                f"subject={mention.get('subject')}" if mention.get("subject") else "",
                f"status={mention.get('status')}" if mention.get("status") else "",
            )
            if part
        )
        lines.append(
            f'  {mention.get("mentionId")} | "{mention.get("phrase")}"'
            + (f" [{context}]" if context else "")
            + f' | sentence: "{mention.get("sentence") or ""}"\n  candidates:\n{candidates or "    (none)"}'
        )
    return "\n".join(
        [
            f"You are a clinical coding assistant reviewing SNOMED CT candidates a search engine already retrieved (language: {language_code or 'en'}).",
            "Do three things:",
            '1. CONFIRM each mention: choose the single best conceptId FROM ITS candidate list. If none fits, use "none". NEVER invent a conceptId that is not in the candidate list.',
            '2. PICK THE PRIMARY DIAGNOSIS: among present, patient mentions, choose the principal conceptId. If none qualifies, use "none".',
            "3. SUMMARIZE in 2-3 plain-language sentences what you confirmed, what you excluded and why, and why you chose the primary diagnosis.",
            'Respond with ONLY this JSON object, no prose, no code fences:',
            '{"summary":"...","primaryConceptId":"...","decisions":[{"mentionId":"...","conceptId":"...","confidence":0.0,"rationale":"one line"}]}',
            "",
            "Mentions:",
            *lines,
        ]
    )
