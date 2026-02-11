# src/mapper/utils/translator/llm_translate.py
from __future__ import annotations
import os
import re

import openai as _openai
if not hasattr(_openai, "OpenAI"):
    raise RuntimeError(
        f"openai>=1.0.0 is required; found {_openai.__version__}. "
        "Upgrade with: pip install -U 'openai>=1.0.0'"
    )
from openai import OpenAI

# Prefer OPENAI_MODEL; fall back to TX_MODEL; default to a light, cheap model
def _model() -> str:
    return (
        os.environ.get("OPENAI_MODEL")
        or os.environ.get("TX_MODEL")
        or "gpt-4o-mini"
    )

_SYS = (
    "You translate text. Output ONLY the translated text, no quotes, no extra words. "
    "Keep numbers and codes unchanged. If the input looks like a code (e.g. at0003), "
    "return it verbatim."
)

_CODE_LIKE = re.compile(r"^[A-Za-z]{2}\d{4}$")  # e.g., at0003

def _get_client() -> OpenAI:
    # Try environment; if missing, attempt to load .env here too (extra safety)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        try:
            from dotenv import load_dotenv, find_dotenv
            load_dotenv(find_dotenv(usecwd=True), override=False)
            api_key = os.environ.get("OPENAI_API_KEY")
        except Exception:
            pass
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    return OpenAI(api_key=api_key)

def llm_translate(text: str, source_lang: str, target_lang: str) -> str:
    if not text:
        return text
    # Hard guard for archetype-ish codes
    if _CODE_LIKE.match(text.strip()):
        return text

    client = _get_client()
    msgs = [
        {"role": "system", "content": _SYS},
        {"role": "user", "content": f"Translate from {source_lang} to {target_lang}: {text}"},
    ]
    resp = client.chat.completions.create(
        model=_model(),
        messages=msgs,
        temperature=0
    )
    return (resp.choices[0].message.content or "").strip()