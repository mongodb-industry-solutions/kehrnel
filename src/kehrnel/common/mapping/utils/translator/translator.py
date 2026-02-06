# src/mapper/utils/translator/translator.py
from __future__ import annotations
import os, re, sys
from .translation_cache import TranslationCache
from .llm_translate import llm_translate

class Translator:
    def __init__(self, source_lang="es", target_lang="en", cache_path=".kehrnel/translations.json"):
        self.sl = source_lang
        self.tl = target_lang
        self.cache = TranslationCache(cache_path)
        self._log = str(os.environ.get("KEHRNEL_TRANSLATE_LOG", "0")).lower() not in ("0","false","off","")

    def _log_print(self, msg: str) -> None:
        if self._log:
            print(msg, file=sys.stderr)

    def translate(self, text: str, *, persist: bool = True) -> str:
        if not text:
            return text
        if text.isdigit() or re.match(r"^\d+([.,]\d+)?$", text) or \
           re.match(r"^\d{4}-\d{2}-\d{2}", text) or \
           re.match(r"^at\d{4}$", text, flags=re.I):
            return text

        hit = self.cache.lookup(self.sl, self.tl, text)
        if hit is not None:
            if os.environ.get("KEHRNEL_TRANSLATE_LOG"):
                import sys
                print(f"[tx hit] {self.sl}->{self.tl} :: '{text[:60]}'", file=sys.stderr)
            return hit
        out = llm_translate(text, self.sl, self.tl)
        if os.environ.get("KEHRNEL_TRANSLATE_LOG"):
            import sys
            print(f"[tx miss] {self.sl}->{self.tl} :: '{text[:60]}' → '{out[:60]}'", file=sys.stderr)
        if persist:
            self.cache.put(self.sl, self.tl, text, out)
        return out