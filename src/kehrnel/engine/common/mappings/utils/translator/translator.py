# src/mapper/utils/translator/translator.py
from __future__ import annotations
import os
import re
import logging
from .translation_cache import TranslationCache
from .llm_translate import llm_translate

logger = logging.getLogger(__name__)

class Translator:
    def __init__(self, source_lang="es", target_lang="en", cache_path=".kehrnel/translations.json"):
        self.sl = source_lang
        self.tl = target_lang
        self.cache = TranslationCache(cache_path)
        self._log = str(os.environ.get("KEHRNEL_TRANSLATE_LOG", "0")).lower() not in ("0","false","off","")

    def _log_print(self, msg: str) -> None:
        if self._log:
            logger.info("%s", msg)

    def translate(self, text: str, *, persist: bool = True) -> str:
        if not text:
            return text
        if text.isdigit() or re.match(r"^\d+([.,]\d+)?$", text) or \
           re.match(r"^\d{4}-\d{2}-\d{2}", text) or \
           re.match(r"^at\d{4}$", text, flags=re.I):
            return text

        hit = self.cache.lookup(self.sl, self.tl, text)
        if hit is not None:
            if self._log:
                logger.info("[tx hit] %s->%s :: '%s'", self.sl, self.tl, text[:60])
            return hit
        out = llm_translate(text, self.sl, self.tl)
        if self._log:
            logger.info("[tx miss] %s->%s :: '%s' -> '%s'", self.sl, self.tl, text[:60], out[:60])
        if persist:
            self.cache.put(self.sl, self.tl, text, out)
        return out
