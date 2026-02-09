from __future__ import annotations

from kehrnel.legacy.core.validator import kehrnelValidator
from kehrnel.legacy.core.parser import TemplateParser


def get_validator(template_path: str) -> kehrnelValidator:
    """
    Build the existing kehrnelValidator from a template path.
    """
    return kehrnelValidator(TemplateParser(template_path))
