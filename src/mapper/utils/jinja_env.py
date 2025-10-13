#src/mapper/utils/jinja_env.py
"""
One central Jinja2 environment that exposes every `transforms.register`-ed
function as a filter – so YAML rules may use {{ value | hl7_to_iso8601 }}.
"""

from __future__ import annotations
from jinja2 import Environment, StrictUndefined
from .transform import attach_to_jinja
from datetime import datetime, timezone

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

# One shared Jinja environment for the mapper
env = Environment(
    undefined=StrictUndefined,
    autoescape=False,
    trim_blocks=True,
    lstrip_blocks=True,
)

# Filters + globals
attach_to_jinja(env)
env.globals["now_utc_iso"] = now_utc_iso