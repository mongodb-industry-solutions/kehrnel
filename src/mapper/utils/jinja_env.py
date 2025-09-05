#src/mapper/utils/jinja_env.py
"""
One central Jinja2 environment that exposes every `transforms.register`-ed
function as a filter – so YAML rules may use {{ value | hl7_to_iso8601 }}.
"""
from __future__ import annotations
from jinja2 import Environment, StrictUndefined
from mapper.transforms import REGISTRY

env = Environment(undefined=StrictUndefined)
env.filters.update(REGISTRY)      # every @register fn becomes a filter