# src/kehrnel/api/legacy/v1/synthetic/__init__.py

"""
Synthetic Data Generation Module

This module provides functionality for generating synthetic EHR data
based on composition templates. It's designed for testing, development,
and demonstration purposes.

Key features:
- Generate multiple EHR records with attached compositions
- Randomize clinical data while maintaining structure
- Support for custom composition templates
- Built-in vaccination template based on HC3 Immunization List
- Statistics and reporting capabilities
"""

from .service import generate_synthetic_data, SyntheticDataGenerator
from .models import (
    SyntheticDataRequest,
    SyntheticDataResponse, 
    SyntheticDataRecord,
    SyntheticDataStats
)
from .routes import router

__all__ = [
    "generate_synthetic_data",
    "SyntheticDataGenerator", 
    "SyntheticDataRequest",
    "SyntheticDataResponse",
    "SyntheticDataRecord", 
    "SyntheticDataStats",
    "router"
]