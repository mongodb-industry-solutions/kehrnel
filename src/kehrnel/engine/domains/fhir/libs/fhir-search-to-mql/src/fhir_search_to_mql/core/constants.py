"""
Constants used throughout the FHIR Search to MQL library.
"""

# FHIR Versions
FHIR_VERSIONS = ["R4", "R5", "R6"]
DEFAULT_FHIR_VERSION = "R5"

# FHIR Search Parameter Types
PARAMETER_TYPES = [
    "string",
    "token",
    "reference",
    "date",
    "number",
    "quantity",
    "uri",
    "composite",
    "special",
]

# FHIR Search Modifiers
STRING_MODIFIERS = ["exact", "contains", "missing"]
TOKEN_MODIFIERS = ["not", "text", "missing", "in", "not-in", "of-type"]
REFERENCE_MODIFIERS = [
    # Reference-specific modifiers
    "identifier",
    "missing",
    "text",
    "above",
    "below",
    # FHIR typed-reference modifiers (`?subject:Patient=pat-1`).
    # These are bare type names — the parser strips the leading colon, so the
    # converter sees "Patient" rather than ":Patient". The error formatter in
    # BaseConverter prefixes a colon for display.
    "Patient",
    "Practitioner",
    "PractitionerRole",
    "Organization",
    "Location",
    "Device",
    "Group",
    "RelatedPerson",
    "Encounter",
    "Observation",
    "Condition",
    "Procedure",
    "Medication",
    "MedicationRequest",
    "ServiceRequest",
    "DiagnosticReport",
    "Specimen",
    "Appointment",
    "Schedule",
    "Slot",
    "CareTeam",
    "EpisodeOfCare",
    "HealthcareService",
]
DATE_MODIFIERS = ["missing"]
NUMBER_MODIFIERS = ["missing"]
QUANTITY_MODIFIERS = ["missing"]
URI_MODIFIERS = ["below", "above", "missing"]
COMPOSITE_MODIFIERS = ["missing"]  # Composite supports :missing; component modifiers handled per component

# FHIR Search Prefixes (for date, number, quantity)
PREFIXES = ["eq", "ne", "gt", "lt", "ge", "le", "sa", "eb", "ap"]

# Special Parameters
SPECIAL_PARAMETERS = [
    "_id",
    "_lastUpdated",
    "_tag",
    "_profile",
    "_security",
    "_text",
    "_content",
    "_list",
    "_has",
    "_type",
    "_sort",
    "_count",
    "_include",
    "_revinclude",
    "_summary",
    "_elements",
    "_contained",
    "_containedType",
]

# Compartment Types
COMPARTMENT_TYPES = [
    "Patient",
    "Encounter",
    "Practitioner",
    "Device",
    "RelatedPerson",
]

# MongoDB Optimization Strategies
OPTIMIZATION_STRATEGIES = {
    "lowercase_range": "Range query on lowercase field",
    "text_index": "MongoDB text index search",
    "token_array": "N-gram token array search",
    "collation": "MongoDB collation (fallback)",
    "exact_match": "Direct field comparison",
}

# Index Types
INDEX_TYPES = {
    "btree": "B-tree index (default)",
    "text": "Text index for full-text search",
    "hashed": "Hashed index for equality",
}

# Query Performance Thresholds (milliseconds)
PERFORMANCE_THRESHOLDS = {
    "fast": 10,      # < 10ms: Excellent
    "medium": 50,    # < 50ms: Good
    "slow": 200,     # < 200ms: Acceptable
    "very_slow": 200 # >= 200ms: Needs optimization
}

# Default Target Field for Denormalization
DEFAULT_SEARCH_TARGET = "_search"

# FHIR Data Types that need extraction
EXTRACTABLE_TYPES = [
    "CodeableConcept",
    "Reference",
    "Identifier",
    "HumanName",
    "ContactPoint",
    "Address",
    "Quantity",
    "Period",
    "Timing",
    "Range",
    "Ratio",
    "RatioRange",
    "Coding",
    "Extension",
    "Money",
    "Age",
    "Duration",
    "Distance",
    "Count",
    "Dosage",
    "Availability",
]

# Logical Operators
LOGICAL_OPERATORS = ["and", "or", "not"]

# Date Precision Levels
DATE_PRECISION = {
    "year": "YYYY",
    "month": "YYYY-MM",
    "day": "YYYY-MM-DD",
    "hour": "YYYY-MM-DDThh",
    "minute": "YYYY-MM-DDThh:mm",
    "second": "YYYY-MM-DDThh:mm:ss",
}

# FHIR String Search Behavior
STRING_SEARCH_DEFAULT = "prefix"  # Default is case-insensitive PREFIX match, not exact
STRING_SEARCH_EXACT = "exact"     # :exact modifier is case-sensitive exact match
STRING_SEARCH_CONTAINS = "contains"  # :contains modifier is substring match

# MongoDB Collation Locale (optional fallback)
DEFAULT_COLLATION_LOCALE = "en"
COLLATION_STRENGTH_CASE_INSENSITIVE = 2

# Maximum number of indexes recommended per collection
MAX_INDEXES_PER_COLLECTION = 64
RECOMMENDED_MAX_INDEXES = 20  # Conservative limit for hybrid strategy
