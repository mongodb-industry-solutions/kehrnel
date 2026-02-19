"""Domain-agnostic ingest exceptions."""


class FlattenerError(Exception):
    """Base exception for flattener-like transformation pipelines."""


class UnknownCodeError(FlattenerError):
    """Raised when a code is not found and role/policy forbids auto-creation."""

    def __init__(self, key, sid):
        self.key = key
        self.sid = sid
        super().__init__(f"Unknown code for key='{key}' and sid='{sid}' in non-primary mode.")
