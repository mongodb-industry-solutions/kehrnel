# src/kehrnel/compatibility/transform/exceptions.py

class FlattenerError(Exception):
    """Base exception for the flattener library."""
    pass

class UnknownCodeError(FlattenerError):
    """Raised when a code is not found and the role is not primary."""
    def __init__(self, key, sid):
        self.key = key
        self.sid = sid
        super().__init__(f"Unknown code for key='{key}' and sid='{sid}' in non-primary mode.")