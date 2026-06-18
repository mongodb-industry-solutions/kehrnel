from __future__ import annotations


def is_version_commit_time_path(path: str | None, version_alias: str | None) -> bool:
    """Return true for the canonical and implicit-value commit-time AQL paths."""
    if not isinstance(path, str) or not path.strip() or not version_alias:
        return False
    base_path = f"{version_alias}/commit_audit/time_committed"
    normalized = path.strip()
    return normalized == base_path or normalized == f"{base_path}/value"


def version_commit_time_paths(version_alias: str | None) -> set[str]:
    if not version_alias:
        return set()
    base_path = f"{version_alias}/commit_audit/time_committed"
    return {base_path, f"{base_path}/value"}
