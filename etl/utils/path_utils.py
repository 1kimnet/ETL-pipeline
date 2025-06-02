# etl/utils/path_utils.py
"""🛤️ Path utilities for workspace management."""

from __future__ import annotations

from pathlib import Path


def derive_authority_from_path(file_path: Path, staging_root: Path) -> str:
    """📂 Helper to derive authority from file path structure."""
    try:
        path_parts: tuple[str, ...] = file_path.relative_to(staging_root).parts
        return path_parts[0] if len(path_parts) > 1 else "UNKNOWN_GLOB_AUTH"
    except (IndexError, ValueError):
        return "UNKNOWN_GLOB_AUTH_EXC"
