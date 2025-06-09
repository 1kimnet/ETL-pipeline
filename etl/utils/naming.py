# etl/utils/naming.py
"""Helpers that turn arbitrary text into safe file or ArcGIS feature-class names."""

from __future__ import annotations

import re
from typing import Final

from .sanitize import slugify  # central helper keeps hyphens for readability

_ILLEGAL_ARCGIS: Final = re.compile(r"[^A-Za-z0-9_]")   # stricter pattern
_ARCGIS_MAX_LEN: Final = 128                             # FGDB feature class limit

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def sanitize_for_filename(name: str) -> str:
    """Return a lower-case, ASCII-safe slug suitable for *file* names."""
    return slugify(name)   # hyphens are fine on disk


def sanitize_for_arcgis_name(name: str) -> str:
    """Return an FGDB-safe identifier (letters, digits, underscores, ≤31 chars)."""
    txt = slugify(name).replace("-", "_")       # 1) drop hyphens
    txt = _ILLEGAL_ARCGIS.sub("_", txt)         # 2) strip anything else
    txt = re.sub(r"__+", "_", txt).strip("_")   # 3) collapse repeats
    if txt and txt[0].isdigit():
        txt = f"_{txt}"                         # 4) SDE can’t start with digit
    return (txt or "unnamed")[:_ARCGIS_MAX_LEN]

def generate_fc_name(authority: str, source: str) -> str:
    """
    Combine authority + source → FGDB-safe name, ≤31 chars.
    """
    base = f"{authority}_{sanitize_for_arcgis_name(source)}"
    return base[:_ARCGIS_MAX_LEN].rstrip("_")
# Ensure the name is not empty and does not end with an underscore

