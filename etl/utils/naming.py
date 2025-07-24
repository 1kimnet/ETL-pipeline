# etl/utils/naming.py
"""Helpers that turn arbitrary text into safe file or ArcGIS feature-class names."""

from __future__ import annotations

import re
from typing import Final

from .sanitize import slugify  # central helper keeps hyphens for readability

_ILLEGAL_ARCGIS: Final = re.compile(r"[^A-Za-z0-9_]")   # stricter pattern
# FGDB feature class limit
_ARCGIS_MAX_LEN: Final = 128


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
    🏷️ Generate a feature class name with authority prefix.

    Examples:
        generate_fc_name("RAA", "byggnader_sverige_point") → "raa_byggnader_sverige_point"
        generate_fc_name("RAA", "raa_byggnader_sverige_point") → "raa_byggnader_sverige_point"
        generate_fc_name("LSTD", "lstd_gi_betesmark_data") → "lstd_gi_betesmark_data"
    """
    authority_lower = authority.lower()

    # Clean the source string and sanitize it first
    source_clean = sanitize_for_arcgis_name(source)

    # Check if it already starts with the authority prefix
    expected_prefix = f"{authority_lower}_"
    if source_clean.lower().startswith(expected_prefix):
        # Already has the prefix, just return it
        return source_clean[:_ARCGIS_MAX_LEN].rstrip("_")

    # Add the authority prefix
    result = f"{authority_lower}_{source_clean}"
    return result[:_ARCGIS_MAX_LEN].rstrip("_")


def sanitize_sde_name(name: str) -> str:
    """🧹 Sanitize feature class name for SDE compatibility.

    SDE naming rules:
    - Must start with letter or underscore
    - Can contain letters, numbers, underscores
    - No spaces, hyphens, or special characters
    - Max 128 characters (plenty of room)
    """
    original_name = name

    # Replace problematic characters
    # Convert common problematic chars to underscores
    name = re.sub(r'[-\s\.]+', '_', name)  # hyphens, spaces, dots → underscore
    name = re.sub(
        r'[åäö]',
        lambda m: {
            'å': 'a',
            'ä': 'a',
            'ö': 'o'}[
            m.group()],
        name)  # Swedish chars
    # Any remaining non-word chars → underscore
    name = re.sub(r'[^\w]', '_', name)
    # Multiple underscores → single underscore
    name = re.sub(r'_{2,}', '_', name)
    name = name.strip('_')  # Remove leading/trailing underscores

    # Ensure it starts with letter or underscore (not number)
    if name and name[0].isdigit():
        name = f"fc_{name}"

    # Ensure not empty
    if not name:
        name = "unnamed_fc"

    return name
