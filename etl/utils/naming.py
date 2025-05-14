"""
Utility functions for generating and sanitizing names, 
especially for ArcGIS feature classes.
"""
from __future__ import annotations

import re
import logging
from typing import Dict

# --- Constants ---
SWEDISH_CHAR_MAP: Dict[str, str] = {
    'å': 'a', 'Å': 'A',
    'ä': 'a', 'Ä': 'A',
    'ö': 'o', 'Ö': 'O',
}

# Maximum length for a feature class name in a File Geodatabase is technically 160 characters.
# However, for practical purposes and to avoid issues with other systems or very long paths,
# a shorter limit is often advisable. Shapefiles have a 10-character limit for field names,
# and while GDBs are more generous, very long names can still be unwieldy.
# We'll use a default practical max length, which can be overridden.
DEFAULT_MAX_FC_NAME_LENGTH = 60
SUFFIX_RESERVATION_LENGTH = 5 # Reserve space for suffixes like "_123"

def sanitize_swedish_chars(text: str) -> str:
    """
    Replaces Swedish characters (å, ä, ö) in a string with their 
    common non-diacritic ASCII counterparts (a, a, o).

    Args:
        text: The input string.

    Returns:
        The string with Swedish characters replaced.
    """
    for swedish_char, replacement in SWEDISH_CHAR_MAP.items():
        text = text.replace(swedish_char, replacement)
    return text

def generate_base_feature_class_name(
    original_stem: str,
    authority: str,
    max_length: int = DEFAULT_MAX_FC_NAME_LENGTH
) -> str:
    """
    Generates a sanitized base name for a feature class using the 
    [AUTHORITY]_[SANITIZED_STEM] format.

    This function handles:
    - Swedish character sanitization.
    - Conversion to lowercase.
    - General sanitization (non-alphanumeric to underscores).
    - Prefixing with the authority.
    - Length truncation to meet max_length, reserving space for potential uniqueness suffixes.

    Args:
        original_stem: The original name stem (e.g., from a filename).
        authority: The authority code (e.g., "FM").
        max_length: The maximum desired length for the base name before uniqueness suffixes.

    Returns:
        A sanitized base feature class name.

    Raises:
        ValueError: If a valid name cannot be generated (e.g., prefix too long).
    """
    if not isinstance(original_stem, str):
        original_stem = str(original_stem)
    if not isinstance(authority, str):
        authority = str(authority)

    if not authority:
        logging.warning("⚠️ Authority is empty for stem '%s'. Name will not be prefixed.", original_stem)
        authority_prefix = ""
    else:
        authority_prefix = authority.upper() + "_"

    # 1. Sanitize Swedish characters and convert to lowercase
    sanitized_stem = sanitize_swedish_chars(original_stem.lower())

    # 2. General sanitization: replace non-alphanumeric (excluding underscore) with underscore
    # Also, replace multiple underscores with a single one and strip leading/trailing underscores.
    sanitized_stem = re.sub(r'[^\w_]', '_', sanitized_stem) 
    sanitized_stem = re.sub(r'_+', '_', sanitized_stem).strip('_')

    if not sanitized_stem:  # Handle cases where stem becomes empty after sanitization
        sanitized_stem = "layer"
    
    base_name = f"{authority_prefix}{sanitized_stem}"

    # 3. Ensure overall length is within limits, reserving space for suffixes
    # The TRUNCATE_LEN is the effective max length for this base_name part.
    truncate_len = max_length - SUFFIX_RESERVATION_LENGTH 
    if truncate_len < len(authority_prefix) + 1: # Ensure at least 1 char for stem after prefix
        raise ValueError(
            f"Authority prefix '{authority_prefix}' is too long (or max_length {max_length} is too short) "
            f"to create a valid name from '{original_stem}'. "
            f"Need at least {len(authority_prefix) + 1 + SUFFIX_RESERVATION_LENGTH} characters for max_length."
        )

    if len(base_name) > truncate_len:
        # Calculate how much of the stem can be kept
        available_len_for_stem = truncate_len - len(authority_prefix)
        if available_len_for_stem < 0: # Should be caught by the check above, but as a safeguard
             available_len_for_stem = 0
        
        sanitized_stem = sanitized_stem[:available_len_for_stem]
        base_name = f"{authority_prefix}{sanitized_stem}".strip('_') # Reconstruct and strip potential trailing underscores

    if not base_name: # Should not happen if logic above is correct
        raise ValueError(f"Could not generate a valid base name for '{original_stem}' with authority '{authority}'.")

    return base_name
