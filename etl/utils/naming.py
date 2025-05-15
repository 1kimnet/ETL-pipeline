"""
Utility functions for generating and sanitizing names, 
especially for ArcGIS feature classes and file/directory names.
"""
from __future__ import annotations

import re
import logging
from typing import Dict, Final # Added Final

# --- Constants ---
SWEDISH_CHAR_MAP: Final[Dict[str, str]] = { # Made SWEDISH_CHAR_MAP a Final
    'å': 'a', 'Å': 'A',
    'ä': 'a', 'Ä': 'A',
    'ö': 'o', 'Ö': 'O',
}

DEFAULT_MAX_FC_NAME_LENGTH: Final[int] = 60 # Made it Final
SUFFIX_RESERVATION_LENGTH: Final[int] = 5 # Made it Final

def sanitize_swedish_chars(text: str) -> str:
    """
    Replaces Swedish characters (å, ä, ö) in a string with their 
    common non-diacritic ASCII counterparts (a, a, o).
    """
    for swedish_char, replacement in SWEDISH_CHAR_MAP.items():
        text = text.replace(swedish_char, replacement)
    return text

def sanitize_for_filename(name: str, allow_dot: bool = False) -> str:
    """
    Sanitizes a string to be suitable for use as a file or directory name.
    Converts to lowercase, replaces Swedish characters, replaces non-alphanumeric 
    (excluding underscore, hyphen, and optionally dot) with underscores, 
    and collapses multiple underscores.
    """
    if not isinstance(name, str):
        name = str(name)
    
    name = name.lower()
    name = sanitize_swedish_chars(name)
    
    if allow_dot:
        # Allow word characters, underscore, dot, hyphen
        name = re.sub(r'[^\w_.-]+', '_', name) # Added + to collapse multiple invalid chars together
    else:
        # Allow word characters, underscore, hyphen (dots become underscores)
        name = re.sub(r'[^\w_-]+', '_', name) # Added +
        
    name = re.sub(r'_+', '_', name).strip('_') # Ensure multiple underscores are one, strip leading/trailing
    if not name: 
        name = "unnamed_dataset"
    return name

def generate_base_feature_class_name(
    original_stem: str,
    authority: str,
    max_length: int = DEFAULT_MAX_FC_NAME_LENGTH
) -> str:
    """
    Generates a sanitized base name for a feature class using the 
    [SANITIZED_AUTHORITY]_[SANITIZED_STEM] format.
    Ensures the authority part is also sanitized for valid GDB characters.
    """
    if not isinstance(original_stem, str):
        original_stem = str(original_stem)
    if not isinstance(authority, str):
        authority = str(authority)

    authority_prefix: str
    if not authority:
        logging.warning("⚠️ Authority is empty for stem '%s'. Name will not be prefixed.", original_stem)
        authority_prefix = ""
    else:
        # Sanitize the authority string: lowercase, swedish chars, then non-alphanumeric to underscore
        sanitized_authority: str = authority.lower()
        sanitized_authority = sanitize_swedish_chars(sanitized_authority)
        # For GDB names, only allow alphanumeric and underscores. Replace others (like hyphens) with underscore.
        sanitized_authority = re.sub(r'[^\w_]+', '_', sanitized_authority) # Replace one or more invalid chars with a single underscore
        sanitized_authority = re.sub(r'_+', '_', sanitized_authority).strip('_') # Collapse multiple underscores and strip ends
        
        if not sanitized_authority: # If authority becomes empty after sanitization
            logging.warning("⚠️ Authority '%s' became empty after sanitization. Name will not be prefixed.", authority)
            authority_prefix = ""
        else:
            authority_prefix = sanitized_authority.upper() + "_" # Uppercase the sanitized authority

    # Sanitize the original_stem
    sanitized_stem: str = sanitize_swedish_chars(original_stem.lower())
    sanitized_stem = re.sub(r'[^\w_]+', '_', sanitized_stem) 
    sanitized_stem = re.sub(r'_+', '_', sanitized_stem).strip('_')

    if not sanitized_stem:
        sanitized_stem = "layer" # Default if stem becomes empty
    
    base_name: str = f"{authority_prefix}{sanitized_stem}"
    
    # Ensure the first character is a letter (File GDB requirement)
    if base_name and not base_name[0].isalpha():
        base_name = "fc_" + base_name # Prepend "fc_" if it doesn't start with a letter
        logging.debug("Sanitized base_name prepended with 'fc_' as it did not start with a letter: %s", base_name)


    # Length truncation logic (ensure it accounts for potential "fc_" prefix)
    truncate_len: int = max_length - SUFFIX_RESERVATION_LENGTH 
    
    # Check if authority_prefix itself is too long or makes the name construction impossible
    # This check should consider the minimum length of a stem part (e.g., 1 character for "layer")
    min_stem_len = 1 
    if len(authority_prefix) + min_stem_len > truncate_len :
         # If fc_ has been added, it also takes space
        prefix_for_check = "fc_" if authority_prefix and not authority_prefix[0].isalpha() and not authority_prefix.startswith("FC_") else ""
        prefix_for_check += authority_prefix

        if len(prefix_for_check) + min_stem_len > truncate_len:
            raise ValueError(
                f"Authority prefix '{authority_prefix}' (potentially with 'fc_') is too long "
                f"(or max_length {max_length} is too short) "
                f"to create a valid name from '{original_stem}'. "
                f"Need at least {len(prefix_for_check) + min_stem_len + SUFFIX_RESERVATION_LENGTH} characters for max_length."
            )

    if len(base_name) > truncate_len:
        # How much to cut from the stem part?
        # Consider the length of the authority_prefix and potential "fc_"
        current_prefix_len = 0
        if base_name.startswith("fc_"):
            current_prefix_len += 3 # "fc_"
            if base_name.startswith(f"fc_{authority_prefix}"): # fc_ was added before authority
                 current_prefix_len = len(f"fc_{authority_prefix}")
            # else: # fc_ was added because authority_prefix itself didn't start with letter
                 # This case is complex, assume authority_prefix is part of what needs to be preserved.
                 # Let's simplify: preserve authority_prefix as much as possible.
        elif base_name.startswith(authority_prefix):
            current_prefix_len = len(authority_prefix)
        
        # available_len_for_stem is how much space is left for the stem after prefix and truncation point
        available_len_for_stem = truncate_len - current_prefix_len 
        if available_len_for_stem < 0: 
             available_len_for_stem = 0 # Should have been caught by ValueError above
        
        # Reconstruct stem part
        original_stem_part_of_base_name = base_name[current_prefix_len:]
        truncated_stem_part = original_stem_part_of_base_name[:available_len_for_stem]
        
        # Reconstruct base_name
        if base_name.startswith("fc_") and base_name.startswith(f"fc_{authority_prefix}"):
            base_name = f"fc_{authority_prefix}{truncated_stem_part}"
        elif base_name.startswith("fc_"): # fc_ was added because authority_prefix was bad
            # This implies authority_prefix was empty or also bad.
            # For simplicity, assume fc_ was for the whole thing.
            base_name = f"fc_{authority_prefix}{truncated_stem_part}" # This might be wrong if authority_prefix was complex
            # A safer reconstruction if fc_ was for the original base_name:
            base_name = ("fc_" if base_name.startswith("fc_") else "") + authority_prefix + truncated_stem_part

        else: # Starts with authority_prefix or is just stem
            base_name = f"{authority_prefix}{truncated_stem_part}"
            
        base_name = base_name.strip('_') # Clean up trailing underscores if stem became empty

    if not base_name: 
        raise ValueError(f"Could not generate a valid base name for '{original_stem}' with authority '{authority}'. Resulted in empty string.")
    
    # Final check: ensure first char is a letter after all truncation
    if base_name and not base_name[0].isalpha():
        base_name = "fc_" + base_name
        # Re-check length after adding fc_ if it wasn't there before and caused by truncation
        if len(base_name) > max_length - SUFFIX_RESERVATION_LENGTH: # Max length for base before suffix
            base_name = base_name[:max_length - SUFFIX_RESERVATION_LENGTH]


    return base_name
