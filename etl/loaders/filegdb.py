"""Loader that consolidates all staged shapefiles into *staging.gdb* using ArcPy.

This **updated** version adds:
* detailed ArcPy error capture with source file context
* duplicate‑safe, FGDB‑legal layer names with [AUTHORITY]_[SANITIZED_NAME] convention
* Sanitization for Swedish characters (åäö)
* `arcpy.env.overwriteOutput = True` so reruns don’t crash
* Enhanced logging in _reset_gdb to pinpoint creation/deletion issues.
"""

from __future__ import annotations

import logging
import shutil
import re
from pathlib import Path
from typing import Set, Dict

import arcpy  # ArcGIS Pro / Server Python ships with this

from ..utils import paths, ensure_dirs


class ArcPyFileGDBLoader:  # noqa: D101
    """Build (or rebuild) *staging.gdb* from everything under *data/staging*."""

    # --- Class attribute for Swedish character mapping ---
    SWEDISH_CHAR_MAP: Dict[str, str] = {
        'å': 'a', 'Å': 'A',
        'ä': 'a', 'Ä': 'A',
        'ö': 'o', 'Ö': 'O',
    }

    def __init__(self, gdb_path: Path | None = None):
        ensure_dirs()
        self.gdb_path = gdb_path or paths.GDB

    # ---------------------------------------------------------------- public

    def load_from_staging(self, staging_root: Path) -> None:  # noqa: D401
        """Recreate the FileGDB and copy every `.shp` inside *staging_root*."""
        self._reset_gdb() # This will now raise an error if it fails, stopping before CopyFeatures

        shp_files = list(staging_root.rglob("*.shp"))
        if not shp_files:
            logging.warning("⚠️ No shapefiles found in %s to load into GDB", staging_root)
            return

        logging.info("⚙️ Setting ArcPy environment for GDB loading...")
        arcpy.env.overwriteOutput = True  # type: ignore[attr-defined]
        arcpy.env.workspace = str(self.gdb_path)  # type: ignore[attr-defined]

        used_names: Set[str] = set()
        for shp in shp_files:
            try:
                # Extract authority from the path structure: staging_root/AUTHORITY/dataset_name/file.shp
                relative_path = shp.relative_to(staging_root)
                if not relative_path.parts:
                    logging.error("❌ Could not determine authority for %s, skipping.", shp)
                    continue
                authority = relative_path.parts[0]

                tgt_name = self._safe_name(shp.stem, authority, used_names)
                logging.info("📥 Copying %s → %s (Original: %s, Authority: %s)",
                             shp.relative_to(paths.ROOT), tgt_name, shp.stem, authority)
                arcpy.management.CopyFeatures(str(shp), tgt_name)
            except arcpy.ExecuteError:  # type: ignore[attr-defined]
                # Ensure tgt_name is defined for logging, even if _safe_name failed (though it shouldn't)
                safe_tgt_name = tgt_name if 'tgt_name' in locals() else "unknown_target"
                logging.error(
                    "❌ CopyFeatures failed for %s → %s: %s", shp.name, safe_tgt_name, arcpy.GetMessages(2)
                )
                # Optionally, re-raise or continue based on project requirements
                # For now, it continues to allow other files to be processed.
                # raise # Uncomment to stop on first CopyFeatures error
                continue
            except ValueError as ve: # Catch errors from _safe_name, e.g., if a name cannot be generated
                logging.error("❌ Naming error for %s: %s", shp.stem, ve)
                continue
            except Exception as e:
                safe_tgt_name = tgt_name if 'tgt_name' in locals() else "unknown_target"
                logging.error(
                    "❌ Unexpected error during CopyFeatures for %s → %s: %s", shp.name, safe_tgt_name, e, exc_info=True
                )
                # raise # Uncomment to stop on first unexpected error
                continue


    # ---------------------------------------------------------------- internals

    def _reset_gdb(self) -> None:
        """
        Delete and recreate the destination GDB fresh for this run.
        Any ArcPy error is logged with *full* tool messages then re‑raised so
        the caller can surface it (and Pipeline will catch & report).
        Other exceptions during deletion are also logged and re-raised.
        """
        gdb_full_path = self.gdb_path.resolve() # Get absolute path for clarity
        logging.info("ℹ️ Target GDB path for reset: %s", gdb_full_path)

        if self.gdb_path.exists():
            logging.info("🗑️ Attempting to remove existing GDB: %s", gdb_full_path)
            try:
                shutil.rmtree(self.gdb_path)
                logging.info("✅ Successfully removed existing GDB: %s", gdb_full_path)
            except Exception as e:
                logging.error("❌ Failed to remove existing GDB '%s': %s", gdb_full_path, e, exc_info=True)
                raise RuntimeError(f"Failed to remove existing GDB '{gdb_full_path}': {e}") from e
        else:
            logging.info("ℹ️ GDB does not currently exist at %s, no removal needed.", gdb_full_path)

        # Ensure parent directory exists (though ensure_dirs should have handled paths.DATA)
        parent_dir = self.gdb_path.parent.resolve()
        if not parent_dir.exists():
            logging.info("🆕 Parent directory %s for GDB does not exist. Attempting to create.", parent_dir)
            try:
                parent_dir.mkdir(parents=True, exist_ok=True)
                logging.info("✅ Successfully created parent directory: %s", parent_dir)
            except Exception as e:
                logging.error("❌ Failed to create parent directory '%s' for GDB: %s", parent_dir, e, exc_info=True)
                raise RuntimeError(f"Failed to create parent directory '{parent_dir}' for GDB: {e}") from e
        
        logging.info("🆕 Attempting to create new FileGDB: %s in folder %s", self.gdb_path.name, self.gdb_path.parent.resolve())
        try:
            arcpy.management.CreateFileGDB(str(self.gdb_path.parent), self.gdb_path.name)
            logging.info("✅ Successfully created new GDB: %s", gdb_full_path)
        except arcpy.ExecuteError:  # type: ignore[attr-defined]
            msg = arcpy.GetMessages(2) # Get detailed ArcPy error messages
            logging.error("❌ arcpy.management.CreateFileGDB failed for '%s': %s", gdb_full_path, msg)
            # Re-raise with the specific ArcPy message
            raise RuntimeError(f"CreateFileGDB failed for '{gdb_full_path}': {msg}") from None
        except Exception as e: # Catch any other unexpected errors during GDB creation
            logging.error("❌ Unexpected error during arcpy.management.CreateFileGDB for '%s': %s", gdb_full_path, e, exc_info=True)
            raise RuntimeError(f"Unexpected error during CreateFileGDB for '{gdb_full_path}': {e}") from e

    @staticmethod
    def _sanitize_swedish_chars(text: str) -> str:
        """Replaces Swedish characters with their non-diacritic counterparts."""
        for swedish_char, replacement in ArcPyFileGDBLoader.SWEDISH_CHAR_MAP.items():
            text = text.replace(swedish_char, replacement)
        return text

    @staticmethod
    def _safe_name(original_stem: str, authority: str, used: Set[str]) -> str:
        """
        Generates a unique, FGDB-legal layer name using [AUTHORITY]_[SANITIZED_STEM] format.
        Sanitizes Swedish characters and ensures the name is <= 60 characters.
        """
        if not authority:
            logging.warning("⚠️ Authority is empty, cannot prefix name for %s", original_stem)
            # Fallback to original stem or handle as an error
            # For now, proceed without authority prefix if empty, but this should be reviewed.
            authority_prefix = ""
        else:
            authority_prefix = authority.upper() + "_"

        # 1. Sanitize Swedish characters and convert to lowercase
        sanitized_stem = ArcPyFileGDBLoader._sanitize_swedish_chars(original_stem.lower())

        # 2. General sanitization: replace non-alphanumeric (excluding underscore) with underscore
        sanitized_stem = re.sub(r'[^\w_]', '_', sanitized_stem)
        sanitized_stem = re.sub(r'_+', '_', sanitized_stem).strip('_') # Replace multiple underscores with one

        if not sanitized_stem: # Handle empty stem after sanitization
            sanitized_stem = "layer"
        
        base_name = f"{authority_prefix}{sanitized_stem}"

        # 3. Ensure overall length is within limits (e.g., 60 chars practical limit)
        # Max length for a feature class name in a File GDB is 160 characters.
        # However, ArcPy tools and other systems might have shorter practical limits.
        # Let's use a conservative limit like 60, as previously discussed.
        # If a shorter limit like 31 (common in SDE or older formats) is needed, adjust MAX_LEN.
        MAX_LEN = 60 
        
        # Truncate if base_name is too long, leaving space for potential suffixes like "_1", "_2"
        # Suffixes can be up to ~ "_999" (4 chars), so reserve space.
        # If MAX_LEN is 60, reserve 5 for suffix, so stem part is 55.
        TRUNCATE_LEN = MAX_LEN - 5 
        if len(base_name) > TRUNCATE_LEN:
            # If authority_prefix itself is long, this could be an issue.
            # We need to truncate from the sanitized_stem part.
            available_len_for_stem = TRUNCATE_LEN - len(authority_prefix)
            if available_len_for_stem < 1: # Not enough space for even one char of stem
                 raise ValueError(f"Authority prefix '{authority_prefix}' is too long to create a valid name from '{original_stem}' within {MAX_LEN} chars.")
            sanitized_stem = sanitized_stem[:available_len_for_stem]
            base_name = f"{authority_prefix}{sanitized_stem}".strip('_') # Reconstruct and strip trailing underscores

        if not base_name: # Should not happen if logic above is correct
            raise ValueError(f"Could not generate a base name for {original_stem} with authority {authority}.")

        # 4. Ensure uniqueness
        candidate = base_name
        idx = 1
        while candidate.lower() in (n.lower() for n in used): # Case-insensitive check for practical uniqueness
            suffix = f"_{idx}"
            # Check if adding suffix exceeds MAX_LEN
            if len(base_name) + len(suffix) > MAX_LEN:
                # Need to truncate base_name further
                chars_to_remove = (len(base_name) + len(suffix)) - MAX_LEN
                truncated_base = base_name[:-(chars_to_remove)]
                if not truncated_base.replace(authority_prefix, ""): # Ensure stem part is not empty
                    raise ValueError(f"Cannot generate a unique name for {original_stem} (authority {authority}) under {MAX_LEN} chars.")
                candidate = f"{truncated_base}{suffix}"
            else:
                candidate = f"{base_name}{suffix}"
            idx += 1
        
        used.add(candidate)
        return candidate
