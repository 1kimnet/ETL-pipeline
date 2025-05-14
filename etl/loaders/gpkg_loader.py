from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Set

import arcpy  # ArcGIS Pro 3.3 built-in
from .filegdb import ArcPyFileGDBLoader

from ..utils.naming import (
    generate_base_feature_class_name,
    DEFAULT_MAX_FC_NAME_LENGTH,
)

MAIN_PREFIX_RE = re.compile(r"^main_?", re.IGNORECASE)


def copy_gpkg_into_gdb(
    gpkg: Path,
    authority: str,
    gdb_path: Path,
    used_names: Set[str],
) -> None:
    """🔄 Copy every layer in *gpkg* into *gdb_path* with cleaned names."""
    arcpy.env.workspace = str(gpkg)  # type: ignore[attr-defined]

    for fc in arcpy.ListFeatureClasses():  # type: ignore[attr-defined]
        cleaned_stem = MAIN_PREFIX_RE.sub("", fc)
        base_name = generate_base_feature_class_name(
            cleaned_stem,
            authority,
            max_length=DEFAULT_MAX_FC_NAME_LENGTH,
        )
        tgt_name = ArcPyFileGDBLoader._ensure_unique_name(base_name, used_names)
        logging.info("📥 Copying %s/%s → %s", gpkg.name, fc, tgt_name)
        arcpy.conversion.FeatureClassToFeatureClass(fc, str(gdb_path), tgt_name)  # type: ignore[attr-defined]
