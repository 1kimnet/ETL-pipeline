# etl/handlers/geoprocess.py
from __future__ import annotations

import logging
from pathlib import Path
from datetime import datetime
import arcpy

log = logging.getLogger("summary")        # human-readable milestones

def clip_project_gdb(
    src_gdb: Path | str,
    aoi_fc: Path | str,
    dest_gdb: Path | str,
    target_srid: int = 3006,
    pp_factor: str = "100",               # "100" = all logical cores
) -> None:
    """Clip every FC in *src_gdb* to *aoi_fc* + project to *target_srid*.

    Writes into *dest_gdb*, preserving original FC names.
    """
    arcpy.env.parallelProcessingFactor = pp_factor        # type: ignore[attr-defined]
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(target_srid)  # type: ignore[attr-defined]
    arcpy.env.overwriteOutput = True                      # type: ignore[attr-defined]

    arcpy.env.workspace = str(src_gdb)                    # type: ignore[attr-defined]
    src_fcs = arcpy.ListFeatureClasses()
    if not src_fcs:
        log.warning("⚠️  No feature classes found in %s", src_gdb)
        return

    # create destination GDB
    dest_gdb = Path(dest_gdb)
    dest_gdb.parent.mkdir(parents=True, exist_ok=True)
    if not dest_gdb.exists():
        arcpy.management.CreateFileGDB(str(dest_gdb.parent), dest_gdb.name)

    for fc in src_fcs:
        dest_fc = f"{dest_gdb}\\{fc}"

        # delete previous run, if any
        if arcpy.Exists(dest_fc):
            arcpy.management.Delete(dest_fc)

        tmp_mem = f"in_memory\\{fc}"
        try:
            # PairwiseClip writes in target SR thanks to env.outputCoordinateSystem
            arcpy.analysis.PairwiseClip(fc, aoi_fc, tmp_mem)
            arcpy.management.CopyFeatures(tmp_mem, dest_fc)

            log.info("   ✂️  clipped ➜ %s", fc)
        except arcpy.ExecuteError as exc:
            log.error("   ❌ clip failed %s: %s", fc, arcpy.GetMessages(2))
        finally:
            arcpy.management.Delete(tmp_mem)  # clean in-memory
