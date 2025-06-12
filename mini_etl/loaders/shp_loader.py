from __future__ import annotations

from pathlib import Path

from mini_etl.utils.arcpy_utils import arcpy
from mini_etl.utils.logging import setup_logger

log = setup_logger(__name__)


def load_shp(path: Path, gdb: Path) -> Path:
    if arcpy is None:  # pragma: no cover - stub fallback
        log.warning("⚠️ arcpy not available")
        return path
    log.info("🔄 loading shp %s", path.name)
    arcpy.conversion.FeatureClassToGeodatabase(str(path), str(gdb))  # type: ignore[attr-defined]
    out_path = gdb / path.stem
    log.info("✅ shp loaded → %s", out_path.name)
    return out_path
