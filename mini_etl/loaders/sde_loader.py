from __future__ import annotations

from pathlib import Path

from mini_etl.utils.arcpy_utils import arcpy
from mini_etl.utils.logging import setup_logger

log = setup_logger(__name__)


def load(feature_class: Path, sde_path: Path) -> None:
    if arcpy is None:  # pragma: no cover - stub fallback
        log.warning("⚠️ arcpy not available")
        return
    log.info("🔄 loading %s", feature_class.name)
    arcpy.management.CopyFeatures(str(feature_class), str(sde_path))  # type: ignore[attr-defined]
    log.info("✅ loaded to SDE")
