from __future__ import annotations

from pathlib import Path

from mini_etl.utils.arcpy_utils import arcpy
from mini_etl.utils.logging import setup_logger

log = setup_logger(__name__)


def clip(src_path: Path, area: Path, out_path: Path) -> Path:
    if arcpy is None:  # pragma: no cover - stub fallback
        log.warning("⚠️ arcpy not available")
        return src_path
    log.info("🔄 clipping %s", src_path.name)
    arcpy.analysis.Clip(str(src_path), str(area), str(out_path))  # type: ignore[attr-defined]
    log.info("✅ clipped → %s", out_path.name)
    return out_path
