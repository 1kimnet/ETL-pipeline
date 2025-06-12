from __future__ import annotations


try:
    import arcpy
except Exception:  # pragma: no cover - fallback when arcpy missing
    arcpy = None


__all__ = ["arcpy"]
