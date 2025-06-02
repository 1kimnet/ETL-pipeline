# etl/loaders/geojson.py
"""🌍 GeoJSON and JSON file loader for FileGDB."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Final, Set

import arcpy

from ..utils.naming import generate_fc_name
from ..utils.run_summary import Summary

log: Final = logging.getLogger(__name__)


def load_geojson_file(
    json_file_path: Path,
    authority: str,
    gdb_path: Path,
    used_names_set: Set[str],
    summary: Summary,
    ensure_unique_name_func
) -> None:
    """🌍 Process a single JSON/GeoJSON file using JSONToFeatures.
    
    Args:
        json_file_path: Path to the JSON/GeoJSON file.
        authority: Source authority for naming.
        gdb_path: Target GDB path.
        used_names_set: Set of already used names for uniqueness.
        summary: Summary object for tracking progress.
        ensure_unique_name_func: Function to ensure unique naming.
    """
    log.debug("🌍 Processing JSON/GeoJSON - Authority: '%s' for file: %s", authority, json_file_path.name)
    lg_sum = logging.getLogger("summary")
    
    if not json_file_path.exists():
        log.error("❌ JSON/GeoJSON file does not exist: %s", json_file_path)
        summary.log_staging("error")
        summary.log_error(json_file_path.name, "File does not exist")
        return
    
    tgt_name: str = "UNKNOWN"
    out_fc_full_path: str = "UNKNOWN_PATH"
    
    try:
        # Detect geometry type from GeoJSON content
        geometry_type = detect_geometry_type(json_file_path)
        log.info("📍 Detected geometry type: %s for %s", geometry_type, json_file_path.name)
        
        input_json_full_path: str = str(json_file_path.resolve())
        base_name: str = generate_fc_name(authority, json_file_path.stem)
        
        # Append geometry type to name for clarity
        base_name_with_geom = f"{base_name}_{geometry_type.lower()}"
        tgt_name = ensure_unique_name_func(base_name_with_geom, used_names_set)
        out_fc_full_path = str(gdb_path / tgt_name)
        
        log.info("📥 Converting JSON/GeoJSON ('%s') → GDB:/'%s' (Authority: '%s', Geom: %s)",
                 json_file_path.name, tgt_name, authority, geometry_type)
        
        # Log file size for debugging
        file_size = json_file_path.stat().st_size
        log.info("🔍 Input file size: %d bytes", file_size)
        
        # Use geometry_type parameter to ensure correct FC creation
        log.info("🔧 Calling arcpy.conversion.JSONToFeatures with geometry_type='%s'", geometry_type.upper())
        arcpy.conversion.JSONToFeatures(
            in_json_file=input_json_full_path, 
            out_features=out_fc_full_path,
            geometry_type=geometry_type.upper()
        )
        
        # Verify the output was created and has records
        if arcpy.Exists(out_fc_full_path):
            try:
                count_result = arcpy.management.GetCount(out_fc_full_path)
                record_count_str = str(count_result.getOutput(0))
                record_count = int(record_count_str) if record_count_str.isdigit() else 0
                
                desc = arcpy.Describe(out_fc_full_path)
                log.info("✅ SUCCESS: Created FC '%s' with %d records, geometry type: %s", 
                         tgt_name, record_count, desc.shapeType)
                
                if record_count == 0:
                    log.warning("⚠️ Created feature class is empty! Check input data and CRS compatibility.")
                    
            except Exception as verify_error:
                log.warning("⚠️ Could not verify output FC: %s", verify_error)
        else:
            log.error("❌ Output feature class was not created: %s", out_fc_full_path)
            
        lg_sum.info("   📄 JSON ➜ staged : %s", tgt_name)
        summary.log_staging("done")
        
    except arcpy.ExecuteError as arc_error:
        arcpy_messages: str = arcpy.GetMessages(2)
        log.error("❌ arcpy.conversion.JSONToFeatures failed for JSON/GeoJSON %s → %s: %s. ArcPy Messages: %s",
                  json_file_path.name, tgt_name, arc_error, arcpy_messages, exc_info=True)
        summary.log_staging("error")
        summary.log_error(json_file_path.name, f"JSONToFeatures failed: {arc_error}")
    except Exception as generic_error:
        log.error("❌ Unexpected error processing JSON/GeoJSON %s → %s: %s",
                  json_file_path.name, out_fc_full_path, generic_error, exc_info=True)
        summary.log_staging("error")
        summary.log_error(json_file_path.name, f"Unexpected error: {generic_error}")


def detect_geometry_type(json_file_path: Path) -> str:
    """🔍 Detect the primary geometry type from GeoJSON file.
    
    Args:
        json_file_path: Path to the JSON/GeoJSON file.
        
    Returns:
        ArcGIS geometry type string (POINT, POLYLINE, POLYGON, etc.).
    """
    try:
        with json_file_path.open('r', encoding='utf-8') as f:
            data = json.load(f)
        
        log.debug("🔍 GeoJSON data structure: type=%s", data.get("type"))
        
        geometry_types = set()
        
        # Handle FeatureCollection
        if data.get("type") == "FeatureCollection":
            features = data.get("features", [])
            log.debug("🔍 Found %d features in FeatureCollection", len(features))
            
            for i, feature in enumerate(features[:10]):  # Sample first 10 features
                geom = feature.get("geometry", {})
                geom_type = geom.get("type")
                if geom_type:
                    geometry_types.add(geom_type)
                    log.debug("🔍 Feature %d: geometry type = %s", i, geom_type)

        # Handle single Feature
        elif data.get("type") == "Feature":
            geom = data.get("geometry", {})
            geom_type = geom.get("type")
            if geom_type:
                geometry_types.add(geom_type)
                log.debug("🔍 Single feature: geometry type = %s", geom_type)
        
        log.info("🔍 Detected geometry types in %s: %s", json_file_path.name, geometry_types)
        
        # Map GeoJSON types to ArcGIS types
        type_mapping = {
            "Point": "POINT",
            "MultiPoint": "MULTIPOINT", 
            "LineString": "POLYLINE",
            "MultiLineString": "POLYLINE",
            "Polygon": "POLYGON",
            "MultiPolygon": "POLYGON"
        }
        
        if len(geometry_types) == 1:
            geojson_type = list(geometry_types)[0]
            arcgis_type = type_mapping.get(geojson_type, "POLYGON")
            log.info("🔍 Mapping %s → %s for %s", geojson_type, arcgis_type, json_file_path.name)
            return arcgis_type
        elif len(geometry_types) > 1:
            log.warning("⚠️ Mixed geometry types found in %s: %s. Using POLYGON as default.", 
                       json_file_path.name, geometry_types)
            return "POLYGON"
        else:
            log.warning("⚠️ No geometry type detected in %s. Using POLYGON as default.", 
                       json_file_path.name)
            return "POLYGON"
            
    except Exception as e:
        log.error("❌ Failed to detect geometry type for %s: %s", 
                   json_file_path.name, e, exc_info=True)
        return "POLYGON"
