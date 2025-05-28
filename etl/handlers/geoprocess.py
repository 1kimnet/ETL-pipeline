# etl/handlers/geoprocess.py (using legacy ArcPy syntax for compatibility)
from __future__ import annotations

import logging
from pathlib import Path
from typing import Final, Dict, List, Optional

import arcpy

log: Final = logging.getLogger("summary")


def geoprocess_staging_gdb(
    staging_gdb: Path | str,
    aoi_fc: Path | str,
    target_srid: int = 3006,
    pp_factor: str = "100",
    create_datasets: bool = True,
    naming_rules: Optional[Dict[str, Dict[str, str]]] = None,
) -> None:
    """🔄 In-place geoprocessing of staging.gdb: clip, project, rename, organize."""
    # Validate inputs
    staging_gdb_path = Path(staging_gdb)
    aoi_fc_path = Path(aoi_fc)
    
    if not staging_gdb_path.exists():
        raise FileNotFoundError(f"Staging GDB not found: {staging_gdb_path}")
    if not aoi_fc_path.exists():
        raise FileNotFoundError(f"AOI feature class not found: {aoi_fc_path}")
    
    log.info("🔄 Starting in-place geoprocessing of %s", staging_gdb_path.name)
    
    # Configure environment using EnvManager
    with arcpy.EnvManager(
        workspace=str(staging_gdb_path),
        outputCoordinateSystem=arcpy.SpatialReference(target_srid),
        overwriteOutput=True,
        parallelProcessingFactor=pp_factor
    ):
        # Get list of feature classes to process
        original_fcs = arcpy.ListFeatureClasses()
        if not original_fcs:
            log.warning("⚠️ No feature classes found in %s", staging_gdb_path)
            return
            
        log.info("🔄 Processing %d feature classes in-place", len(original_fcs))
        
        # Step 1: Clip and project all FCs
        clip_and_project_fcs(original_fcs, aoi_fc_path)
        
        # Step 2: Apply naming rules and set aliases
        apply_naming_and_aliases(original_fcs, naming_rules or {})
        
        # Step 3: Create datasets and organize FCs
        if create_datasets:
            organize_into_datasets(str(staging_gdb_path), target_srid)
            
        log.info("✅ Geoprocessing complete for %s", staging_gdb_path.name)


def clip_and_project_fcs(feature_classes: List[str], aoi_fc: Path) -> None:
    """🔄 Clip and project all feature classes in-place."""
    log.info("✂️ Clipping and projecting feature classes")
    
    processed_count = 0
    error_count = 0
    
    for fc_name in feature_classes:
        try:
            # Create temporary clipped version
            temp_clipped = f"in_memory\\{fc_name}_temp"
            
            # Clip (projection handled by environment)
            arcpy.analysis.PairwiseClip(fc_name, str(aoi_fc), temp_clipped)
            
            # Replace original with clipped version
            arcpy.management.Delete(fc_name)
            arcpy.management.CopyFeatures(temp_clipped, fc_name)
            
            # Clean up temp
            arcpy.management.Delete(temp_clipped)

            log.info("   ✂️ clipped & projected ➜ %s", fc_name)
            processed_count += 1
            
        except arcpy.ExecuteError:
            log.error("   ❌ failed to process %s: %s", fc_name, arcpy.GetMessages(2))
            error_count += 1
            
    log.info("📊 Clip/project complete: %d processed, %d errors", processed_count, error_count)


def apply_naming_and_aliases(feature_classes: List[str], naming_rules: Dict[str, Dict[str, str]]) -> None:
    """📝 Apply naming rules and set aliases for feature classes."""
    log.info("📝 Applying naming rules and aliases")
    
    for fc_name in feature_classes:
        current_name = fc_name
        try:
            # Check if there's a naming rule for this FC
            if fc_name in naming_rules:
                rule = naming_rules[fc_name]
                new_name = rule.get("name")
                alias = rule.get("alias")
                
                # Rename if specified
                if new_name and new_name != fc_name:
                    arcpy.management.Rename(fc_name, new_name)
                    log.info("   📝 renamed: %s → %s", fc_name, new_name)
                    current_name = new_name
                
                # Set alias if specified
                if alias:
                    arcpy.AlterAliasName(current_name, alias)
                    log.info("   🏷️ alias set: %s → '%s'", current_name, alias)
            else:
                # Apply default alias based on FC name
                default_alias = generate_default_alias(current_name)
                arcpy.AlterAliasName(current_name, default_alias)
                log.info("   🏷️ default alias: %s → '%s'", current_name, default_alias)
                
        except arcpy.ExecuteError:
            log.error("   ❌ failed to rename/alias %s: %s", current_name, arcpy.GetMessages(2))


def generate_default_alias(fc_name: str) -> str:
    """📝 Generate a human-readable default alias from FC name."""
    if "_" in fc_name:
        _, _, rest = fc_name.partition("_")
        alias = rest.replace("_", " ").title()
    else:
        alias = fc_name.replace("_", " ").title()
    
    return alias


def organize_into_datasets(staging_gdb: str, target_srid: int) -> None:
    """📁 Create feature datasets and organize FCs by authority."""
    log.info("📁 Organizing feature classes into datasets")
    
    # Get current feature classes (after any renaming) using EnvManager
    with arcpy.EnvManager(workspace=staging_gdb):
        feature_classes = arcpy.ListFeatureClasses()
        
        # Group FCs by authority (prefix before first underscore)
        authority_groups: Dict[str, List[str]] = {}
        for fc in feature_classes:
            authority = fc.split("_")[0] if "_" in fc else "MISC"
            if authority not in authority_groups:
                authority_groups[authority] = []
            authority_groups[authority].append(fc)
        
        # Create datasets and move FCs
        for authority, fcs in authority_groups.items():
            if len(fcs) == 0:
                continue
                
            dataset_name = f"{authority}_Dataset"
            
            try:
                # Create feature dataset
                arcpy.management.CreateFeatureDataset(
                    out_dataset_path=staging_gdb,
                    out_name=dataset_name,
                    spatial_reference=arcpy.SpatialReference(target_srid)
                )
                log.info("   📁 created dataset: %s", dataset_name)
                
                # Move FCs into dataset
                for fc in fcs:
                    try:
                        arcpy.conversion.FeatureClassToFeatureClass(
                            in_features=fc,
                            out_path=f"{staging_gdb}\\{dataset_name}",
                            out_name=fc
                        )
                        # Delete original FC outside dataset
                        arcpy.management.Delete(fc)
                        log.info("   📁 moved to dataset: %s → %s", fc, dataset_name)
                    except arcpy.ExecuteError:
                        log.error("   ❌ failed to move %s to dataset: %s", fc, arcpy.GetMessages(2))
                    
            except arcpy.ExecuteError:
                log.error("   ❌ failed to create dataset %s: %s", dataset_name, arcpy.GetMessages(2))


def create_naming_rules_from_config(config: Dict) -> Dict[str, Dict[str, str]]:
    """🔧 Create naming rules from configuration."""
    naming_rules = {}
    
    # Check if there are naming overrides in config
    overrides = config.get("geoprocessing", {}).get("naming_overrides", {})
    
    for fc_name, rule in overrides.items():
        if isinstance(rule, dict):
            naming_rules[fc_name] = rule
        elif isinstance(rule, str):
            # Simple string means it's just a new name
            naming_rules[fc_name] = {"name": rule}
    
    return naming_rules