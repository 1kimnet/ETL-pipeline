# etl/pipeline.py (complete working version)
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml
import arcpy

from .handlers import HANDLER_MAP
from .handlers import geoprocess
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths
from .utils.run_summary import Summary


class Pipeline:
    """End-to-end ETL: Download → Stage → Geoprocess → Load SDE."""

    def __init__(
        self,
        sources_yaml: Path,
        *,
        config_yaml_path: Optional[Path] = None,
        extra_handler_map: Dict[str, Any] | None = None,
        summary: Summary | None = None,
    ) -> None:
        self.sources_yaml_path = sources_yaml
        self.handler_map: Dict[str, Any] = {
            **HANDLER_MAP,
            **(extra_handler_map or {}),
        }
        self.summary = summary or Summary()

        # Load global config
        if config_yaml_path and config_yaml_path.exists():
            try:
                with config_yaml_path.open(encoding="utf-8") as fh:
                    self.global_cfg = yaml.safe_load(fh) or {}
                logging.getLogger("summary").info("🛠  Using global config %s", config_yaml_path)
            except Exception as exc:
                logging.getLogger("summary").warning("⚠️  Could not load %s (%s) – using defaults", config_yaml_path, exc)
                self.global_cfg = {}
        else:
            self.global_cfg = {}
            logging.getLogger("summary").info("ℹ️  No global config file supplied – using defaults")

        ensure_dirs()

    def run(self) -> None:
        lg_sum = logging.getLogger("summary")

        # ---------- 1. DOWNLOAD & STAGING ---------------------------------
        for src in Source.load_all(self.sources_yaml_path):
            if not src.enabled:
                lg_sum.info("⏭  Skipped (disabled): %s", src.name)
                self.summary.log_download("skip")
                continue

            handler_cls = self.handler_map.get(src.type)
            if not handler_cls:
                lg_sum.warning("🤷  Unknown type '%s' → skipped: %s", src.type, src.name)
                self.summary.log_download("skip")
                continue

            try:
                lg_sum.info("🚚 Downloading : %s", src.name)
                handler_cls(src, global_config=self.global_cfg).fetch()
                self.summary.log_download("done")
            except Exception as exc:
                self.summary.log_download("error")
                self.summary.log_error(src.name, str(exc))
                lg_sum.error("❌ Failed        : %s  (%s)", src.name, exc)
                if not self.global_cfg.get("continue_on_failure", True):
                    raise

        # ---------- 2. STAGE → staging.gdb --------------------------------
        lg_sum.info("📦 Staging complete → building FileGDB …")
        try:
            loader = ArcPyFileGDBLoader(
                summary=self.summary,
                gdb_path=paths.GDB,
                sources_yaml_path=self.sources_yaml_path,
            )
            loader.load_from_staging(paths.STAGING)
            lg_sum.info("✅ Staging.gdb built successfully")
        except Exception as exc:
            self.summary.log_staging("error")
            self.summary.log_error("GDB loader", str(exc))
            lg_sum.error("❌ GDB load failed (%s)", exc, exc_info=True)
            if not self.global_cfg.get("continue_on_failure", True):
                raise

        # ---------- 3. GEOPROCESS staging.gdb IN-PLACE -------------------
        self._apply_geoprocessing_inplace()

        # ---------- 4. LOAD TO SDE from staging.gdb -----------------------
        self._load_to_sde(paths.GDB)

        lg_sum.info("🏁 Pipeline finished – data live in PROD SDE")
        self.summary.dump()

    def _apply_geoprocessing_inplace(self) -> None:
        """🔄 Step 3: In-place geoprocessing of staging.gdb (clip + project only)"""
        lg_sum = logging.getLogger("summary")
        
        # Check if geoprocessing is enabled
        geoprocessing_config = self.global_cfg.get("geoprocessing", {})
        if not geoprocessing_config.get("enabled", True):
            lg_sum.info("⏭️ Geoprocessing disabled, staging.gdb unchanged")
            return
            
        # Get AOI boundary path
        aoi_boundary = Path(geoprocessing_config.get("aoi_boundary", "data/connections/municipality_boundary.shp"))
        if not aoi_boundary.exists():
            lg_sum.error("❌ AOI boundary not found: %s", aoi_boundary)
            if not self.global_cfg.get("continue_on_failure", True):
                raise FileNotFoundError(f"AOI boundary not found: {aoi_boundary}")
            return
            
        try:
            lg_sum.info("🔄 Geoprocessing staging.gdb in-place: clip + project")
            
            # Perform simplified in-place geoprocessing (clip + project only)
            geoprocess.geoprocess_staging_gdb(
                staging_gdb=paths.GDB,
                aoi_fc=aoi_boundary,
                target_srid=geoprocessing_config.get("target_srid", 3006),
                pp_factor=geoprocessing_config.get("parallel_processing_factor", "100")
            )
            
            lg_sum.info("✅ In-place geoprocessing complete")
            
        except Exception as exc:
            lg_sum.error("❌ Geoprocessing failed: %s", exc, exc_info=True)
            if not self.global_cfg.get("continue_on_failure", True):
                raise

    def _load_to_sde(self, source_gdb: Path) -> None:
        """🚚 Step 4: Load processed GDB to production SDE"""
        lg_sum = logging.getLogger("summary")
        
        if not source_gdb.exists():
            lg_sum.error("❌ Source GDB not found: %s", source_gdb)
            return

        # Get SDE connection from config
        sde_connection = self.global_cfg.get("sde_connection_file", "data/connections/prod.sde")
        sde_connection_path = Path(sde_connection)
        
        if not sde_connection_path.exists():
            lg_sum.error("❌ SDE connection file not found: %s", sde_connection_path)
            return

        lg_sum.info("🚚 Loading to SDE from processed %s", source_gdb.name)
        
        # Use EnvManager for clean environment handling
        with arcpy.EnvManager(workspace=str(source_gdb), overwriteOutput=True):
            # List all feature classes (from root and datasets)
            all_feature_classes = []
            
            # Get standalone feature classes from root
            standalone_fcs = arcpy.ListFeatureClasses()
            if standalone_fcs:
                lg_sum.info("📄 Found %d feature classes in root of GDB", len(standalone_fcs))
                for fc in standalone_fcs:
                    all_feature_classes.append((fc, fc))  # (path, name)
            
            # Get feature classes from datasets (if any exist)
            feature_datasets = arcpy.ListDatasets(feature_type="Feature")
            if feature_datasets:
                lg_sum.info("📁 Found %d feature datasets", len(feature_datasets))
                for dataset in feature_datasets:
                    dataset_fcs = arcpy.ListFeatureClasses(feature_dataset=dataset)
                    if dataset_fcs:
                        for fc in dataset_fcs:
                            fc_path = f"{dataset}\\{fc}"
                            all_feature_classes.append((fc_path, fc))
            
            if not all_feature_classes:
                lg_sum.warning("⚠️ No feature classes found in %s", source_gdb)
                return
                
            lg_sum.info("📋 Found %d total feature classes to load", len(all_feature_classes))
            
            loaded_count = 0
            error_count = 0
            
            for fc_path, fc_name in all_feature_classes:
                try:
                    self._load_fc_to_sde(fc_path, fc_name, sde_connection)
                    loaded_count += 1
                except Exception as exc:
                    error_count += 1
                    lg_sum.error("❌ Failed to load %s to SDE: %s", fc_path, exc)
                    if not self.global_cfg.get("continue_on_failure", True):
                        raise
                        
            lg_sum.info("📊 SDE loading complete: %d loaded, %d errors", loaded_count, error_count)

    def _load_fc_to_sde(self, source_fc_path: str, fc_name: str, sde_connection: str) -> None:
        """🚚 Load single FC to SDE with create-if-not-exists logic."""
        lg_sum = logging.getLogger("summary")
        
        # Apply naming logic: TRV_viltstangsel → TRV\viltstangsel
        dataset, sde_fc_name = self._get_sde_names(fc_name)
        sde_dataset_path = f"{sde_connection}\\{dataset}"
        target_path = f"{sde_dataset_path}\\{sde_fc_name}"
        
        try:
            # Check if target dataset exists in SDE
            if not arcpy.Exists(sde_dataset_path):
                lg_sum.error("❌ SDE dataset does not exist: %s", dataset)
                lg_sum.error("   Create the dataset '%s' in SDE first, then re-run the pipeline", dataset)
                return
                
            # Check if target FC exists
            if arcpy.Exists(target_path):
                # FC exists - append data
                lg_sum.info("📄 Appending to existing FC: %s\\%s", dataset, sde_fc_name)
                arcpy.management.Append(
                    inputs=source_fc_path,
                    target=target_path,
                    schema_type="NO_TEST"  # Assume schema matches
                )
                lg_sum.info("🚚→  %s\\%s (appended)", dataset, sde_fc_name)
                
            else:
                # FC doesn't exist - copy to create new
                lg_sum.info("🆕 Creating new FC: %s\\%s", dataset, sde_fc_name)
                arcpy.conversion.FeatureClassToFeatureClass(
                    in_features=source_fc_path,
                    out_path=sde_dataset_path,
                    out_name=sde_fc_name
                )
                lg_sum.info("🚚→  %s\\%s (created)", dataset, sde_fc_name)
                
        except arcpy.ExecuteError:
            lg_sum.error("❌ SDE operation failed for %s: %s", source_fc_path, arcpy.GetMessages(2))
            raise

    def _get_sde_names(self, fc_name: str) -> Tuple[str, str]:
        """📝 Extract SDE dataset and feature class names from staging name.
        
        Default logic: TRV_tv_viltstangsel → dataset="GNG.TRV", fc="tv_viltstangsel"
        """
        dataset_suffix, _, rest = fc_name.partition("_")
        
        # Get schema prefix from config (default: GNG)
        schema_prefix = self.global_cfg.get("sde_schema", "GNG")
        dataset = f"{schema_prefix}.{dataset_suffix}"
        
        return dataset, rest.lower()