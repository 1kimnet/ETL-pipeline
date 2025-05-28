# etl/pipeline.py (correct pipeline flow)
from __future__ import annotations

import logging
from datetime import datetime
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
                return

        # ---------- 3. GEOPROCESS staging.gdb IN-PLACE -------------------
        self._apply_geoprocessing_inplace()

        # ---------- 4. LOAD TO SDE from staging.gdb -----------------------
        self._load_to_sde(paths.GDB)

        lg_sum.info("🏁 Pipeline finished – data live in PROD SDE")
        self.summary.dump()

    def _apply_geoprocessing_inplace(self) -> None:
        """🔄 Step 3: In-place geoprocessing of staging.gdb"""
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
            lg_sum.info("🔄 Geoprocessing staging.gdb in-place: clip + project + rename + organize")
            
            # Create naming rules from config
            naming_rules = geoprocess.create_naming_rules_from_config(self.global_cfg)
            
            # Perform in-place geoprocessing
            geoprocess.geoprocess_staging_gdb(
                staging_gdb=paths.GDB,
                aoi_fc=aoi_boundary,
                target_srid=geoprocessing_config.get("target_srid", 3006),
                pp_factor=geoprocessing_config.get("parallel_processing_factor", "100"),
                create_datasets=geoprocessing_config.get("create_datasets", True),
                naming_rules=naming_rules
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
            feature_classes = arcpy.ListFeatureClasses()
            if not feature_classes:
                lg_sum.warning("⚠️ No feature classes found in %s", source_gdb)
                return
                
            lg_sum.info("📋 Found %d feature classes to load", len(feature_classes))
            
            loaded_count = 0
            error_count = 0
            
            for fc_name in feature_classes:
                try:
                    self._append_fc_to_sde(fc_name, sde_connection)
                    loaded_count += 1
                except Exception as exc:
                    error_count += 1
                    lg_sum.error("❌ Failed to load %s to SDE: %s", fc_name, exc)
                    if not self.global_cfg.get("continue_on_failure", True):
                        raise
                        
            lg_sum.info("📊 SDE loading complete: %d loaded, %d errors", loaded_count, error_count)

    def _append_fc_to_sde(self, staging_fc_name: str, sde_connection: str) -> None:
        """🚚 Append single FC to SDE with proper naming."""
        lg_sum = logging.getLogger("summary")
        
        # Apply naming logic: TRV_tv_viltstangsel → TRV\tv_viltstangsel
        dataset, fc_name = self._get_sde_names(staging_fc_name)
        target_path = f"{sde_connection}\\{dataset}\\{fc_name}"
        
        try:
            # Check if target exists
            if not arcpy.Exists(target_path):
                lg_sum.warning("⚠️ Target FC does not exist: %s\\%s", dataset, fc_name)
                return
                
            # Perform append
            arcpy.management.Append(
                inputs=staging_fc_name,
                target=target_path,
                schema_type="NO_TEST"  # Assume schema matches
            )
            
            lg_sum.info("🚚→  %s\\%s", dataset, fc_name)
            
        except arcpy.ExecuteError:
            lg_sum.error("❌ Append failed for %s: %s", staging_fc_name, arcpy.GetMessages(2))
            raise

    def _get_sde_names(self, fc_name: str) -> Tuple[str, str]:
        """📝 Extract SDE dataset and feature class names from staging name.
        
        Default logic: TRV_tv_viltstangsel → dataset="TRV", fc="tv_viltstangsel"
        """
        dataset, _, rest = fc_name.partition("_")
        return dataset, rest.lower()