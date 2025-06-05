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

        if not self._validate_sde_connection_file(sde_connection_path):
            return

        lg_sum.info("🚚 Loading to SDE from processed %s", source_gdb.name)
        
        # Use EnvManager for clean environment handling
        with arcpy.EnvManager(workspace=str(source_gdb), overwriteOutput=True):
            all_feature_classes = self._discover_feature_classes(source_gdb)
            
            if not all_feature_classes:
                lg_sum.warning("⚠️ No feature classes found in %s", source_gdb)
                return
                
            lg_sum.info("📋 Found %d total feature classes to load", len(all_feature_classes))
            
            for fc_path, fc_name in all_feature_classes:
                try:
                    self._load_fc_to_sde(fc_path, fc_name, sde_connection)
                    self.summary.log_sde("done")  # Track success
                except Exception as exc:
                    self.summary.log_sde("error")  # Track error
                    self.summary.log_error(fc_name, f"SDE load failed: {exc}")
                    lg_sum.error("❌ Failed to load %s to SDE: %s", fc_path, exc)
                    if not self.global_cfg.get("continue_on_failure", True):
                        raise
                
            lg_sum.info("📊 SDE loading complete: %d loaded, %d errors",
                        self.summary.sde["done"], self.summary.sde["error"])

    def _validate_sde_connection_file(self, sde_connection: Path) -> bool:
        """✅ Ensure the SDE connection file exists."""
        lg_sum = logging.getLogger("summary")
        if not sde_connection.exists():
            lg_sum.error("❌ SDE connection file not found: %s", sde_connection)
            return False
        return True

    def _discover_feature_classes(self, gdb_path: Path) -> list[tuple[str, str]]:
        """🔎 List feature classes in a GDB, including datasets."""
        lg_sum = logging.getLogger("summary")
        all_feature_classes: list[tuple[str, str]] = []
        with arcpy.EnvManager(workspace=str(gdb_path), overwriteOutput=True):
            standalone_fcs = arcpy.ListFeatureClasses()
            if standalone_fcs:
                lg_sum.info("📄 Found %d feature classes in root of GDB", len(standalone_fcs))
                for fc in standalone_fcs:
                    all_feature_classes.append((fc, fc))

            feature_datasets = arcpy.ListDatasets(feature_type="Feature")
            if feature_datasets:
                lg_sum.info("📁 Found %d feature datasets", len(feature_datasets))
                for dataset in feature_datasets:
                    dataset_fcs = arcpy.ListFeatureClasses(feature_dataset=dataset)
                    if dataset_fcs:
                        for fc in dataset_fcs:
                            fc_path = f"{dataset}\\{fc}"
                            all_feature_classes.append((fc_path, fc))
        return all_feature_classes

    def _load_feature_class(
        self,
        source_fc_path: str,
        dataset: str,
        sde_dataset_path: str,
        sde_fc_name: str,
        load_strategy: str,
    ) -> None:
        """📥 Load data into SDE based on the chosen strategy."""
        lg_sum = logging.getLogger("summary")
        target_path = f"{sde_dataset_path}\\{sde_fc_name}"

        if arcpy.Exists(target_path):
            if load_strategy == "truncate_and_load":
                lg_sum.info("🗑️ Truncating existing FC: %s\\%s", dataset, sde_fc_name)
                arcpy.management.TruncateTable(target_path)

                lg_sum.info("📄 Loading fresh data to: %s\\%s", dataset, sde_fc_name)
                arcpy.management.Append(
                    inputs=source_fc_path,
                    target=target_path,
                    schema_type="NO_TEST",
                )
                lg_sum.info("🚚→  %s\\%s (truncated + loaded)", dataset, sde_fc_name)

            elif load_strategy == "replace":
                lg_sum.info("🗑️ Deleting existing FC: %s\\%s", dataset, sde_fc_name)
                arcpy.management.Delete(target_path)

                lg_sum.info("🆕 Creating replacement FC: %s\\%s", dataset, sde_fc_name)
                arcpy.conversion.FeatureClassToFeatureClass(
                    in_features=source_fc_path,
                    out_path=sde_dataset_path,
                    out_name=sde_fc_name,
                )
                lg_sum.info("🚚→  %s\\%s (replaced)", dataset, sde_fc_name)

            elif load_strategy == "append":
                lg_sum.warning(
                    "⚠️ Appending to existing FC (may create duplicates): %s\\%s",
                    dataset,
                    sde_fc_name,
                )
                arcpy.management.Append(
                    inputs=source_fc_path,
                    target=target_path,
                    schema_type="NO_TEST",
                )
                lg_sum.info("🚚→  %s\\%s (appended)", dataset, sde_fc_name)

            else:
                lg_sum.error("❌ Unknown sde_load_strategy: %s", load_strategy)
                return
        else:
            lg_sum.info("🆕 Creating new FC: %s\\%s", dataset, sde_fc_name)
            lg_sum.info(
                "🔍 Using: in_features='%s', out_path='%s', out_name='%s'",
                source_fc_path,
                sde_dataset_path,
                sde_fc_name,
            )
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_fc_path,
                out_path=sde_dataset_path,
                out_name=sde_fc_name,
            )
            lg_sum.info("🚚→  %s\\%s (created)", dataset, sde_fc_name)

    def _load_fc_to_sde(self, source_fc_path: str, fc_name: str, sde_connection: str) -> None:
        """🚚 Load single FC to SDE with truncate-and-load strategy."""
        lg_sum = logging.getLogger("summary")
        
        # Apply naming logic: RAA_byggnader_sverige_point → GNG.RAA\byggnader_sverige_point
        dataset, sde_fc_name = self._get_sde_names(fc_name)
        sde_dataset_path = f"{sde_connection}\\{dataset}"
        target_path = f"{sde_dataset_path}\\{sde_fc_name}"
        
        lg_sum.info("🔍 SDE mapping: '%s' → dataset='%s', fc='%s'", fc_name, dataset, sde_fc_name)
        lg_sum.info("🔍 Target paths: dataset='%s', fc='%s'", sde_dataset_path, target_path)
        
        # Get load strategy from config (default: truncate_and_load)
        load_strategy = self.global_cfg.get("sde_load_strategy", "truncate_and_load")
        
        try:
            # Check if target dataset exists in SDE
            if not arcpy.Exists(sde_dataset_path):
                lg_sum.error("❌ SDE dataset does not exist: %s", dataset)
                lg_sum.error("   Create the dataset '%s' in SDE first, then re-run the pipeline", dataset)
                lg_sum.error("   Run: python scripts/create_sde_datasets.py")
                return
                
            # Verify source FC exists and get its properties
            if not arcpy.Exists(source_fc_path):
                lg_sum.error("❌ Source FC does not exist: %s", source_fc_path)
                return
                
            # Get source FC geometry type for debugging
            desc = arcpy.Describe(source_fc_path)
            try:
                count_result = arcpy.management.GetCount(source_fc_path)
                record_count_str = str(count_result.getOutput(0))
                record_count = int(record_count_str) if record_count_str.isdigit() else 0
            except (ValueError, AttributeError):
                record_count = 0
                
            lg_sum.info("🔍 Source FC info: type=%s, geom=%s, records=%d", 
                       desc.dataType, desc.shapeType, record_count)
            self._load_feature_class(
                source_fc_path=source_fc_path,
                dataset=dataset,
                sde_dataset_path=sde_dataset_path,
                sde_fc_name=sde_fc_name,
                load_strategy=load_strategy,
            )
                
        except arcpy.ExecuteError:
            lg_sum.error("❌ SDE operation failed for %s: %s", source_fc_path, arcpy.GetMessages(2))
            lg_sum.error("❌ Check SDE permissions and ensure dataset '%s' exists", dataset)
            raise

    def _get_sde_names(self, fc_name: str) -> Tuple[str, str]:
        """📝 Extract SDE dataset and feature class names from staging name.
        
        Logic: SKS_naturvarden_point → dataset="GNG.Underlag_SKS", fc="naturvarden_point"
        """
        parts = fc_name.split("_", 1)
        if len(parts) < 2:
            dataset_suffix = "MISC"
            fc_name_clean = fc_name.lower()
        else:
            dataset_suffix, fc_name_clean = parts
            fc_name_clean = fc_name_clean.lower()
        
        # Use your existing Underlag pattern
        schema = self.global_cfg.get("sde_schema", "GNG")
        
        # Special case for LSTD → LstD
        if dataset_suffix == "LSTD":
            dataset = f"{schema}.Underlag_LstD"
        else:
            dataset = f"{schema}.Underlag_{dataset_suffix}"
            
        return dataset, fc_name_clean