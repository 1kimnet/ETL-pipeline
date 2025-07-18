# etl/pipeline.py (complete working version)
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import arcpy
import yaml

from .handlers import HANDLER_MAP, geoprocess
from .loaders import ArcPyFileGDBLoader
from .models import Source
from .utils import ensure_dirs, paths
from .utils.cleanup import cleanup_before_pipeline_run
from .utils.performance import monitor_performance
from .utils.run_summary import Summary
from .utils.naming import sanitize_sde_name
from .mapping import get_mapping_manager, MappingManager
from .monitoring import (
    get_structured_logger,
    get_metrics_collector,
    get_pipeline_monitor,
)
from .utils.performance import ParallelProcessor
from .utils.recovery import (
    get_global_recovery_manager,
    graceful_degradation,
    recoverable_operation,
    GracefulDegradationConfig,
    RecoveryStrategy
)
from .utils.rollback import get_global_rollback_manager, execute_pipeline_rollback


class Pipeline:
    """End-to-end ETL: Download → Stage → Geoprocess → Load SDE."""

    def __init__(
        self,
        sources_yaml: Path,
        *,
        config_yaml_path: Optional[Path] = None,
        mappings_yaml_path: Optional[Path] = None,
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
                logging.getLogger("summary").info(
                    "🛠  Using global config %s", config_yaml_path
                )
            except (yaml.YAMLError, OSError) as exc:
                logging.getLogger("summary").warning(
                    "⚠️  Could not load %s (%s) – using defaults",
                    config_yaml_path,
                    exc,
                )
                self.global_cfg = {}
        else:
            self.global_cfg = {}
            logging.getLogger("summary").info(
                "ℹ️  No global config file supplied – using defaults"
            )

        # Initialize mapping manager
        self.mapping_manager = get_mapping_manager(mappings_yaml_path)

        # Initialize monitoring and metrics
        self.logger = get_structured_logger("pipeline")
        self.metrics = get_metrics_collector()
        self.monitor = get_pipeline_monitor()

        # Initialize performance processor
        max_workers = self.global_cfg.get("parallel_workers", 2)
        self.parallel_processor = ParallelProcessor(max_workers=max_workers)

        # Initialize recovery and rollback systems
        self.recovery_manager = get_global_recovery_manager()
        self.rollback_manager = get_global_rollback_manager()
        self.degradation_config = GracefulDegradationConfig()
        
        # Configure recovery strategies based on config
        self._setup_pipeline_recovery_strategies()

        ensure_dirs()

    def _setup_pipeline_recovery_strategies(self) -> None:
        """Setup pipeline-specific recovery strategies."""
        # Configure degradation thresholds based on config
        max_concurrent = self.global_cfg.get("concurrent_download_workers", 5)
        timeout = self.global_cfg.get("timeout", 30)
        
        # Update degradation config
        self.degradation_config.max_concurrent_downloads = max_concurrent
        self.degradation_config.timeout_seconds = timeout
        
        # Register pipeline-specific recovery strategies
        from .exceptions import NetworkError, SourceError, SystemError
        
        # Network recovery with config-aware degradation
        def degrade_network_config():
            level = self.recovery_manager.get_degradation_level()
            degraded = self.degradation_config.get_degraded_config(level + 1)
            self.logger.info(
                "🔄 Degrading network settings: concurrent=%d, timeout=%d",
                degraded["concurrent_downloads"],
                degraded["timeout"]
            )
            # Update global config for subsequent operations
            self.global_cfg["concurrent_download_workers"] = degraded["concurrent_downloads"]
            self.global_cfg["timeout"] = degraded["timeout"]
            return degraded
        
        self.recovery_manager.register_recovery_strategy(
            NetworkError,
            RecoveryStrategy.DEGRADE,
            action_func=degrade_network_config,
            description="Reduce concurrent downloads and increase timeout",
            priority=4
        )
        
        # Source recovery with cached data fallback
        def use_cached_data():
            # Look for previous successful downloads
            cache_dir = paths.DOWNLOADS
            if cache_dir.exists():
                self.logger.info("🔄 Looking for cached data in %s", cache_dir)
                return list(cache_dir.glob("*.json"))
            return []
        
        self.recovery_manager.register_recovery_strategy(
            SourceError,
            RecoveryStrategy.FALLBACK,
            action_func=use_cached_data,
            description="Use previously downloaded data",
            priority=3
        )

    @monitor_performance("pipeline_run")
    def run(self) -> None:
        lg_sum = logging.getLogger("summary")

        # Start pipeline monitoring
        run_id = f"pipeline_{int(time.time())}"
        current_run = self.monitor.start_run(run_id)

        self.logger.info(
            "🚀 Starting ETL pipeline run",
            run_id=run_id,
            sources_file=str(self.sources_yaml_path),
        )
        self.metrics.set_gauge("pipeline.status", 1)  # 1 = running

        # ---------- 0. PRE-PIPELINE CLEANUP -------------------------------
        # Clean downloads and staging folders for fresh data
        cleanup_downloads = self.global_cfg.get("cleanup_downloads_before_run", True)
        cleanup_staging = self.global_cfg.get("cleanup_staging_before_run", True)

        if cleanup_downloads or cleanup_staging:
            lg_sum.info("🧹 Starting pre-pipeline cleanup...")
            cleanup_before_pipeline_run(cleanup_downloads, cleanup_staging)

        # ---------- 1. DOWNLOAD & STAGING ---------------------------------
        sources = list(Source.load_all(self.sources_yaml_path))
        self.logger.info("📋 Found sources to process", source_count=len(sources))

        # Create SDE loader for proper source-to-dataset mapping
        from .models import SdeLoader, AppConfig
        app_config = AppConfig(sde_dataset_pattern=self.global_cfg.get("sde_dataset_pattern", "Underlag_{authority}"))
        self.sde_loader = SdeLoader(app_config, sources)

        # Log concurrent download configuration
        if self.global_cfg.get("enable_concurrent_downloads", True):
            self.logger.info("🚀 Concurrent downloads enabled: REST=%d, OGC=%d, Files=%d workers",
                           self.global_cfg.get("concurrent_download_workers", 5),
                           self.global_cfg.get("concurrent_collection_workers", 3),
                           self.global_cfg.get("concurrent_file_workers", 4))
        else:
            self.logger.info("⚠️ Concurrent downloads disabled - using sequential processing")

        for src in sources:
            if not src.enabled:
                self.logger.info("⏭ Skipped (disabled)", source_name=src.name)
                self.summary.log_download("skip")
                continue

            handler_cls = self.handler_map.get(src.type)
            if not handler_cls:
                self.logger.warning(
                    "🤷 Unknown type, skipped",
                    source_name=src.name,
                    source_type=src.type,
                )
                self.summary.log_download("skip")
                continue

            # Wrap source processing in graceful degradation
            with graceful_degradation(f"download_source_{src.name}", self.recovery_manager):
                try:
                    start_time = time.time()
                    self.logger.info("🚚 %s" % src.name)

                    # Create handler with context manager for proper cleanup
                    with handler_cls(src, global_config=self.global_cfg) as handler:
                        handler.fetch()

                    download_duration = time.time() - start_time
                    self.metrics.record_timing(
                        "download.duration_ms",
                        download_duration * 1000,
                        tags={"source": src.name, "type": src.type, "concurrent": str(self.global_cfg.get("enable_concurrent_downloads", True))},
                    )
                    self.metrics.increment_counter(
                        "download.success", tags={"source": src.name}
                    )

                    # Log performance improvement hint
                    if download_duration > 60 and src.type in ["rest_api", "ogc_api"] and not self.global_cfg.get("enable_concurrent_downloads", True):
                        self.logger.info("💡 Performance hint: Enable concurrent downloads for faster processing of %s sources", src.type)

                    self.summary.log_download("done")
                    self.monitor.record_source_processed(success=True)

                except Exception as exc:
                    # Let recovery manager handle the error
                    recovery_result = self.recovery_manager.recover_from_error(
                        error=exc,
                        operation_context=f"download_source_{src.name}"
                    )
                    
                    if recovery_result.success:
                        self.logger.info("✅ Recovered from download error for %s", src.name)
                        self.summary.log_download("recovered")
                        self.monitor.record_source_processed(success=True, error=f"Recovered: {exc}")
                    else:
                        self.summary.log_download("error")
                        self.summary.log_error(src.name, str(exc))
                        self.logger.error("❌ Download failed and recovery failed", source_name=src.name, error=exc)

                        self.metrics.increment_counter(
                            "download.error", tags={"source": src.name}
                        )
                        self.monitor.record_source_processed(success=False, error=str(exc))

                        if not self.global_cfg.get("continue_on_failure", True):
                            self.monitor.end_run("failed")
                            # Execute pipeline rollback before raising
                            execute_pipeline_rollback(f"Source download failed: {src.name}")
                            raise  # ---------- 2. STAGE → staging.gdb --------------------------------
        self.logger.info("📦 Starting staging phase")

        # Reset staging GDB to avoid conflicts with existing feature classes
        try:
            from .utils.gdb_utils import reset_gdb

            if paths.GDB.exists():
                self.logger.info("🗑️ Resetting existing staging.gdb")
                reset_gdb(paths.GDB)
            self.logger.info("✅ Staging GDB reset complete")
        except (ImportError, arcpy.ExecuteError, OSError) as reset_exc:
            self.logger.warning("⚠️ Failed to reset staging GDB", error=reset_exc)
            if not self.global_cfg.get("continue_on_failure", True):
                self.monitor.end_run("failed")
                raise

        staging_success = True
        
        # Wrap staging in graceful degradation
        with graceful_degradation("staging_phase", self.recovery_manager):
            try:
                start_time = time.time()
                loader = ArcPyFileGDBLoader(
                    summary=self.summary,
                    gdb_path=paths.GDB,
                    sources_yaml_path=self.sources_yaml_path,
                )
                loader.run()

                staging_duration = time.time() - start_time
                self.metrics.record_timing("staging.duration_ms", staging_duration * 1000)
                self.metrics.increment_counter("staging.success")

                self.logger.info(
                    "✅ Staging.gdb built successfully", duration_seconds=staging_duration
                )

            except Exception as exc:
                # Attempt recovery for staging failures
                recovery_result = self.recovery_manager.recover_from_error(
                    error=exc,
                    operation_context="staging_phase"
                )
                
                if recovery_result.success:
                    self.logger.info("✅ Recovered from staging error")
                    self.summary.log_staging("recovered")
                else:
                    staging_success = False
                    self.summary.log_staging("error")
                    self.summary.log_error("GDB loader", str(exc))

                    self.logger.error("❌ GDB load failed and recovery failed", error=exc)
                    self.metrics.increment_counter("staging.error")

                    if not self.global_cfg.get("continue_on_failure", True):
                        self.monitor.end_run("failed")
                        execute_pipeline_rollback(f"Staging failed: {exc}")
                        raise
            else:
                self.logger.warning("⚠️ Continuing despite staging failures")

        # ---------- 3. GEOPROCESS staging.gdb IN-PLACE -------------------
        if staging_success or self.global_cfg.get("continue_on_failure", True):
            self._apply_geoprocessing_inplace()

        # ---------- 4. LOAD TO SDE from staging.gdb -----------------------
        if staging_success or self.global_cfg.get("continue_on_failure", True):
            self._load_to_sde(paths.GDB)
        else:
            lg_sum.warning(
                "⚠️ Skipping SDE loading due to staging failures"
            )

        # Pipeline completion
        self.metrics.set_gauge("pipeline.status", 0)  # 0 = completed
        self.monitor.end_run("completed")

        # Clean up HTTP sessions
        try:
            from .utils.http_session import close_all_http_sessions
            close_all_http_sessions()
            self.logger.debug("🧹 HTTP sessions closed")
        except Exception as e:
            self.logger.warning("Failed to close HTTP sessions: %s", e)

        # Log final metrics and recovery statistics
        pipeline_stats = self.monitor.get_current_run()
        recovery_stats = self.recovery_manager.get_recovery_stats()
        
        if pipeline_stats:
            self.logger.info(
                "🏁 Pipeline completed successfully",
                duration_seconds=pipeline_stats.duration,
                sources_processed=pipeline_stats.sources_processed,
                success_rate=pipeline_stats.success_rate,
                degradation_level=self.recovery_manager.get_degradation_level()
            )
        
        # Log recovery statistics
        if recovery_stats:
            self.logger.info("📊 Recovery Statistics:")
            for operation, stats in recovery_stats.items():
                self.logger.info(
                    "   %s: %d attempts, %.1f%% success rate",
                    operation,
                    stats["attempts"],
                    stats["success_rate"]
                )
        
        # Reset degradation level for next run
        self.recovery_manager.reset_degradation_level()

        self.summary.dump()

    @monitor_performance("geoprocessing")
    def _apply_geoprocessing_inplace(self) -> None:
        """🔄 Step 3: In-place geoprocessing of staging.gdb (clip + project only)"""

        # Check if geoprocessing is enabled
        geoprocessing_config = self.global_cfg.get("geoprocessing", {})
        if not geoprocessing_config.get("enabled", True):
            self.logger.info("⏭️ Geoprocessing disabled")
            return

        # Get AOI boundary path
        aoi_boundary = Path(
            geoprocessing_config.get(
                "aoi_boundary", "data/connections/municipality_boundary.shp"
            )
        )
        if not aoi_boundary.exists():
            self.logger.error("❌ AOI boundary not found", aoi_path=str(aoi_boundary))
            if not self.global_cfg.get("continue_on_failure", True):
                raise FileNotFoundError(f"AOI boundary not found: {aoi_boundary}")
            return

        try:
            start_time = time.time()
            self.logger.info(
                "🔄 Starting geoprocessing",
                target_srid=geoprocessing_config.get("target_srid", 3006),
                aoi_path=str(aoi_boundary),
            )

            # Perform simplified in-place geoprocessing (clip + project only)
            geoprocess.geoprocess_staging_gdb(
                staging_gdb=paths.GDB,
                aoi_fc=aoi_boundary,
                target_srid=geoprocessing_config.get("target_srid", 3006),
                pp_factor=geoprocessing_config.get("parallel_processing_factor", "100"),
            )

            geoprocessing_duration = time.time() - start_time
            self.metrics.record_timing(
                "geoprocessing.duration_ms", geoprocessing_duration * 1000
            )
            self.metrics.increment_counter("geoprocessing.success")

            self.logger.info(
                "✅ Geoprocessing complete", duration_seconds=geoprocessing_duration
            )

        except arcpy.ExecuteError as exc:
            self.logger.error("❌ Geoprocessing failed", error=exc)
            self.metrics.increment_counter("geoprocessing.error")
            if not self.global_cfg.get("continue_on_failure", True):
                raise

    @monitor_performance("sde_loading")
    def _load_to_sde(self, source_gdb: Path) -> None:
        """🚚 Step 4: Load processed GDB to production SDE with parallel processing"""

        if not source_gdb.exists():
            self.logger.error("❌ Source GDB not found", gdb_path=str(source_gdb))
            return

        # Get SDE connection from config and validate
        sde_connection = self.global_cfg.get(
            "sde_connection_file",
            "data/connections/prod.sde",
        )
        sde_connection_path = Path(sde_connection)

        if not self._validate_sde_connection_file(sde_connection_path):
            return

        self.logger.info(
            "🚚 Starting SDE loading",
            source_gdb=source_gdb.name,
            sde_connection=sde_connection,
        )

        all_feature_classes = self._discover_feature_classes(source_gdb)
        if not all_feature_classes:
            self.logger.warning("⚠️ No feature classes found", gdb_path=str(source_gdb))
            return

        self.logger.info(
            "📋 Feature classes discovered", fc_count=len(all_feature_classes)
        )

        # Check if parallel loading is enabled
        use_parallel = self.global_cfg.get("parallel_sde_loading", True)

        if use_parallel and len(all_feature_classes) > 1:
            self._load_to_sde_parallel(all_feature_classes, sde_connection)
        else:
            self._load_to_sde_sequential(all_feature_classes, sde_connection)

        self.logger.info(
            "📊 SDE loading complete",
            loaded=self.summary.sde["done"],
            errors=self.summary.sde["error"],
        )

    def _validate_sde_connection_file(self, path: Path) -> bool:
        if not path.exists():
            self.logger.error("❌ SDE connection file not found", sde_path=str(path))
            return False
        return True

    def _discover_feature_classes(self, gdb: Path) -> list[tuple[str, str]]:
        with arcpy.EnvManager(workspace=str(gdb), overwriteOutput=True):
            all_fcs: list[tuple[str, str]] = []
            standalone = arcpy.ListFeatureClasses()
            if standalone:
                self.logger.debug(
                    "📄 Found standalone feature classes", count=len(standalone)
                )
                for fc in standalone:
                    # Use full path for source, just name for target
                    fc_full_path = str(gdb / fc)
                    all_fcs.append((fc_full_path, fc))
            datasets = arcpy.ListDatasets(feature_type="Feature")
            if datasets:
                self.logger.debug("📁 Found feature datasets", count=len(datasets))
                for ds in datasets:
                    ds_fcs = arcpy.ListFeatureClasses(feature_dataset=ds)
                    if ds_fcs:
                        for fc in ds_fcs:
                            # Use full path for source, just name for target
                            fc_full_path = str(gdb / ds / fc)
                            all_fcs.append((fc_full_path, fc))
        return all_fcs

    def _load_fc_to_sde(
        self, source_fc_path: str, fc_name: str, sde_connection: str
    ) -> None:
        """🚚 Load single FC to SDE with truncate-and-load strategy."""
        lg_sum = logging.getLogger("summary")

        # Apply naming logic: RAA_byggnader_sverige_point → GNG.RAA\byggnader_sverige_point
        dataset, sde_fc_name = self._get_sde_names(fc_name)
        sde_dataset_path = f"{sde_connection}\\{dataset}"
        target_path = f"{sde_dataset_path}\\{sde_fc_name}"

        lg_sum.info(
            "🔍 SDE mapping: '%s' → dataset='%s', fc='%s'",
            fc_name,
            dataset,
            sde_fc_name,
        )
        lg_sum.info(
            "🔍 Target paths: dataset='%s', fc='%s'", sde_dataset_path, target_path
        )

        # Get load strategy from config (default: truncate_and_load)
        load_strategy = self.global_cfg.get("sde_load_strategy", "truncate_and_load")

        try:
            # Check if target dataset exists in SDE
            if not arcpy.Exists(sde_dataset_path):
                lg_sum.error("❌ SDE dataset does not exist: %s", dataset)
                lg_sum.error(
                    "   Create the dataset '%s' in SDE first, then re-run the pipeline",
                    dataset,
                )
                lg_sum.error(
                    "   Create the dataset '%s' in SDE first, then re-run the pipeline",
                    dataset,
                )
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
                record_count = (
                    int(record_count_str) if record_count_str.isdigit() else 0
                )
                record_count = (
                    int(record_count_str) if record_count_str.isdigit() else 0
                )
            except (ValueError, AttributeError):
                record_count = 0

            lg_sum.info(
                "🔍 Source FC info: type=%s, geom=%s, records=%d",
                desc.dataType,
                desc.shapeType,
                record_count,
            )

            lg_sum.info(
                "🔍 Source FC info: type=%s, geom=%s, records=%d",
                desc.dataType,
                desc.shapeType,
                record_count,
            )

            self._load_single_feature_class(
                source_fc_path,
                target_path,
                sde_dataset_path,
                dataset,
                sde_fc_name,
                load_strategy,
                record_count,
            )

        except arcpy.ExecuteError:
            lg_sum.error(
                "❌ SDE operation failed for %s: %s",
                source_fc_path,
                arcpy.GetMessages(2),
            )
            lg_sum.error(
                "❌ Check SDE permissions and ensure dataset '%s' exists", dataset
            )
            lg_sum.error(
                "❌ SDE operation failed for %s: %s",
                source_fc_path,
                arcpy.GetMessages(2),
            )
            lg_sum.error(
                "❌ Check SDE permissions and ensure dataset '%s' exists", dataset
            )
            raise

    def _load_single_feature_class(
        self,
        source_fc_path: str,
        target_path: str,
        sde_dataset_path: str,
        dataset: str,
        sde_fc_name: str,
        load_strategy: str,
        record_count: int = 0,
    ) -> None:
        start_time = time.time()

        if arcpy.Exists(target_path):
            if load_strategy == "truncate_and_load":
                lg_sum.info("🗑️ Truncating existing FC: %s\\%s", dataset, sde_fc_name)
                arcpy.management.TruncateTable(target_path)
                lg_sum.info("📄 Loading fresh data to: %s\\%s", dataset, sde_fc_name)
                arcpy.management.Append(
                    inputs=source_fc_path, target=target_path, schema_type="NO_TEST"
                )
                lg_sum.info("🚚→  %s\\%s (truncated + loaded)", dataset, sde_fc_name)
            elif load_strategy == "replace":
                self.logger.info(
                    "🗑️ Deleting existing FC", dataset=dataset, fc=sde_fc_name
                )
                arcpy.management.Delete(target_path)
                self.logger.info(
                    "🆕 Creating replacement FC",
                    dataset=dataset,
                    fc=sde_fc_name,
                    records=record_count,
                )
                arcpy.conversion.FeatureClassToFeatureClass(
                    in_features=source_fc_path,
                    out_path=sde_dataset_path,
                    out_name=sde_fc_name,
                )

                duration = time.time() - start_time
                self.metrics.record_timing("sde.replace.duration_ms", duration * 1000)
                self.logger.info(
                    "🚚→ Replaced",
                    dataset=dataset,
                    fc=sde_fc_name,
                    duration_seconds=duration,
                )
            elif load_strategy == "append":
                self.logger.warning(
                    "⚠️ Appending to existing FC (may create duplicates)",
                    dataset=dataset,
                    fc=sde_fc_name,
                )
                arcpy.management.Append(
                    inputs=source_fc_path, target=target_path, schema_type="NO_TEST"
                )
                lg_sum.info("🚚→  %s\\%s (appended)", dataset, sde_fc_name)
            else:
                self.logger.error(
                    "❌ Unknown sde_load_strategy", strategy=load_strategy
                )
        else:
            self.logger.info(
                "🆕 Creating new FC",
                dataset=dataset,
                fc=sde_fc_name,
                records=record_count,
            )

            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_fc_path,
                out_path=sde_dataset_path,
                out_name=sde_fc_name,
            )

            duration = time.time() - start_time
            self.metrics.record_timing("sde.create.duration_ms", duration * 1000)
            self.logger.info(
                "🚚→ Created",
                dataset=dataset,
                fc=sde_fc_name,
                duration_seconds=duration,
            )

    def _get_sde_names(self, fc_name: str) -> Tuple[str, str]:
        """📝 Extract SDE dataset and feature class names from staging name.

        Logic: SKS_naturvarden_point → dataset="GNG.Underlag_SKS", fc="naturvarden_point"
        """
        parts = fc_name.split("_", 1)
        if len(parts) < 2:
            # No underscore → treat as MISC
            authority = "MISC"
            fc_remainder = fc_name.lower()
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
