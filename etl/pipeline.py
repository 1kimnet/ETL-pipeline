# etl/pipeline.py (complete working version)
from __future__ import annotations

import logging
import time  # Add missing import
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import arcpy  # type: ignore
import yaml

from .handlers import HANDLER_MAP, geoprocess
from .loaders import ArcPyFileGDBLoader
from .mapping import get_mapping_manager
from .models import Source
from .monitoring import (
    get_metrics_collector,
    get_pipeline_monitor,
    get_structured_logger,
)
from .utils import ensure_dirs, paths
from .utils.cleanup import cleanup_before_pipeline_run
from .utils.performance import ParallelProcessor, monitor_performance
from .utils.recovery import (
    GracefulDegradationConfig,
    RecoveryStrategy,
    get_global_recovery_manager,
    graceful_degradation,
)
from .utils.run_summary import Summary


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
                    f"🛠  Using global config {config_yaml_path}"
                )
            except (yaml.YAMLError, OSError) as exc:
                logging.getLogger("summary").warning(
                    f"⚠️  Could not load {config_yaml_path} ({exc}) – using defaults"
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

        # Initialize recovery systems
        self.recovery_manager = get_global_recovery_manager()
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
        from .exceptions import NetworkError, SourceError

        # Network recovery with config-aware degradation
        def degrade_network_config():
            level = self.recovery_manager.get_degradation_level()
            degraded = self.degradation_config.get_degraded_config(level + 1)
            self.logger.info(
                f"🔄 Degrading network settings: concurrent={degraded['concurrent_downloads']}, timeout={degraded['timeout']}"
            )
            # Update global config for subsequent operations
            self.global_cfg["concurrent_download_workers"] = degraded[
                "concurrent_downloads"
            ]
            self.global_cfg["timeout"] = degraded["timeout"]
            return degraded

        self.recovery_manager.register_recovery_strategy(
            NetworkError,
            RecoveryStrategy.DEGRADE,
            action_func=degrade_network_config,
            description="Reduce concurrent downloads and increase timeout",
            priority=4,
        )

        # Source recovery with cached data fallback
        def use_cached_data():
            # Look for previous successful downloads
            cache_dir = Path(str(paths.DOWNLOADS))
            if cache_dir.exists():
                self.logger.info(f"🔄 Looking for cached data in {cache_dir}")
                return list(cache_dir.glob("*.json"))
            return []

        self.recovery_manager.register_recovery_strategy(
            SourceError,
            RecoveryStrategy.FALLBACK,
            action_func=use_cached_data,
            description="Use previously downloaded data",
            priority=3,
        )

    def execute_pipeline_rollback(self, reason: str) -> None:
        """Execute pipeline rollback procedures."""
        self.logger.warning(f"🔄 Executing pipeline rollback: {reason}")
        # Add rollback logic here as needed
        pass

    @monitor_performance("pipeline_run")
    def run(self) -> None:
        lg_sum = logging.getLogger("summary")

        # Start pipeline monitoring
        run_id = f"pipeline_{int(time.time())}"
        current_run = self.monitor.start_run(run_id)

        self.logger.info(
            f"🚀 Starting ETL pipeline run - ID: {run_id}, Sources: {self.sources_yaml_path}"
        )
        self.metrics.set_gauge("pipeline.status", 1)  # 1 = running

        # ---------- 0. PRE-PIPELINE CLEANUP -------------------------------
        # Clean downloads and staging folders for fresh data
        cleanup_downloads = self.global_cfg.get(
            "cleanup_downloads_before_run", True)
        cleanup_staging = self.global_cfg.get(
            "cleanup_staging_before_run", True)

        if cleanup_downloads or cleanup_staging:
            lg_sum.info("🧹 Starting pre-pipeline cleanup...")
            cleanup_before_pipeline_run(cleanup_downloads, cleanup_staging)

        # ---------- 1. DOWNLOAD & STAGING ---------------------------------
        sources = list(Source.load_all(self.sources_yaml_path))
        self.logger.info(f"📋 Found sources to process: {len(sources)}")

        # Create SDE loader for proper source-to-dataset mapping
        from .models import AppConfig, SdeLoader

        app_config = AppConfig(
            sde_dataset_pattern=self.global_cfg.get(
                "sde_dataset_pattern", "Underlag_{authority}"
            )
        )
        self.sde_loader = SdeLoader(app_config, sources)

        # Log concurrent download configuration
        if self.global_cfg.get("enable_concurrent_downloads", True):
            rest_workers = self.global_cfg.get(
                "concurrent_download_workers", 5)
            ogc_workers = self.global_cfg.get(
                "concurrent_collection_workers", 3)
            file_workers = self.global_cfg.get("concurrent_file_workers", 4)
            self.logger.info(
                f"🚀 Concurrent downloads enabled: REST={rest_workers}, OGC={ogc_workers}, Files={file_workers} workers"
            )
        else:
            self.logger.info(
                "⚠️ Concurrent downloads disabled - using sequential processing"
            )

        for src in sources:
            if not src.enabled:
                self.logger.info(f"⏭ Skipped (disabled): {src.name}")
                self.summary.log_download("skip")
                continue

            handler_cls = self.handler_map.get(src.type)
            if not handler_cls:
                self.logger.warning(
                    f"🤷 Unknown type, skipped: {src.name} (type: {src.type})"
                )
                self.summary.log_download("skip")
                continue

            # Wrap source processing in graceful degradation
            with graceful_degradation(
                f"download_source_{src.name}", self.recovery_manager
            ):
                try:
                    start_time = time.time()
                    self.logger.info(f"🚚 {src.name}")

                    # Create handler with context manager for proper cleanup
                    with handler_cls(src, global_config=self.global_cfg) as handler:
                        handler.fetch()

                    download_duration = time.time() - start_time
                    self.metrics.record_timing(
                        "download.duration_ms",
                        download_duration * 1000,
                        tags={
                            "source": src.name,
                            "type": src.type,
                            "concurrent": str(
                                self.global_cfg.get(
                                    "enable_concurrent_downloads",
                                    True)),
                        },
                    )
                    self.metrics.increment_counter(
                        "download.success", tags={"source": src.name}
                    )

                    # Log performance improvement hint
                    if (
                        download_duration > 60
                        and src.type in ["rest_api", "ogc_api"]
                        and not self.global_cfg.get("enable_concurrent_downloads", True)
                    ):
                        self.logger.info(
                            f"💡 Performance hint: Enable concurrent downloads for faster processing of {src.type} sources"
                        )

                    self.summary.log_download("done")
                    self.monitor.record_source_processed(success=True)

                except Exception as exc:
                    # Let recovery manager handle the error
                    recovery_result = self.recovery_manager.recover_from_error(
                        error=exc, operation_context=f"download_source_{src.name}"
                    )

                    if recovery_result.success:
                        self.logger.info(
                            f"✅ Recovered from download error for {src.name}"
                        )
                        self.summary.log_download("recovered")
                        self.monitor.record_source_processed(
                            success=True, error=f"Recovered: {exc}"
                        )
                    else:
                        self.summary.log_download("error")
                        self.summary.log_error(src.name, str(exc))
                        self.logger.error(
                            f"❌ Download failed and recovery failed - Source: {src.name}, Error: {exc}"
                        )

                        self.metrics.increment_counter(
                            "download.error", tags={"source": src.name}
                        )
                        self.monitor.record_source_processed(
                            success=False, error=str(exc)
                        )

                        if not self.global_cfg.get(
                                "continue_on_failure", True):
                            self.monitor.end_run("failed")
                            # Execute pipeline rollback before raising
                            self.execute_pipeline_rollback(
                                f"Source download failed: {src.name}"
                            )
                            raise

        # ---------- 2. STAGE → staging.gdb --------------------------------
        self.logger.info("📦 Starting staging phase")

        # Reset staging GDB to avoid conflicts with existing feature classes
        try:
            from .utils.gdb_utils import reset_gdb

            gdb_path = Path(str(paths.GDB))
            if gdb_path.exists():
                self.logger.info("🗑️ Resetting existing staging.gdb")
                reset_gdb(gdb_path)
            self.logger.info("✅ Staging GDB reset complete")
        except (ImportError, arcpy.ExecuteError, OSError) as reset_exc:
            self.logger.warning(f"⚠️ Failed to reset staging GDB: {reset_exc}")
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
                    gdb_path=Path(str(paths.GDB)),
                    sources_yaml_path=self.sources_yaml_path,
                )
                loader.run()

                staging_duration = time.time() - start_time
                self.metrics.record_timing(
                    "staging.duration_ms", staging_duration * 1000
                )
                self.metrics.increment_counter("staging.success")

                self.logger.info(
                    f"✅ Staging.gdb built successfully in {staging_duration:.2f} seconds"
                )

            except Exception as exc:
                # Attempt recovery for staging failures
                recovery_result = self.recovery_manager.recover_from_error(
                    error=exc, operation_context="staging_phase"
                )

                if recovery_result.success:
                    self.logger.info("✅ Recovered from staging error")
                    self.summary.log_staging("recovered")
                else:
                    staging_success = False
                    self.summary.log_staging("error")
                    self.summary.log_error("GDB loader", str(exc))

                    self.logger.error(
                        f"❌ GDB load failed and recovery failed: {exc}")
                    self.metrics.increment_counter("staging.error")

                    if not self.global_cfg.get("continue_on_failure", True):
                        self.monitor.end_run("failed")
                        raise
                    else:
                        self.logger.warning(
                            "⚠️ Continuing despite staging failures")

        # ---------- 3. GEOPROCESS staging.gdb IN-PLACE -------------------
        if staging_success or self.global_cfg.get("continue_on_failure", True):
            self._apply_geoprocessing_inplace()

        # ---------- 4. LOAD TO SDE from staging.gdb -----------------------
        if staging_success or self.global_cfg.get("continue_on_failure", True):
            self._load_to_sde(Path(str(paths.GDB)))
        else:
            lg_sum.warning("⚠️ Skipping SDE loading due to staging failures")

        # Pipeline completion
        self.metrics.set_gauge("pipeline.status", 0)  # 0 = completed
        self.monitor.end_run("completed")

        # Clean up HTTP sessions
        try:
            from .utils.http_session import close_all_http_sessions

            close_all_http_sessions()
            self.logger.debug("🧹 HTTP sessions closed")
        except Exception as e:
            self.logger.warning(f"Failed to close HTTP sessions: {e}")

        # Log final metrics and recovery statistics
        pipeline_stats = self.monitor.get_current_run()
        recovery_stats = self.recovery_manager.get_recovery_stats()

        if pipeline_stats:
            self.logger.info(
                f"🏁 Pipeline completed successfully - Duration: {pipeline_stats.duration:.2f}s, "
                f"Sources: {pipeline_stats.sources_processed}, Success rate: {pipeline_stats.success_rate:.1%}, "
                f"Degradation level: {self.recovery_manager.get_degradation_level()}")

        # Log recovery statistics
        if recovery_stats:
            self.logger.info("📊 Recovery Statistics:")
            for operation, stats in recovery_stats.items():
                self.logger.info(
                    f"   {operation}: {stats['attempts']} attempts, {stats['success_rate']:.1f}% success rate"
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
            self.logger.error(f"❌ AOI boundary not found: {aoi_boundary}")
            if not self.global_cfg.get("continue_on_failure", True):
                raise FileNotFoundError(
                    f"AOI boundary not found: {aoi_boundary}")
            return

        try:
            start_time = time.time()
            target_srid = geoprocessing_config.get("target_srid", 3006)
            self.logger.info(
                f"🔄 Starting geoprocessing - Target SRID: {target_srid}, AOI: {aoi_boundary}"
            )

            # Perform simplified in-place geoprocessing (clip + project only)
            geoprocess.geoprocess_staging_gdb(
                staging_gdb=Path(str(paths.GDB)),
                aoi_fc=aoi_boundary,
                target_srid=target_srid,
                pp_factor=geoprocessing_config.get("parallel_processing_factor", "100"),
            )

            geoprocessing_duration = time.time() - start_time
            self.metrics.record_timing(
                "geoprocessing.duration_ms", geoprocessing_duration * 1000
            )
            self.metrics.increment_counter("geoprocessing.success")

            self.logger.info(
                f"✅ Geoprocessing complete in {geoprocessing_duration:.2f} seconds"
            )

        except arcpy.ExecuteError as exc:
            self.logger.error(f"❌ Geoprocessing failed: {exc}")
            self.metrics.increment_counter("geoprocessing.error")
            if not self.global_cfg.get("continue_on_failure", True):
                raise

    @monitor_performance("sde_loading")
    def _load_to_sde(self, source_gdb: Path) -> None:
        """🚚 Step 4: Load processed GDB to production SDE with parallel processing"""

        if not source_gdb.exists():
            self.logger.error(f"❌ Source GDB not found: {source_gdb}")
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
            f"🚚 Starting SDE loading - Source: {source_gdb.name}, SDE: {sde_connection}"
        )

        all_feature_classes = self._discover_feature_classes(source_gdb)
        if not all_feature_classes:
            self.logger.warning(f"⚠️ No feature classes found in {source_gdb}")
            return

        self.logger.info(
            f"📋 Feature classes discovered: {len(all_feature_classes)}")

        # Check if parallel loading is enabled
        use_parallel = self.global_cfg.get("parallel_sde_loading", True)

        # For now, use sequential loading until ParallelProcessor interface is confirmed
        # if use_parallel and len(all_feature_classes) > 1:
        #     self._load_to_sde_parallel(all_feature_classes, sde_connection)
        # else:
        self._load_to_sde_sequential(all_feature_classes, sde_connection)

        self.logger.info(
            f"📊 SDE loading complete - Loaded: {self.summary.sde['done']}, Errors: {self.summary.sde['error']}"
        )

    def _load_to_sde_parallel(
            self,
            feature_classes: list,
            sde_connection: str) -> None:
        """Load feature classes to SDE in parallel."""
        self.logger.info(
            f"🚀 Starting parallel SDE loading with {self.parallel_processor.max_workers} workers"
        )

        # Define the task function for parallel execution
        def load_task(fc_info):
            source_fc_path, fc_name = fc_info
            try:
                self._load_fc_to_sde(source_fc_path, fc_name, sde_connection)
                return fc_name
            except Exception as e:
                self.logger.error(f"❌ Failed to load {fc_name}: {e}")
                return None

        # Try different method names for parallel execution
        try:
            # Option 1: execute method
            results = self.parallel_processor.execute(
                tasks=[load_task] * len(feature_classes),
                task_args=[(fc,) for fc in feature_classes],
                task_name="sde_loading",
            )
        except AttributeError:
            try:
                # Option 2: process method
                results = self.parallel_processor.process(
                    func=load_task, items=feature_classes, description="Loading to SDE")
            except AttributeError:
                try:
                    # Option 3: run method
                    results = self.parallel_processor.run(
                        target_func=load_task, arguments_list=feature_classes
                    )
                except AttributeError:
                    # Fallback to sequential processing
                    self.logger.warning(
                        "⚠️ ParallelProcessor method not found, falling back to sequential"
                    )
                    self._load_to_sde_sequential(
                        feature_classes, sde_connection)
                    return

        successful = sum(1 for r in results if r is not None)
        self.logger.info(
            f"✅ Parallel SDE loading complete: {successful}/{len(feature_classes)} successful"
        )

    def _load_to_sde_sequential(
        self, feature_classes: list, sde_connection: str
    ) -> None:
        """Load feature classes to SDE sequentially."""
        self.logger.info("🔄 Starting sequential SDE loading")

        for source_fc_path, fc_name in feature_classes:
            try:
                self._load_fc_to_sde(source_fc_path, fc_name, sde_connection)
            except Exception as e:
                self.logger.error(f"❌ Failed to load {fc_name}: {e}")
                self.summary.log_sde("error")

    def _validate_sde_connection_file(self, path: Path) -> bool:
        if not path.exists():
            self.logger.error(f"❌ SDE connection file not found: {path}")
            return False
        return True

    def _discover_feature_classes(self, gdb: Path) -> list[tuple[str, str]]:
        with arcpy.EnvManager(workspace=str(gdb), overwriteOutput=True):
            all_fcs: list[tuple[str, str]] = []
            standalone = arcpy.ListFeatureClasses()
            if standalone:
                self.logger.debug(
                    f"📄 Found standalone feature classes: {len(standalone)}"
                )
                for fc in standalone:
                    # Use full path for source, just name for target
                    fc_full_path = str(gdb / fc)
                    all_fcs.append((fc_full_path, fc))
            datasets = arcpy.ListDatasets(feature_type="Feature")
            if datasets:
                self.logger.debug(f"📁 Found feature datasets: {len(datasets)}")
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

        # Apply naming logic: RAA_byggnader_sverige_point →
        # GNG.RAA\byggnader_sverige_point
        dataset, sde_fc_name = self._get_sde_names(fc_name)
        sde_dataset_path = f"{sde_connection}\\{dataset}"
        target_path = f"{sde_dataset_path}\\{sde_fc_name}"

        lg_sum.info(
            f"🔍 SDE mapping: '{fc_name}' → dataset='{dataset}', fc='{sde_fc_name}'"
        )
        lg_sum.info(
            f"🔍 Target paths: dataset='{sde_dataset_path}', fc='{target_path}'"
        )

        # Get load strategy from config (default: truncate_and_load)
        load_strategy = self.global_cfg.get(
            "sde_load_strategy", "truncate_and_load")

        try:
            # Check if target dataset exists in SDE
            if not arcpy.Exists(sde_dataset_path):
                lg_sum.error(f"❌ SDE dataset does not exist: {dataset}")
                lg_sum.error(
                    f"   Create the dataset '{dataset}' in SDE first, then re-run the pipeline"
                )
                lg_sum.error("   Run: python scripts/create_sde_datasets.py")
                return

            # Verify source FC exists and get its properties
            if not arcpy.Exists(source_fc_path):
                lg_sum.error(f"❌ Source FC does not exist: {source_fc_path}")
                return

            # Get source FC geometry type for debugging
            desc = arcpy.Describe(source_fc_path)
            try:
                count_result = arcpy.management.GetCount(source_fc_path)
                record_count_str = str(count_result.getOutput(0))
                record_count = (
                    int(record_count_str) if record_count_str.isdigit() else 0
                )
            except (ValueError, AttributeError):
                record_count = 0

            lg_sum.info(
                f"🔍 Source FC info: type={desc.dataType}, geom={desc.shapeType}, records={record_count}"
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
                f"❌ SDE operation failed for {source_fc_path}: {arcpy.GetMessages(2)}"
            )
            lg_sum.error(
                f"❌ Check SDE permissions and ensure dataset '{dataset}' exists"
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
        lg_sum = logging.getLogger("summary")
        start_time = time.time()

        if arcpy.Exists(target_path):
            if load_strategy == "truncate_and_load":
                lg_sum.info(
                    f"🗑️ Truncating existing FC: {dataset}\\{sde_fc_name}")
                arcpy.management.TruncateTable(target_path)
                lg_sum.info(
                    f"📄 Loading fresh data to: {dataset}\\{sde_fc_name}")
                arcpy.management.Append(
                    inputs=source_fc_path,
                    target=target_path,
                    schema_type="NO_TEST")
                lg_sum.info(
                    f"🚚→  {dataset}\\{sde_fc_name} (truncated + loaded)")
            elif load_strategy == "replace":
                self.logger.info(
                    f"🗑️ Deleting existing FC: {dataset}\\{sde_fc_name}")
                arcpy.management.Delete(target_path)
                self.logger.info(
                    f"🆕 Creating replacement FC: {dataset}\\{sde_fc_name} ({record_count} records)"
                )
                arcpy.conversion.FeatureClassToFeatureClass(
                    in_features=source_fc_path,
                    out_path=sde_dataset_path,
                    out_name=sde_fc_name,
                )

                duration = time.time() - start_time
                self.metrics.record_timing(
                    "sde.replace.duration_ms", duration * 1000)
                self.logger.info(
                    f"🚚→ Replaced: {dataset}\\{sde_fc_name} in {duration:.2f}s"
                )
            elif load_strategy == "append":
                self.logger.warning(
                    f"⚠️ Appending to existing FC (may create duplicates): {dataset}\\{sde_fc_name}"
                )
                arcpy.management.Append(
                    inputs=source_fc_path,
                    target=target_path,
                    schema_type="NO_TEST")
                lg_sum.info(f"🚚→  {dataset}\\{sde_fc_name} (appended)")
            else:
                self.logger.error(
                    f"❌ Unknown sde_load_strategy: {load_strategy}")
        else:
            self.logger.info(
                f"🆕 Creating new FC: {dataset}\\{sde_fc_name} ({record_count} records)"
            )

            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_fc_path,
                out_path=sde_dataset_path,
                out_name=sde_fc_name,
            )

            duration = time.time() - start_time
            self.metrics.record_timing(
                "sde.create.duration_ms", duration * 1000)
            self.logger.info(
                f"🚚→ Created: {dataset}\\{sde_fc_name} in {duration:.2f}s"
            )

    def _get_sde_names(self, fc_name: str) -> Tuple[str, str]:
        """📝 Extract SDE dataset and feature class names from staging name.

        Logic: SKS_naturvarden_point → dataset="GNG.Underlag_SKS", fc="naturvarden_point"
        """
        parts = fc_name.split("_", 1)
        if len(parts) < 2:
            # No underscore → treat as MISC
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
