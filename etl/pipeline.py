# etl/pipeline.py (complete working version)
from __future__ import annotations

import logging
import time
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
from .utils.naming import sanitize_sde_name
from .mapping import get_mapping_manager, MappingManager
from .monitoring import (
    get_structured_logger,
    get_metrics_collector,
    get_pipeline_monitor,
)
from .utils.performance import ParallelProcessor, monitor_performance
from .utils.cleanup import cleanup_before_pipeline_run


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
            )  # Initialize mapping manager
        self.mapping_manager = get_mapping_manager(mappings_yaml_path)

        # Initialize monitoring and metrics
        self.logger = get_structured_logger("pipeline")
        self.metrics = get_metrics_collector()
        self.monitor = get_pipeline_monitor()

        # Initialize performance processor
        max_workers = self.global_cfg.get("parallel_workers", 2)
        self.parallel_processor = ParallelProcessor(max_workers=max_workers)

        ensure_dirs()

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

            try:
                start_time = time.time()
                self.logger.info("🚚 %s" % src.name)

                handler_cls(src, global_config=self.global_cfg).fetch()

                download_duration = time.time() - start_time
                self.metrics.record_timing(
                    "download.duration_ms",
                    download_duration * 1000,
                    tags={"source": src.name, "type": src.type},
                )
                self.metrics.increment_counter(
                    "download.success", tags={"source": src.name}
                )

                self.summary.log_download("done")
                self.monitor.record_source_processed(success=True)

            except (FileNotFoundError, arcpy.ExecuteError) as exc:
                self.summary.log_download("error")
                self.summary.log_error(src.name, str(exc))
                self.logger.error("❌ Download failed", source_name=src.name, error=exc)

                self.metrics.increment_counter(
                    "download.error", tags={"source": src.name}
                )
                self.monitor.record_source_processed(success=False, error=str(exc))

                if not self.global_cfg.get("continue_on_failure", True):
                    self.monitor.end_run("failed")
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

        except (arcpy.ExecuteError, FileNotFoundError) as exc:
            staging_success = False
            self.summary.log_staging("error")
            self.summary.log_error("GDB loader", str(exc))

            self.logger.error("❌ GDB load failed", error=exc)
            self.metrics.increment_counter("staging.error")

            if not self.global_cfg.get("continue_on_failure", True):
                self.monitor.end_run("failed")
                raise
            else:
                self.logger.warning("⚠️ Continuing despite staging failures")

        # ---------- 3. GEOPROCESS staging.gdb IN-PLACE -------------------
        if staging_success or self.global_cfg.get("continue_on_failure", True):
            self._apply_geoprocessing_inplace()

            # ---------- 4. LOAD TO SDE from staging.gdb -----------------------
            self._load_to_sde(paths.GDB)
        else:
            lg_sum.warning(
                "⚠️ Skipping geoprocessing and SDE loading due to staging failures"
            )

        # Pipeline completion
        self.metrics.set_gauge("pipeline.status", 0)  # 0 = completed
        self.monitor.end_run("completed")

        # Log final metrics
        pipeline_stats = self.monitor.get_current_run()
        if pipeline_stats:
            self.logger.info(
                "🏁 Pipeline completed successfully",
                duration_seconds=pipeline_stats.duration,
                sources_processed=pipeline_stats.sources_processed,
                success_rate=pipeline_stats.success_rate,
            )

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

    def _load_to_sde_parallel(
        self, feature_classes: list[tuple[str, str]], sde_connection: str
    ) -> None:
        """🚀 Load feature classes to SDE in parallel."""
        self.logger.info("🚀 Using parallel SDE loading", fc_count=len(feature_classes))

        def load_single_fc(fc_data: tuple[str, str]) -> tuple[str, bool, Optional[str]]:
            """Load a single feature class and return result."""
            fc_path, fc_name = fc_data
            try:
                self._load_fc_to_sde(fc_path, fc_name, sde_connection)
                return fc_name, True, None
            except Exception as e:
                return fc_name, False, str(e)

        # Process in parallel
        start_time = time.time()
        results = self.parallel_processor.process_sources_parallel(
            feature_classes, load_single_fc
        )

        # Process results
        success_count = 0
        error_count = 0

        for (_, fc_name), result in results:
            # Handle cases where result might be an exception or tuple
            if isinstance(result, Exception):
                self.summary.log_sde("error")
                self.summary.log_error(fc_name, f"SDE load failed: {result}")
                error_count += 1
                self.metrics.increment_counter("sde.load.error", tags={"fc": fc_name})

                if not self.global_cfg.get("continue_on_failure", True):
                    raise Exception(f"SDE loading failed for {fc_name}: {result}")
            else:
                result_fc_name, success, error = result
                if success:
                    self.summary.log_sde("done")
                    success_count += 1
                    self.metrics.increment_counter(
                        "sde.load.success", tags={"fc": fc_name}
                    )
                else:
                    self.summary.log_sde("error")
                    self.summary.log_error(fc_name, f"SDE load failed: {error}")
                    error_count += 1
                    self.metrics.increment_counter(
                        "sde.load.error", tags={"fc": fc_name}
                    )

                    if not self.global_cfg.get("continue_on_failure", True):
                        raise Exception(f"SDE loading failed for {fc_name}: {error}")

        duration = time.time() - start_time
        self.metrics.record_timing("sde.parallel_load.duration_ms", duration * 1000)

        self.logger.info(
            "✅ Parallel SDE loading complete",
            duration_seconds=duration,
            success_count=success_count,
            error_count=error_count,
        )

    def _load_to_sde_sequential(
        self, feature_classes: list[tuple[str, str]], sde_connection: str
    ) -> None:
        """🔄 Load feature classes to SDE sequentially."""
        self.logger.info(
            "🔄 Using sequential SDE loading", fc_count=len(feature_classes)
        )

        for fc_path, fc_name in feature_classes:
            try:
                start_time = time.time()
                self._load_fc_to_sde(fc_path, fc_name, sde_connection)

                duration = time.time() - start_time
                self.metrics.record_timing(
                    "sde.load.duration_ms", duration * 1000, tags={"fc": fc_name}
                )
                self.metrics.increment_counter("sde.load.success", tags={"fc": fc_name})

                self.summary.log_sde("done")
            except arcpy.ExecuteError as exc:
                self.summary.log_sde("error")
                self.summary.log_error(fc_name, f"SDE load failed: {exc}")
                self.logger.error(
                    "❌ Failed to load to SDE",
                    fc_name=fc_name,
                    fc_path=fc_path,
                    error=exc,
                )

                self.metrics.increment_counter("sde.load.error", tags={"fc": fc_name})

                if not self.global_cfg.get("continue_on_failure", True):
                    raise

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
        lg_sum.info("🔍 DEBUG: source_fc_path='%s'", source_fc_path)

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
            except (ValueError, AttributeError):
                record_count = 0

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
                try:
                    self.logger.info(
                        "🗑️ Truncating existing FC", dataset=dataset, fc=sde_fc_name
                    )
                    arcpy.management.TruncateTable(target_path)
                    self.logger.info(
                        "📄 Loading fresh data",
                        dataset=dataset,
                        fc=sde_fc_name,
                        records=record_count,
                    )
                    arcpy.management.Append(
                        inputs=source_fc_path, target=target_path, schema_type="NO_TEST"
                    )

                    duration = time.time() - start_time
                    self.metrics.record_timing(
                        "sde.truncate_load.duration_ms", duration * 1000
                    )
                    self.logger.info(
                        "🚚→ Truncated and loaded",
                        dataset=dataset,
                        fc=sde_fc_name,
                        duration_seconds=duration,
                    )
                except arcpy.ExecuteError as exc:
                    # If truncate_and_load fails (e.g., geometry type mismatch), try replace strategy
                    if (
                        "shape type" in str(exc).lower()
                        or "geometry" in str(exc).lower()
                    ):
                        self.logger.warning(
                            "⚠️ Geometry type mismatch, switching to replace strategy",
                            dataset=dataset,
                            fc=sde_fc_name,
                        )
                        self.logger.info(
                            "🗑️ Deleting existing FC", dataset=dataset, fc=sde_fc_name
                        )
                        arcpy.management.Delete(target_path)
                        self.logger.info(
                            "🆕 Creating replacement FC",
                            dataset=dataset,
                            fc=sde_fc_name,
                        )
                        arcpy.conversion.FeatureClassToFeatureClass(
                            in_features=source_fc_path,
                            out_path=sde_dataset_path,
                            out_name=sde_fc_name,
                        )

                        duration = time.time() - start_time
                        self.metrics.record_timing(
                            "sde.replace_load.duration_ms", duration * 1000
                        )
                        self.logger.info(
                            "🚚→ Replaced due to geometry mismatch",
                            dataset=dataset,
                            fc=sde_fc_name,
                            duration_seconds=duration,
                        )
                    else:
                        raise
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

                duration = time.time() - start_time
                self.metrics.record_timing("sde.append.duration_ms", duration * 1000)
                self.logger.info(
                    "🚚→ Appended",
                    dataset=dataset,
                    fc=sde_fc_name,
                    duration_seconds=duration,
                    records=record_count,
                )
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
        """📝 Derive target SDE dataset and feature class names."""
        if self.mapping_manager:
            authority = fc_name.split("_", 1)[0]
            source_stub = Source(name=fc_name, authority=authority)
            mapping = self.mapping_manager.get_output_mapping(source_stub, fc_name)
            dataset = (
                f"{mapping.schema}.{mapping.sde_dataset}"
                if mapping.schema
                else mapping.sde_dataset
            )
            return dataset, mapping.sde_fc

        parts = fc_name.split("_", 1)
        if len(parts) < 2:
            dataset_suffix = "MISC"
            fc_name_clean = fc_name.lower()
        else:
            dataset_suffix, fc_name_clean = parts
            fc_name_clean = fc_name_clean.lower()

        fc_name_clean = sanitize_sde_name(fc_name_clean)

        schema = self.global_cfg.get("sde_schema", "GNG")

        if dataset_suffix == "LSTD":
            dataset = f"{schema}.Underlag_LstD"
        else:
            dataset = f"{schema}.Underlag_{dataset_suffix}"

        return dataset, fc_name_clean
