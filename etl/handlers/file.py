# etl/handlers/file.py
from __future__ import annotations

import logging
import shutil
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import unquote

from ..exceptions import SourceError, format_error_context
from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths
from ..utils.concurrent import get_file_downloader
from ..utils.http import fetch_true_filename_parts
from ..utils.naming import sanitize_for_filename
from ..utils.retry import RetryConfig, smart_retry

log = logging.getLogger(__name__)


class FileDownloadHandler:
    """
    Handles downloading various file types (ZIPs, direct GPKGs, etc.) and staging them.
    Determines 'true' filenames by checking Content-Disposition or unquoting URL parts.
    For ZIPs, extracts contents. For direct GPKGs, copies them to the staging location.
    """

    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}

        # Initialize retry configuration
        retry_config = self.global_config.get("retry", {})
        self.retry_config = RetryConfig(
            max_attempts=retry_config.get("max_attempts", 3),
            base_delay=retry_config.get("base_delay", 1.0),
            backoff_factor=retry_config.get("backoff_factor", 2.0),
            max_delay=retry_config.get("max_delay", 300.0),
        )

        ensure_dirs()

    def fetch(self) -> None:
        """
        Fetches data for the source.
        Differentiates between downloading a single file (e.g., a GPKG) vs. multiple files
        (e.g., a collection of ZIPs based on an 'include' list).
        """
        if not self.src.enabled:
            log.info("⏭️ Source '%s' is disabled, skipping fetch.", self.src.name)
            return

        if not self.src.url:
            raise SourceError(
                f"Source '{self.src.name}' has no URL configured",
                source_name=self.src.name,
            )

        try:
            is_single_direct_download_gpkg = (
                self.src.download_format and self.src.download_format.lower() == "gpkg"
            ) or (
                self.src.staged_data_type
                and self.src.staged_data_type.lower() == "gpkg"
            )

            if self.src.include and not is_single_direct_download_gpkg:
                self._download_multiple_files()
            else:
                self._download_single_resource()
        except Exception as e:
            log.error(
                "❌ Failed to fetch source '%s': %s",
                self.src.name,
                format_error_context(e) if hasattr(e, "context") else str(e),
            )
            raise

    def _download_multiple_files(self) -> None:
        """Handle multi-part downloads defined by src.include."""
        log.info(
            "🔗 Multi-part download mode for '%s' (%d archives from base URL '%s')…",
            self.src.name,
            len(self.src.include),
            self.src.url,
        )

        file_stems = list(self._iter_included_file_stems())

        # Use concurrent downloads for multiple files if enabled
        if len(file_stems) > 1 and self.global_config.get(
            "enable_concurrent_downloads", True
        ):
            self._download_files_concurrent(file_stems)
        else:
            # Single file or concurrent downloads disabled - use original
            # sequential approach
            for included_filename_stem in file_stems:
                self._download_single_file_stem(included_filename_stem)

    def _download_files_concurrent(self, file_stems: List[str]) -> None:
        """Download multiple files concurrently for improved performance."""
        log.info("🚀 Starting concurrent download of %d files", len(file_stems))

        # Get concurrent downloader
        downloader = get_file_downloader()

        # Get configuration - use max_workers parameter instead of mutating
        # singleton
        max_workers = self.global_config.get("concurrent_file_workers", 4)
        fail_fast = self.global_config.get("fail_fast_downloads", False)

        # Execute concurrent downloads with max_workers parameter
        results = downloader.download_files_concurrent(
            handler=self,
            file_stems=file_stems,
            fail_fast=fail_fast,
            max_workers=max_workers,  # Pass as parameter instead of mutating singleton
        )

        # Process results and log statistics
        successful_downloads = sum(1 for r in results if r.success)
        failed_downloads = len(results) - successful_downloads

        log.info(
            "🏁 Concurrent file downloads completed: %d successful, %d failed",
            successful_downloads,
            failed_downloads,
        )

        # Log any failures
        for result in results:
            if not result.success:
                file_name = result.metadata.get("task_name", "unknown")
                log.error("❌ File download failed: %s - %s", file_name, result.error)

    def _download_single_file_stem(self, included_filename_stem: str) -> None:
        """Download a single file stem (extracted from original loop)."""
        file_ext_from_format = ".zip"  # Default for collections
        if self.src.download_format:
            fmt = self.src.download_format.lower().lstrip(".")
            if fmt:
                file_ext_from_format = f".{fmt}"

        actual_filename_for_download_part = (
            included_filename_stem + file_ext_from_format
        )
        base_url = self.src.url.rstrip("/") + "/"
        download_url_for_part = base_url + actual_filename_for_download_part

        sanitized_included_stem = sanitize_for_filename(included_filename_stem)
        self._download_and_stage_one(
            download_url=download_url_for_part,
            explicit_local_filename_stem=sanitized_included_stem,
            explicit_local_file_ext=file_ext_from_format,
            staging_subdir_name_override=sanitized_included_stem,
        )

    def _download_single_resource(self) -> None:
        """Handle downloading a single resource from src.url."""
        log.info(
            "🔗 Single resource download mode for '%s' from URL: %s",
            self.src.name,
            self.src.url,
        )

        true_stem_from_web, true_ext_from_web = fetch_true_filename_parts(self.src.url)
        consistent_local_stem = sanitize_for_filename(self.src.name)
        final_extension = true_ext_from_web

        if self.src.download_format:
            expected_ext_from_format = (
                f".{self.src.download_format.lower().lstrip('.')}"
            )
            if final_extension and expected_ext_from_format != final_extension:
                log.warning(
                    "For source '%s', download_format ('%s') differs from determined true extension ('%s'). "
                    "Using determined true extension: '%s'.",
                    self.src.name,
                    expected_ext_from_format,
                    final_extension,
                    final_extension,
                )
            elif not final_extension:
                log.info(
                    "Could not determine true extension for '%s', using download_format: '%s'.",
                    self.src.name,
                    expected_ext_from_format,
                )
                final_extension = expected_ext_from_format

        if not final_extension:
            # Try to infer from URL if all else fails
            path_ext = Path(unquote(self.src.url)).suffix.lower()
            if path_ext in [".zip", ".gpkg", ".geojson", ".json"]:
                final_extension = path_ext
                log.info(
                    "Could not determine true extension for '%s', inferred from URL path: '%s'.",
                    self.src.name,
                    final_extension,
                )
            else:
                final_extension = ".data"  # Ultimate fallback
                log.warning(
                    "Could not determine any extension for '%s', defaulting to '%s'.",
                    self.src.name,
                    final_extension,
                )

        self._download_and_stage_one(
            download_url=self.src.url,
            explicit_local_filename_stem=consistent_local_stem,
            explicit_local_file_ext=final_extension,
            staging_subdir_name_override=consistent_local_stem,
        )

    def _iter_included_file_stems(self) -> Iterable[str]:
        """Yields stems from the source's include list."""
        for stem in self.src.include:
            yield stem

    @smart_retry("file_download_and_stage")
    def _download_and_stage_one(
        self,
        download_url: str,
        explicit_local_filename_stem: str,
        explicit_local_file_ext: str,
        staging_subdir_name_override: str,
    ) -> None:
        """
        Downloads a single file and stages it.
        Local download filename: explicit_local_filename_stem + explicit_local_file_ext.
        Staging subdirectory name: staging_subdir_name_override (sanitized).
        """
        local_download_filename = explicit_local_filename_stem + explicit_local_file_ext
        download_target_path = paths.DOWNLOADS / local_download_filename

        sanitized_staging_subdir_name = sanitize_for_filename(
            staging_subdir_name_override
        )
        final_staging_destination_dir = (
            paths.STAGING / self.src.authority / sanitized_staging_subdir_name
        )
        final_staging_destination_dir.mkdir(parents=True, exist_ok=True)

        log.debug(
            "Attempting to download: %s \n    -> to local file: %s \n    -> staging dir: %s",
            download_url,
            download_target_path.name,
            final_staging_destination_dir.relative_to(paths.ROOT),
        )

        try:
            downloaded_file_path = download(download_url, download_target_path)
        except Exception as e:
            log.error(
                "❌ Download failed for %s (Source: %s): %s",
                download_url,
                self.src.name,
                e,
                exc_info=True,
            )
            return

        effective_staged_data_type = self.src.staged_data_type or ""

        if not effective_staged_data_type:
            if self.src.download_format and self.src.download_format.lower() == "gpkg":
                effective_staged_data_type = "gpkg"
            elif explicit_local_file_ext.lower() == ".gpkg":
                effective_staged_data_type = "gpkg"
            elif explicit_local_file_ext.lower() in [".geojson", ".json"]:
                effective_staged_data_type = "geojson"
            else:  # Default to shapefile_collection for zips or unknown
                effective_staged_data_type = "shapefile_collection"

        log.info(
            "Determined staged_data_type: '%s' for downloaded file '%s'",
            effective_staged_data_type,
            downloaded_file_path.name,
        )

        if effective_staged_data_type == "gpkg":
            staged_gpkg_filename = (
                sanitized_staging_subdir_name + explicit_local_file_ext
            )
            staged_gpkg_path = final_staging_destination_dir / staged_gpkg_filename
            try:
                if downloaded_file_path.resolve() != staged_gpkg_path.resolve():
                    shutil.copy(downloaded_file_path, staged_gpkg_path)
                    log.info(
                        "➕ Staged GPKG %s to %s",
                        downloaded_file_path.name,
                        staged_gpkg_path.relative_to(paths.ROOT),
                    )
                else:
                    log.info(
                        "ℹ️ Downloaded GPKG '%s' is already in the target staging location.",
                        downloaded_file_path.name,
                    )
            except Exception as e:
                log.error(
                    "❌ Failed to copy downloaded GPKG %s to staging location %s: %s",
                    downloaded_file_path.name,
                    staged_gpkg_path,
                    e,
                    exc_info=True,
                )

        elif effective_staged_data_type == "geojson":
            staged_json_filename = (
                sanitized_staging_subdir_name + explicit_local_file_ext
            )
            staged_json_path = final_staging_destination_dir / staged_json_filename
            try:
                if downloaded_file_path.resolve() != staged_json_path.resolve():
                    shutil.copy(downloaded_file_path, staged_json_path)
                    log.info(
                        "➕ Staged GeoJSON/JSON file %s to %s",
                        downloaded_file_path.name,
                        staged_json_path.relative_to(paths.ROOT),
                    )
                else:
                    log.info(
                        "ℹ️ Downloaded GeoJSON/JSON '%s' is already in the target staging location.",
                        downloaded_file_path.name,
                    )
            except Exception as e:
                log.error(
                    "❌ Failed to copy downloaded GeoJSON/JSON %s to staging location %s: %s",
                    downloaded_file_path.name,
                    staged_json_path,
                    e,
                    exc_info=True,
                )

        elif effective_staged_data_type == "shapefile_collection":
            if explicit_local_file_ext.lower() != ".zip":
                log.warning(
                    "⚠️ Expected a ZIP file for source '%s' (staged_data_type='shapefile_collection' or inferred) "
                    "but actual extension is '%s'. Attempting extraction anyway for '%s'.",
                    self.src.name,
                    explicit_local_file_ext,
                    downloaded_file_path.name,
                )

            if explicit_local_file_ext.lower() == ".zip":
                try:
                    extract_zip(downloaded_file_path, final_staging_destination_dir)
                    log.info(
                        "➕ Extracted and staged archive %s to %s",
                        downloaded_file_path.name,
                        final_staging_destination_dir.relative_to(paths.ROOT),
                    )
                except zipfile.BadZipFile:
                    log.error(
                        "❌ File '%s' is not a valid ZIP file. Cannot extract for shapefile_collection.",
                        downloaded_file_path.name,
                    )
                except Exception as e:
                    log.error(
                        "❌ Failed to extract archive %s to %s: %s",
                        downloaded_file_path.name,
                        final_staging_destination_dir,
                        e,
                        exc_info=True,
                    )

        else:
            log.warning(
                "🤷 Don't know how to stage data with effective_staged_data_type '%s' "
                "for source '%s'. File downloaded to %s.",
                effective_staged_data_type,
                self.src.name,
                downloaded_file_path,
            )

    def __enter__(self) -> "FileDownloadHandler":
        """Enter the context manager for use with 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager. No cleanup needed for file downloads."""
        pass
