# etl/handlers/file.py
from __future__ import annotations

import logging
import shutil
import zipfile
import re # For parsing Content-Disposition
from pathlib import Path
from typing import Iterable, Tuple, Final, Optional, Dict, Any # Added Dict, Any
from urllib.parse import unquote
from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths
from ..utils.naming import sanitize_for_filename
from ..utils.http import fetch_true_filename_parts

class FileDownloadHandler:
    """
    Handles downloading various file types (ZIPs, direct GPKGs, etc.) and staging them.
    Determines 'true' filenames by checking Content-Disposition or unquoting URL parts.
    For ZIPs, extracts contents. For direct GPKGs, copies them to the staging location.
    """
    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None): # Added global_config
        self.src: Source = src
        self.global_config: Dict[str, Any] = global_config or {} # Store or ignore
        ensure_dirs()
        # logging.debug("FileDownloadHandler initialized for '%s' with global_config: %s", src.name, self.global_config if self.global_config else "None")

    def fetch(self) -> None:
        """
        Fetches data for the source.
        Differentiates between downloading a single file (e.g., a GPKG) vs. multiple files
        (e.g., a collection of ZIPs based on an 'include' list).
        """
        if not self.src.enabled:
            logging.info("⏭️ Source '%s' is disabled, skipping fetch.", self.src.name)
            return

        is_single_direct_download_gpkg = (
            (self.src.download_format and self.src.download_format.lower() == "gpkg") or
            (self.src.staged_data_type and self.src.staged_data_type.lower() == "gpkg")
        )

        if self.src.include and not is_single_direct_download_gpkg:
            logging.info("🔗 Multi-part download mode for '%s' (%d archives from base URL '%s')…",
                         self.src.name, len(self.src.include), self.src.url)

            for included_filename_stem in self._iter_included_file_stems():
                file_ext_from_format: str = ".zip" # Default for collections
                if self.src.download_format:
                    fmt = self.src.download_format.lower().lstrip('.')
                    if fmt:
                        file_ext_from_format = f".{fmt}"

                actual_filename_for_download_part: str = included_filename_stem + file_ext_from_format
                base_url: str = self.src.url.rstrip("/") + "/"
                download_url_for_part: str = base_url + actual_filename_for_download_part

                sanitized_included_stem: str = sanitize_for_filename(included_filename_stem)
                self._download_and_stage_one(
                    download_url=download_url_for_part,
                    explicit_local_filename_stem=sanitized_included_stem,
                    explicit_local_file_ext=file_ext_from_format,
                    staging_subdir_name_override=sanitized_included_stem
                )
        elif self.src.url:
            logging.info("🔗 Single resource download mode for '%s' from URL: %s", self.src.name, self.src.url)

            true_stem_from_web, true_ext_from_web = fetch_true_filename_parts(self.src.url)
            consistent_local_stem: str = sanitize_for_filename(self.src.name)
            final_extension: str = true_ext_from_web

            if self.src.download_format:
                expected_ext_from_format: str = f".{self.src.download_format.lower().lstrip('.')}"
                if final_extension and expected_ext_from_format != final_extension:
                    logging.warning(
                        f"    For source '{self.src.name}', download_format ('{expected_ext_from_format}') "
                        f"differs from determined true extension ('{final_extension}'). "
                        f"Using determined true extension: '{final_extension}'."
                    )
                elif not final_extension:
                    logging.info(f"    Could not determine true extension for '{self.src.name}', using download_format: '{expected_ext_from_format}'.")
                    final_extension = expected_ext_from_format

            if not final_extension:
                # Try to infer from URL if all else fails
                path_ext = Path(unquote(self.src.url)).suffix.lower()
                if path_ext in [".zip", ".gpkg", ".geojson", ".json"]: # Add more if needed
                    final_extension = path_ext
                    logging.info(f"    Could not determine true extension for '{self.src.name}', inferred from URL path: '{final_extension}'.")
                else:
                    final_extension = ".data" # Ultimate fallback
                    logging.warning(f"    Could not determine any extension for '{self.src.name}', defaulting to '{final_extension}'.")


            self._download_and_stage_one(
                download_url=self.src.url,
                explicit_local_filename_stem=consistent_local_stem,
                explicit_local_file_ext=final_extension,
                staging_subdir_name_override=consistent_local_stem
            )
        else:
            logging.warning("🤷 Source '%s' (type: '%s') has no URL. Cannot fetch.", self.src.name, self.src.type)


    def _iter_included_file_stems(self) -> Iterable[str]:
        """Yields stems from the source's include list."""
        for stem in self.src.include:
            yield stem

    def _download_and_stage_one(
        self,
        download_url: str,
        explicit_local_filename_stem: str,
        explicit_local_file_ext: str,
        staging_subdir_name_override: str
    ) -> None:
        """
        Downloads a single file and stages it.
        Local download filename: explicit_local_filename_stem + explicit_local_file_ext.
        Staging subdirectory name: staging_subdir_name_override (sanitized).
        """

        local_download_filename: str = explicit_local_filename_stem + explicit_local_file_ext
        download_target_path: Path = paths.DOWNLOADS / local_download_filename

        sanitized_staging_subdir_name: str = sanitize_for_filename(staging_subdir_name_override)
        final_staging_destination_dir: Path = paths.STAGING / self.src.authority / sanitized_staging_subdir_name
        final_staging_destination_dir.mkdir(parents=True, exist_ok=True)

        logging.info("    Attempting to download: %s \n    -> to local file: %s \n    -> staging dir: %s",
                     download_url, download_target_path.name, final_staging_destination_dir.relative_to(paths.ROOT))

        try:
            downloaded_file_path: Path = download(download_url, download_target_path)
        except Exception as e:
            logging.error(f"❌ Download failed for {download_url} (Source: {self.src.name}): {e}", exc_info=True)
            return

        effective_staged_data_type: str = self.src.staged_data_type or ""

        if not effective_staged_data_type:
            if self.src.download_format and self.src.download_format.lower() == "gpkg":
                effective_staged_data_type = "gpkg"
            elif explicit_local_file_ext.lower() == ".gpkg":
                 effective_staged_data_type = "gpkg"
            elif explicit_local_file_ext.lower() in [".geojson", ".json"]:
                 effective_staged_data_type = "geojson" # Assume geojson for loader
            else: # Default to shapefile_collection for zips or unknown
                effective_staged_data_type = "shapefile_collection"

        logging.info("    Determined staged_data_type: '%s' for downloaded file '%s'",
                     effective_staged_data_type, downloaded_file_path.name)

        if effective_staged_data_type == "gpkg":
            staged_gpkg_filename: str = sanitized_staging_subdir_name + explicit_local_file_ext # Use original ext
            staged_gpkg_path: Path = final_staging_destination_dir / staged_gpkg_filename
            try:
                if downloaded_file_path.resolve() != staged_gpkg_path.resolve():
                    shutil.copy(downloaded_file_path, staged_gpkg_path)
                    logging.info("➕ Staged GPKG %s to %s",
                                 downloaded_file_path.name,
                                 staged_gpkg_path.relative_to(paths.ROOT))
                else:
                    logging.info("ℹ️ Downloaded GPKG '%s' is already in the target staging location. [V1.2 LOG]", downloaded_file_path.name)

            except Exception as e:
                logging.error(f"❌ Failed to copy downloaded GPKG {downloaded_file_path.name} to staging location {staged_gpkg_path}: {e}", exc_info=True)
        
        elif effective_staged_data_type == "geojson": # Handling for geojson by FileDownloadHandler
            staged_json_filename: str = sanitized_staging_subdir_name + explicit_local_file_ext # Use original ext
            staged_json_path: Path = final_staging_destination_dir / staged_json_filename
            try:
                if downloaded_file_path.resolve() != staged_json_path.resolve():
                    shutil.copy(downloaded_file_path, staged_json_path)
                    logging.info("➕ Staged GeoJSON/JSON file %s to %s",
                                 downloaded_file_path.name,
                                 staged_json_path.relative_to(paths.ROOT))
                else:
                    logging.info("ℹ️ Downloaded GeoJSON/JSON '%s' is already in the target staging location.", downloaded_file_path.name)
            except Exception as e:
                logging.error(f"❌ Failed to copy downloaded GeoJSON/JSON {downloaded_file_path.name} to staging location {staged_json_path}: {e}", exc_info=True)


        elif effective_staged_data_type == "shapefile_collection":
            if explicit_local_file_ext.lower() != ".zip":
                logging.warning(
                    f"⚠️ Expected a ZIP file for source '{self.src.name}' (staged_data_type='shapefile_collection' or inferred) "
                    f"but actual extension is '{explicit_local_file_ext}'. Attempting extraction anyway for '{downloaded_file_path.name}'."
                )
            # If it's not a zip, but shapefile_collection is specified, it implies the files are loose
            # and might already be in a staging-like structure in the download.
            # However, the current logic always tries to extract. This might need refinement
            # if you have non-zipped shapefile collections. For now, we assume it's a ZIP.
            if explicit_local_file_ext.lower() == ".zip":
                try:
                    extract_zip(downloaded_file_path, final_staging_destination_dir)
                    logging.info("➕ Extracted and staged archive %s to %s",
                                 downloaded_file_path.name,
                                 final_staging_destination_dir.relative_to(paths.ROOT))
                except zipfile.BadZipFile:
                    logging.error(f"❌ File '{downloaded_file_path.name}' is not a valid ZIP file. Cannot extract for shapefile_collection.")
                except Exception as e:
                    logging.error(f"❌ Failed to extract archive {downloaded_file_path.name} to {final_staging_destination_dir}: {e}", exc_info=True)
            # else: # If it's not a ZIP but shapefile_collection, what to do?
            #    logging.warning(f"⚠️  Source '{self.src.name}' is 'shapefile_collection' but downloaded file '{downloaded_file_path.name}' is not a ZIP. Staging as single file.")
            #    # Potentially copy the single file if it's e.g. a lone .shp (though this is unusual for a collection)
            #    # This part of the logic is a bit ambiguous if not a ZIP.

        else:
            logging.warning(
                f"🤷 Don't know how to stage data with effective_staged_data_type '{effective_staged_data_type}' "
                f"for source '{self.src.name}'. File downloaded to {downloaded_file_path}."
            )