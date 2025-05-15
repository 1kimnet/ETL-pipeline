from __future__ import annotations

import logging
import shutil
import re # For parsing Content-Disposition
from pathlib import Path
from typing import Iterable, Tuple, Final, Optional 

# urllib.request is used by the http utility, no direct import needed here if using the utility
# from urllib.parse import unquote # Moved to http utility
# import requests # Removed

from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths
from ..utils.naming import sanitize_for_filename
from ..utils.http import fetch_true_filename_parts # Import the new HTTP utility

class FileDownloadHandler: # Renamed from FileShapefileHandler
    """
    Handles downloading various file types (ZIPs, direct GPKGs, etc.) and staging them.
    Determines 'true' filenames by checking Content-Disposition or unquoting URL parts.
    For ZIPs, extracts contents. For direct GPKGs, copies them to the staging location.
    """
    def __init__(self, src: Source):
        self.src: Source = src
        ensure_dirs() # Ensures paths.DOWNLOADS and paths.STAGING exist

    def fetch(self) -> None:
        """
        Fetches data for the source.
        Differentiates between downloading a single file (e.g., a GPKG) vs. multiple files 
        (e.g., a collection of ZIPs based on an 'include' list).
        """
        if not self.src.enabled:
            logging.info("⏭️ Source '%s' is disabled, skipping fetch.", self.src.name)
            return

        # Determine if this source represents a single downloadable file (like a GPKG)
        # or a collection of files (like NVV's multiple ZIPs).
        # A single GPKG download typically has download_format: "gpkg" or staged_data_type: "gpkg".
        # The 'include' list for a single GPKG is for the *loader*, not the downloader.
        
        is_single_direct_download_gpkg = (
            (self.src.download_format and self.src.download_format.lower() == "gpkg") or
            (self.src.staged_data_type and self.src.staged_data_type.lower() == "gpkg")
        )

        # Scenario 1: Multi-part download (collection of files, e.g., multiple ZIPs)
        # This applies if 'include' is present AND it's NOT a single direct GPKG download.
        if self.src.include and not is_single_direct_download_gpkg:
            # This mode is for sources where self.src.url is a base path
            # and self.src.include lists are filename stems to append.
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
        # Scenario 2: Direct download of a single resource (e.g., a single GPKG or a single ZIP)
        # This applies if there's a URL and it's either a single GPKG or no 'include' list (or 'include' is for loader).
        elif self.src.url: 
            logging.info("🔗 Single resource download mode for '%s' from URL: %s", self.src.name, self.src.url)
            
            true_stem_from_web: str
            true_ext_from_web: str
            true_stem_from_web, true_ext_from_web = fetch_true_filename_parts(self.src.url)
            
            consistent_local_stem: str = sanitize_for_filename(self.src.name)
            
            final_extension: str = true_ext_from_web 
            
            if self.src.download_format: # User-specified format can override/clarify
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
                final_extension = ".data" 
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

        # Determine how to stage based on source config or inferred type
        effective_staged_data_type: str = self.src.staged_data_type or "" 
        
        if not effective_staged_data_type: 
            if self.src.download_format and self.src.download_format.lower() == "gpkg":
                effective_staged_data_type = "gpkg"
            elif explicit_local_file_ext.lower() == ".gpkg":
                 effective_staged_data_type = "gpkg"
            else: 
                effective_staged_data_type = "shapefile_collection" 

        logging.info("    Determined staged_data_type: '%s' for downloaded file '%s'", 
                     effective_staged_data_type, downloaded_file_path.name)

        if effective_staged_data_type == "gpkg":
            staged_gpkg_filename: str = sanitized_staging_subdir_name + explicit_local_file_ext
            staged_gpkg_path: Path = final_staging_destination_dir / staged_gpkg_filename
            try:
                shutil.copy(downloaded_file_path, staged_gpkg_path)
                logging.info("➕ Staged GPKG %s to %s", 
                             downloaded_file_path.name, 
                             staged_gpkg_path.relative_to(paths.ROOT))
            except Exception as e:
                logging.error(f"❌ Failed to copy downloaded GPKG {downloaded_file_path.name} to staging location {staged_gpkg_path}: {e}", exc_info=True)
        
        elif effective_staged_data_type == "shapefile_collection": 
            if explicit_local_file_ext.lower() != ".zip":
                logging.warning(
                    f"⚠️ Expected a ZIP file for source '{self.src.name}' (staged_data_type='shapefile_collection' or inferred) "
                    f"but actual extension is '{explicit_local_file_ext}'. Attempting extraction anyway for '{downloaded_file_path.name}'."
                )
            
            try:
                extract_zip(downloaded_file_path, final_staging_destination_dir)
                logging.info("➕ Extracted and staged archive %s to %s", 
                             downloaded_file_path.name, 
                             final_staging_destination_dir.relative_to(paths.ROOT))
            except Exception as e:
                logging.error(f"❌ Failed to extract archive {downloaded_file_path.name} to {final_staging_destination_dir}: {e}", exc_info=True)
        else:
            logging.warning(
                f"🤷 Don't know how to stage data with effective_staged_data_type '{effective_staged_data_type}' "
                f"for source '{self.src.name}'. File downloaded to {downloaded_file_path}."
            )
