# etl/handlers/atom_feed.py
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Final, Set, Optional, Dict, Any
import shutil
from urllib.parse import unquote
import zipfile

from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths
from ..utils.naming import sanitize_for_filename  # Updated import
from ..utils.http import fetch_true_filename_parts

log = logging.getLogger(__name__)

_ATOM_NS: Final = {"atom": "http://www.w3.org/2005/Atom"}


class AtomFeedDownloadHandler:
    """
    Downloads every unique <link rel="enclosure"> or <link href="...">
    from an Atom feed and stages the content.
    """

    def __init__(self, src: Source,
                 global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}
        ensure_dirs()

    def fetch(self) -> None:
        if not self.src.enabled:
            log.info(
                "⏭️ Source '%s' (Atom Feed) is disabled, skipping fetch.",
                self.src.name)
            return

        log.info(
            "🌐 Reading Atom feed for source '%s' from URL: %s",
            self.src.name,
            self.src.url)

        sanitized_source_name = sanitize_for_filename(self.src.name)
        feed_xml_filename = sanitized_source_name + "_feed.xml"
        feed_xml_download_path = paths.DOWNLOADS / feed_xml_filename

        try:
            downloaded_feed_xml_path = download(
                self.src.url, feed_xml_download_path)
            feed_xml_content = downloaded_feed_xml_path.read_text(
                encoding="utf-8")
        except Exception as e:
            log.error(
                "❌ Failed to download or read Atom feed XML for '%s' from %s: %s",
                self.src.name,
                self.src.url,
                e,
                exc_info=True)
            return

        try:
            root = ET.fromstring(feed_xml_content)
        except ET.ParseError as exc:
            log.error(
                "❌ Malformed Atom XML for source '%s': %s",
                self.src.name,
                exc,
                exc_info=True)
            return

        source_specific_staging_dir = paths.STAGING / \
            self.src.authority / sanitized_source_name
        source_specific_staging_dir.mkdir(parents=True, exist_ok=True)
        log.info(
            "Target staging directory for all content from Atom source '%s': %s",
            self.src.name,
            source_specific_staging_dir.relative_to(
                paths.ROOT))

        urls_seen: Set[str] = set()
        processed_any_links = False

        for entry in root.findall("atom:entry", _ATOM_NS):
            link_element = (
                entry.find("atom:link[@rel='enclosure']", _ATOM_NS)
                or entry.find("atom:link[@href]", _ATOM_NS)
            )
            if link_element is None or not link_element.attrib.get("href"):
                log.debug("Skipping entry with no suitable link.")
                continue

            dl_url = link_element.attrib["href"]
            if not dl_url.strip():
                log.debug("Skipping entry with empty href.")
                continue

            if dl_url in urls_seen:
                log.debug("Skipping already processed URL: %s", dl_url)
                continue
            urls_seen.add(dl_url)

            log.info(
                "Found linked resource in Atom feed for '%s': %s",
                self.src.name,
                dl_url)
            self._download_and_stage_linked_resource(
                dl_url, source_specific_staging_dir, sanitized_source_name)
            processed_any_links = True

        if not processed_any_links:
            log.info(
                "No downloadable resources found or processed in Atom feed for '%s'.",
                self.src.name)

    def _download_and_stage_linked_resource(
        self,
        dl_url: str,
        target_staging_dir: Path,
        sanitized_atom_source_name: str
    ) -> None:
        """
        Downloads a resource linked from an Atom feed entry and stages it into the
        provided target_staging_dir.
        """
        try:
            true_stem_from_web, true_ext_from_web = fetch_true_filename_parts(
                dl_url)
            sanitized_true_stem = sanitize_for_filename(true_stem_from_web)

            final_ext_for_download = true_ext_from_web
            if not final_ext_for_download or len(final_ext_for_download) > 5:
                log.warning(
                    "Suspicious or missing extension ('%s') for %s. Checking source.download_format or defaulting.",
                    final_ext_for_download,
                    dl_url)
                if self.src.download_format:
                    final_ext_for_download = f".{self.src.download_format.lower().lstrip('.')}"
                    log.info(
                        "Using source.download_format for extension: '%s' for %s",
                        final_ext_for_download,
                        dl_url)
                else:
                    path_ext = Path(unquote(dl_url)).suffix.lower()
                    if path_ext in [".zip", ".gpkg"]:
                        final_ext_for_download = path_ext
                    else:
                        final_ext_for_download = ".zip"  # Default to .zip if truly unknown
                    log.info("Inferred or defaulted extension to '%s' for %s",
                             final_ext_for_download, dl_url)

            download_filename = sanitized_true_stem + final_ext_for_download
            download_target_path = paths.DOWNLOADS / download_filename

            log.info(
                "Attempting to download linked resource: %s \n        -> to local file: %s",
                dl_url,
                download_target_path.name)

            downloaded_file_path = download(dl_url, download_target_path)

            is_downloaded_zip = downloaded_file_path.suffix.lower() == ".zip"
            is_downloaded_gpkg = downloaded_file_path.suffix.lower() == ".gpkg"

            if is_downloaded_zip:
                log.info(
                    "Downloaded file '%s' is a ZIP archive. Extracting...",
                    downloaded_file_path.name)
                try:
                    extract_zip(downloaded_file_path, target_staging_dir)
                    log.info(
                        "➕ Extracted and staged archive %s into %s",
                        downloaded_file_path.name,
                        target_staging_dir.relative_to(
                            paths.ROOT))

                    if self.src.staged_data_type == "gpkg":
                        log.info(
                            "Source '%s' expects a GPKG. Searching in extracted contents of %s...",
                            self.src.name,
                            downloaded_file_path.name)
                        extracted_gpkgs = list(
                            target_staging_dir.rglob("*.gpkg"))
                        if extracted_gpkgs:
                            if len(extracted_gpkgs) > 1:
                                log.warning(
                                    "Found multiple GPKGs in extracted archive, using the first: %s",
                                    extracted_gpkgs[0].name)

                            expected_staged_gpkg_name = sanitized_atom_source_name + ".gpkg"
                            final_staged_gpkg_path = target_staging_dir / expected_staged_gpkg_name

                            if extracted_gpkgs[0].resolve(
                            ) != final_staged_gpkg_path.resolve():
                                log.info(
                                    "Renaming/moving extracted GPKG '%s' to '%s'",
                                    extracted_gpkgs[0].name,
                                    final_staged_gpkg_path.name)
                                shutil.move(str(extracted_gpkgs[0]), str(
                                    final_staged_gpkg_path))
                            else:
                                log.info(
                                    "Extracted GPKG '%s' already has the expected name and location.",
                                    final_staged_gpkg_path.name)
                            log.info(
                                "✅ Successfully staged GPKG '%s' for source '%s'.",
                                final_staged_gpkg_path.name,
                                self.src.name)
                        else:
                            log.warning(
                                "⚠️ Source '%s' expected a GPKG, but no .gpkg file found after extracting %s.",
                                self.src.name,
                                downloaded_file_path.name)
                except zipfile.BadZipFile:
                    log.error(
                        "❌ File '%s' from Atom link is not a valid ZIP file. Cannot extract.",
                        downloaded_file_path.name)
                except Exception as e:
                    log.error(
                        "❌ Failed to extract archive %s to %s: %s",
                        downloaded_file_path.name,
                        target_staging_dir,
                        e,
                        exc_info=True)

            elif is_downloaded_gpkg:
                staged_gpkg_filename = sanitized_atom_source_name + ".gpkg"
                staged_gpkg_path = target_staging_dir / staged_gpkg_filename
                try:
                    if downloaded_file_path.resolve() != staged_gpkg_path.resolve():
                        shutil.copy(downloaded_file_path, staged_gpkg_path)
                        log.info(
                            "➕ Staged direct GPKG %s to %s",
                            downloaded_file_path.name,
                            staged_gpkg_path.relative_to(
                                paths.ROOT))
                    else:
                        log.info(
                            "ℹ️ Downloaded GPKG '%s' is already in the target staging location.",
                            downloaded_file_path.name)
                except Exception as e:
                    log.error(
                        "❌ Failed to copy downloaded GPKG %s to staging location %s: %s",
                        downloaded_file_path.name,
                        staged_gpkg_path,
                        e,
                        exc_info=True)

            else:
                log.warning(
                    "🤷 Downloaded file '%s' from Atom link is not a recognized ZIP or GPKG. "
                    "Source's staged_data_type is '%s'. Staging not performed for this link.",
                    downloaded_file_path.name,
                    self.src.staged_data_type)

        except Exception as e:
            log.error(
                "❌ Failed to download/stage resource from Atom link %s: %s",
                dl_url,
                e,
                exc_info=True)

    def __enter__(self) -> 'AtomFeedDownloadHandler':
        """Enter the context manager for use with 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager. No cleanup needed for Atom feed downloads."""
        pass
