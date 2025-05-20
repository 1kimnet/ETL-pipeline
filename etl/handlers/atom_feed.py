# etl/handlers/atom_feed.py
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Final, Set, Optional, Tuple, Dict, Any # Added Dict, Any
import shutil
from urllib.parse import unquote
import zipfile # For BadZipFile exception

from ..models import Source
from ..utils import download, ensure_dirs, extract_zip, paths
from ..utils.naming import sanitize_for_filename
from ..utils.http import fetch_true_filename_parts

_ATOM_NS: Final = {"atom": "http://www.w3.org/2005/Atom"}


class AtomFeedDownloadHandler:
    """
    Downloads every unique <link rel="enclosure"> or <link href="...">
    from an Atom feed and stages the content.
    """

    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None): # Added global_config
        self.src: Source = src
        self.global_config: Dict[str, Any] = global_config or {} # Store or ignore global_config as needed
        ensure_dirs()
        # You can log if global_config is used or just accept it
        # logging.debug("AtomFeedDownloadHandler initialized for '%s' with global_config: %s", src.name, self.global_config if self.global_config else "None")


    def fetch(self) -> None:
        if not self.src.enabled:
            logging.info("⏭️ Source '%s' (Atom Feed) is disabled, skipping fetch. [V1.2 LOG]", self.src.name)
            return

        logging.info("🌐 Reading Atom feed for source '%s' from URL: %s [V1.2 LOG]", self.src.name, self.src.url)

        sanitized_source_name: str = sanitize_for_filename(self.src.name)
        feed_xml_filename: str = sanitized_source_name + "_feed.xml"
        feed_xml_download_path: Path = paths.DOWNLOADS / feed_xml_filename

        try:
            downloaded_feed_xml_path = download(self.src.url, feed_xml_download_path)
            feed_xml_content: str = downloaded_feed_xml_path.read_text(encoding="utf-8")
        except Exception as e:
            logging.error("❌ Failed to download or read Atom feed XML for '%s' from %s: %s [V1.2 LOG]",
                          self.src.name, self.src.url, e, exc_info=True)
            return

        try:
            root: ET.Element = ET.fromstring(feed_xml_content)
        except ET.ParseError as exc:
            logging.error("❌ Malformed Atom XML for source '%s': %s [V1.2 LOG]", self.src.name, exc, exc_info=True)
            return

        source_specific_staging_dir: Path = paths.STAGING / self.src.authority / sanitized_source_name
        source_specific_staging_dir.mkdir(parents=True, exist_ok=True)
        logging.info("    Target staging directory for all content from Atom source '%s': %s [V1.2 LOG]",
                     self.src.name, source_specific_staging_dir.relative_to(paths.ROOT))

        urls_seen: Set[str] = set()
        processed_any_links = False
        for entry in root.findall("atom:entry", _ATOM_NS):
            link_element: Optional[ET.Element] = (
                entry.find("atom:link[@rel='enclosure']", _ATOM_NS)
                or entry.find("atom:link[@href]", _ATOM_NS)
            )
            if link_element is None or not link_element.attrib.get("href"):
                logging.debug("    Skipping entry with no suitable link. [V1.2 LOG]")
                continue

            dl_url: str = link_element.attrib["href"]
            if not dl_url.strip():
                logging.debug("    Skipping entry with empty href. [V1.2 LOG]")
                continue

            if dl_url in urls_seen:
                logging.debug("    Skipping already processed URL: %s [V1.2 LOG]", dl_url)
                continue
            urls_seen.add(dl_url)

            logging.info("    Found linked resource in Atom feed for '%s': %s [V1.2 LOG]", self.src.name, dl_url)
            self._download_and_stage_linked_resource(dl_url, source_specific_staging_dir, sanitized_source_name)
            processed_any_links = True

        if not processed_any_links:
            logging.info("    No downloadable resources found or processed in Atom feed for '%s'. [V1.2 LOG]", self.src.name)


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
            true_stem_from_web: str
            true_ext_from_web: str
            true_stem_from_web, true_ext_from_web = fetch_true_filename_parts(dl_url)

            sanitized_true_stem: str = sanitize_for_filename(true_stem_from_web, allow_dot=True)

            final_ext_for_download: str = true_ext_from_web
            if not final_ext_for_download or len(final_ext_for_download) > 5 :
                logging.warning(f"        Suspicious or missing extension ('{final_ext_for_download}') for {dl_url}. Checking source.download_format or defaulting. [V1.2 LOG]")
                if self.src.download_format:
                    final_ext_for_download = f".{self.src.download_format.lower().lstrip('.')}"
                    logging.info(f"        Using source.download_format for extension: '{final_ext_for_download}' for {dl_url} [V1.2 LOG]")
                else:
                    path_ext = Path(unquote(dl_url)).suffix.lower()
                    if path_ext in [".zip", ".gpkg"]:
                        final_ext_for_download = path_ext
                    else:
                        final_ext_for_download = ".zip" # Default to .zip if truly unknown and not specified
                    logging.info(f"        Inferred or defaulted extension to '{final_ext_for_download}' for {dl_url} [V1.2 LOG]")

            download_filename: str = sanitized_true_stem + final_ext_for_download
            download_target_path: Path = paths.DOWNLOADS / download_filename

            logging.info("        Attempting to download linked resource: %s \n        -> to local file: %s [V1.2 LOG]",
                         dl_url, download_target_path.name)

            downloaded_file_path: Path = download(dl_url, download_target_path)

            is_downloaded_zip = downloaded_file_path.suffix.lower() == ".zip"
            is_downloaded_gpkg = downloaded_file_path.suffix.lower() == ".gpkg"

            if is_downloaded_zip:
                logging.info("        Downloaded file '%s' is a ZIP archive. Extracting... [V1.2 LOG]", downloaded_file_path.name)
                try:
                    extract_zip(downloaded_file_path, target_staging_dir)
                    logging.info("        ➕ Extracted and staged archive %s into %s [V1.2 LOG]",
                                 downloaded_file_path.name,
                                 target_staging_dir.relative_to(paths.ROOT))

                    if self.src.staged_data_type == "gpkg":
                        logging.info("        Source '%s' expects a GPKG. Searching in extracted contents of %s... [V1.2 LOG]",
                                     self.src.name, downloaded_file_path.name)
                        extracted_gpkgs = list(target_staging_dir.rglob("*.gpkg"))
                        if extracted_gpkgs:
                            if len(extracted_gpkgs) > 1:
                                logging.warning("            Found multiple GPKGs in extracted archive, using the first: %s [V1.2 LOG]", extracted_gpkgs[0].name)

                            expected_staged_gpkg_name = sanitized_atom_source_name + ".gpkg"
                            final_staged_gpkg_path = target_staging_dir / expected_staged_gpkg_name

                            if extracted_gpkgs[0].resolve() != final_staged_gpkg_path.resolve(): # Check if it's not the same file
                                logging.info("            Renaming/moving extracted GPKG '%s' to '%s' [V1.2 LOG]",
                                             extracted_gpkgs[0].name, final_staged_gpkg_path.name)
                                shutil.move(str(extracted_gpkgs[0]), str(final_staged_gpkg_path))
                            else:
                                logging.info("            Extracted GPKG '%s' already has the expected name and location. [V1.2 LOG]", final_staged_gpkg_path.name)
                            logging.info("        ✅ Successfully staged GPKG '%s' for source '%s'. [V1.2 LOG]",
                                         final_staged_gpkg_path.name, self.src.name)
                        else:
                            logging.warning("        ⚠️ Source '%s' expected a GPKG, but no .gpkg file found after extracting %s. [V1.2 LOG]",
                                          self.src.name, downloaded_file_path.name)
                except zipfile.BadZipFile:
                     logging.error(f"        ❌ File '{downloaded_file_path.name}' from Atom link is not a valid ZIP file. Cannot extract. [V1.2 LOG]")
                except Exception as e:
                    logging.error(f"        ❌ Failed to extract archive {downloaded_file_path.name} to {target_staging_dir}: {e} [V1.2 LOG]", exc_info=True)

            elif is_downloaded_gpkg:
                staged_gpkg_filename: str = sanitized_atom_source_name + ".gpkg"
                staged_gpkg_path: Path = target_staging_dir / staged_gpkg_filename
                try:
                    if downloaded_file_path.resolve() != staged_gpkg_path.resolve():
                         shutil.copy(downloaded_file_path, staged_gpkg_path)
                         logging.info("        ➕ Staged direct GPKG %s to %s [V1.2 LOG]",
                                     downloaded_file_path.name,
                                     staged_gpkg_path.relative_to(paths.ROOT))
                    else:
                        logging.info("        ℹ️ Downloaded GPKG '%s' is already in the target staging location. [V1.2 LOG]", downloaded_file_path.name)

                except Exception as e:
                    logging.error(f"        ❌ Failed to copy downloaded GPKG {downloaded_file_path.name} to staging location {staged_gpkg_path}: {e} [V1.2 LOG]", exc_info=True)

            else:
                logging.warning(
                    f"        🤷 Downloaded file '{downloaded_file_path.name}' from Atom link is not a recognized ZIP or GPKG. "
                    f"Source's staged_data_type is '{self.src.staged_data_type}'. Staging not performed for this link. [V1.2 LOG]"
                )

        except Exception as e:
            logging.error("    ❌ Failed to download/stage resource from Atom link %s: %s [V1.2 LOG]", dl_url, e, exc_info=True)