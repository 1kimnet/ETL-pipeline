# etl/handlers/ogc_api.py
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Final, Tuple
import re
from urllib.parse import urljoin # For resolving relative URLs

import requests

from ..models import Source
from ..utils import ensure_dirs, paths
from ..utils.naming import sanitize_for_filename

log: Final = logging.getLogger(__name__)

# Constants for OGC API
CRS84_URI: Final = "http://www.opengis.net/def/crs/OGC/1.3/CRS84" # WGS84 Lon/Lat
SWEREF99TM_URI: Final = "http://www.opengis.net/def/crs/EPSG/0/3006" # SWEREF99 TM
DEFAULT_OGC_BBOX_COORDS: Final = "16.504,59.090,17.618,59.610"
DEFAULT_OGC_BBOX_CRS_URI: Final = CRS84_URI
DEFAULT_TIMEOUT: Final = 60
# CRS URIs
CRS_URIS: Final = {
    "CRS84": CRS84_URI,
    "SWEREF99TM": SWEREF99TM_URI
}
DEFAULT_BBOX_SR: Final = "3006"  # EPSG code for SWEREF99 TM
# Default CRS for the BBOX coordinates
# Default BBOX values

class OgcApiDownloadHandler:
    """🔄 Downloads data from OGC API Features endpoints."""
    
    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}
        ensure_dirs()
        logging.info("🚀 Initializing OgcApiDownloadHandler for source: %s", self.src.name)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ETL-Pipeline/1.0 (+https://your-contact-info-or-project-url)",
            "Accept": "application/geo+json, application/json;q=0.9, application/vnd.ogc.fg+json;q=0.8"
        })
        
    def fetch(self) -> bool:
        """🔄 Main entry point - downloads OGC API data, one file per collection."""
        if not self.src.enabled:
            log.info("⏭️ Source '%s' (OGC API) is disabled, skipping fetch.", self.src.name)
            return True 

        log.info("🌐 Processing OGC API source: '%s' from URL: %s", self.src.name, self.src.url)
        
        try:
            collections = self._get_collections()
            if not collections:
                log.warning("⚠️ No collections found or discovery failed for source: %s", self.src.name)
                return False 

            processed_at_least_one_collection_successfully = False
            for collection_data in collections:
                collection_id = collection_data.get("id")
                if not collection_id:
                    log.warning("    Skipping a collection with no ID for source '%s'. Title: '%s'", 
                                self.src.name, collection_data.get("title", "N/A"))
                    continue
                
                log.info("Processing collection: %s (Source: %s)", collection_id, self.src.name)
                if self._fetch_collection(collection_data):
                    processed_at_least_one_collection_successfully = True
                else:
                    log.warning("    Collection '%s' for source '%s' may not have been processed successfully or had no data.",
                                collection_id, self.src.name)
            
            if not processed_at_least_one_collection_successfully and collections:
                 log.warning("⚠️ No collections were successfully processed/saved with data for source: %s", self.src.name)
                 return False 
            
            return True 

        except Exception as e: 
            log.error("❌ Error during OGC API processing for source %s: %s", self.src.name, e, exc_info=True)
            return False
        finally:
            self.session.close()

    def _get_collections(self) -> List[Dict[str, Any]]:
        """Get list of collections to download, respecting source config if present."""
        configured_collection_ids = self.src.raw.get("collections")
        
        all_discovered_collections = self._discover_collections()
        if not all_discovered_collections:
            return []

        if configured_collection_ids:
            if not isinstance(configured_collection_ids, list):
                log.warning("    'collections' in source.raw for '%s' is not a list. Will use all discovered.", self.src.name)
                return all_discovered_collections

            log.info("🔧 Filtering for configured collections: %s for source '%s'", configured_collection_ids, self.src.name)
            configured_collection_ids_str = {str(cid) for cid in configured_collection_ids}
            
            selected_collections = [
                col for col in all_discovered_collections 
                if str(col.get("id")) in configured_collection_ids_str
            ]
            if len(selected_collections) != len(configured_collection_ids_str):
                found_ids = {str(col.get("id")) for col in selected_collections}
                missing_ids = configured_collection_ids_str - found_ids
                if missing_ids:
                    log.warning("    Not all configured collections for '%s' were found in discovered collections. Missing or mismatched IDs: %s",
                                self.src.name, list(missing_ids))
            return selected_collections
        
        log.info("🔍 No specific collections configured for '%s', using all %d discovered collections.", self.src.name, len(all_discovered_collections))
        return all_discovered_collections

    def _discover_collections(self) -> List[Dict[str, Any]]:
        """🔄 Discover available collections from the API."""
        try:
            collections_url = self.src.url.rstrip('/')
            log.info("🔄 Discovering collections from: %s", collections_url)
            
            response = self.session.get(collections_url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            discovered = data.get("collections", []) 
            
            if not discovered and "links" in data:
                for link in data["links"]:
                    if link.get("rel") == "data": 
                        new_collections_url = link["href"]
                        if not new_collections_url.startswith(('http://', 'https://')):
                            new_collections_url = urljoin(collections_url if collections_url.endswith('/') else collections_url + '/', new_collections_url)
                        log.info("    Following 'data' link to collections: %s", new_collections_url)
                        response = self.session.get(new_collections_url, timeout=DEFAULT_TIMEOUT)
                        response.raise_for_status()
                        data = response.json()
                        discovered = data.get("collections", [])
                        break
            
            log.info("✅ Discovered %d collections for source '%s'", len(discovered), self.src.name)
            for col in discovered:
                log.debug("        - Collection ID: %s, Title: %s", col.get("id"), col.get("title"))
            return discovered if isinstance(discovered, list) else []
            
        except requests.RequestException as e:
            log.error("❌ Failed to discover collections for source '%s': %s", self.src.name, e)
            return []
        except (json.JSONDecodeError, KeyError) as e:
            log.error("❌ Invalid collections response format for source '%s': %s", self.src.name, e)
            return []
        except Exception as e:
            log.error("❌ Unexpected error discovering collections for source '%s': %s", self.src.name, e, exc_info=True)
            return []

    def _fetch_collection(self, collection_data: Dict[str, Any]) -> bool:
        """Fetch all features from a single collection and save to a file. Returns True on success."""
        collection_id = collection_data.get("id", "unknown_collection")
        collection_title = collection_data.get("title", collection_id)
        
        log.info("    📦 Fetching collection: %s (%s)", collection_id, collection_title)
        
        items_link = self._find_items_link(collection_data)
        if not items_link:
            log.error("    ❌ No suitable 'items' link found for collection '%s'", collection_id)
            return False

        sanitized_source_name = sanitize_for_filename(self.src.name)
        sanitized_collection_id = sanitize_for_filename(collection_id)
        base_staging_dir = paths.STAGING / self.src.authority / sanitized_source_name
        base_staging_dir.mkdir(parents=True, exist_ok=True)
        output_path = base_staging_dir / f"{sanitized_collection_id}.geojson"
        
        all_features_for_this_collection: List[Dict[str,Any]] = []
        next_url: Optional[str] = items_link
        page = 1
        collection_fetch_had_critical_error = False

        # --- Determine BBOX query parameters for this collection's items ---
        bbox_query_params: Dict[str, str] = {}
        if self.global_config.get("use_bbox_filter", False):
            bbox_coords_str = self.src.raw.get("ogc_bbox", 
                                self.global_config.get("global_ogc_bbox_coords", DEFAULT_OGC_BBOX_COORDS))
            
            bbox_crs_input = str(self.src.raw.get("ogc_bbox_crs", 
                                   self.global_config.get("global_ogc_bbox_crs_uri", DEFAULT_OGC_BBOX_CRS_URI)))

            bbox_crs_uri_str = bbox_crs_input 
            if bbox_crs_input.upper() == "CRS84":
                bbox_crs_uri_str = CRS84_URI
            elif bbox_crs_input.isdigit(): 
                bbox_crs_uri_str = f"http://www.opengis.net/def/crs/EPSG/0/{bbox_crs_input}"
            
            if bbox_coords_str:
                bbox_query_params["bbox"] = bbox_coords_str
                bbox_query_params["bbox-crs"] = bbox_crs_uri_str
                log.info("        Applying BBOX to OGC API items request for collection '%s': %s (CRS: %s)",
                         collection_id, bbox_coords_str, bbox_crs_uri_str)
        # --- End BBOX parameter determination ---

        while next_url:
            log.info("        Fetching page %d for collection '%s' from %s", page, collection_id, next_url)
            
            current_page_params = bbox_query_params.copy() if page == 1 and bbox_query_params else {}
            # If server's 'next' links don't preserve bbox, enable this for all pages:
            # current_page_params.update(bbox_query_params) 

            features_page, next_url_from_page = self._fetch_page(next_url, query_params_to_add=current_page_params)

            if features_page is None: 
                log.error("    ❌ Critical error during page fetch for collection '%s'. Aborting this collection.", collection_id)
                collection_fetch_had_critical_error = True
                break 
                
            all_features_for_this_collection.extend(features_page)
            log.info("        Retrieved %d features on this page (total for this collection: %d)", 
                     len(features_page), len(all_features_for_this_collection))

            next_url = next_url_from_page 
            if not next_url: 
                break 
            
            page += 1
            ogc_api_delay_val = self.global_config.get("ogc_api_delay", 0.1)
            if isinstance(ogc_api_delay_val, (int, float)) and ogc_api_delay_val > 0:
                 time.sleep(ogc_api_delay_val)
        
        if collection_fetch_had_critical_error:
            return False

        if all_features_for_this_collection:
            feature_collection_output = {
                "type": "FeatureCollection",
                "features": all_features_for_this_collection,
                "name": collection_title 
            }
            
            crs_to_set = None
            output_crs_epsg_override = self.src.raw.get("output_crs_epsg")
            if output_crs_epsg_override:
                crs_to_set = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{output_crs_epsg_override}"}}
                log.info("    Setting CRS for collection '%s' to user-defined EPSG:%s from source.raw.output_crs_epsg", collection_id, output_crs_epsg_override)
            else:
                storage_crs_uri = collection_data.get("storageCrs")
                if storage_crs_uri:
                    epsg_match = re.search(r'EPSG/(?:0/)?(\d+)', storage_crs_uri) or \
                                 re.search(r'EPSG::(\d+)', storage_crs_uri)
                    if epsg_match:
                        epsg_code = epsg_match.group(1)
                        if self.src.authority == "SGU" and epsg_code == "3006" and all_features_for_this_collection:
                            try:
                                first_feature_geometry = all_features_for_this_collection[0].get("geometry", {})
                                first_coords_set = first_feature_geometry.get("coordinates", [])
                                coord_to_check = None
                                if first_coords_set:
                                    current_level = first_coords_set
                                    while isinstance(current_level, list) and current_level and isinstance(current_level[0], list):
                                        current_level = current_level[0]
                                    if isinstance(current_level, list) and len(current_level) >=2 and \
                                       isinstance(current_level[0], (int, float)) and isinstance(current_level[1], (int, float)):
                                        coord_to_check = current_level
                                if coord_to_check and abs(coord_to_check[0]) <= 180 and abs(coord_to_check[1]) <= 90:
                                    log.warning(f"    SGU service for collection '{collection_id}' reported storageCrs EPSG:3006, but coordinates ({coord_to_check}) appear to be WGS84 decimal degrees. Overriding CRS to EPSG:4326.")
                                    epsg_code = "4326"
                            except (KeyError, IndexError, TypeError) as inspect_err:
                                log.debug("    Could not inspect coordinates for SGU CRS heuristic for collection '%s': %s. Will use reported EPSG: %s.", collection_id, inspect_err, epsg_code)
                        crs_to_set = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{epsg_code}"}}
                        log.info("    Setting CRS for collection '%s' based on (potentially heuristically adjusted) storageCrs: %s", collection_id, crs_to_set["properties"]["name"])
                
                # Corrected Fallback Logic for CRS (Option 3)
                if not crs_to_set and self.global_config.get("use_sweref99_ogc_fallback", False):
                    # If this global flag is true, implies user wants SWEREF99TM as a fallback for OGC outputs
                    # when no other CRS info is determined.
                    epsg_code_for_fallback = "3006" # Directly use the EPSG code for SWEREF99TM
                    crs_to_set = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{epsg_code_for_fallback}"}}
                    log.info("    No CRS determined from override or storageCrs for collection '%s'. Defaulting to EPSG:%s based on global config 'use_sweref99_ogc_fallback'.", collection_id, epsg_code_for_fallback)
            
            if crs_to_set:
                 feature_collection_output["crs"] = crs_to_set
            else:
                # Final fallback: If no specific CRS override, no storageCrs, and no SWEREF99TM fallback flag, default to WGS84 (EPSG:4326)
                # This is a common default for GeoJSON if CRS is unknown, especially if coordinates are small numbers.
                feature_collection_output["crs"] = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}
                log.warning("    Could not determine CRS for collection '%s' from any configuration or metadata. Defaulting to EPSG:4326. Please verify this is correct.", collection_id)

            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(feature_collection_output, f, ensure_ascii=False, indent=2)
                log.info("    💾 Saved %d features for collection '%s' to %s", 
                         len(all_features_for_this_collection), collection_id, output_path.relative_to(paths.ROOT))
                if self.src.staged_data_type is None:
                    self.src.staged_data_type = "geojson"
                return True
            except IOError as e:
                log.error("    ❌ Failed to write features for collection '%s': %s", collection_id, e)
                return False
        else: # No features were retrieved for this collection (but no critical fetch error)
            log.info("    ℹ️ No features retrieved or written for collection '%s'.", collection_id)
            return True # Successfully processed the collection, even if it had no data.


    def _find_items_link(self, collection_data: Dict[str, Any]) -> Optional[str]:
        links = collection_data.get("links", [])
        preferred_formats = ["application/geo+json", "application/json", "application/vnd.ogc.fg+json"]
        
        # First pass for preferred formats
        for link_info in links:
            if link_info.get("rel") == "items" and link_info.get("type") in preferred_formats:
                href = link_info.get("href")
                if href:
                    if not href.startswith(('http://', 'https://')):
                        # Resolve relative URL against the collection's own canonical URL if available,
                        # or fall back to the service root (self.src.url)
                        collection_self_link = next((l.get("href") for l in links if l.get("rel") == "self" and l.get("href")), self.src.url)
                        href = urljoin(collection_self_link if collection_self_link.endswith('/') else collection_self_link + '/', href)
                    return href
        
        # Fallback if preferred not found, take first "items" link
        for link_info in links:
            if link_info.get("rel") == "items":
                href = link_info.get("href")
                if href:
                    log.warning("    Using potentially non-preferred format ('%s') for items link in collection '%s'.", 
                                link_info.get("type", "Unknown"), collection_data.get("id"))
                    if not href.startswith(('http://', 'https://')):
                        collection_self_link = next((l.get("href") for l in links if l.get("rel") == "self" and l.get("href")), self.src.url)
                        href = urljoin(collection_self_link if collection_self_link.endswith('/') else collection_self_link + '/', href)
                    return href
        
        log.error("    No 'items' link found in collection: %s", collection_data.get("id"))
        return None

    def _fetch_page(self, url: str, query_params_to_add: Optional[Dict[str, str]] = None) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """Fetch a single page of features. query_params_to_add are merged with existing URL params."""
        
        # Prepare parameters: start with an empty dict if None is passed
        current_request_params = query_params_to_add.copy() if query_params_to_add else {}
        response = None

        try:
            log.debug("        Requesting OGC API page: %s with params: %s", url, current_request_params)
            response = self.session.get(url, params=current_request_params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            features_on_page: List[Dict[str, Any]] = []
            if isinstance(data, dict) and data.get("type") == "FeatureCollection":
                features_on_page = data.get("features", [])
            elif isinstance(data, list): 
                features_on_page = data 
            elif isinstance(data, dict) and "features" in data:
                 features_on_page = data.get("features", [])
            else:
                log.warning("        Unexpected JSON structure for OGC API features page at %s. Content snippet: %s", url, str(data)[:200])

            next_page_url: Optional[str] = None
            if isinstance(data, dict):
                 next_page_url = self._find_next_link(data.get("links", []))
            
            if next_page_url and not next_page_url.startswith(('http://', 'https://')):
                # Resolve relative 'next' link against the current page's *full* URL
                # The 'url' parameter to _fetch_page is the one that might contain base path and existing query params
                # from a previous next link or the initial items_link.
                base_url_for_next_link = response.url # Use the actual URL from the response to resolve relative links
                next_page_url = urljoin(base_url_for_next_link, next_page_url) 
                log.debug("        Resolved relative next link to: %s", next_page_url)

            return features_on_page, next_page_url
            
        except requests.exceptions.Timeout:
            log.error("        ❌ Timeout error fetching OGC API page: %s", url)
            return None, None 
        except requests.exceptions.HTTPError as e:
            log.error("        ❌ HTTP error %s fetching OGC API page: %s (Response snippet: %s)", e.response.status_code, url, e.response.text[:200])
            return None, None 
        except requests.exceptions.RequestException as e:
            log.error("        ❌ Network error fetching OGC API page %s: %s", url, e)
            return None, None 
        except json.JSONDecodeError as e:
            log.error("        ❌ Invalid JSON response from OGC API page %s: %s", url, e)
            if response and hasattr(response, 'text'): # Check if response exists and has text
                 log.debug("        Raw response text for JSON error: %s", response.text[:500])
            return None, None
        except Exception as e_unexpected: 
            log.error("        ❌ Unexpected error fetching OGC API page %s: %s", url, e_unexpected, exc_info=True)
            return None, None

    def _find_next_link(self, links: list[dict[str, Any]]) -> Optional[str]:
        for link_info in links:
            if link_info.get("rel") == "next" and link_info.get("href"):
                return link_info["href"]
        return None

    def __enter__(self) -> OgcApiDownloadHandler:
        return self

    def __exit__(self, exc_type: Optional[type[BaseException]], 
                 exc_val: Optional[BaseException], 
                 exc_tb: Optional[Any]) -> None:
        self.session.close()