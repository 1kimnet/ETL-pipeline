# etl/handlers/ogc_api.py
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Final, Tuple
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests

from ..models import Source
from ..utils import ensure_dirs, paths
from ..utils.naming import sanitize_for_filename

log: Final = logging.getLogger(__name__)

# Constants
CRS84_URI: Final = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
SWEREF99TM_URI: Final = "http://www.opengis.net/def/crs/EPSG/0/3006"
DEFAULT_OGC_BBOX_COORDS: Final = "16.504,59.090,17.618,59.610"
DEFAULT_OGC_BBOX_CRS_URI: Final = CRS84_URI
DEFAULT_TIMEOUT: Final = 10  # seconds

class OgcApiDownloadHandler:
    """🔄 Downloads data from OGC API Features endpoints with BBOX filtering."""
    
    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}
        self.bbox_params: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ETL-Pipeline/1.0",
            "Accept": "application/geo+json, application/json;q=0.9"
        })
        self._setup_bbox_params()
        
    def _setup_bbox_params(self) -> None:
        """🔧 Setup BBOX query parameters based on configuration."""
        if not self.global_config.get("use_bbox_filter", False):
            log.info("    BBOX filtering disabled for source '%s'", self.src.name)
            return
            
        bbox_coords_str = self.src.raw.get(
            "ogc_bbox", 
            self.global_config.get("global_ogc_bbox_coords", DEFAULT_OGC_BBOX_COORDS)
        )
        
        bbox_crs_input = str(self.src.raw.get(
            "ogc_bbox_crs", 
            self.global_config.get("global_ogc_bbox_crs_uri", DEFAULT_OGC_BBOX_CRS_URI)
        ))
        
        bbox_crs_uri_str = self._normalize_crs_uri(bbox_crs_input)
        
        if bbox_coords_str:
            self.bbox_params = {"bbox": bbox_coords_str}
            
            # Only add bbox-crs if not using the default CRS84
            # Many OGC APIs assume CRS84 for BBOX when bbox-crs is omitted
            if bbox_crs_uri_str != CRS84_URI:
                # Check if service supports bbox-crs (can be configured per source)
                if self.src.raw.get("supports_bbox_crs", True):
                    self.bbox_params["bbox-crs"] = bbox_crs_uri_str
                else:
                    log.warning(
                        "    ⚠️ Source '%s' doesn't support bbox-crs. "
                        "BBOX will be interpreted as CRS84 (WGS84 lon/lat)",
                        self.src.name
                    )
            
            log.info(
                "    🗺️ BBOX configured for source '%s': %s (CRS: %s)",
                self.src.name, bbox_coords_str, 
                bbox_crs_uri_str if "bbox-crs" in self.bbox_params else "CRS84 (default)"
            )
    
    def _add_bbox_to_url(self, url: str) -> str:
        """🔧 Add BBOX parameters to URL, preserving existing parameters."""
        if not self.bbox_params:
            return url
            
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        
        for key, value in self.bbox_params.items():
            query_params[key] = [value]
        
        new_query = urlencode(query_params, doseq=True)
        new_parsed = parsed._replace(query=new_query)
        return urlunparse(new_parsed)
    
    def _normalize_crs_uri(self, crs_input: str) -> str:
        """🔧 Normalize CRS input to proper URI format."""
        if crs_input.upper() == "CRS84":
            return CRS84_URI
        elif crs_input == "3006":
            return SWEREF99TM_URI
        elif crs_input.isdigit():
            return f"http://www.opengis.net/def/crs/EPSG/0/{crs_input}"
        else:
            return crs_input
    
    def _test_bbox_support(self, test_url: str) -> bool:
        """🔧 Test if service supports bbox-crs parameter."""
        try:
            # Try a minimal request with bbox-crs
            test_params = {
                "bbox": "0,0,1,1",
                "bbox-crs": CRS84_URI,
                "limit": "1",
                "f": "json"
            }
            
            response = self.session.get(test_url, params=test_params, timeout=10)
            
            # If we get 500 or 400 with bbox-crs, try without it
            if response.status_code in [400, 500]:
                test_params.pop("bbox-crs")
                response2 = self.session.get(test_url, params=test_params, timeout=10)
                
                # If it works without bbox-crs, the service doesn't support it
                if response2.status_code == 200:
                    return False
            
            return response.status_code == 200
            
        except Exception:
            # Assume it supports bbox-crs if we can't determine
            return True
        
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
                
                log.info("📦 Processing collection: %s (Source: %s)", collection_id, self.src.name)
                if self._fetch_collection(collection_data):
                    processed_at_least_one_collection_successfully = True
                else:
                    log.warning("    ⚠️ Collection '%s' for source '%s' may not have been processed successfully or had no data.",
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
        """🔍 Get list of collections to download, respecting source config if present."""
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
                    log.warning("    ⚠️ Not all configured collections for '%s' were found. Missing: %s",
                                self.src.name, list(missing_ids))
            return selected_collections
        
        log.info("🔍 No specific collections configured for '%s', using all %d discovered collections.", 
                 self.src.name, len(all_discovered_collections))
        return all_discovered_collections

    def _discover_collections(self) -> List[Dict[str, Any]]:
        """🔍 Discover available collections from the API."""
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
                log.debug("    - Collection ID: %s, Title: %s", col.get("id"), col.get("title"))
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
        """📦 Fetch all features from a single collection and save to a file."""
        collection_id = collection_data.get("id", "unknown_collection")
        collection_title = collection_data.get("title", collection_id)
        
        log.info("    📦 Fetching collection: %s (%s)", collection_id, collection_title)
        
        items_link = self._find_items_link(collection_data)
        if not items_link:
            log.error("    ❌ No suitable 'items' link found for collection '%s'", collection_id)
            return False

        # 🔧 Apply BBOX to initial items link
        items_link_with_bbox = self._add_bbox_to_url(items_link)
        if items_link_with_bbox != items_link:
            log.info("    🗺️ Applied BBOX to items URL for collection '%s'", collection_id)

        sanitized_source_name = sanitize_for_filename(self.src.name)
        sanitized_collection_id = sanitize_for_filename(collection_id)
        base_staging_dir = paths.STAGING / self.src.authority / sanitized_source_name
        base_staging_dir.mkdir(parents=True, exist_ok=True)
        output_path = base_staging_dir / f"{sanitized_collection_id}.geojson"
        
        all_features_for_this_collection: List[Dict[str,Any]] = []
        next_url: Optional[str] = items_link_with_bbox
        page = 1
        collection_fetch_had_critical_error = False

        while next_url:
            log.info("        📄 Fetching page %d for collection '%s' from %s", page, collection_id, next_url)
            
            features_page, next_url_from_page = self._fetch_page(next_url)

            if features_page is None: 
                log.error("    ❌ Critical error during page fetch for collection '%s'. Aborting this collection.", collection_id)
                collection_fetch_had_critical_error = True
                break 
                
            all_features_for_this_collection.extend(features_page)
            log.info("        ✅ Retrieved %d features on this page (total: %d)", 
                     len(features_page), len(all_features_for_this_collection))

            # 🔧 Apply BBOX to next URL as well
            if next_url_from_page:
                next_url = self._add_bbox_to_url(next_url_from_page)
            else:
                next_url = None
                
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
            
            # 🔧 Simplified CRS handling
            crs_to_set = self._determine_output_crs(collection_data, all_features_for_this_collection)
            if crs_to_set:
                feature_collection_output["crs"] = crs_to_set

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
        else:
            log.info("    ℹ️ No features retrieved for collection '%s'.", collection_id)
            return True

    def _determine_output_crs(self, collection_data: Dict[str, Any], features: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """🔧 Determine the correct CRS for output, with improved logic."""
        collection_id = collection_data.get("id", "unknown")
        
        # 1. Check for explicit override
        output_crs_epsg_override = self.src.raw.get("output_crs_epsg")
        if output_crs_epsg_override:
            crs_override = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{output_crs_epsg_override}"}}
            log.info("    🔧 Using user-defined CRS for collection '%s': EPSG:%s", collection_id, output_crs_epsg_override)
            return crs_override
        
        # 2. Check storageCrs from collection metadata
        storage_crs_uri = collection_data.get("storageCrs")
        if storage_crs_uri:
            epsg_match = re.search(r'EPSG/(?:0/)?(\d+)', storage_crs_uri) or \
                         re.search(r'EPSG::(\d+)', storage_crs_uri)
            if epsg_match:
                epsg_code = epsg_match.group(1)
                
                # 🔧 Improved coordinate inspection for SGU services
                if self.src.authority.upper() == "SGU" and epsg_code == "3006" and features:
                    coordinate_appears_to_be_wgs84 = self._inspect_coordinates_for_wgs84(features[0])
                    if coordinate_appears_to_be_wgs84:
                        log.warning("    ⚠️ SGU service for collection '%s' reports EPSG:3006, but coordinates appear to be WGS84. Using EPSG:4326.", collection_id)
                        epsg_code = "4326"
                
                crs_from_storage = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{epsg_code}"}}
                log.info("    🗺️ Using CRS from storageCrs for collection '%s': EPSG:%s", collection_id, epsg_code)
                return crs_from_storage
        
        # 3. Fallback logic
        if self.global_config.get("use_sweref99_ogc_fallback", False):
            fallback_crs = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::3006"}}
            log.info("    🔄 No CRS determined for collection '%s'. Using SWEREF99TM fallback (EPSG:3006).", collection_id)
            return fallback_crs
        else:
            fallback_crs = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}
            log.warning("    ⚠️ Could not determine CRS for collection '%s'. Defaulting to WGS84 (EPSG:4326).", collection_id)
            return fallback_crs

    def _inspect_coordinates_for_wgs84(self, feature: Dict[str, Any]) -> bool:
        """🔍 Check if coordinates look like WGS84 (lat/lon in decimal degrees)."""
        try:
            geometry = feature.get("geometry", {})
            coordinates = geometry.get("coordinates", [])
            
            # Navigate to the first coordinate pair
            coord_to_check = None
            current_level = coordinates
            while isinstance(current_level, list) and current_level and isinstance(current_level[0], list):
                current_level = current_level[0]
            
            if (isinstance(current_level, list) and len(current_level) >= 2 and 
                isinstance(current_level[0], (int, float)) and isinstance(current_level[1], (int, float))):
                coord_to_check = current_level
            
            if coord_to_check:
                # WGS84 coordinates should be within [-180, 180] for longitude and [-90, 90] for latitude
                # SWEREF99 TM coordinates are much larger (hundreds of thousands)
                return abs(coord_to_check[0]) <= 180 and abs(coord_to_check[1]) <= 90
                
        except (KeyError, IndexError, TypeError) as e:
            log.debug("    Could not inspect coordinates: %s", e)
        
        return False

    def _find_items_link(self, collection_data: Dict[str, Any]) -> Optional[str]:
        """🔍 Find the best items link from collection metadata."""
        links = collection_data.get("links", [])
        preferred_formats = ["application/geo+json", "application/json", "application/vnd.ogc.fg+json"]
        
        # First pass for preferred formats
        for link_info in links:
            if link_info.get("rel") == "items" and link_info.get("type") in preferred_formats:
                href = link_info.get("href")
                if href:
                    if not href.startswith(('http://', 'https://')):
                        collection_self_link = next((l.get("href") for l in links if l.get("rel") == "self" and l.get("href")), self.src.url)
                        href = urljoin(collection_self_link if collection_self_link.endswith('/') else collection_self_link + '/', href)
                    return href
        
        # Fallback if preferred not found
        for link_info in links:
            if link_info.get("rel") == "items":
                href = link_info.get("href")
                if href:
                    log.warning("    ⚠️ Using potentially non-preferred format ('%s') for items link in collection '%s'.", 
                                link_info.get("type", "Unknown"), collection_data.get("id"))
                    if not href.startswith(('http://', 'https://')):
                        collection_self_link = next((l.get("href") for l in links if l.get("rel") == "self" and l.get("href")), self.src.url)
                        href = urljoin(collection_self_link if collection_self_link.endswith('/') else collection_self_link + '/', href)
                    return href
        
        log.error("    ❌ No 'items' link found in collection: %s", collection_data.get("id"))
        return None

    def _fetch_page(self, url: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """📄 Fetch a single page of features."""
        response = None
        try:
            log.debug("        Requesting OGC API page: %s", url)
            response = self.session.get(url, timeout=DEFAULT_TIMEOUT)
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
                log.warning("        ⚠️ Unexpected JSON structure for OGC API features page at %s", url)

            next_page_url: Optional[str] = None
            if isinstance(data, dict):
                 next_page_url = self._find_next_link(data.get("links", []))
            
            if next_page_url and not next_page_url.startswith(('http://', 'https://')):
                base_url_for_next_link = response.url
                next_page_url = urljoin(base_url_for_next_link, next_page_url) 
                log.debug("        Resolved relative next link to: %s", next_page_url)

            return features_on_page, next_page_url
            
        except requests.exceptions.Timeout:
            log.error("        ❌ Timeout error fetching OGC API page: %s", url)
            return None, None 
        except requests.exceptions.HTTPError as e:
            log.error("        ❌ HTTP error %s fetching OGC API page: %s", e.response.status_code, url)
            return None, None 
        except requests.exceptions.RequestException as e:
            log.error("        ❌ Network error fetching OGC API page %s: %s", url, e)
            return None, None 
        except json.JSONDecodeError as e:
            log.error("        ❌ Invalid JSON response from OGC API page %s: %s", url, e)
            if response and hasattr(response, 'text'):
                 log.debug("        Raw response snippet: %s", response.text[:500])
            return None, None
        except Exception as e_unexpected: 
            log.error("        ❌ Unexpected error fetching OGC API page %s: %s", url, e_unexpected, exc_info=True)
            return None, None

    def _find_next_link(self, links: List[Dict[str, Any]]) -> Optional[str]:
        """🔍 Find the next page link from response links."""
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