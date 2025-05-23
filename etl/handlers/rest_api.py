# ETL-pipeline/etl/handlers/rest_api.py
from __future__ import annotations

import logging
import requests # Available in ArcGIS Pro Python
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List # Added List
import re
import time

from ..models import Source
from ..utils import paths, ensure_dirs
from ..utils.naming import sanitize_for_filename

# Default BBOX from your document (SWEREF99 TM) [cite: 4]
# Values: xmin,ymin,xmax,ymax [cite: 25]
DEFAULT_BBOX_COORDS = "586206,6551160,647910,6610992"
DEFAULT_BBOX_SR = "3006"

class RestApiDownloadHandler:
    """
    Handles downloading data from ESRI REST API MapServer and FeatureServer Query endpoints.
    """

    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None):
        self.src: Source = src
        self.global_config: Dict[str, Any] = global_config or {}
        ensure_dirs()
        logging.info("🚀 Initializing RestApiDownloadHandler for source: %s", self.src.name)

    def _get_service_metadata(self, service_url: str) -> Optional[Dict[str, Any]]:
        """Fetches base metadata for the service (MapServer/FeatureServer) with retries."""
        params = {"f": "json"}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        }
        try:
            total_attempts = int(self.global_config.get("max_retries", 1))
            if total_attempts < 1:
                total_attempts = 1
        except ValueError:
            total_attempts = 1
            logging.warning("    Invalid value for 'max_retries' in global_config. Defaulting to 1 attempt for metadata fetch.")

        for attempt in range(total_attempts):
            try:
                logging.debug("    Attempt %d/%d to fetch service metadata from %s", attempt + 1, total_attempts, service_url)
                response = requests.get(service_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                logging.error(
                    f"❌ HTTP error fetching service metadata from {service_url} "
                    f"(Attempt {attempt + 1}/{total_attempts}): {e}"
                )
                if 400 <= e.response.status_code < 500:
                    logging.warning(f"    Client error {e.response.status_code}, not retrying metadata fetch for {service_url}.")
                    return None
                if attempt + 1 == total_attempts:
                    logging.error(f"    Final attempt failed for {service_url} with HTTP error.")
                    return None
                sleep_time = 5 * (attempt + 1)
                logging.info(f"    Server error. Retrying metadata fetch for {service_url} in {sleep_time}s...")
                time.sleep(sleep_time)
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"❌ Request exception fetching service metadata from {service_url} "
                    f"(Attempt {attempt + 1}/{total_attempts}): {e}"
                )
                if attempt + 1 == total_attempts:
                    logging.error(f"    Final attempt failed for {service_url} with RequestException.")
                    return None
                sleep_time = 5 * (attempt + 1)
                logging.info(f"    Request exception. Retrying metadata fetch for {service_url} in {sleep_time}s...")
                time.sleep(sleep_time)
            except Exception as e:
                logging.error(
                    f"❌ Unexpected error during metadata fetch for {service_url} "
                    f"(Attempt {attempt + 1}/{total_attempts}): {e}", exc_info=True
                )
                return None
        logging.error(f"    All {total_attempts} attempts to fetch metadata from {service_url} failed.")
        return None

    def _get_layer_metadata(self, layer_url: str) -> Optional[Dict[str, Any]]:
        """Fetches metadata for a specific layer."""
        try:
            params = {"f": "json"}
            response = requests.get(layer_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error("❌ Failed to fetch layer metadata from %s: %s", layer_url, e)
            return None

    def fetch(self) -> None:
        if not self.src.enabled:
            logging.info("⏭️ Source '%s' (REST API) is disabled, skipping fetch.", self.src.name)
            return

        logging.info("🌐 Processing REST API source: '%s' from URL: %s", self.src.name, self.src.url)

        service_meta = self._get_service_metadata(self.src.url)
        if not service_meta:
            logging.error("❌ Could not retrieve service metadata for %s. Skipping source.", self.src.name)
            return

        layers_to_iterate_final: List[Dict[str, Any]] = []
        configured_layer_ids_from_yaml = self.src.raw.get("layer_ids")
       
        logging.info(
            f"    DEBUG FOR SOURCE: '{self.src.name}' - self.src.raw contents: {self.src.raw}"
        )
        logging.info(
            f"    DEBUG FOR SOURCE: '{self.src.name}' - configured_layer_ids_from_yaml value: {configured_layer_ids_from_yaml} (type: {type(configured_layer_ids_from_yaml)})"
        )
        # --- END DEBUG LOGGING ---

        if configured_layer_ids_from_yaml:
            logging.info("    Found explicit layer_ids in config: %s for source '%s'. Processing only these.", configured_layer_ids_from_yaml, self.src.name)
            # ... rest of the if block
        elif "layers" in service_meta:
            logging.info("    No explicit layer_ids in config for source '%s'. Discovering all layers from service metadata.", self.src.name)
            # ... rest of the elif block
        # Create a lookup for all layer details from the service metadata
        metadata_layers_details = {str(lyr.get("id")): lyr
                                   for lyr in service_meta.get("layers", []) if "id" in lyr}

        if configured_layer_ids_from_yaml:
            logging.info("    Found explicit layer_ids in config: %s for source '%s'. Processing only these.", configured_layer_ids_from_yaml, self.src.name)
            if not isinstance(configured_layer_ids_from_yaml, list):
                configured_layer_ids_from_yaml = [configured_layer_ids_from_yaml] # Ensure it's a list

            for lid_val in configured_layer_ids_from_yaml:
                lid_str = str(lid_val)
                layer_detail = metadata_layers_details.get(lid_str) # Get full metadata for this layer ID
                
                if layer_detail:
                    layer_name = layer_detail.get("name", f"layer_{lid_str}") # Use actual name if available
                    layers_to_iterate_final.append({"id": lid_str, "name": layer_name, "metadata": layer_detail})
                else:
                    # Layer ID specified in config but not found in the service's layer list from metadata.
                    # Still add it for an attempt, as some services might respond to direct ID queries
                    # even if not listed in the main /layers endpoint (less common for MapServers).
                    logging.warning(
                        f"    Layer ID '{lid_str}' specified in config for source '{self.src.name}' "
                        f"was not found in the service's layer metadata list. "
                        f"Will attempt to query it using this ID and a placeholder name."
                    )
                    layers_to_iterate_final.append({"id": lid_str, "name": f"layer_{lid_str}_cfg_only", "metadata": None})
        
        elif "layers" in service_meta: # No layer_ids in config, so get all layers from service_meta
            logging.info("    No explicit layer_ids in config for source '%s'. Discovering all layers from service metadata.", self.src.name)
            for layer_id_str, layer_detail_from_meta in metadata_layers_details.items():
                layers_to_iterate_final.append({
                    "id": layer_id_str,
                    "name": layer_detail_from_meta.get("name", f"layer_{layer_id_str}"),
                    "metadata": layer_detail_from_meta # Pass the full layer metadata
                })
        
        # Fallback for single-layer FeatureServer (if layers_to_iterate_final is still empty)
        elif not layers_to_iterate_final and "/featureserver" in self.src.url.lower() and service_meta.get("type") == "Feature Layer":
            logging.info("    Source '%s' appears to be a single-layer FeatureServer and no layers were previously identified. Adding layer from service root or URL.", self.src.name)
            layer_id_from_url_match = re.search(r'/(\d+)/?$', self.src.url)
            # Use ID from URL if present, else service's root ID, else default to "0"
            fs_layer_id = layer_id_from_url_match.group(1) if layer_id_from_url_match else service_meta.get("id", "0")
            fs_layer_id_str = str(fs_layer_id)
            fs_layer_name = service_meta.get("name", f"feature_layer_{fs_layer_id_str}")
            # For a single FeatureServer layer, the service_meta itself is the layer's metadata
            layers_to_iterate_final.append({"id": fs_layer_id_str, "name": fs_layer_name, "metadata": service_meta})


        if not layers_to_iterate_final:
            logging.warning("⚠️ No layers identified or specified to query for source '%s'. Check service metadata and `layer_ids` config.", self.src.name)
            return

        log_layer_ids_to_query = [layer['id'] for layer in layers_to_iterate_final]
        logging.info("    Source '%s': Will attempt to query %d layer(s): %s",
                     self.src.name, len(layers_to_iterate_final), log_layer_ids_to_query)

        for layer_info_to_query in layers_to_iterate_final:
            # Pass the layer's own metadata (if found from the service root) to _fetch_layer_data
            self._fetch_layer_data(
                layer_info=layer_info_to_query,
                layer_metadata_from_service=layer_info_to_query.get("metadata")
            )

    def _fetch_layer_data(self, layer_info: Dict[str, Any], layer_metadata_from_service: Optional[Dict[str, Any]] = None) -> None:
        """Fetches data for a single layer."""
        layer_id = layer_info.get("id")
        layer_name_original = layer_info.get("name", f"layer_{layer_id}")
        layer_name_sanitized = sanitize_for_filename(layer_name_original)

        query_url = f"{self.src.url.rstrip('/')}/{layer_id}/query"
        logging.info("    Querying Layer ID: %s (Sanitized Name: %s, Original: %s) from %s",
                     layer_id, layer_name_sanitized, layer_name_original, query_url)

        # Initialize layer_meta_to_use with what's passed from the service root metadata (could be None)
        # This will be used for maxRecordCount and potentially for CRS info later.
        layer_meta_to_use: Optional[Dict[str, Any]] = layer_metadata_from_service

        # --- Determine maxRecordCount ---
        max_record_count_from_config = self.src.raw.get("max_record_count")

        if max_record_count_from_config is not None:
            try:
                max_record_count = int(max_record_count_from_config)
                logging.info("        Using max_record_count from source.raw config: %d", max_record_count)
            except ValueError:
                logging.warning("        Invalid 'max_record_count' in source.raw: '%s'. Falling back to metadata or default.", max_record_count_from_config)
                max_record_count = None # Force fallback to metadata
        else:
            max_record_count = None # Not in source.raw, will use metadata or default

        if max_record_count is None: # Not set from config or invalid value in config
            if not layer_meta_to_use: # If not already available from service root metadata
                layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
                logging.debug("        Fetching specific layer metadata for layer ID %s to determine maxRecordCount.", layer_id)
                layer_meta_to_use = self._get_layer_metadata(layer_metadata_url)

            if layer_meta_to_use:
                if layer_meta_to_use.get("maxRecordCount") is not None:
                    max_record_count = layer_meta_to_use["maxRecordCount"]
                    logging.info("        Service layer metadata indicates maxRecordCount: %d", max_record_count)
                elif layer_meta_to_use.get("standardMaxRecordCount") is not None:
                    max_record_count = layer_meta_to_use["standardMaxRecordCount"]
                    logging.info("        Service layer metadata indicates standardMaxRecordCount: %d", max_record_count)
                else:
                    max_record_count = 2000 # Default if not found in specific layer metadata
                    logging.info("        maxRecordCount not found in specific layer metadata, using default: %d", max_record_count)
            else:
                max_record_count = 2000 # Default if specific layer metadata fetch also fails
                logging.warning("        Could not fetch specific layer metadata for maxRecordCount, using default: %d", max_record_count)
        
        # Ensure max_record_count is an integer after all determinations
        if not isinstance(max_record_count, int):
            logging.warning("        max_record_count ended up non-integer: '%s'. Defaulting to 2000.", max_record_count)
            max_record_count = 2000


        # --- Setup Query Parameters ---
        use_bbox = self.global_config.get("use_bbox_filter", False)
        bbox_coords = self.src.raw.get("bbox", DEFAULT_BBOX_COORDS)
        bbox_sr = self.src.raw.get("bbox_sr", DEFAULT_BBOX_SR)

        params: Dict[str, Any] = {
            "where": self.src.raw.get("where_clause", "1=1"),
            "outFields": self.src.raw.get("out_fields", "*"),
            "returnGeometry": "true",
            "f": self.src.raw.get("format", "geojson"),
        }

        if use_bbox and bbox_coords:
            params["geometry"] = bbox_coords
            params["geometryType"] = "esriGeometryEnvelope"
            params["inSR"] = bbox_sr
            params["spatialRel"] = "esriSpatialRelIntersects"
            logging.info("        Applying BBOX: %s (SRID: %s)", bbox_coords, bbox_sr)

        # --- Staging Path ---
        source_name_sanitized = sanitize_for_filename(self.src.name)
        staging_dir = paths.STAGING / self.src.authority / source_name_sanitized
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        output_filename = f"{layer_name_sanitized}.{params['f']}"
        output_path = staging_dir / output_filename
        
        # --- Pagination and Data Fetching ---
        current_offset = 0
        features_written_total = 0
        all_features = []
        page_num = 1

        while True:
            response_obj: Optional[requests.Response] = None
            
            effective_page_limit = max_record_count
            if max_record_count == 0: # Server claims no limit
                effective_page_limit = 2000 # Use a sensible page size anyway for stability
                # No need to log this every time if it's already logged when max_record_count was determined
            
            logging.info("        Fetching page %d for layer %s (offset %d, limit %d)", 
                         page_num, layer_name_sanitized, current_offset, effective_page_limit)
            
            page_params = params.copy()
            page_params["resultOffset"] = current_offset
            page_params["resultRecordCount"] = effective_page_limit
            
            try:
                response_obj = requests.get(query_url, params=page_params, timeout=120)
                response_obj.raise_for_status()
                data = response_obj.json()

                if "error" in data:
                    logging.error("        ❌ API_ERROR_REPORTED: Error from REST API for layer %s: %s", layer_name_sanitized, data["error"])
                    logging.error("        ❌ API_ERROR_REPORTED: Breaking from pagination loop for this layer.")
                    break 

                features = data.get("features", [])
                if not features:
                    if page_num == 1:
                        logging.info("        ℹ️ No features returned for layer %s with current parameters.", layer_name_sanitized)
                    else:
                        logging.info("        🏁 All features retrieved for layer %s (empty page).", layer_name_sanitized)
                    break

                all_features.extend(features)
                features_written_total += len(features)
                
                exceeded_transfer_limit = data.get("exceededTransferLimit", False)
                
                if exceeded_transfer_limit:
                    logging.info("        ⚠️ Exceeded transfer limit for layer %s, fetching next page.", layer_name_sanitized)
                    current_offset += len(features) 
                    page_num +=1
                elif (len(features) < effective_page_limit and effective_page_limit > 0) or max_record_count == 0 :
                    logging.info("        🏁 All features likely retrieved for layer %s (less than page limit or server maxRecordCount is 0).", layer_name_sanitized)
                    break
                else: 
                     current_offset += len(features)
                     page_num +=1

            except requests.exceptions.RequestException as e:
                logging.error("        ❌ PAGINATION_LOOP_REQUEST_ERROR: Failed to download data for layer %s, page %d: %s", layer_name_sanitized, page_num, e)
                logging.error("        ❌ PAGINATION_LOOP_REQUEST_ERROR: Breaking from pagination loop for this layer.")
                break 
            except json.JSONDecodeError as e:
                logging.error("        ❌ PAGINATION_LOOP_JSON_ERROR: Failed to decode JSON for layer %s, page %d: %s", layer_name_sanitized, page_num, e)
                if response_obj:
                    logging.debug("        Raw response text for JSON error: %s", response_obj.text[:500])
                logging.error("        ❌ PAGINATION_LOOP_JSON_ERROR: Breaking from pagination loop for this layer.")
                break
            except Exception as e_unexpected: 
                logging.error("        ❌ PAGINATION_LOOP_UNEXPECTED_ERROR: Unexpected error for layer %s, page %d: %s", layer_name_sanitized, page_num, e_unexpected, exc_info=True)
                logging.error("        ❌ PAGINATION_LOOP_UNEXPECTED_ERROR: Breaking from pagination loop for this layer.")
                break
        # End of while loop

        if all_features:
            final_output_data = {
                "type": "FeatureCollection",
                "features": all_features
            }
            
            # Add CRS information if output is GeoJSON
            if params['f'] == 'geojson':
                # Ensure layer_meta_to_use is populated if it wasn't for maxRecordCount
                # (e.g., if max_record_count came from source.raw config)
                if not layer_meta_to_use: 
                    layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
                    logging.debug("        Fetching specific layer metadata for layer ID %s (for CRS info).", layer_id)
                    layer_meta_to_use = self._get_layer_metadata(layer_metadata_url)

                if layer_meta_to_use and layer_meta_to_use.get("spatialReference"):
                    sr_info = layer_meta_to_use.get("spatialReference")
                    if sr_info and sr_info.get("wkid") == 3006: # Assuming SWEREF99 TM
                         final_output_data["crs"] = {
                            "type": "name",
                            "properties": {"name": "urn:ogc:def:crs:EPSG::3006"}
                        }
                    # Add other common WKIDs if needed, e.g., for WGS84
                    # elif sr_info and sr_info.get("wkid") == 4326:
                    #      final_output_data["crs"] = {
                    #         "type": "name",
                    #         "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}
                    #     }
            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(final_output_data, f, ensure_ascii=False, indent=2)
                logging.info("        💾 Successfully saved %d features for layer %s to %s",
                             features_written_total, layer_name_sanitized, output_path)
                if not self.src.staged_data_type : # If not already set
                     self.src.staged_data_type = params['f']
            except IOError as e:
                logging.error("        ❌ Failed to write data for layer %s to %s: %s", layer_name_sanitized, output_path, e)
        elif page_num == 1 and not all_features : # No features found on the first attempt
             logging.info("        ℹ️ No features found or written for layer %s for source '%s'.", layer_name_sanitized, self.src.name)
