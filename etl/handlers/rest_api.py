from __future__ import annotations

import logging
import requests # Available in ArcGIS Pro Python
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import re

from ..models import Source
from ..utils import paths, ensure_dirs
from ..utils.naming import sanitize_for_filename

# Default BBOX from your document (SWEREF99 TM) [cite: 4]
# Values: xmin,ymin,xmax,ymax [cite: 25]
DEFAULT_BBOX_COORDS = "586206.428348,6551159.789694,647910.442029,6610991.889353"
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
        """Fetches base metadata for the service (MapServer/FeatureServer)."""
        try:
            params = {"f": "json"}
            response = requests.get(service_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error("❌ Failed to fetch service metadata from %s: %s", service_url, e)
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
        """
        Fetches data for the configured REST API source.
        """
        if not self.src.enabled:
            logging.info("⏭️ Source '%s' (REST API) is disabled, skipping fetch.", self.src.name)
            return

        logging.info("🌐 Processing REST API source: '%s' from URL: %s", self.src.name, self.src.url)

        # Determine if it's a MapServer or FeatureServer (convention based on URL)
        # This could be made more robust or configurable in sources.yaml if needed
        is_feature_server = "/featureserver" in self.src.url.lower()
        
        # Get service metadata to find layers if not specified
        service_meta = self._get_service_metadata(self.src.url)
        if not service_meta:
            logging.error("❌ Could not retrieve service metadata for %s. Skipping.", self.src.name)
            return

        # Identify layers to query
        layers_to_query = []
        if self.src.raw.get("layer_ids"): # Explicit layer IDs in sources.yaml
            layer_ids = self.src.raw.get("layer_ids")
            if not isinstance(layer_ids, list):
                layer_ids = [layer_ids]
            for layer_id in layer_ids:
                layers_to_query.append({"id": str(layer_id), "name": f"layer_{layer_id}"})
        elif "layers" in service_meta: # Get layers from service metadata
            for layer_info in service_meta.get("layers", []):
                layers_to_query.append({"id": str(layer_info.get("id")), "name": layer_info.get("name", f"layer_{layer_info.get('id')}")})
        else: # Fallback for FeatureServer if layers aren't directly in root, check common single layer scenario
             if is_feature_server and service_meta.get("type") == "Feature Layer": # It's a single layer FeatureServer URL
                 # Layer ID is typically part of the URL for single Feature Layer services, or defaults to 0 if querying from base FS URL
                 # For simplicity, we'll assume if it's a Feature Layer type at root, layer ID 0 is implied for query
                 # A more robust approach might check for a `layers` collection even on FS.
                 # The example "Biotopskydd" is a MapServer, so this path is less critical for it.
                 # Defaulting to layer '0' if it's a FeatureServer URL ending without a layer ID.
                 layer_id_from_url_match = re.search(r'/(\d+)/?$', self.src.url)
                 layer_id = layer_id_from_url_match.group(1) if layer_id_from_url_match else "0"
                 layer_name = service_meta.get("name", f"feature_layer_{layer_id}")
                 layers_to_query.append({"id": layer_id, "name": layer_name})


        if not layers_to_query:
            logging.warning("⚠️ No layers found or specified to query for source '%s'.", self.src.name)
            return
            
        logging.info("Found %d layer(s) to query for source '%s': %s", len(layers_to_query), self.src.name, layers_to_query)

        for layer_info in layers_to_query:
            self._fetch_layer_data(layer_info)

    def _fetch_layer_data(self, layer_info: Dict[str, Any]) -> None:
        """Fetches data for a single layer."""
        layer_id = layer_info.get("id")
        layer_name = sanitize_for_filename(layer_info.get("name", f"layer_{layer_id}"))

        # Construct the query URL for the layer [cite: 21, 24]
        # Ensure no double slashes if src.url already ends with /
        query_url = f"{self.src.url.rstrip('/')}/{layer_id}/query"
        logging.info("    Querying Layer ID: %s (%s) from %s", layer_id, layer_name, query_url)

        # --- Parameters ---
        # BBOX handling (example from your document)
        use_bbox = self.global_config.get("use_bbox_filter", False) # From global config.yaml
        bbox_coords = self.src.raw.get("bbox", DEFAULT_BBOX_COORDS)
        bbox_sr = self.src.raw.get("bbox_sr", DEFAULT_BBOX_SR)

        params: Dict[str, Any] = {
            "where": self.src.raw.get("where_clause", "1=1"), # [cite: 33]
            "outFields": self.src.raw.get("out_fields", "*"), # [cite: 29]
            "returnGeometry": "true", # [cite: 30]
            "f": self.src.raw.get("format", "geojson"), # Prefer GeoJSON for easier handling
            # Add outSR if you want to transform, e.g. to WGS84 (4326)
            # "outSR": self.src.raw.get("out_sr", ""),
        }

        if use_bbox and bbox_coords:
            params["geometry"] = bbox_coords
            params["geometryType"] = "esriGeometryEnvelope" # [cite: 26]
            params["inSR"] = bbox_sr # [cite: 27]
            params["spatialRel"] = "esriSpatialRelIntersects" # [cite: 28]
            logging.info("        Applying BBOX: %s (SRID: %s)", bbox_coords, bbox_sr)

        # Staging path
        # Example: data/staging/SKS/biotopskydd_beslutade_av_skogsstyrelsen/biotopskydd_skogsstyrelsen_layer_0.geojson
        source_name_sanitized = sanitize_for_filename(self.src.name)
        staging_dir = paths.STAGING / self.src.authority / source_name_sanitized
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Use the layer name (or a sanitized version of it) for the output file
        output_filename = f"{layer_name}.{params['f']}"
        output_path = staging_dir / output_filename
        
        current_offset = 0
        features_written_total = 0
        max_record_count = self.src.raw.get("max_record_count", 2000) # Default or from source config [cite: 10, 46]
        
        # Get layer metadata to confirm actual maxRecordCount
        layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
        layer_meta = self._get_layer_metadata(layer_metadata_url)
        if layer_meta and layer_meta.get("maxRecordCount"):
            max_record_count = layer_meta["maxRecordCount"]
            logging.info("        Service layer metadata indicates maxRecordCount: %d", max_record_count)
        elif layer_meta and layer_meta.get("standardMaxRecordCount"): # Some services use this
            max_record_count = layer_meta["standardMaxRecordCount"]
            logging.info("        Service layer metadata indicates standardMaxRecordCount: %d", max_record_count)

        all_features = []
        
        page_num = 1
        while True:
            response: Optional[requests.Response] = None # Initialize for current iteration
            logging.info("        Fetching page %d for layer %s (offset %d, limit %d)", 
                         page_num, layer_name, current_offset, max_record_count)
            
            page_params = params.copy()
            if max_record_count > 0 : # Only add pagination params if server supports it (typically > 0)
                page_params["resultOffset"] = current_offset
                page_params["resultRecordCount"] = max_record_count
            
            try:
                response = requests.get(query_url, params=page_params, timeout=120) # Increased timeout
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    logging.error("        ❌ Error from REST API for layer %s: %s", layer_name, data["error"])
                    break 

                features = data.get("features", [])
                if not features and page_num == 1:
                    logging.info("        ℹ️ No features returned for layer %s with current parameters.", layer_name)
                    break
                if not features: # No more features on subsequent pages
                    logging.info("        🏁 All features retrieved for layer %s.", layer_name)
                    break

                all_features.extend(features)
                features_written_total += len(features)
                
                # Check for exceededTransferLimit or if count is less than maxRecordCount
                # Some servers might not explicitly set exceededTransferLimit but just return fewer records
                exceeded_transfer_limit = data.get("exceededTransferLimit", False)
                
                if exceeded_transfer_limit:
                    logging.info("        ⚠️ Exceeded transfer limit for layer %s, fetching next page.", layer_name)
                    current_offset += len(features) # Or current_offset += max_record_count
                    page_num +=1
                elif len(features) < max_record_count and max_record_count > 0: # Got less than requested, assume end of features
                    logging.info("        🏁 Received fewer features than maxRecordCount, assuming all features retrieved for layer %s.", layer_name)
                    break
                elif max_record_count == 0: # Server does not support pagination or limit
                    logging.info("        🏁 MaxRecordCount is 0, assuming all features retrieved in one go for layer %s.", layer_name)
                    break
                else: # Got a full page, continue
                     current_offset += len(features)
                     page_num +=1


            except requests.exceptions.RequestException as e:
                logging.error("        ❌ Failed to download data for layer %s, page %d: %s", layer_name, page_num, e)
                # Consider retry logic here based on global_config.max_retries
            except json.JSONDecodeError as e:
                logging.error("        ❌ Failed to decode JSON response for layer %s, page %d: %s", layer_name, page_num, e)
                if response:
                    logging.debug("        Raw response text: %s", response.text[:500]) # Log snippet of raw response
                break
                break


        if all_features:
            # Reconstruct the full feature collection (especially if format is geojson)
            final_output_data = {
                "type": "FeatureCollection",
                "features": all_features
            }
            # Add CRS if format is GeoJSON and we know it (e.g. from first page or layer metadata)
            # For SWEREF99 TM (EPSG:3006)
            if params['f'] == 'geojson' and layer_meta and layer_meta.get("spatialReference"):
                 # Use the layer's own spatialReference if outSR is not used,
                 # otherwise, this should reflect outSR.
                sr_info = layer_meta.get("spatialReference")
                # Basic GeoJSON CRS (ESRI's JSON is more complex)
                # This part might need refinement based on actual service output for GeoJSON SR
                if sr_info and sr_info.get("wkid") == 3006:
                     final_output_data["crs"] = {
                        "type": "name",
                        "properties": {"name": "urn:ogc:def:crs:EPSG::3006"}
                    }
                # If an outSR was used, reflect that instead.
                # Example for WGS84:
                # elif self.src.raw.get("out_sr") == "4326":
                #    final_output_data["crs"] = {
                #        "type": "name",
                #        "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}
                #    }


            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(final_output_data, f, ensure_ascii=False, indent=2)
                logging.info("        💾 Successfully saved %d features for layer %s to %s",
                             features_written_total, layer_name, output_path)
                # Update source model if needed (e.g. for loader to know it's geojson)
                if not self.src.staged_data_type :
                     self.src.staged_data_type = params['f'] # e.g. "geojson"

            except IOError as e:
                logging.error("        ❌ Failed to write data for layer %s to %s: %s", layer_name, output_path, e)
        elif page_num == 1 and not all_features : # No features found on first attempt and loop broke
             logging.info("        ℹ️ No features found or written for layer %s for source '%s'.", layer_name, self.src.name)