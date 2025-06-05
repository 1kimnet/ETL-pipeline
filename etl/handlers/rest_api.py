# etl/handlers/rest_api.py
from __future__ import annotations

import logging
import requests
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
import re
import time

from ..models import Source
from ..utils import paths, ensure_dirs
from ..utils.naming import sanitize_for_filename

log = logging.getLogger(__name__)

# Default BBOX from your document (SWEREF99 TM)
DEFAULT_BBOX_COORDS = "586206,6551160,647910,6610992"
DEFAULT_BBOX_SR = "3006"


class RestApiDownloadHandler:
    """Handles downloading data from ESRI REST API MapServer and FeatureServer Query endpoints."""

    def __init__(self, src: Source, global_config: Optional[Dict[str, Any]] = None):
        self.src = src
        self.global_config = global_config or {}
        ensure_dirs()
        log.info("🚀 Initializing RestApiDownloadHandler for source: %s", self.src.name)

    def _get_service_metadata(self, service_url: str) -> Optional[Dict[str, Any]]:
        """Fetches base metadata for the service (MapServer/FeatureServer) with retries."""
        params = {"f": "json"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }

        try:
            total_attempts = int(self.global_config.get("max_retries", 1))
            if total_attempts < 1:
                total_attempts = 1
        except ValueError:
            total_attempts = 1
            log.warning(
                "Invalid value for 'max_retries' in global_config. Defaulting to 1 attempt for metadata fetch."
            )

        for attempt in range(total_attempts):
            try:
                log.debug(
                    "Attempt %d/%d to fetch service metadata from %s",
                    attempt + 1,
                    total_attempts,
                    service_url,
                )
                response = requests.get(
                    service_url, params=params, headers=headers, timeout=30
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                log.error(
                    "❌ HTTP error fetching service metadata from %s (Attempt %d/%d): %s",
                    service_url,
                    attempt + 1,
                    total_attempts,
                    e,
                )
                if 400 <= e.response.status_code < 500:
                    log.warning(
                        "Client error %d, not retrying metadata fetch for %s.",
                        e.response.status_code,
                        service_url,
                    )
                    return None
                if attempt + 1 == total_attempts:
                    log.error(
                        "Final attempt failed for %s with HTTP error.", service_url
                    )
                    return None
                sleep_time = 5 * (attempt + 1)
                log.info(
                    "Server error. Retrying metadata fetch for %s in %ds...",
                    service_url,
                    sleep_time,
                )
                time.sleep(sleep_time)
            except requests.exceptions.RequestException as e:
                log.error(
                    "❌ Request exception fetching service metadata from %s (Attempt %d/%d): %s",
                    service_url,
                    attempt + 1,
                    total_attempts,
                    e,
                )
                if attempt + 1 == total_attempts:
                    log.error(
                        "Final attempt failed for %s with RequestException.",
                        service_url,
                    )
                    return None
                sleep_time = 5 * (attempt + 1)
                log.info(
                    "Request exception. Retrying metadata fetch for %s in %ds...",
                    service_url,
                    sleep_time,
                )
                time.sleep(sleep_time)
            except Exception as e:
                log.error(
                    "❌ Unexpected error during metadata fetch for %s (Attempt %d/%d): %s",
                    service_url,
                    attempt + 1,
                    total_attempts,
                    e,
                    exc_info=True,
                )
                return None

        log.error(
            "All %d attempts to fetch metadata from %s failed.",
            total_attempts,
            service_url,
        )
        return None

    def _get_layer_metadata(self, layer_url: str) -> Optional[Dict[str, Any]]:
        """Fetches metadata for a specific layer."""
        try:
            params = {"f": "json"}
            response = requests.get(layer_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            log.error("❌ Failed to fetch layer metadata from %s: %s", layer_url, e)
            return None

    def _prepare_query_params(self) -> Dict[str, Any]:
        """Prepare base query parameters including optional BBOX."""
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
            log.info("Applying BBOX: %s (SRID: %s)", bbox_coords, bbox_sr)

        return params

    def _request_page(
        self,
        query_url: str,
        params: Dict[str, Any],
        page_num: int,
        layer_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Execute the HTTP request for a single page."""
        response_obj: Optional[requests.Response] = None
        try:
            response_obj = requests.get(query_url, params=params, timeout=120)
            response_obj.raise_for_status()
            return response_obj.json()
        except requests.exceptions.RequestException as e:
            log.error(
                "❌ PAGINATION_LOOP_REQUEST_ERROR: Failed to download data for layer %s, page %d: %s",
                layer_name,
                page_num,
                e,
            )
            log.error(
                "❌ PAGINATION_LOOP_REQUEST_ERROR: Breaking from pagination loop for this layer."
            )
            return None
        except json.JSONDecodeError as e:
            log.error(
                "❌ PAGINATION_LOOP_JSON_ERROR: Failed to decode JSON for layer %s, page %d: %s",
                layer_name,
                page_num,
                e,
            )
            if response_obj:
                log.debug(
                    "Raw response text for JSON error: %s", response_obj.text[:500]
                )
            log.error(
                "❌ PAGINATION_LOOP_JSON_ERROR: Breaking from pagination loop for this layer."
            )
            return None
        except Exception as e_unexpected:  # pragma: no cover - safety net
            log.error(
                "❌ PAGINATION_LOOP_UNEXPECTED_ERROR: Unexpected error for layer %s, page %d: %s",
                layer_name,
                page_num,
                e_unexpected,
                exc_info=True,
            )
            log.error(
                "❌ PAGINATION_LOOP_UNEXPECTED_ERROR: Breaking from pagination loop for this layer."
            )
            return None

    @staticmethod
    def _append_features(
        accumulator: List[Dict[str, Any]], new_features: List[Dict[str, Any]]
    ) -> int:
        """Extend feature list and return count of new features."""
        accumulator.extend(new_features)
        return len(new_features)

    def fetch(self) -> None:
        """Main fetch method for REST API sources."""
        if not self.src.enabled:
            log.info(
                "⏭️ Source '%s' (REST API) is disabled, skipping fetch.", self.src.name
            )
            return

        log.info(
            "🌐 Processing REST API source: '%s' from URL: %s",
            self.src.name,
            self.src.url,
        )

        service_meta = self._get_service_metadata(self.src.url)
        if not service_meta:
            log.error(
                "❌ Could not retrieve service metadata for %s. Skipping source.",
                self.src.name,
            )
            return

        layers_to_iterate_final: List[Dict[str, Any]] = []
        configured_layer_ids_from_yaml = self.src.raw.get("layer_ids")

        # Create a lookup for all layer details from the service metadata
        metadata_layers_details = {
            str(lyr.get("id")): lyr
            for lyr in service_meta.get("layers", [])
            if "id" in lyr
        }

        if configured_layer_ids_from_yaml:
            log.info(
                "Found explicit layer_ids in config: %s for source '%s'. Processing only these.",
                configured_layer_ids_from_yaml,
                self.src.name,
            )
            if not isinstance(configured_layer_ids_from_yaml, list):
                configured_layer_ids_from_yaml = [configured_layer_ids_from_yaml]

            for lid_val in configured_layer_ids_from_yaml:
                lid_str = str(lid_val)
                layer_detail = metadata_layers_details.get(lid_str)

                if layer_detail:
                    layer_name = layer_detail.get("name", f"layer_{lid_str}")
                    layers_to_iterate_final.append(
                        {"id": lid_str, "name": layer_name, "metadata": layer_detail}
                    )
                else:
                    log.warning(
                        "Layer ID '%s' specified in config for source '%s' "
                        "was not found in the service's layer metadata list. "
                        "Will attempt to query it using this ID and a placeholder name.",
                        lid_str,
                        self.src.name,
                    )
                    layers_to_iterate_final.append(
                        {
                            "id": lid_str,
                            "name": f"layer_{lid_str}_cfg_only",
                            "metadata": None,
                        }
                    )

        elif "layers" in service_meta:
            log.info(
                "No explicit layer_ids in config for source '%s'. Discovering all layers from service metadata.",
                self.src.name,
            )
            for layer_id_str, layer_detail_from_meta in metadata_layers_details.items():
                layers_to_iterate_final.append(
                    {
                        "id": layer_id_str,
                        "name": layer_detail_from_meta.get(
                            "name", f"layer_{layer_id_str}"
                        ),
                        "metadata": layer_detail_from_meta,
                    }
                )

        # Fallback for single-layer FeatureServer
        elif (
            not layers_to_iterate_final
            and "/featureserver" in self.src.url.lower()
            and service_meta.get("type") == "Feature Layer"
        ):
            log.info(
                "Source '%s' appears to be a single-layer FeatureServer and no layers were previously identified. "
                "Adding layer from service root or URL.",
                self.src.name,
            )
            layer_id_from_url_match = re.search(r"/(\d+)/?$", self.src.url)
            fs_layer_id = (
                layer_id_from_url_match.group(1)
                if layer_id_from_url_match
                else service_meta.get("id", "0")
            )
            fs_layer_id_str = str(fs_layer_id)
            fs_layer_name = service_meta.get("name", f"feature_layer_{fs_layer_id_str}")
            layers_to_iterate_final.append(
                {"id": fs_layer_id_str, "name": fs_layer_name, "metadata": service_meta}
            )

        if not layers_to_iterate_final:
            log.warning(
                "⚠️ No layers identified or specified to query for source '%s'. "
                "Check service metadata and `layer_ids` config.",
                self.src.name,
            )
            return

        log_layer_ids_to_query = [layer["id"] for layer in layers_to_iterate_final]
        log.info(
            "Source '%s': Will attempt to query %d layer(s): %s",
            self.src.name,
            len(layers_to_iterate_final),
            log_layer_ids_to_query,
        )

        for layer_info_to_query in layers_to_iterate_final:
            self._fetch_layer_data(
                layer_info=layer_info_to_query,
                layer_metadata_from_service=layer_info_to_query.get("metadata"),
            )

    def _fetch_layer_data(
        self,
        layer_info: Dict[str, Any],
        layer_metadata_from_service: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Fetches data for a single layer."""
        layer_id = layer_info.get("id")
        layer_name_original = layer_info.get("name", f"layer_{layer_id}")
        layer_name_sanitized = sanitize_for_filename(layer_name_original)

        query_url = f"{self.src.url.rstrip('/')}/{layer_id}/query"
        log.info(
            "Querying Layer ID: %s (Sanitized Name: %s, Original: %s) from %s",
            layer_id,
            layer_name_sanitized,
            layer_name_original,
            query_url,
        )

        # Determine maxRecordCount
        max_record_count_from_config = self.src.raw.get("max_record_count")
        layer_meta_to_use = layer_metadata_from_service

        if max_record_count_from_config is not None:
            try:
                max_record_count = int(max_record_count_from_config)
                log.info(
                    "Using max_record_count from source.raw config: %d",
                    max_record_count,
                )
            except ValueError:
                log.warning(
                    "Invalid 'max_record_count' in source.raw: '%s'. Falling back to metadata or default.",
                    max_record_count_from_config,
                )
                max_record_count = None
        else:
            max_record_count = None

        if max_record_count is None:
            if not layer_meta_to_use:
                layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
                log.debug(
                    "Fetching specific layer metadata for layer ID %s to determine maxRecordCount.",
                    layer_id,
                )
                layer_meta_to_use = self._get_layer_metadata(layer_metadata_url)

            if layer_meta_to_use:
                if layer_meta_to_use.get("maxRecordCount") is not None:
                    max_record_count = layer_meta_to_use["maxRecordCount"]
                    log.info(
                        "Service layer metadata indicates maxRecordCount: %d",
                        max_record_count,
                    )
                elif layer_meta_to_use.get("standardMaxRecordCount") is not None:
                    max_record_count = layer_meta_to_use["standardMaxRecordCount"]
                    log.info(
                        "Service layer metadata indicates standardMaxRecordCount: %d",
                        max_record_count,
                    )
                else:
                    max_record_count = 2000
                    log.info(
                        "maxRecordCount not found in specific layer metadata, using default: %d",
                        max_record_count,
                    )
            else:
                max_record_count = 2000
                log.warning(
                    "Could not fetch specific layer metadata for maxRecordCount, using default: %d",
                    max_record_count,
                )

        # Ensure max_record_count is an integer after all determinations
        if not isinstance(max_record_count, int):
            log.warning(
                "max_record_count ended up non-integer: '%s'. Defaulting to 2000.",
                max_record_count,
            )
            max_record_count = 2000

        # Setup Query Parameters
        base_params = self._prepare_query_params()

        # Staging Path
        source_name_sanitized = sanitize_for_filename(self.src.name)
        staging_dir = paths.STAGING / self.src.authority / source_name_sanitized
        staging_dir.mkdir(parents=True, exist_ok=True)

        output_filename = f"{layer_name_sanitized}.{base_params['f']}"
        output_path = staging_dir / output_filename

        # Pagination and Data Fetching
        current_offset = 0
        features_written_total = 0
        all_features = []
        page_num = 1

        while True:
            effective_page_limit = max_record_count
            if max_record_count == 0:
                effective_page_limit = 2000

            log.info(
                "Fetching page %d for layer %s (offset %d, limit %d)",
                page_num,
                layer_name_sanitized,
                current_offset,
                effective_page_limit,
            )

            page_params = base_params.copy()
            page_params["resultOffset"] = current_offset
            page_params["resultRecordCount"] = effective_page_limit

            data = self._request_page(
                query_url, page_params, page_num, layer_name_sanitized
            )
            if data is None:
                break

            if "error" in data:
                log.error(
                    "❌ API_ERROR_REPORTED: Error from REST API for layer %s: %s",
                    layer_name_sanitized,
                    data["error"],
                )
                log.error(
                    "❌ API_ERROR_REPORTED: Breaking from pagination loop for this layer."
                )
                break

            features = data.get("features", [])
            if not features:
                if page_num == 1:
                    log.info(
                        "ℹ️ No features returned for layer %s with current parameters.",
                        layer_name_sanitized,
                    )
                else:
                    log.info(
                        "🏁 All features retrieved for layer %s (empty page).",
                        layer_name_sanitized,
                    )
                break

            features_written_total += self._append_features(all_features, features)

            exceeded_transfer_limit = data.get("exceededTransferLimit", False)

            if exceeded_transfer_limit:
                log.info(
                    "⚠️ Exceeded transfer limit for layer %s, fetching next page.",
                    layer_name_sanitized,
                )
                current_offset += len(features)
                page_num += 1
            elif (
                len(features) < effective_page_limit and effective_page_limit > 0
            ) or max_record_count == 0:
                log.info(
                    "🏁 All features likely retrieved for layer %s (less than page limit or server maxRecordCount is 0).",
                    layer_name_sanitized,
                )
                break
            else:
                current_offset += len(features)
                page_num += 1

        if all_features:
            final_output_data = {"type": "FeatureCollection", "features": all_features}

            # Add CRS information if output is GeoJSON
            if base_params["f"] == "geojson":
                if not layer_meta_to_use:
                    layer_metadata_url = f"{self.src.url.rstrip('/')}/{layer_id}"
                    log.debug(
                        "Fetching specific layer metadata for layer ID %s (for CRS info).",
                        layer_id,
                    )
                    layer_meta_to_use = self._get_layer_metadata(layer_metadata_url)

                if layer_meta_to_use and layer_meta_to_use.get("spatialReference"):
                    sr_info = layer_meta_to_use.get("spatialReference")
                    if sr_info and sr_info.get("wkid") == 3006:  # SWEREF99 TM
                        final_output_data["crs"] = {
                            "type": "name",
                            "properties": {"name": "urn:ogc:def:crs:EPSG::3006"},
                        }

            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(final_output_data, f, ensure_ascii=False, indent=2)
                log.info(
                    "💾 Successfully saved %d features for layer %s to %s",
                    features_written_total,
                    layer_name_sanitized,
                    output_path,
                )
                if not self.src.staged_data_type:
                    self.src.staged_data_type = base_params["f"]
            except IOError as e:
                log.error(
                    "❌ Failed to write data for layer %s to %s: %s",
                    layer_name_sanitized,
                    output_path,
                    e,
                )
        elif page_num == 1 and not all_features:
            log.info(
                "ℹ️ No features found or written for layer %s for source '%s'.",
                layer_name_sanitized,
                self.src.name,
            )
