"""
Concurrent download utilities for the ETL pipeline.

This module provides thread-safe concurrent download functionality to replace
singleton-based download patterns that can lead to race conditions.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urljoin

from .io import download
from .performance import get_connection_pool
from ..exceptions import NetworkError, format_error_context

log = logging.getLogger(__name__)


@dataclass
class ConcurrentResult:
    """Result of a concurrent download operation."""
    success: bool
    item: Any
    result: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0


class ConcurrentDownloadManager:
    """
    Thread-safe manager for concurrent download operations.
    
    This class replaces singleton-based download patterns with explicit
    configuration parameters to avoid race conditions.
    """
    
    def __init__(self, default_max_workers: Optional[int] = None):
        """
        Initialize the concurrent download manager.
        
        Args:
            default_max_workers: Default number of worker threads to use
        """
        self.default_max_workers = default_max_workers
        self.connection_pool = get_connection_pool()
        log.info("🚀 ConcurrentDownloadManager initialized with default_max_workers=%s", 
                default_max_workers or "auto")
    
    def execute_concurrent(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
        max_workers: Optional[int] = None,
        fail_fast: bool = False
    ) -> List[ConcurrentResult]:
        """
        Execute a processor function concurrently on a list of items.
        
        Args:
            items: List of items to process
            processor: Function to process each item
            max_workers: Number of worker threads (overrides default)
            fail_fast: If True, stop on first failure
            
        Returns:
            List of ConcurrentResult objects
        """
        if not items:
            return []
        
        workers = max_workers or self.default_max_workers
        results: List[ConcurrentResult] = []
        
        log.info("⚡ Starting concurrent execution: %d items, %s workers", 
                len(items), workers or "auto")
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_item = {
                executor.submit(self._safe_processor, processor, item): item
                for item in items
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                start_time = time.time()
                
                try:
                    result = future.result()
                    duration = time.time() - start_time
                    
                    concurrent_result = ConcurrentResult(
                        success=True,
                        item=item,
                        result=result,
                        duration=duration
                    )
                    results.append(concurrent_result)
                    
                    log.debug("✅ Completed item: %s (%.2fs)", 
                            self._get_item_name(item), duration)
                    
                except Exception as e:
                    duration = time.time() - start_time
                    
                    concurrent_result = ConcurrentResult(
                        success=False,
                        item=item,
                        error=e,
                        duration=duration
                    )
                    results.append(concurrent_result)
                    
                    log.error("❌ Failed item: %s (%.2fs) - %s", 
                            self._get_item_name(item), duration, 
                            format_error_context(e) if hasattr(e, 'context') else str(e))
                    
                    if fail_fast:
                        log.warning("💥 Fail-fast enabled, stopping on first failure")
                        break
        
        success_count = sum(1 for r in results if r.success)
        total_time = sum(r.duration for r in results)
        
        log.info("📊 Concurrent execution completed: %d/%d successful, %.2fs total", 
                success_count, len(items), total_time)
        
        return results
    
    def _safe_processor(self, processor: Callable, item: Any) -> Any:
        """Safely execute processor with error handling."""
        try:
            return processor(item)
        except Exception as e:
            log.error("Processor failed for item %s: %s", self._get_item_name(item), e)
            raise
    
    def _get_item_name(self, item: Any) -> str:
        """Get a readable name for an item."""
        if hasattr(item, 'name'):
            return str(item.name)
        elif hasattr(item, 'url'):
            return str(item.url)
        elif isinstance(item, dict):
            return item.get('name', item.get('id', str(item)))
        else:
            return str(item)
    
    def download_layers_concurrent(
        self,
        handler: Any,
        layers_info: List[Dict[str, Any]],
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple layers concurrently.
        
        Args:
            handler: Handler instance that provides layer download functionality
            layers_info: List of layer information dictionaries
            fail_fast: Stop on first failure
            max_workers: Number of worker threads
            
        Returns:
            List of ConcurrentResult objects
        """
        log.info("🔄 Starting concurrent layer downloads: %d layers", len(layers_info))
        
        def process_layer(layer_info: Dict[str, Any]) -> Dict[str, Any]:
            """Process a single layer download."""
            if hasattr(handler, '_fetch_layer_data'):
                handler._fetch_layer_data(layer_info)
                return {'layer_id': layer_info.get('id'), 'status': 'completed'}
            else:
                raise AttributeError(f"Handler {type(handler).__name__} does not support layer downloads")
        
        return self.execute_concurrent(
            items=layers_info,
            processor=process_layer,
            max_workers=max_workers,
            fail_fast=fail_fast
        )
    
    def download_collections_concurrent(
        self,
        collections: List[Dict[str, Any]],
        base_url: str,
        download_dir: Path,
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple collections concurrently.
        
        Args:
            collections: List of collection information
            base_url: Base URL for downloads
            download_dir: Directory to save downloads
            fail_fast: Stop on first failure
            max_workers: Number of worker threads
            
        Returns:
            List of ConcurrentResult objects
        """
        log.info("🔄 Starting concurrent collection downloads: %d collections", len(collections))
        
        def process_collection(collection: Dict[str, Any]) -> Path:
            """Process a single collection download."""
            collection_id = collection.get('id', collection.get('name', 'unknown'))
            download_url = urljoin(base_url, collection.get('download_path', f"{collection_id}.zip"))
            
            filename = f"{collection_id}.zip"
            dest_path = download_dir / filename
            
            return download(download_url, dest_path)
        
        return self.execute_concurrent(
            items=collections,
            processor=process_collection,
            max_workers=max_workers,
            fail_fast=fail_fast
        )
    
    def download_files_concurrent(
        self,
        file_urls: List[Union[str, Dict[str, str]]],
        download_dir: Path,
        fail_fast: bool = False,
        max_workers: Optional[int] = None
    ) -> List[ConcurrentResult]:
        """
        Download multiple files concurrently.
        
        Args:
            file_urls: List of URLs or dicts with url/filename info
            download_dir: Directory to save downloads
            fail_fast: Stop on first failure
            max_workers: Number of worker threads
            
        Returns:
            List of ConcurrentResult objects
        """
        log.info("🔄 Starting concurrent file downloads: %d files", len(file_urls))
        
        def process_file(file_info: Union[str, Dict[str, str]]) -> Path:
            """Process a single file download."""
            if isinstance(file_info, str):
                url = file_info
                filename = Path(url).name or "downloaded_file"
            else:
                url = file_info['url']
                filename = file_info.get('filename', Path(url).name or "downloaded_file")
            
            dest_path = download_dir / filename
            return download(url, dest_path)
        
        return self.execute_concurrent(
            items=file_urls,
            processor=process_file,
            max_workers=max_workers,
            fail_fast=fail_fast
        )


# Global instance for convenience
_concurrent_download_manager: Optional[ConcurrentDownloadManager] = None


def get_concurrent_download_manager(max_workers: Optional[int] = None) -> ConcurrentDownloadManager:
    """
    Get or create the global concurrent download manager.
    
    Args:
        max_workers: Default max workers for the manager
        
    Returns:
        ConcurrentDownloadManager instance
    """
    global _concurrent_download_manager
    if _concurrent_download_manager is None:
        _concurrent_download_manager = ConcurrentDownloadManager(max_workers)
    return _concurrent_download_manager


def reset_concurrent_download_manager():
    """Reset the global concurrent download manager."""
    global _concurrent_download_manager
    _concurrent_download_manager = None