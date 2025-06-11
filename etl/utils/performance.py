"""Performance and scalability utilities for ETL pipeline.

This module provides utilities for connection pooling, caching, memory management,
and parallel processing to improve ETL pipeline performance and scalability.
"""
from __future__ import annotations

import functools
import hashlib
import json
import logging
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..exceptions import ResourceError, NetworkError

log = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""
    start_time: float
    end_time: Optional[float] = None
    operation_count: int = 0
    bytes_processed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    errors: int = 0
    
    @property
    def duration(self) -> float:
        """Get operation duration in seconds."""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
    
    @property
    def throughput_ops_per_sec(self) -> float:
        """Get operations per second."""
        duration = self.duration
        return self.operation_count / duration if duration > 0 else 0.0
    
    @property
    def throughput_bytes_per_sec(self) -> float:
        """Get bytes processed per second."""
        duration = self.duration
        return self.bytes_processed / duration if duration > 0 else 0.0
    
    @property
    def cache_hit_rate(self) -> float:
        """Get cache hit rate as percentage."""
        total_requests = self.cache_hits + self.cache_misses
        return (self.cache_hits / total_requests * 100) if total_requests > 0 else 0.0


class ConnectionPool:
    """HTTP connection pool with session management and retry logic."""
    
    def __init__(
        self,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        pool_block: bool = False
    ):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.pool_block = pool_block
        
        # Thread-local storage for sessions
        self._local = threading.local()
        
        # Weak references to track sessions for cleanup
        self._sessions = weakref.WeakSet()
        
        log.info("🔗 Initialized connection pool: connections=%d, maxsize=%d", 
                pool_connections, pool_maxsize)
    
    def get_session(self) -> requests.Session:
        """Get or create a thread-local HTTP session with connection pooling."""
        if not hasattr(self._local, 'session'):
            session = requests.Session()
            
            # Configure retry strategy
            retry_strategy = Retry(
                total=self.max_retries,
                backoff_factor=self.backoff_factor,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
            )
            
            # Configure adapter with connection pooling
            adapter = HTTPAdapter(
                pool_connections=self.pool_connections,
                pool_maxsize=self.pool_maxsize,
                pool_block=self.pool_block,
                max_retries=retry_strategy
            )
            
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            # Set default headers
            session.headers.update({
                'User-Agent': 'ETL-Pipeline/1.0 (Python requests)',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
            
            self._local.session = session
            self._sessions.add(session)
            
            log.debug("Created new HTTP session for thread %s", threading.current_thread().name)
        
        return self._local.session
    
    def close_all_sessions(self):
        """Close all active sessions and clean up connections."""
        closed_count = 0
        for session in self._sessions:
            try:
                session.close()
                closed_count += 1
            except Exception as e:
                log.warning("Error closing session: %s", e)
        
        # Clear thread-local storage
        if hasattr(self._local, 'session'):
            delattr(self._local, 'session')
        
        log.info("🔒 Closed %d HTTP sessions", closed_count)


class ResponseCache:
    """In-memory cache for HTTP responses with TTL and size limits."""
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: int = 3600,  # 1 hour
        max_response_size: int = 10 * 1024 * 1024  # 10MB
    ):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.max_response_size = max_response_size
        
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = threading.RLock()
        
        log.info("💾 Initialized response cache: max_size=%d, ttl=%ds", max_size, default_ttl)
    
    def _generate_key(self, url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> str:
        """Generate cache key from request parameters."""
        key_data = {
            'url': url,
            'params': sorted((params or {}).items()),
            'headers': sorted((headers or {}).items()) if headers else None
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get(self, url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Optional[Any]:
        """Get cached response if available and not expired."""
        key = self._generate_key(url, params, headers)
        
        with self._lock:
            if key not in self._cache:
                return None
            
            data, expire_time = self._cache[key]
            
            # Check if expired
            if time.time() > expire_time:
                del self._cache[key]
                self._access_times.pop(key, None)
                return None
            
            # Update access time for LRU
            self._access_times[key] = time.time()
            
            log.debug("Cache HIT for key: %s", key[:8])
            return data
    
    def set(
        self, 
        url: str, 
        data: Any, 
        params: Optional[Dict] = None, 
        headers: Optional[Dict] = None,
        ttl: Optional[int] = None
    ):
        """Cache response data with TTL."""
        # Check response size
        try:
            if hasattr(data, '__len__') and len(str(data)) > self.max_response_size:
                log.debug("Response too large to cache: %d bytes", len(str(data)))
                return
        except Exception:
            pass  # If we can't determine size, proceed with caching
        
        key = self._generate_key(url, params, headers)
        expire_time = time.time() + (ttl or self.default_ttl)
        
        with self._lock:
            # Evict oldest items if cache is full
            while len(self._cache) >= self.max_size:
                self._evict_lru()
            
            self._cache[key] = (data, expire_time)
            self._access_times[key] = time.time()
            
            log.debug("Cache SET for key: %s", key[:8])
    
    def _evict_lru(self):
        """Evict least recently used item."""
        if not self._access_times:
            return
        
        lru_key = min(self._access_times.items(), key=lambda x: x[1])[0]
        self._cache.pop(lru_key, None)
        self._access_times.pop(lru_key, None)
        
        log.debug("Evicted LRU cache entry: %s", lru_key[:8])
    
    def clear(self):
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()
            self._access_times.clear()
        log.info("🗑️ Cleared response cache")
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'hit_rate': 0.0,  # Would need to track hits/misses for accurate rate
                'memory_usage_estimate': len(str(self._cache))
            }


class MemoryManager:
    """Memory management utilities for large data processing."""
    
    def __init__(self, memory_limit_mb: int = 1024):
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.chunk_size = min(50 * 1024 * 1024, self.memory_limit_bytes // 4)  # 50MB or 1/4 of limit
        
        log.info("🧠 Memory manager initialized: limit=%dMB, chunk_size=%dMB", 
                memory_limit_mb, self.chunk_size // (1024 * 1024))
    
    def get_optimal_chunk_size(self, total_size: int) -> int:
        """Calculate optimal chunk size based on total data size and memory limits."""
        if total_size <= self.chunk_size:
            return total_size
        
        # Calculate number of chunks needed
        num_chunks = (total_size + self.chunk_size - 1) // self.chunk_size
        
        # Optimize chunk size to minimize number of chunks while staying under limit
        optimal_size = min(self.chunk_size, total_size // max(1, num_chunks - 1))
        
        return optimal_size
    
    def process_in_chunks(
        self, 
        data: Union[List, str, bytes], 
        processor: Callable[[Any], Any],
        chunk_size: Optional[int] = None
    ) -> List[Any]:
        """Process large data in memory-efficient chunks."""
        if chunk_size is None:
            chunk_size = self.chunk_size
        
        results = []
        total_size = len(data)
        
        log.debug("Processing %d items in chunks of %d", total_size, chunk_size)
        
        for i in range(0, total_size, chunk_size):
            chunk = data[i:i + chunk_size]
            try:
                result = processor(chunk)
                results.append(result)
            except Exception as e:
                log.error("Error processing chunk %d-%d: %s", i, i + len(chunk), e)
                raise ResourceError(f"Chunk processing failed: {e}", resource_type="memory") from e
        
        return results
    
    def stream_file_chunks(self, file_path: Path, chunk_size: Optional[int] = None):
        """Generator to stream file contents in chunks."""
        if chunk_size is None:
            chunk_size = self.chunk_size
        
        try:
            with file_path.open('rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        except Exception as e:
            raise ResourceError(f"File streaming failed: {e}", resource_type="file_io") from e


class ParallelProcessor:
    """Parallel processing utilities for ETL operations."""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers
        self._metrics = PerformanceMetrics(start_time=time.time())
        
        log.info("⚡ Parallel processor initialized: max_workers=%s", max_workers or "auto")
    
    def process_sources_parallel(
        self,
        sources: List[Any],
        processor: Callable[[Any], Any],
        max_workers: Optional[int] = None
    ) -> List[Tuple[Any, Union[Any, Exception]]]:
        """Process multiple sources in parallel with error handling."""
        workers = max_workers or self.max_workers
        results = []
        
        self._metrics.start_time = time.time()
        self._metrics.operation_count = len(sources)
        
        log.info("🚀 Processing %d sources in parallel (workers=%s)", len(sources), workers)
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_source = {
                executor.submit(self._safe_processor, processor, source): source 
                for source in sources
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    result = future.result()
                    results.append((source, result))
                    log.debug("✅ Completed processing source: %s", getattr(source, 'name', str(source)))
                except Exception as e:
                    self._metrics.errors += 1
                    results.append((source, e))
                    log.error("❌ Failed processing source %s: %s", getattr(source, 'name', str(source)), e)
        
        self._metrics.end_time = time.time()
        
        success_count = len([r for r in results if not isinstance(r[1], Exception)])
        log.info("📊 Parallel processing completed: %d/%d successful in %.2fs",
                success_count, len(sources), self._metrics.duration)
        
        return results
    
    def _safe_processor(self, processor: Callable, source: Any) -> Any:
        """Wrapper to safely execute processor with error handling."""
        try:
            return processor(source)
        except Exception as e:
            log.error("Processing failed for source %s: %s", getattr(source, 'name', str(source)), e)
            raise
    
    def get_metrics(self) -> PerformanceMetrics:
        """Get performance metrics for the last parallel operation."""
        return self._metrics


def cached_request(
    cache: ResponseCache,
    ttl: Optional[int] = None
) -> Callable:
    """Decorator to cache HTTP requests."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract URL and parameters for caching
            url = kwargs.get('url') or (args[0] if args else None)
            params = kwargs.get('params')
            headers = kwargs.get('headers')
            
            if not url:
                return func(*args, **kwargs)
            
            # Check cache first
            cached_result = cache.get(url, params, headers)
            if cached_result is not None:
                return cached_result
            
            # Execute request and cache result
            try:
                result = func(*args, **kwargs)
                cache.set(url, result, params, headers, ttl)
                return result
            except Exception as e:
                log.debug("Request failed, not caching: %s", e)
                raise
        
        return wrapper
    return decorator


def monitor_performance(operation_name: str) -> Callable:
    """Decorator to monitor performance of operations."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                duration = end_time - start_time
                
                log.info("⏱️  %s completed in %.3fs", operation_name, duration)
                return result
            except Exception as e:
                end_time = time.time()
                duration = end_time - start_time
                log.error("⏱️  %s failed after %.3fs: %s", operation_name, duration, e)
                raise
        
        return wrapper
    return decorator


# Global instances for shared use
_connection_pool = None
_response_cache = None
_memory_manager = None


def get_connection_pool() -> ConnectionPool:
    """Get global connection pool instance."""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = ConnectionPool()
    return _connection_pool


def get_response_cache() -> ResponseCache:
    """Get global response cache instance."""
    global _response_cache
    if _response_cache is None:
        _response_cache = ResponseCache()
    return _response_cache


def get_memory_manager() -> MemoryManager:
    """Get global memory manager instance."""
    global _memory_manager
    if _memory_manager is None:
        _memory_manager = MemoryManager()
    return _memory_manager


def cleanup_performance_resources():
    """Clean up global performance resources."""
    global _connection_pool, _response_cache, _memory_manager
    
    if _connection_pool:
        _connection_pool.close_all_sessions()
    
    if _response_cache:
        _response_cache.clear()
    
    _connection_pool = None
    _response_cache = None
    _memory_manager = None
    
    log.info("🧹 Performance resources cleaned up")