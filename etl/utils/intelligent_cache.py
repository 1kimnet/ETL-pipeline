"""Intelligent caching system for ETL pipeline operations."""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
from contextlib import contextmanager

log = logging.getLogger(__name__)

T = TypeVar('T')


@dataclass
class CacheEntry:
    """Represents a cached item with metadata."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    size_bytes: int = 0
    ttl_seconds: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.ttl_seconds is None:
            return False
        return time.time() - self.created_at > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return time.time() - self.created_at

    @property
    def last_access_seconds_ago(self) -> float:
        """Get time since last access in seconds."""
        return time.time() - self.last_accessed

    def touch(self) -> None:
        """Update last accessed time and increment access count."""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache performance statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_size_bytes: int = 0
    total_entries: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as percentage."""
        total_requests = self.hits + self.misses
        return (self.hits / total_requests * 100) if total_requests > 0 else 0

    @property
    def miss_rate(self) -> float:
        """Calculate cache miss rate as percentage."""
        return 100 - self.hit_rate


class IntelligentCache:
    """Intelligent multi-level cache with adaptive policies."""

    def __init__(
        self,
        max_memory_mb: float = 256,
        max_disk_mb: float = 1024,
        default_ttl_seconds: float = 3600,
        cache_dir: Optional[Path] = None
    ):
        self.max_memory_bytes = int(max_memory_mb * 1024 * 1024)
        self.max_disk_bytes = int(max_disk_mb * 1024 * 1024)
        self.default_ttl_seconds = default_ttl_seconds
        self.cache_dir = cache_dir or Path("cache")
        self.cache_dir.mkdir(exist_ok=True)

        # In-memory cache (L1)
        self.memory_cache: Dict[str, CacheEntry] = {}

        # Disk cache tracking (L2)
        self.disk_cache_index: Dict[str, Path] = {}

        # Statistics and monitoring
        self.stats = CacheStats()
        self.lock = threading.RLock()

        # Cache policies
        self.enable_adaptive_ttl = True
        self.enable_predictive_loading = True
        self.access_patterns: Dict[str, List[float]] = {}

        log.info(
            "Initialized IntelligentCache: memory=%.1fMB, disk=%.1fMB, ttl=%ds",
            max_memory_mb,
            max_disk_mb,
            default_ttl_seconds)

    def get(self, key: str, default: Any = None) -> Any:
        """Get item from cache with intelligent retrieval."""
        with self.lock:
            # Try memory cache first (L1)
            if key in self.memory_cache:
                entry = self.memory_cache[key]

                if entry.is_expired:
                    self._remove_memory_entry(key)
                    self.stats.misses += 1
                    return default

                entry.touch()
                self._record_access_pattern(key)
                self.stats.hits += 1

                log.debug("Cache hit (memory): %s", key)
                return entry.value

            # Try disk cache (L2)
            if key in self.disk_cache_index:
                disk_path = self.disk_cache_index[key]

                if disk_path.exists():
                    try:
                        entry = self._load_from_disk(disk_path)

                        if entry.is_expired:
                            self._remove_disk_entry(key)
                            self.stats.misses += 1
                            return default

                        # Promote to memory cache if frequently accessed
                        if self._should_promote_to_memory(entry):
                            self._promote_to_memory(key, entry)

                        entry.touch()
                        self._record_access_pattern(key)
                        self.stats.hits += 1

                        log.debug("Cache hit (disk): %s", key)
                        return entry.value

                    except Exception as e:
                        log.warning("Failed to load from disk cache: %s", e)
                        self._remove_disk_entry(key)

                else:
                    # Disk file missing, clean up index
                    self._remove_disk_entry(key)

            # Cache miss
            self.stats.misses += 1
            log.debug("Cache miss: %s", key)
            return default

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        tags: Optional[List[str]] = None,
        force_disk: bool = False
    ) -> None:
        """Set item in cache with intelligent placement."""
        with self.lock:
            ttl = ttl_seconds or self._calculate_adaptive_ttl(key)
            size_bytes = self._estimate_size(value)

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                access_count=1,
                size_bytes=size_bytes,
                ttl_seconds=ttl,
                tags=tags or [],
                metadata={"storage_tier": "memory"}
            )

            # Decide storage tier
            if force_disk or size_bytes > self.max_memory_bytes * 0.1:
                # Large items go directly to disk
                self._store_to_disk(key, entry)
                log.debug(
                    "Cached to disk (large item): %s (%.1fKB)",
                    key,
                    size_bytes / 1024)
            else:
                # Try memory first
                if self._can_fit_in_memory(size_bytes):
                    self._store_to_memory(key, entry)
                    log.debug(
                        "Cached to memory: %s (%.1fKB)",
                        key,
                        size_bytes / 1024)
                else:
                    # Memory full, evict and try again
                    self._evict_memory_entries(size_bytes)
                    if self._can_fit_in_memory(size_bytes):
                        self._store_to_memory(key, entry)
                        log.debug(
                            "Cached to memory (after eviction): %s (%.1fKB)",
                            key,
                            size_bytes / 1024)
                    else:
                        # Still can't fit, use disk
                        self._store_to_disk(key, entry)
                        log.debug(
                            "Cached to disk (memory full): %s (%.1fKB)",
                            key,
                            size_bytes / 1024)

    def delete(self, key: str) -> bool:
        """Delete item from cache."""
        with self.lock:
            removed = False

            if key in self.memory_cache:
                self._remove_memory_entry(key)
                removed = True

            if key in self.disk_cache_index:
                self._remove_disk_entry(key)
                removed = True

            if removed:
                log.debug("Cache entry deleted: %s", key)

            return removed

    def clear(self, tags: Optional[List[str]] = None) -> int:
        """Clear cache entries, optionally by tags."""
        with self.lock:
            cleared_count = 0

            if tags is None:
                # Clear all
                cleared_count = len(self.memory_cache) + \
                    len(self.disk_cache_index)
                self.memory_cache.clear()

                for disk_path in self.disk_cache_index.values():
                    if disk_path.exists():
                        disk_path.unlink()

                self.disk_cache_index.clear()
                self.stats = CacheStats()

                log.info("Cache cleared completely: %d entries", cleared_count)

            else:
                # Clear by tags
                keys_to_remove = []

                for key, entry in self.memory_cache.items():
                    if any(tag in entry.tags for tag in tags):
                        keys_to_remove.append(key)

                for key in keys_to_remove:
                    self._remove_memory_entry(key)
                    cleared_count += 1

                # Check disk cache (need to load metadata)
                keys_to_remove = []
                for key in self.disk_cache_index:
                    try:
                        entry = self._load_from_disk(
                            self.disk_cache_index[key])
                        if any(tag in entry.tags for tag in tags):
                            keys_to_remove.append(key)
                    except Exception:
                        pass

                for key in keys_to_remove:
                    self._remove_disk_entry(key)
                    cleared_count += 1

                log.info(
                    "Cache cleared by tags %s: %d entries",
                    tags,
                    cleared_count)

            return cleared_count

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        with self.lock:
            memory_size = sum(
                entry.size_bytes for entry in self.memory_cache.values())
            disk_size = sum(
                path.stat().st_size for path in self.disk_cache_index.values()
                if path.exists()
            )

            return {
                "memory_cache": {
                    "entries": len(self.memory_cache),
                    "size_mb": memory_size / (1024 * 1024),
                    "utilization_percent": (memory_size / self.max_memory_bytes) * 100
                },
                "disk_cache": {
                    "entries": len(self.disk_cache_index),
                    "size_mb": disk_size / (1024 * 1024),
                    "utilization_percent": (disk_size / self.max_disk_bytes) * 100
                },
                "performance": {
                    "hits": self.stats.hits,
                    "misses": self.stats.misses,
                    "hit_rate_percent": self.stats.hit_rate,
                    "evictions": self.stats.evictions
                },
                "efficiency": {
                    "total_requests": self.stats.hits + self.stats.misses,
                    "memory_efficiency": memory_size / max(1, len(self.memory_cache)),
                    "access_patterns": len(self.access_patterns)
                }
            }

    def optimize(self) -> None:
        """Perform cache optimization and cleanup."""
        with self.lock:
            log.info("🔄 Starting cache optimization...")

            # Clean expired entries
            expired_count = self._clean_expired_entries()

            # Optimize memory layout
            optimized_count = self._optimize_memory_layout()

            # Clean up disk cache
            disk_cleaned = self._cleanup_disk_cache()

            # Update statistics
            self._update_cache_stats()

            log.info(
                "✅ Cache optimization complete: expired=%d, optimized=%d, disk_cleaned=%d",
                expired_count,
                optimized_count,
                disk_cleaned)

    def _calculate_adaptive_ttl(self, key: str) -> float:
        """Calculate adaptive TTL based on access patterns."""
        if not self.enable_adaptive_ttl or key not in self.access_patterns:
            return self.default_ttl_seconds

        # Analyze access pattern
        accesses = self.access_patterns[key]
        if len(accesses) < 2:
            return self.default_ttl_seconds

        # Calculate access frequency
        time_span = accesses[-1] - accesses[0]
        frequency = len(accesses) / max(time_span, 1)  # accesses per second

        # Adjust TTL based on frequency
        if frequency > 0.1:  # More than once every 10 seconds
            return self.default_ttl_seconds * 2  # Cache longer
        elif frequency < 0.01:  # Less than once every 100 seconds
            return self.default_ttl_seconds * 0.5  # Cache shorter

        return self.default_ttl_seconds

    def _record_access_pattern(self, key: str) -> None:
        """Record access pattern for predictive caching."""
        current_time = time.time()

        if key not in self.access_patterns:
            self.access_patterns[key] = []

        self.access_patterns[key].append(current_time)

        # Keep only recent accesses (last hour)
        cutoff_time = current_time - 3600
        self.access_patterns[key] = [
            t for t in self.access_patterns[key] if t > cutoff_time
        ]

    def _should_promote_to_memory(self, entry: CacheEntry) -> bool:
        """Determine if disk entry should be promoted to memory."""
        # Promote if frequently accessed
        access_frequency = entry.access_count / max(entry.age_seconds, 1)
        return access_frequency > 0.01 and entry.size_bytes < self.max_memory_bytes * 0.1

    def _promote_to_memory(self, key: str, entry: CacheEntry) -> None:
        """Promote disk entry to memory cache."""
        if self._can_fit_in_memory(entry.size_bytes):
            self._store_to_memory(key, entry)
            self._remove_disk_entry(key)
            log.debug("Promoted to memory: %s", key)

    def _can_fit_in_memory(self, size_bytes: int) -> bool:
        """Check if item can fit in memory cache."""
        current_size = sum(
            entry.size_bytes for entry in self.memory_cache.values())
        return current_size + size_bytes <= self.max_memory_bytes

    def _store_to_memory(self, key: str, entry: CacheEntry) -> None:
        """Store entry in memory cache."""
        entry.metadata["storage_tier"] = "memory"
        self.memory_cache[key] = entry
        self.stats.total_entries += 1
        self.stats.total_size_bytes += entry.size_bytes

    def _store_to_disk(self, key: str, entry: CacheEntry) -> None:
        """Store entry in disk cache."""
        cache_file = self.cache_dir / f"{self._hash_key(key)}.cache"

        try:
            with cache_file.open('wb') as f:
                pickle.dump(entry, f)

            entry.metadata["storage_tier"] = "disk"
            self.disk_cache_index[key] = cache_file
            self.stats.total_entries += 1
            self.stats.total_size_bytes += entry.size_bytes

        except Exception as e:
            log.error("Failed to store to disk cache: %s", e)

    def _load_from_disk(self, disk_path: Path) -> CacheEntry:
        """Load entry from disk cache."""
        with disk_path.open('rb') as f:
            return pickle.load(f)

    def _remove_memory_entry(self, key: str) -> None:
        """Remove entry from memory cache."""
        if key in self.memory_cache:
            entry = self.memory_cache.pop(key)
            self.stats.total_entries -= 1
            self.stats.total_size_bytes -= entry.size_bytes

    def _remove_disk_entry(self, key: str) -> None:
        """Remove entry from disk cache."""
        if key in self.disk_cache_index:
            disk_path = self.disk_cache_index.pop(key)
            if disk_path.exists():
                try:
                    entry = self._load_from_disk(disk_path)
                    self.stats.total_entries -= 1
                    self.stats.total_size_bytes -= entry.size_bytes
                    disk_path.unlink()
                except Exception as e:
                    log.warning("Failed to remove disk cache file: %s", e)

    def _evict_memory_entries(self, needed_bytes: int) -> None:
        """Evict entries from memory cache using LFU + LRU policy."""
        if not self.memory_cache:
            return

        # Sort by access frequency (ascending) and last access time (ascending)
        sorted_entries = sorted(self.memory_cache.items(), key=lambda x: (
            x[1].access_count / max(x[1].age_seconds, 1), x[1].last_accessed))

        freed_bytes = 0
        evicted_keys = []

        for key, entry in sorted_entries:
            if freed_bytes >= needed_bytes:
                break

            # Move to disk if possible, otherwise just remove
            if entry.size_bytes < self.max_disk_bytes * 0.1:
                self._store_to_disk(key, entry)

            self._remove_memory_entry(key)
            freed_bytes += entry.size_bytes
            evicted_keys.append(key)
            self.stats.evictions += 1

        if evicted_keys:
            log.debug("Evicted %d entries from memory (%.1fKB freed)",
                      len(evicted_keys), freed_bytes / 1024)

    def _clean_expired_entries(self) -> int:
        """Clean expired entries from both caches."""
        expired_count = 0

        # Clean memory cache
        expired_keys = [
            key for key, entry in self.memory_cache.items()
            if entry.is_expired
        ]

        for key in expired_keys:
            self._remove_memory_entry(key)
            expired_count += 1

        # Clean disk cache
        expired_keys = []
        for key, disk_path in self.disk_cache_index.items():
            try:
                if disk_path.exists():
                    entry = self._load_from_disk(disk_path)
                    if entry.is_expired:
                        expired_keys.append(key)
            except Exception:
                expired_keys.append(key)  # Remove corrupted entries

        for key in expired_keys:
            self._remove_disk_entry(key)
            expired_count += 1

        return expired_count

    def _optimize_memory_layout(self) -> int:
        """Optimize memory cache layout for better performance."""
        # Move large, infrequently accessed items to disk
        optimized_count = 0
        keys_to_move = []

        for key, entry in self.memory_cache.items():
            access_frequency = entry.access_count / max(entry.age_seconds, 1)
            if (entry.size_bytes > self.max_memory_bytes * 0.05 and
                    access_frequency < 0.001):  # Less than once per 1000 seconds
                keys_to_move.append(key)

        for key in keys_to_move:
            entry = self.memory_cache[key]
            self._store_to_disk(key, entry)
            self._remove_memory_entry(key)
            optimized_count += 1

        return optimized_count

    def _cleanup_disk_cache(self) -> int:
        """Clean up disk cache directory."""
        cleaned_count = 0

        # Remove orphaned cache files
        cache_files = set(self.cache_dir.glob("*.cache"))
        indexed_files = set(self.disk_cache_index.values())
        orphaned_files = cache_files - indexed_files

        for orphan in orphaned_files:
            try:
                orphan.unlink()
                cleaned_count += 1
            except Exception as e:
                log.warning(
                    "Failed to remove orphaned cache file %s: %s", orphan, e)

        return cleaned_count

    def _update_cache_stats(self) -> None:
        """Update cache statistics."""
        memory_size = sum(
            entry.size_bytes for entry in self.memory_cache.values())
        disk_size = sum(
            path.stat().st_size for path in self.disk_cache_index.values()
            if path.exists()
        )

        self.stats.total_entries = len(
            self.memory_cache) + len(self.disk_cache_index)
        self.stats.total_size_bytes = memory_size + disk_size

    def _estimate_size(self, obj: Any) -> int:
        """Estimate memory size of object."""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            # Fallback estimation
            if isinstance(obj, (str, bytes)):
                return len(obj)
            elif isinstance(obj, (list, tuple)):
                return sum(self._estimate_size(item)
                           for item in obj[:10]) * len(obj) // 10
            elif isinstance(obj, dict):
                sample_items = list(obj.items())[:10]
                item_size = sum(self._estimate_size(k) + self._estimate_size(v)
                                for k, v in sample_items)
                return item_size * len(obj) // max(len(sample_items), 1)
            else:
                return 1024  # Default estimate

    def _hash_key(self, key: str) -> str:
        """Generate hash for cache key."""
        return hashlib.md5(key.encode()).hexdigest()


# Cache decorators for easy usage
def cached(
    ttl_seconds: Optional[float] = None,
    tags: Optional[List[str]] = None,
    cache_instance: Optional[IntelligentCache] = None
):
    """Decorator to cache function results."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            cache = cache_instance or get_global_cache()

            # Generate cache key from function name and arguments
            key_data = {
                "function": func.__name__,
                "args": args,
                "kwargs": sorted(kwargs.items())
            }
            cache_key = f"func_{hashlib.md5(str(key_data).encode()).hexdigest()}"

            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl_seconds=ttl_seconds, tags=tags)

            return result

        return wrapper
    return decorator


@contextmanager
def cache_scope(cache: IntelligentCache, tags: List[str]):
    """Context manager for scoped caching with automatic cleanup."""
    try:
        yield cache
    finally:
        # Clean up cache entries with these tags
        cache.clear(tags=tags)


# Global cache instance
_global_cache = IntelligentCache(
    max_memory_mb=256,
    max_disk_mb=1024,
    default_ttl_seconds=3600
)


def get_global_cache() -> IntelligentCache:
    """Get the global cache instance."""
    return _global_cache


def configure_global_cache(
    max_memory_mb: float = 256,
    max_disk_mb: float = 1024,
    default_ttl_seconds: float = 3600,
    cache_dir: Optional[Path] = None
) -> IntelligentCache:
    """Configure the global cache instance."""
    global _global_cache
    _global_cache = IntelligentCache(
        max_memory_mb=max_memory_mb,
        max_disk_mb=max_disk_mb,
        default_ttl_seconds=default_ttl_seconds,
        cache_dir=cache_dir
    )
    return _global_cache
