"""
Cache Manager for Columnar Database
Implements LRU caching for performance optimization
"""

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Optional, TypeVar, List, Callable
import hashlib
import json

K = TypeVar('K')
V = TypeVar('V')


@dataclass
class CacheEntry(Generic[V]):
    value: V
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    size_bytes: int = 0
    
    def access(self):
        self.last_accessed = time.time()
        self.access_count += 1


class LRUCache(Generic[K, V]):
    def __init__(self, max_size: int = 1000, max_memory_bytes: Optional[int] = None, ttl_seconds: Optional[int] = None):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_bytes
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[K, CacheEntry[V]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._current_memory = 0
        
        self._hits = 0
        self._misses = 0
    
    def _estimate_size(self, value: Any) -> int:
        try:
            if isinstance(value, (str, bytes)):
                return len(value)
            elif isinstance(value, (int, float, bool)):
                return 8
            elif isinstance(value, dict):
                return len(json.dumps(value, default=str))
            elif isinstance(value, (list, tuple)):
                return sum(self._estimate_size(v) for v in value)
            else:
                return len(str(value))
        except:
            return 100
    
    async def get(self, key: K) -> Optional[V]:
        async with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]
            
            if self.ttl_seconds is not None:
                if time.time() - entry.created_at > self.ttl_seconds:
                    self._current_memory -= entry.size_bytes
                    del self._cache[key]
                    self._misses += 1
                    return None
            
            entry.access()
            self._cache.move_to_end(key)
            self._hits += 1
            return entry.value
    
    async def set(self, key: K, value: V):
        async with self._lock:
            size = self._estimate_size(value)
            
            if key in self._cache:
                old_entry = self._cache[key]
                self._current_memory -= old_entry.size_bytes
                del self._cache[key]
            
            while len(self._cache) >= self.max_size:
                oldest_key = next(iter(self._cache))
                oldest_entry = self._cache[oldest_key]
                self._current_memory -= oldest_entry.size_bytes
                del self._cache[oldest_key]
            
            if self.max_memory_bytes is not None:
                while self._current_memory + size > self.max_memory_bytes and self._cache:
                    oldest_key = next(iter(self._cache))
                    oldest_entry = self._cache[oldest_key]
                    self._current_memory -= oldest_entry.size_bytes
                    del self._cache[oldest_key]
            
            entry = CacheEntry(value=value, size_bytes=size)
            self._cache[key] = entry
            self._current_memory += size
    
    async def delete(self, key: K) -> bool:
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                self._current_memory -= entry.size_bytes
                del self._cache[key]
                return True
            return False
    
    async def clear(self):
        async with self._lock:
            self._cache.clear()
            self._current_memory = 0
            self._hits = 0
            self._misses = 0
    
    async def contains(self, key: K) -> bool:
        async with self._lock:
            if key not in self._cache:
                return False
            
            if self.ttl_seconds is not None:
                entry = self._cache[key]
                if time.time() - entry.created_at > self.ttl_seconds:
                    self._current_memory -= entry.size_bytes
                    del self._cache[key]
                    return False
            
            return True
    
    def size(self) -> int:
        return len(self._cache)
    
    def memory_usage(self) -> int:
        return self._current_memory
    
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        if total == 0:
            return 0.0
        return self._hits / total
    
    def stats(self) -> Dict[str, Any]:
        return {
            'size': len(self._cache),
            'max_size': self.max_size,
            'memory_bytes': self._current_memory,
            'max_memory_bytes': self.max_memory_bytes,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': self.hit_rate(),
            'ttl_seconds': self.ttl_seconds
        }
    
    async def cleanup_expired(self) -> int:
        if self.ttl_seconds is None:
            return 0
        
        async with self._lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self._cache.items():
                if current_time - entry.created_at > self.ttl_seconds:
                    expired_keys.append(key)
            
            for key in expired_keys:
                entry = self._cache[key]
                self._current_memory -= entry.size_bytes
                del self._cache[key]
            
            return len(expired_keys)
    
    async def keys(self) -> List[K]:
        async with self._lock:
            return list(self._cache.keys())
    
    async def values(self) -> List[V]:
        async with self._lock:
            return [entry.value for entry in self._cache.values()]


class CacheManager:
    def __init__(self):
        self._caches: Dict[str, LRUCache] = {}
        self._lock = asyncio.Lock()
    
    async def create_cache(
        self,
        name: str,
        max_size: int = 1000,
        max_memory_bytes: Optional[int] = None,
        ttl_seconds: Optional[int] = None
    ) -> LRUCache:
        async with self._lock:
            if name not in self._caches:
                self._caches[name] = LRUCache(
                    max_size=max_size,
                    max_memory_bytes=max_memory_bytes,
                    ttl_seconds=ttl_seconds
                )
            return self._caches[name]
    
    async def get_cache(self, name: str) -> Optional[LRUCache]:
        async with self._lock:
            return self._caches.get(name)
    
    async def delete_cache(self, name: str) -> bool:
        async with self._lock:
            if name in self._caches:
                del self._caches[name]
                return True
            return False
    
    async def clear_all(self):
        async with self._lock:
            for cache in self._caches.values():
                await cache.clear()
    
    async def cleanup_all_expired(self) -> Dict[str, int]:
        results = {}
        async with self._lock:
            for name, cache in self._caches.items():
                results[name] = await cache.cleanup_expired()
        return results
    
    def list_caches(self) -> List[str]:
        return list(self._caches.keys())
    
    def stats(self) -> Dict[str, Dict[str, Any]]:
        return {name: cache.stats() for name, cache in self._caches.items()}


class QueryCache:
    def __init__(self, max_size: int = 500, ttl_seconds: int = 300):
        self._cache = LRUCache[str, Any](max_size=max_size, ttl_seconds=ttl_seconds)
    
    def _make_key(self, table_name: str, query_params: Dict[str, Any]) -> str:
        key_data = {
            'table': table_name,
            'params': query_params
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    async def get(self, table_name: str, query_params: Dict[str, Any]) -> Optional[Any]:
        key = self._make_key(table_name, query_params)
        return await self._cache.get(key)
    
    async def set(self, table_name: str, query_params: Dict[str, Any], result: Any):
        key = self._make_key(table_name, query_params)
        await self._cache.set(key, result)
    
    async def invalidate_table(self, table_name: str):
        keys_to_delete = []
        async with self._cache._lock:
            for key in self._cache._cache.keys():
                if table_name in str(key):
                    keys_to_delete.append(key)
        
        for key in keys_to_delete:
            await self._cache.delete(key)
    
    async def clear(self):
        await self._cache.clear()
    
    def stats(self) -> Dict[str, Any]:
        return self._cache.stats()
