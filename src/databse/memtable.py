"""
MemTable and SSTable components for LSM-style storage
Provides high write throughput by buffering writes in memory before flushing to disk
"""

import asyncio
import os
import struct
import time
import hashlib
import zlib
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Set, Callable, Awaitable
from datetime import datetime
import json
import threading
from concurrent.futures import ThreadPoolExecutor

from .storage import DataType, ColumnMetadata, BinaryEncoder, BinaryDecoder


class MemTableState(Enum):
    ACTIVE = 1
    IMMUTABLE = 2
    FLUSHING = 3
    FLUSHED = 4


@dataclass
class MemTableEntry:
    """Single entry in memtable"""
    row_id: int
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    deleted: bool = False
    
    def __lt__(self, other: 'MemTableEntry') -> bool:
        return self.row_id < other.row_id


class SkipListNode:
    """Node for skip list implementation"""
    def __init__(self, key: Any, value: Any, level: int):
        self.key = key
        self.value = value
        self.forward: List['SkipListNode'] = [None] * (level + 1)  # type: ignore


class SkipList:
    """
    Skip List for ordered in-memory storage
    Provides O(log n) insert, search, and delete operations
    Thread-safe with read-write lock
    """
    MAX_LEVEL = 16
    P = 0.5
    
    def __init__(self):
        self.header = SkipListNode(None, None, self.MAX_LEVEL)
        self.level = 0
        self._size = 0
        self._lock = threading.RLock()
        
    def _random_level(self) -> int:
        import random
        level = 0
        while random.random() < self.P and level < self.MAX_LEVEL:
            level += 1
        return level
    
    def insert(self, key: Any, value: Any) -> bool:
        with self._lock:
            update: List[SkipListNode] = [self.header] * (self.MAX_LEVEL + 1)
            current = self.header
            
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
                update[i] = current
            
            current = current.forward[0]
            
            if current and current.key == key:
                current.value = value
                return False
            
            new_level = self._random_level()
            
            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.header
                self.level = new_level
            
            new_node = SkipListNode(key, value, new_level)
            
            for i in range(new_level + 1):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node
            
            self._size += 1
            return True
    
    def search(self, key: Any) -> Optional[Any]:
        with self._lock:
            current = self.header
            
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
            
            current = current.forward[0]
            
            if current and current.key == key:
                return current.value
            return None
    
    def delete(self, key: Any) -> bool:
        with self._lock:
            update: List[SkipListNode] = [self.header] * (self.MAX_LEVEL + 1)
            current = self.header
            
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
                update[i] = current
            
            current = current.forward[0]
            
            if current and current.key == key:
                for i in range(self.level + 1):
                    if update[i].forward[i] != current:
                        break
                    update[i].forward[i] = current.forward[i]
                
                while self.level > 0 and self.header.forward[self.level] is None:
                    self.level -= 1
                
                self._size -= 1
                return True
            return False
    
    def range_search(self, min_key: Any, max_key: Any) -> List[Tuple[Any, Any]]:
        with self._lock:
            results = []
            current = self.header
            
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < min_key:
                    current = current.forward[i]
            
            current = current.forward[0]
            
            while current and current.key <= max_key:
                if current.key >= min_key:
                    results.append((current.key, current.value))
                current = current.forward[0]
            
            return results
    
    def items(self) -> List[Tuple[Any, Any]]:
        with self._lock:
            results = []
            current = self.header.forward[0]
            while current:
                results.append((current.key, current.value))
                current = current.forward[0]
            return results
    
    def __len__(self) -> int:
        return self._size
    
    def clear(self):
        with self._lock:
            self.header = SkipListNode(None, None, self.MAX_LEVEL)
            self.level = 0
            self._size = 0


class MemTable:
    """
    In-memory buffer for writes before flushing to disk (SSTable)
    Uses Skip List for ordered storage with O(log n) operations
    """
    
    DEFAULT_SIZE_LIMIT = 64 * 1024 * 1024  # 64MB
    DEFAULT_ENTRY_LIMIT = 100000  # 100k entries
    
    def __init__(
        self,
        table_name: str,
        columns: List[ColumnMetadata],
        size_limit: int = DEFAULT_SIZE_LIMIT,
        entry_limit: int = DEFAULT_ENTRY_LIMIT
    ):
        self.table_name = table_name
        self.columns = columns
        self.size_limit = size_limit
        self.entry_limit = entry_limit
        
        self._data = SkipList()
        self._state = MemTableState.ACTIVE
        self._size_bytes = 0
        self._lock = asyncio.Lock()
        self._created_at = time.time()
        self._row_counter = 0
        self._delete_markers: Set[int] = set()
    
    @property
    def state(self) -> MemTableState:
        return self._state
    
    @property
    def size_bytes(self) -> int:
        return self._size_bytes
    
    @property
    def entry_count(self) -> int:
        return len(self._data)
    
    def should_flush(self) -> bool:
        return (
            self._size_bytes >= self.size_limit or
            len(self._data) >= self.entry_limit
        )
    
    def _estimate_entry_size(self, data: Dict[str, Any]) -> int:
        try:
            return len(json.dumps(data, default=str))
        except:
            return 256
    
    async def insert(self, row_id: int, data: Dict[str, Any]) -> bool:
        async with self._lock:
            if self._state != MemTableState.ACTIVE:
                raise RuntimeError("MemTable is not active")
            
            entry = MemTableEntry(row_id=row_id, data=data)
            entry_size = self._estimate_entry_size(data)
            
            is_new = self._data.insert(row_id, entry)
            
            if is_new:
                self._size_bytes += entry_size
                self._row_counter = max(self._row_counter, row_id + 1)
            
            if row_id in self._delete_markers:
                self._delete_markers.remove(row_id)
            
            return is_new
    
    async def update(self, row_id: int, data: Dict[str, Any]) -> bool:
        async with self._lock:
            if self._state != MemTableState.ACTIVE:
                raise RuntimeError("MemTable is not active")
            
            existing = self._data.search(row_id)
            if existing:
                old_size = self._estimate_entry_size(existing.data)
                existing.data.update(data)
                existing.timestamp = time.time()
                new_size = self._estimate_entry_size(existing.data)
                self._size_bytes += (new_size - old_size)
                return True
            else:
                entry = MemTableEntry(row_id=row_id, data=data)
                self._data.insert(row_id, entry)
                self._size_bytes += self._estimate_entry_size(data)
                return True
    
    async def delete(self, row_id: int) -> bool:
        async with self._lock:
            if self._state != MemTableState.ACTIVE:
                raise RuntimeError("MemTable is not active")
            
            existing = self._data.search(row_id)
            if existing:
                existing.deleted = True
                existing.timestamp = time.time()
                self._delete_markers.add(row_id)
                return True
            else:
                entry = MemTableEntry(row_id=row_id, data={}, deleted=True)
                self._data.insert(row_id, entry)
                self._delete_markers.add(row_id)
                return True
    
    async def get(self, row_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            entry = self._data.search(row_id)
            if entry and not entry.deleted:
                return entry.data.copy()
            return None
    
    async def get_range(
        self,
        min_row_id: int,
        max_row_id: int
    ) -> List[Tuple[int, Dict[str, Any]]]:
        async with self._lock:
            entries = self._data.range_search(min_row_id, max_row_id)
            return [
                (row_id, entry.data.copy())
                for row_id, entry in entries
                if not entry.deleted
            ]
    
    async def get_all(self) -> List[Tuple[int, Dict[str, Any]]]:
        async with self._lock:
            entries = self._data.items()
            return [
                (row_id, entry.data.copy())
                for row_id, entry in entries
                if not entry.deleted
            ]
    
    async def make_immutable(self) -> 'MemTable':
        async with self._lock:
            if self._state != MemTableState.ACTIVE:
                raise RuntimeError("MemTable is already immutable")
            self._state = MemTableState.IMMUTABLE
            return self
    
    def get_next_row_id(self) -> int:
        return self._row_counter
    
    def stats(self) -> Dict[str, Any]:
        return {
            'table_name': self.table_name,
            'state': self._state.name,
            'entry_count': len(self._data),
            'size_bytes': self._size_bytes,
            'size_limit': self.size_limit,
            'entry_limit': self.entry_limit,
            'delete_markers': len(self._delete_markers),
            'created_at': self._created_at,
            'age_seconds': time.time() - self._created_at
        }


class BloomFilter:
    """
    Space-efficient probabilistic data structure for membership testing
    Used in SSTable for quick negative lookups
    """
    
    def __init__(self, expected_items: int = 10000, false_positive_rate: float = 0.01):
        import math
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate
        
        self.size = int(-expected_items * math.log(false_positive_rate) / (math.log(2) ** 2))
        self.hash_count = int(self.size / expected_items * math.log(2))
        
        self.size = max(self.size, 64)
        self.hash_count = max(self.hash_count, 1)
        
        self.bit_array = bytearray((self.size + 7) // 8)
    
    def _hash(self, item: Any, seed: int) -> int:
        data = str(item).encode('utf-8')
        h = hashlib.md5(data + struct.pack('I', seed)).digest()
        return int.from_bytes(h[:4], 'little') % self.size
    
    def add(self, item: Any):
        for i in range(self.hash_count):
            pos = self._hash(item, i)
            self.bit_array[pos // 8] |= (1 << (pos % 8))
    
    def might_contain(self, item: Any) -> bool:
        for i in range(self.hash_count):
            pos = self._hash(item, i)
            if not (self.bit_array[pos // 8] & (1 << (pos % 8))):
                return False
        return True
    
    def to_bytes(self) -> bytes:
        header = struct.pack('III', self.size, self.hash_count, len(self.bit_array))
        return header + bytes(self.bit_array)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'BloomFilter':
        size, hash_count, array_len = struct.unpack('III', data[:12])
        bf = cls.__new__(cls)
        bf.size = size
        bf.hash_count = hash_count
        bf.bit_array = bytearray(data[12:12+array_len])
        bf.expected_items = 0
        bf.false_positive_rate = 0
        return bf


@dataclass
class SSTableMetadata:
    """Metadata for an SSTable segment"""
    segment_id: str
    table_name: str
    level: int
    min_row_id: int
    max_row_id: int
    entry_count: int
    size_bytes: int
    created_at: float
    columns: List[str]
    bloom_filter_offset: int
    data_offset: int
    index_offset: int


class SSTableWriter:
    """
    Writes memtable data to SSTable format on disk
    Format:
    [Header][Bloom Filter][Sparse Index][Column Data Blocks][Footer]
    """
    
    MAGIC = b'SSTB'
    VERSION = 1
    
    def __init__(self, base_path: str, table_name: str, columns: List[ColumnMetadata]):
        self.base_path = base_path
        self.table_name = table_name
        self.columns = columns
        self._executor = ThreadPoolExecutor(max_workers=2)
    
    def _generate_segment_id(self) -> str:
        timestamp = int(time.time() * 1000)
        random_suffix = hashlib.md5(os.urandom(16)).hexdigest()[:8]
        return f"{self.table_name}_{timestamp}_{random_suffix}"
    
    async def write(self, memtable: MemTable, level: int = 0) -> Optional[SSTableMetadata]:
        entries = await memtable.get_all()
        if not entries:
            return None
        
        segment_id = self._generate_segment_id()
        segment_path = os.path.join(self.base_path, f"{segment_id}.sst")
        
        entries.sort(key=lambda x: x[0])
        
        bloom = BloomFilter(expected_items=len(entries))
        for row_id, _ in entries:
            bloom.add(row_id)
        
        sparse_index: List[Tuple[int, int]] = []
        index_interval = max(1, len(entries) // 100)
        
        loop = asyncio.get_event_loop()
        metadata = await loop.run_in_executor(
            self._executor,
            self._write_segment,
            segment_path, segment_id, entries, bloom, sparse_index, index_interval, level
        )
        
        return metadata
    
    def _write_segment(
        self,
        path: str,
        segment_id: str,
        entries: List[Tuple[int, Dict[str, Any]]],
        bloom: BloomFilter,
        sparse_index: List[Tuple[int, int]],
        index_interval: int,
        level: int
    ) -> SSTableMetadata:
        min_row_id = entries[0][0]
        max_row_id = entries[-1][0]
        
        with open(path, 'wb') as f:
            f.write(self.MAGIC)
            f.write(struct.pack('B', self.VERSION))
            
            table_bytes = self.table_name.encode('utf-8')
            f.write(struct.pack('H', len(table_bytes)))
            f.write(table_bytes)
            
            f.write(struct.pack('I', len(entries)))
            f.write(struct.pack('Q', min_row_id))
            f.write(struct.pack('Q', max_row_id))
            f.write(struct.pack('B', level))
            
            col_count = len(self.columns)
            f.write(struct.pack('H', col_count))
            for col in self.columns:
                col_bytes = col.name.encode('utf-8')
                f.write(struct.pack('H', len(col_bytes)))
                f.write(col_bytes)
                f.write(struct.pack('B', col.data_type.value))
            
            bloom_filter_offset = f.tell()
            bloom_bytes = bloom.to_bytes()
            f.write(struct.pack('I', len(bloom_bytes)))
            f.write(bloom_bytes)
            
            index_offset = f.tell()
            index_placeholder_pos = f.tell()
            f.write(struct.pack('I', 0))
            
            data_offset = f.tell()
            
            column_data: Dict[str, List[Any]] = {col.name: [] for col in self.columns}
            row_ids: List[int] = []
            
            for i, (row_id, data) in enumerate(entries):
                row_ids.append(row_id)
                
                if i % index_interval == 0:
                    sparse_index.append((row_id, f.tell() - data_offset))
                
                for col in self.columns:
                    value = data.get(col.name)
                    column_data[col.name].append(value)
            
            row_id_bytes = struct.pack(f'{len(row_ids)}Q', *row_ids)
            compressed_row_ids = zlib.compress(row_id_bytes, level=6)
            f.write(struct.pack('I', len(compressed_row_ids)))
            f.write(compressed_row_ids)
            
            for col in self.columns:
                values = column_data[col.name]
                encoded = BinaryEncoder.encode_column(values, col.data_type, compress=True)
                f.write(struct.pack('I', len(encoded)))
                f.write(encoded)
            
            actual_index_offset = f.tell()
            f.write(struct.pack('I', len(sparse_index)))
            for row_id, offset in sparse_index:
                f.write(struct.pack('Q', row_id))
                f.write(struct.pack('I', offset))
            
            footer_offset = f.tell()
            f.write(struct.pack('Q', bloom_filter_offset))
            f.write(struct.pack('Q', data_offset))
            f.write(struct.pack('Q', actual_index_offset))
            f.write(struct.pack('Q', footer_offset))
            f.write(self.MAGIC)
            
            final_size = f.tell()
            
            f.seek(index_placeholder_pos)
            f.write(struct.pack('I', actual_index_offset))
        
        return SSTableMetadata(
            segment_id=segment_id,
            table_name=self.table_name,
            level=level,
            min_row_id=min_row_id,
            max_row_id=max_row_id,
            entry_count=len(entries),
            size_bytes=final_size,
            created_at=time.time(),
            columns=[col.name for col in self.columns],
            bloom_filter_offset=bloom_filter_offset,
            data_offset=data_offset,
            index_offset=actual_index_offset
        )


class SSTableReader:
    """
    Reads data from SSTable segments
    Supports point lookups and range scans with bloom filter optimization
    """
    
    def __init__(self, segment_path: str):
        self.path = segment_path
        self._metadata: Optional[SSTableMetadata] = None
        self._bloom: Optional[BloomFilter] = None
        self._sparse_index: List[Tuple[int, int]] = []
        self._columns: List[ColumnMetadata] = []
        self._loaded = False
    
    async def load_metadata(self) -> SSTableMetadata:
        if self._loaded and self._metadata is not None:
            return self._metadata
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._load_metadata_sync)
        
        if self._metadata is None:
            raise RuntimeError(f"Failed to load SSTable metadata from {self.path}")
        return self._metadata
    
    def _load_metadata_sync(self):
        with open(self.path, 'rb') as f:
            magic = f.read(4)
            if magic != SSTableWriter.MAGIC:
                raise ValueError("Invalid SSTable file")
            
            version = struct.unpack('B', f.read(1))[0]
            
            table_name_len = struct.unpack('H', f.read(2))[0]
            table_name = f.read(table_name_len).decode('utf-8')
            
            entry_count = struct.unpack('I', f.read(4))[0]
            min_row_id = struct.unpack('Q', f.read(8))[0]
            max_row_id = struct.unpack('Q', f.read(8))[0]
            level = struct.unpack('B', f.read(1))[0]
            
            col_count = struct.unpack('H', f.read(2))[0]
            columns = []
            for _ in range(col_count):
                col_name_len = struct.unpack('H', f.read(2))[0]
                col_name = f.read(col_name_len).decode('utf-8')
                data_type = DataType(struct.unpack('B', f.read(1))[0])
                columns.append(ColumnMetadata(name=col_name, data_type=data_type))
            self._columns = columns
            
            bloom_filter_offset = f.tell()
            bloom_len = struct.unpack('I', f.read(4))[0]
            bloom_data = f.read(bloom_len)
            self._bloom = BloomFilter.from_bytes(bloom_data)
            
            index_offset_pos = struct.unpack('I', f.read(4))[0]
            data_offset = f.tell()
            
            f.seek(-28, 2)
            bloom_off = struct.unpack('Q', f.read(8))[0]
            data_off = struct.unpack('Q', f.read(8))[0]
            index_off = struct.unpack('Q', f.read(8))[0]
            
            f.seek(index_off)
            index_count = struct.unpack('I', f.read(4))[0]
            for _ in range(index_count):
                row_id = struct.unpack('Q', f.read(8))[0]
                offset = struct.unpack('I', f.read(4))[0]
                self._sparse_index.append((row_id, offset))
            
            file_size = f.seek(0, 2)
            
            self._metadata = SSTableMetadata(
                segment_id=os.path.basename(self.path).replace('.sst', ''),
                table_name=table_name,
                level=level,
                min_row_id=min_row_id,
                max_row_id=max_row_id,
                entry_count=entry_count,
                size_bytes=file_size,
                created_at=os.path.getctime(self.path),
                columns=[col.name for col in columns],
                bloom_filter_offset=bloom_off,
                data_offset=data_off,
                index_offset=index_off
            )
            
            self._loaded = True
    
    def might_contain(self, row_id: int) -> bool:
        if not self._bloom:
            return True
        return self._bloom.might_contain(row_id)
    
    def in_range(self, row_id: int) -> bool:
        if not self._metadata:
            return True
        return self._metadata.min_row_id <= row_id <= self._metadata.max_row_id
    
    async def get(self, row_id: int) -> Optional[Dict[str, Any]]:
        await self.load_metadata()
        
        if not self.in_range(row_id):
            return None
        
        if not self.might_contain(row_id):
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_sync, row_id)
    
    def _get_sync(self, row_id: int) -> Optional[Dict[str, Any]]:
        with open(self.path, 'rb') as f:
            f.seek(-28, 2)
            f.read(8)
            data_off = struct.unpack('Q', f.read(8))[0]
            
            f.seek(data_off)
            
            row_ids_len = struct.unpack('I', f.read(4))[0]
            compressed_row_ids = f.read(row_ids_len)
            row_id_bytes = zlib.decompress(compressed_row_ids)
            row_ids = list(struct.unpack(f'{len(row_id_bytes)//8}Q', row_id_bytes))
            
            try:
                idx = row_ids.index(row_id)
            except ValueError:
                return None
            
            column_data = {}
            for col in self._columns:
                col_len = struct.unpack('I', f.read(4))[0]
                col_data = f.read(col_len)
                values, _ = BinaryDecoder.decode_column(col_data, col.data_type)
                column_data[col.name] = values[idx] if idx < len(values) else None
            
            return column_data
    
    async def scan(
        self,
        min_row_id: Optional[int] = None,
        max_row_id: Optional[int] = None
    ) -> List[Tuple[int, Dict[str, Any]]]:
        await self.load_metadata()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._scan_sync, min_row_id, max_row_id
        )
    
    def _scan_sync(
        self,
        min_row_id: Optional[int],
        max_row_id: Optional[int]
    ) -> List[Tuple[int, Dict[str, Any]]]:
        results = []
        
        with open(self.path, 'rb') as f:
            f.seek(-28, 2)
            f.read(8)
            data_off = struct.unpack('Q', f.read(8))[0]
            
            f.seek(data_off)
            
            row_ids_len = struct.unpack('I', f.read(4))[0]
            compressed_row_ids = f.read(row_ids_len)
            row_id_bytes = zlib.decompress(compressed_row_ids)
            row_ids = list(struct.unpack(f'{len(row_id_bytes)//8}Q', row_id_bytes))
            
            column_data = {}
            for col in self._columns:
                col_len = struct.unpack('I', f.read(4))[0]
                col_data = f.read(col_len)
                values, _ = BinaryDecoder.decode_column(col_data, col.data_type)
                column_data[col.name] = values
            
            for i, rid in enumerate(row_ids):
                if min_row_id is not None and rid < min_row_id:
                    continue
                if max_row_id is not None and rid > max_row_id:
                    continue
                
                row = {}
                for col in self._columns:
                    row[col.name] = column_data[col.name][i] if i < len(column_data[col.name]) else None
                results.append((rid, row))
        
        return results


class FlushService:
    """
    Background service that flushes memtables to SSTables
    Manages the flush queue and coordinates writes
    """
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self._flush_queue: asyncio.Queue[MemTable] = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._callbacks: List[Callable[[SSTableMetadata], Awaitable[None]]] = []
        self._lock = asyncio.Lock()
        
        os.makedirs(base_path, exist_ok=True)
    
    def on_flush_complete(self, callback: Callable[[SSTableMetadata], Awaitable[None]]):
        self._callbacks.append(callback)
    
    async def start(self):
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._flush_loop())
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def schedule_flush(self, memtable: MemTable):
        await memtable.make_immutable()
        await self._flush_queue.put(memtable)
    
    async def _flush_loop(self):
        while self._running:
            try:
                memtable = await asyncio.wait_for(
                    self._flush_queue.get(),
                    timeout=1.0
                )
                
                await self._flush_memtable(memtable)
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in flush loop: {e}")
    
    async def _flush_memtable(self, memtable: MemTable):
        async with self._lock:
            try:
                memtable._state = MemTableState.FLUSHING
                
                writer = SSTableWriter(
                    self.base_path,
                    memtable.table_name,
                    memtable.columns
                )
                
                metadata = await writer.write(memtable)
                
                if metadata:
                    memtable._state = MemTableState.FLUSHED
                    
                    for callback in self._callbacks:
                        try:
                            await callback(metadata)
                        except Exception as e:
                            print(f"Error in flush callback: {e}")
                
            except Exception as e:
                print(f"Error flushing memtable: {e}")
                memtable._state = MemTableState.IMMUTABLE
    
    async def flush_immediately(self, memtable: MemTable) -> Optional[SSTableMetadata]:
        await memtable.make_immutable()
        
        writer = SSTableWriter(
            self.base_path,
            memtable.table_name,
            memtable.columns
        )
        
        metadata = await writer.write(memtable)
        memtable._state = MemTableState.FLUSHED
        
        return metadata
    
    def pending_count(self) -> int:
        return self._flush_queue.qsize()


class SSTableRegistry:
    """
    Persists SSTable metadata to disk for recovery after restart
    """
    REGISTRY_FILE = "sstable_registry.json"
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.registry_path = os.path.join(base_path, self.REGISTRY_FILE)
        self._metadata: Dict[str, List[SSTableMetadata]] = {}
        self._lock = asyncio.Lock()
        
        os.makedirs(base_path, exist_ok=True)
    
    async def load(self):
        async with self._lock:
            if not os.path.exists(self.registry_path):
                return
            
            try:
                with open(self.registry_path, 'r') as f:
                    data = json.load(f)
                
                for table_name, sstables in data.items():
                    self._metadata[table_name] = []
                    for sst_data in sstables:
                        segment_path = os.path.join(self.base_path, f"{sst_data['segment_id']}.sst")
                        if os.path.exists(segment_path):
                            self._metadata[table_name].append(SSTableMetadata(
                                segment_id=sst_data['segment_id'],
                                table_name=sst_data['table_name'],
                                level=sst_data['level'],
                                min_row_id=sst_data['min_row_id'],
                                max_row_id=sst_data['max_row_id'],
                                entry_count=sst_data['entry_count'],
                                size_bytes=sst_data['size_bytes'],
                                created_at=sst_data['created_at'],
                                columns=sst_data['columns'],
                                bloom_filter_offset=sst_data.get('bloom_filter_offset', 0),
                                data_offset=sst_data.get('data_offset', 0),
                                index_offset=sst_data.get('index_offset', 0)
                            ))
            except Exception as e:
                print(f"Error loading SSTable registry: {e}")
    
    async def save(self):
        async with self._lock:
            data = {}
            for table_name, sstables in self._metadata.items():
                data[table_name] = [
                    {
                        'segment_id': sst.segment_id,
                        'table_name': sst.table_name,
                        'level': sst.level,
                        'min_row_id': sst.min_row_id,
                        'max_row_id': sst.max_row_id,
                        'entry_count': sst.entry_count,
                        'size_bytes': sst.size_bytes,
                        'created_at': sst.created_at,
                        'columns': sst.columns,
                        'bloom_filter_offset': sst.bloom_filter_offset,
                        'data_offset': sst.data_offset,
                        'index_offset': sst.index_offset
                    }
                    for sst in sstables
                ]
            
            with open(self.registry_path, 'w') as f:
                json.dump(data, f, indent=2)
    
    async def register(self, metadata: SSTableMetadata):
        async with self._lock:
            if metadata.table_name not in self._metadata:
                self._metadata[metadata.table_name] = []
            self._metadata[metadata.table_name].append(metadata)
        
        await self.save()
    
    async def unregister(self, table_name: str, segment_ids: List[str]):
        async with self._lock:
            if table_name not in self._metadata:
                return
            self._metadata[table_name] = [
                sst for sst in self._metadata[table_name]
                if sst.segment_id not in segment_ids
            ]
        
        await self.save()
    
    def get_sstables(self, table_name: str) -> List[SSTableMetadata]:
        return self._metadata.get(table_name, [])
    
    def get_all_tables(self) -> List[str]:
        return list(self._metadata.keys())


class CompactionService:
    """
    Background service that compacts SSTables at the same level
    Reduces read amplification by merging overlapping segments
    """
    
    LEVEL_THRESHOLD = 4
    MAX_LEVELS = 7
    
    def __init__(
        self, 
        base_path: str, 
        registry: SSTableRegistry,
        table_columns: Dict[str, List[ColumnMetadata]]
    ):
        self.base_path = base_path
        self.registry = registry
        self.table_columns = table_columns
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=1)
    
    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._compaction_loop())
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def _compaction_loop(self):
        while self._running:
            try:
                await asyncio.sleep(30)
                
                for table_name in self.registry.get_all_tables():
                    await self._maybe_compact_table(table_name)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in compaction loop: {e}")
    
    async def _maybe_compact_table(self, table_name: str):
        sstables = self.registry.get_sstables(table_name)
        if not sstables:
            return
        
        by_level: Dict[int, List[SSTableMetadata]] = {}
        for sst in sstables:
            if sst.level not in by_level:
                by_level[sst.level] = []
            by_level[sst.level].append(sst)
        
        for level, level_sstables in by_level.items():
            if len(level_sstables) >= self.LEVEL_THRESHOLD and level < self.MAX_LEVELS:
                await self._compact_level(table_name, level, level_sstables)
                break
    
    async def _compact_level(
        self, 
        table_name: str, 
        level: int, 
        sstables: List[SSTableMetadata]
    ):
        if table_name not in self.table_columns:
            return
        
        columns = self.table_columns[table_name]
        
        merged_entries: Dict[int, Dict[str, Any]] = {}
        
        sstables_sorted = sorted(sstables, key=lambda x: x.created_at)
        
        for sst in sstables_sorted:
            segment_path = os.path.join(self.base_path, f"{sst.segment_id}.sst")
            if not os.path.exists(segment_path):
                continue
            
            reader = SSTableReader(segment_path)
            try:
                entries = await reader.scan()
                for row_id, data in entries:
                    merged_entries[row_id] = data
            except Exception as e:
                print(f"Error reading SSTable {sst.segment_id}: {e}")
        
        if not merged_entries:
            return
        
        writer = SSTableWriter(self.base_path, table_name, columns)
        
        temp_memtable = MemTable(table_name, columns)
        temp_memtable._data = SkipList()
        
        for row_id, data in sorted(merged_entries.items()):
            entry = MemTableEntry(row_id=row_id, data=data)
            temp_memtable._data.insert(row_id, entry)
            temp_memtable._size_bytes += len(str(data))
        
        new_metadata = await writer.write(temp_memtable, level=level + 1)
        
        if new_metadata:
            await self.registry.register(new_metadata)
            
            segment_ids = [sst.segment_id for sst in sstables]
            await self.registry.unregister(table_name, segment_ids)
            
            for sst in sstables:
                segment_path = os.path.join(self.base_path, f"{sst.segment_id}.sst")
                try:
                    os.remove(segment_path)
                except Exception as e:
                    print(f"Error removing compacted SSTable {sst.segment_id}: {e}")


class MemTableManager:
    """
    Manages multiple memtables per table with automatic rotation and flushing
    Persists SSTable metadata for recovery after restart
    """
    
    def __init__(self, base_path: str, flush_service: FlushService):
        self.base_path = base_path
        self.flush_service = flush_service
        
        self._active_memtables: Dict[str, MemTable] = {}
        self._immutable_memtables: Dict[str, List[MemTable]] = {}
        self._sstables: Dict[str, List[SSTableReader]] = {}
        self._table_columns: Dict[str, List[ColumnMetadata]] = {}
        self._lock = asyncio.Lock()
        
        self._registry = SSTableRegistry(base_path)
        self._compaction: Optional[CompactionService] = None
        self._initialized = False
    
    async def initialize(self):
        if self._initialized:
            return
        
        await self._registry.load()
        
        self.flush_service.on_flush_complete(self._on_flush_complete)
        
        self._initialized = True
    
    async def load_sstables_for_table(self, table_name: str, columns: List[ColumnMetadata]):
        """Load existing SSTables for a table after schema is registered"""
        if table_name not in self._sstables:
            self._sstables[table_name] = []
        
        sstable_metas = self._registry.get_sstables(table_name)
        
        for meta in sorted(sstable_metas, key=lambda x: x.created_at):
            segment_path = os.path.join(self.base_path, f"{meta.segment_id}.sst")
            if os.path.exists(segment_path):
                already_loaded = any(
                    sst.path == segment_path 
                    for sst in self._sstables.get(table_name, [])
                )
                if not already_loaded:
                    reader = SSTableReader(segment_path)
                    await reader.load_metadata()
                    self._sstables[table_name].append(reader)
    
    async def _on_flush_complete(self, metadata: SSTableMetadata):
        await self._registry.register(metadata)
        
        segment_path = os.path.join(self.base_path, f"{metadata.segment_id}.sst")
        if os.path.exists(segment_path):
            reader = SSTableReader(segment_path)
            await reader.load_metadata()
            await self.add_sstable(metadata.table_name, reader)
        
        async with self._lock:
            if metadata.table_name in self._immutable_memtables:
                self._immutable_memtables[metadata.table_name] = [
                    m for m in self._immutable_memtables[metadata.table_name]
                    if m._state != MemTableState.FLUSHED
                ]
    
    async def start_compaction(self):
        if self._compaction:
            return
        
        self._compaction = CompactionService(
            self.base_path,
            self._registry,
            self._table_columns
        )
        await self._compaction.start()
    
    async def stop_compaction(self):
        if self._compaction:
            await self._compaction.stop()
            self._compaction = None
    
    async def wait_for_pending_flushes(self, timeout: float = 30.0):
        """Wait for pending flushes to complete before shutdown"""
        start_time = time.time()
        while self.flush_service.pending_count() > 0:
            if time.time() - start_time > timeout:
                break
            await asyncio.sleep(0.1)
        
        await asyncio.sleep(0.5)
    
    async def register_table(self, table_name: str, columns: List[ColumnMetadata]):
        async with self._lock:
            self._table_columns[table_name] = columns
            if table_name not in self._active_memtables:
                self._active_memtables[table_name] = MemTable(table_name, columns)
                self._immutable_memtables[table_name] = []
                if table_name not in self._sstables:
                    self._sstables[table_name] = []
        
        await self.load_sstables_for_table(table_name, columns)
    
    async def get_memtable(self, table_name: str) -> Optional[MemTable]:
        async with self._lock:
            memtable = self._active_memtables.get(table_name)
            
            if memtable and memtable.should_flush():
                columns = self._table_columns.get(table_name, [])
                new_memtable = MemTable(table_name, columns)
                self._active_memtables[table_name] = new_memtable
                self._immutable_memtables[table_name].append(memtable)
                
                await self.flush_service.schedule_flush(memtable)
                
                return new_memtable
            
            return memtable
    
    async def add_sstable(self, table_name: str, reader: SSTableReader):
        async with self._lock:
            if table_name not in self._sstables:
                self._sstables[table_name] = []
            self._sstables[table_name].append(reader)
    
    async def get(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        memtable = self._active_memtables.get(table_name)
        if memtable:
            result = await memtable.get(row_id)
            if result is not None:
                return result
        
        for imm in self._immutable_memtables.get(table_name, []):
            result = await imm.get(row_id)
            if result is not None:
                return result
        
        for sst in reversed(self._sstables.get(table_name, [])):
            result = await sst.get(row_id)
            if result is not None:
                return result
        
        return None
    
    async def scan(
        self,
        table_name: str,
        min_row_id: Optional[int] = None,
        max_row_id: Optional[int] = None
    ) -> List[Tuple[int, Dict[str, Any]]]:
        results: Dict[int, Dict[str, Any]] = {}
        
        for sst in self._sstables.get(table_name, []):
            for row_id, data in await sst.scan(min_row_id, max_row_id):
                if row_id not in results:
                    results[row_id] = data
        
        for imm in self._immutable_memtables.get(table_name, []):
            for row_id, data in await imm.get_all():
                if min_row_id is not None and row_id < min_row_id:
                    continue
                if max_row_id is not None and row_id > max_row_id:
                    continue
                results[row_id] = data
        
        memtable = self._active_memtables.get(table_name)
        if memtable:
            for row_id, data in await memtable.get_all():
                if min_row_id is not None and row_id < min_row_id:
                    continue
                if max_row_id is not None and row_id > max_row_id:
                    continue
                results[row_id] = data
        
        return sorted(results.items(), key=lambda x: x[0])
    
    async def find_by_column(
        self,
        table_name: str,
        column_name: str,
        column_value: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Search for a row by column value across all data sources:
        1. Active memtable (most recent unflushed writes)
        2. Immutable memtables (pending flush)
        3. SSTables (persisted data)
        Returns the first matching row or None.
        """
        memtable = self._active_memtables.get(table_name)
        if memtable:
            for row_id, data in await memtable.get_all():
                if data.get(column_name) == column_value:
                    return data
        
        for imm in self._immutable_memtables.get(table_name, []):
            for row_id, data in await imm.get_all():
                if data.get(column_name) == column_value:
                    return data
        
        for sst in reversed(self._sstables.get(table_name, [])):
            try:
                for row_id, data in await sst.scan():
                    if data.get(column_name) == column_value:
                        return data
            except Exception:
                continue
        
        return None
    
    def stats(self) -> Dict[str, Any]:
        stats = {
            'tables': {}
        }
        
        for table_name in self._active_memtables:
            active = self._active_memtables[table_name]
            immutable = self._immutable_memtables.get(table_name, [])
            sstables = self._sstables.get(table_name, [])
            
            stats['tables'][table_name] = {
                'active_memtable': active.stats() if active else None,
                'immutable_count': len(immutable),
                'sstable_count': len(sstables)
            }
        
        return stats
