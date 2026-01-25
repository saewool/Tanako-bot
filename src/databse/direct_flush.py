"""
KotonexusTakako Direct Flush Storage
SSTable direct flush - ghi trực tiếp xuống disk thay vì buffer trong memory
"""

import asyncio
import os
import struct
import time
import hashlib
import zlib
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from .storage import DataType, ColumnMetadata, BinaryEncoder, BinaryDecoder
from .crypto import get_crypto_manager, encrypt_row, decrypt_row


MAGIC = b'KTDB'
VERSION = 3


@dataclass
class TableSegment:
    """Metadata for a table segment file"""
    segment_id: str
    table_name: str
    min_row_id: int
    max_row_id: int
    entry_count: int
    size_bytes: int
    created_at: float
    columns: List[str]
    encrypted: bool = True


class DirectFlushWriter:
    """
    Ghi trực tiếp xuống SSTable file.
    Mỗi write operation tạo một segment mới hoặc append vào segment hiện tại.
    """
    
    def __init__(self, base_path: str, table_name: str, columns: List[ColumnMetadata]):
        self.base_path = base_path
        self.table_name = table_name
        self.columns = columns
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._lock = asyncio.Lock()
        self._crypto = get_crypto_manager()
        
        os.makedirs(base_path, exist_ok=True)
    
    def _generate_segment_id(self) -> str:
        """Generate unique segment ID"""
        timestamp = int(time.time() * 1000000)
        random_suffix = hashlib.md5(os.urandom(16)).hexdigest()[:8]
        return f"{self.table_name}_{timestamp}_{random_suffix}"
    
    async def write_single(self, row_id: int, data: Dict[str, Any]) -> Optional[TableSegment]:
        """
        Ghi một row trực tiếp xuống disk.
        Tạo micro-segment cho mỗi write operation.
        """
        async with self._lock:
            segment_id = self._generate_segment_id()
            segment_path = os.path.join(self.base_path, f"{segment_id}.sstd")
            
            encrypted_data = encrypt_row(data)
            
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(
                self._executor,
                self._write_segment_sync,
                segment_path, segment_id, [(row_id, encrypted_data)]
            )
            
            return metadata
    
    async def write_batch(self, rows: List[Tuple[int, Dict[str, Any]]]) -> Optional[TableSegment]:
        """
        Ghi nhiều rows trực tiếp xuống disk trong một segment.
        """
        if not rows:
            return None
        
        async with self._lock:
            segment_id = self._generate_segment_id()
            segment_path = os.path.join(self.base_path, f"{segment_id}.sstd")
            
            encrypted_rows = [
                (row_id, encrypt_row(data))
                for row_id, data in rows
            ]
            
            encrypted_rows.sort(key=lambda x: x[0])
            
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(
                self._executor,
                self._write_segment_sync,
                segment_path, segment_id, encrypted_rows
            )
            
            return metadata
    
    def _write_segment_sync(
        self,
        path: str,
        segment_id: str,
        entries: List[Tuple[int, Dict[str, Any]]]
    ) -> TableSegment:
        """
        Write segment to disk synchronously.
        Format: [Header][Row Data Blocks][Footer]
        """
        min_row_id = entries[0][0]
        max_row_id = entries[-1][0]
        
        with open(path, 'wb') as f:
            f.write(MAGIC)
            f.write(struct.pack('B', VERSION))
            
            table_bytes = self.table_name.encode('utf-8')
            f.write(struct.pack('H', len(table_bytes)))
            f.write(table_bytes)
            
            f.write(struct.pack('I', len(entries)))
            f.write(struct.pack('Q', min_row_id))
            f.write(struct.pack('Q', max_row_id))
            f.write(struct.pack('B', 1))
            
            col_count = len(self.columns)
            f.write(struct.pack('H', col_count))
            for col in self.columns:
                col_bytes = col.name.encode('utf-8')
                f.write(struct.pack('H', len(col_bytes)))
                f.write(col_bytes)
                f.write(struct.pack('B', col.data_type.value))
            
            row_index: List[Tuple[int, int]] = []
            data_start = f.tell()
            
            for row_id, data in entries:
                row_offset = f.tell() - data_start
                row_index.append((row_id, row_offset))
                
                row_json = json.dumps(data, default=str).encode('utf-8')
                compressed = zlib.compress(row_json, level=6)
                
                f.write(struct.pack('Q', row_id))
                f.write(struct.pack('I', len(compressed)))
                f.write(compressed)
            
            index_offset = f.tell()
            f.write(struct.pack('I', len(row_index)))
            for row_id, offset in row_index:
                f.write(struct.pack('Q', row_id))
                f.write(struct.pack('I', offset))
            
            footer_offset = f.tell()
            f.write(struct.pack('Q', data_start))
            f.write(struct.pack('Q', index_offset))
            f.write(struct.pack('Q', footer_offset))
            f.write(MAGIC)
            
            final_size = f.tell()
        
        return TableSegment(
            segment_id=segment_id,
            table_name=self.table_name,
            min_row_id=min_row_id,
            max_row_id=max_row_id,
            entry_count=len(entries),
            size_bytes=final_size,
            created_at=time.time(),
            columns=[col.name for col in self.columns],
            encrypted=True
        )


class DirectFlushReader:
    """
    Đọc data từ SSTable segments.
    Hỗ trợ point lookups và range scans.
    """
    
    def __init__(self, segment_path: str):
        self.path = segment_path
        self._metadata: Optional[TableSegment] = None
        self._row_index: Dict[int, int] = {}
        self._data_offset: int = 0
        self._columns: List[ColumnMetadata] = []
        self._loaded = False
        self._crypto = get_crypto_manager()
    
    async def load_metadata(self) -> TableSegment:
        """Load segment metadata and index"""
        if self._loaded and self._metadata is not None:
            return self._metadata
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._load_metadata_sync)
        
        if self._metadata is None:
            raise RuntimeError(f"Failed to load segment metadata from {self.path}")
        return self._metadata
    
    def _load_metadata_sync(self):
        """Load metadata synchronously"""
        with open(self.path, 'rb') as f:
            magic = f.read(4)
            if magic != MAGIC:
                raise ValueError("Invalid segment file")
            
            version = struct.unpack('B', f.read(1))[0]
            
            table_name_len = struct.unpack('H', f.read(2))[0]
            table_name = f.read(table_name_len).decode('utf-8')
            
            entry_count = struct.unpack('I', f.read(4))[0]
            min_row_id = struct.unpack('Q', f.read(8))[0]
            max_row_id = struct.unpack('Q', f.read(8))[0]
            encrypted = struct.unpack('B', f.read(1))[0] == 1
            
            col_count = struct.unpack('H', f.read(2))[0]
            columns = []
            for _ in range(col_count):
                col_name_len = struct.unpack('H', f.read(2))[0]
                col_name = f.read(col_name_len).decode('utf-8')
                data_type = DataType(struct.unpack('B', f.read(1))[0])
                columns.append(ColumnMetadata(name=col_name, data_type=data_type))
            self._columns = columns
            
            self._data_offset = f.tell()
            
            f.seek(-28, 2)
            data_off = struct.unpack('Q', f.read(8))[0]
            index_off = struct.unpack('Q', f.read(8))[0]
            footer_off = struct.unpack('Q', f.read(8))[0]
            
            f.seek(index_off)
            index_count = struct.unpack('I', f.read(4))[0]
            for _ in range(index_count):
                row_id = struct.unpack('Q', f.read(8))[0]
                offset = struct.unpack('I', f.read(4))[0]
                self._row_index[row_id] = offset
            
            file_size = f.seek(0, 2)
            
            self._metadata = TableSegment(
                segment_id=os.path.basename(self.path).replace('.sstd', ''),
                table_name=table_name,
                min_row_id=min_row_id,
                max_row_id=max_row_id,
                entry_count=entry_count,
                size_bytes=file_size,
                created_at=os.path.getctime(self.path),
                columns=[col.name for col in columns],
                encrypted=encrypted
            )
            
            self._loaded = True
    
    def contains_row(self, row_id: int) -> bool:
        """Check if row_id might be in this segment"""
        if not self._metadata:
            return False
        return self._metadata.min_row_id <= row_id <= self._metadata.max_row_id
    
    async def get(self, row_id: int) -> Optional[Dict[str, Any]]:
        """Get a single row by row_id"""
        await self.load_metadata()
        
        if row_id not in self._row_index:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_sync, row_id)
    
    def _get_sync(self, row_id: int) -> Optional[Dict[str, Any]]:
        """Get row synchronously"""
        offset = self._row_index.get(row_id)
        if offset is None:
            return None
        
        with open(self.path, 'rb') as f:
            f.seek(self._data_offset + offset)
            
            stored_row_id = struct.unpack('Q', f.read(8))[0]
            if stored_row_id != row_id:
                return None
            
            data_len = struct.unpack('I', f.read(4))[0]
            compressed = f.read(data_len)
            
            row_json = zlib.decompress(compressed)
            encrypted_data = json.loads(row_json.decode('utf-8'))
            
            return decrypt_row(encrypted_data)
    
    async def scan(
        self,
        min_row_id: Optional[int] = None,
        max_row_id: Optional[int] = None
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """Scan rows in range"""
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
        """Scan synchronously"""
        results = []
        
        with open(self.path, 'rb') as f:
            for row_id, offset in sorted(self._row_index.items()):
                if min_row_id is not None and row_id < min_row_id:
                    continue
                if max_row_id is not None and row_id > max_row_id:
                    continue
                
                f.seek(self._data_offset + offset)
                
                stored_row_id = struct.unpack('Q', f.read(8))[0]
                data_len = struct.unpack('I', f.read(4))[0]
                compressed = f.read(data_len)
                
                row_json = zlib.decompress(compressed)
                encrypted_data = json.loads(row_json.decode('utf-8'))
                
                results.append((row_id, decrypt_row(encrypted_data)))
        
        return results
    
    async def get_all(self) -> List[Tuple[int, Dict[str, Any]]]:
        """Get all rows in segment"""
        return await self.scan()


class DirectFlushManager:
    """
    Quản lý SSTable direct flush cho nhiều tables.
    - Ghi trực tiếp xuống disk
    - Quản lý segments
    - Hỗ trợ reads và compaction
    """
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self._writers: Dict[str, DirectFlushWriter] = {}
        self._segments: Dict[str, List[TableSegment]] = {}
        self._row_counters: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        
        os.makedirs(base_path, exist_ok=True)
    
    async def initialize(self):
        """Initialize manager and load existing segments"""
        async with self._lock:
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path, exist_ok=True)
                return
            
            for filename in os.listdir(self.base_path):
                if filename.endswith('.sstd'):
                    segment_path = os.path.join(self.base_path, filename)
                    try:
                        reader = DirectFlushReader(segment_path)
                        metadata = await reader.load_metadata()
                        
                        if metadata.table_name not in self._segments:
                            self._segments[metadata.table_name] = []
                        self._segments[metadata.table_name].append(metadata)
                        
                        if metadata.table_name not in self._row_counters:
                            self._row_counters[metadata.table_name] = 0
                        self._row_counters[metadata.table_name] = max(
                            self._row_counters[metadata.table_name],
                            metadata.max_row_id + 1
                        )
                    except Exception as e:
                        print(f"Error loading segment {filename}: {e}")
    
    async def register_table(self, table_name: str, columns: List[ColumnMetadata]):
        """Register a table for writing"""
        async with self._lock:
            table_path = os.path.join(self.base_path, table_name)
            os.makedirs(table_path, exist_ok=True)
            
            self._writers[table_name] = DirectFlushWriter(table_path, table_name, columns)
            
            if table_name not in self._segments:
                self._segments[table_name] = []
            if table_name not in self._row_counters:
                self._row_counters[table_name] = 0
    
    def get_next_row_id(self, table_name: str) -> int:
        """Get next available row ID for a table"""
        if table_name not in self._row_counters:
            self._row_counters[table_name] = 0
        
        row_id = self._row_counters[table_name]
        self._row_counters[table_name] += 1
        return row_id
    
    async def insert(self, table_name: str, row_id: int, data: Dict[str, Any]) -> bool:
        """Insert a row directly to disk"""
        if table_name not in self._writers:
            raise ValueError(f"Table {table_name} not registered")
        
        writer = self._writers[table_name]
        segment = await writer.write_single(row_id, data)
        
        if segment:
            async with self._lock:
                if table_name not in self._segments:
                    self._segments[table_name] = []
                self._segments[table_name].append(segment)
                self._row_counters[table_name] = max(
                    self._row_counters.get(table_name, 0),
                    row_id + 1
                )
            return True
        return False
    
    async def insert_batch(
        self,
        table_name: str,
        rows: List[Tuple[int, Dict[str, Any]]]
    ) -> bool:
        """Insert multiple rows in a single segment"""
        if table_name not in self._writers:
            raise ValueError(f"Table {table_name} not registered")
        
        writer = self._writers[table_name]
        segment = await writer.write_batch(rows)
        
        if segment:
            async with self._lock:
                if table_name not in self._segments:
                    self._segments[table_name] = []
                self._segments[table_name].append(segment)
                
                max_row_id = max(row_id for row_id, _ in rows)
                self._row_counters[table_name] = max(
                    self._row_counters.get(table_name, 0),
                    max_row_id + 1
                )
            return True
        return False
    
    async def get(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        """Get a row by row_id"""
        if table_name not in self._segments:
            return None
        
        for segment_meta in reversed(self._segments[table_name]):
            if segment_meta.min_row_id <= row_id <= segment_meta.max_row_id:
                table_path = os.path.join(self.base_path, table_name)
                segment_path = os.path.join(table_path, f"{segment_meta.segment_id}.sstd")
                
                if os.path.exists(segment_path):
                    reader = DirectFlushReader(segment_path)
                    result = await reader.get(row_id)
                    if result is not None:
                        return result
        
        return None
    
    async def scan(
        self,
        table_name: str,
        min_row_id: Optional[int] = None,
        max_row_id: Optional[int] = None
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """Scan all rows in range"""
        if table_name not in self._segments:
            return []
        
        all_results: Dict[int, Dict[str, Any]] = {}
        
        for segment_meta in self._segments[table_name]:
            if min_row_id is not None and segment_meta.max_row_id < min_row_id:
                continue
            if max_row_id is not None and segment_meta.min_row_id > max_row_id:
                continue
            
            table_path = os.path.join(self.base_path, table_name)
            segment_path = os.path.join(table_path, f"{segment_meta.segment_id}.sstd")
            
            if os.path.exists(segment_path):
                reader = DirectFlushReader(segment_path)
                results = await reader.scan(min_row_id, max_row_id)
                
                for row_id, data in results:
                    all_results[row_id] = data
        
        return sorted(all_results.items(), key=lambda x: x[0])
    
    async def get_all(self, table_name: str) -> List[Tuple[int, Dict[str, Any]]]:
        """Get all rows in table"""
        return await self.scan(table_name)
    
    async def find_by_column(
        self,
        table_name: str,
        column: str,
        value: Any
    ) -> Optional[Dict[str, Any]]:
        """Find first row where column equals value"""
        all_rows = await self.get_all(table_name)
        
        for row_id, data in all_rows:
            if column in data:
                col_value = data[column]
                if col_value == value:
                    return data
                if str(col_value) == str(value):
                    return data
        
        return None
    
    async def find_all_by_column(
        self,
        table_name: str,
        column: str,
        value: Any
    ) -> List[Dict[str, Any]]:
        """Find all rows where column equals value"""
        all_rows = await self.get_all(table_name)
        results = []
        
        for row_id, data in all_rows:
            if column in data:
                col_value = data[column]
                if col_value == value or str(col_value) == str(value):
                    results.append(data)
        
        return results
    
    async def update(
        self,
        table_name: str,
        row_id: int,
        data: Dict[str, Any]
    ) -> bool:
        """Update a row (appends new version)"""
        existing = await self.get(table_name, row_id)
        if existing:
            existing.update(data)
            return await self.insert(table_name, row_id, existing)
        return False
    
    async def delete(self, table_name: str, row_id: int) -> bool:
        """Delete a row (marks as deleted)"""
        return await self.insert(table_name, row_id, {'__deleted__': True})
    
    async def compact(self, table_name: str) -> bool:
        """
        Compact all segments into fewer, larger segments.
        Removes deleted rows and old versions.
        """
        if table_name not in self._segments:
            return False
        
        all_rows = await self.get_all(table_name)
        
        active_rows = [
            (row_id, data) for row_id, data in all_rows
            if not data.get('__deleted__', False)
        ]
        
        if not active_rows:
            return True
        
        old_segments = self._segments[table_name]
        
        if table_name in self._writers:
            writer = self._writers[table_name]
            segment = await writer.write_batch(active_rows)
            
            if segment:
                self._segments[table_name] = [segment]
                
                table_path = os.path.join(self.base_path, table_name)
                for old_segment in old_segments:
                    old_path = os.path.join(table_path, f"{old_segment.segment_id}.sstd")
                    if os.path.exists(old_path) and old_segment.segment_id != segment.segment_id:
                        try:
                            os.remove(old_path)
                        except:
                            pass
                
                return True
        
        return False
    
    def get_entry_count(self, table_name: str) -> int:
        """Get total entry count for a table"""
        if table_name not in self._segments:
            return 0
        return sum(s.entry_count for s in self._segments[table_name])
    
    async def clear_table(self, table_name: str) -> bool:
        """Clear all data from a table"""
        if table_name not in self._segments:
            return True
        
        async with self._lock:
            table_path = os.path.join(self.base_path, table_name)
            
            for segment in self._segments.get(table_name, []):
                segment_path = os.path.join(table_path, f"{segment.segment_id}.sstd")
                if os.path.exists(segment_path):
                    try:
                        os.remove(segment_path)
                    except:
                        pass
            
            self._segments[table_name] = []
            self._row_counters[table_name] = 0
            
            return True
    
    async def flush_all(self) -> bool:
        """Flush all pending data to disk (no-op for direct flush)"""
        return True
    
    async def close(self) -> None:
        """Close all resources"""
        async with self._lock:
            for writer in self._writers.values():
                if hasattr(writer, '_executor'):
                    writer._executor.shutdown(wait=True)
            self._writers.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get statistics"""
        table_stats = {}
        total_segments = 0
        total_size = 0
        
        for table_name, segments in self._segments.items():
            table_segments = len(segments)
            table_size = sum(s.size_bytes for s in segments)
            table_entries = sum(s.entry_count for s in segments)
            
            table_stats[table_name] = {
                'segments': table_segments,
                'size_bytes': table_size,
                'entries': table_entries,
                'row_counter': self._row_counters.get(table_name, 0)
            }
            
            total_segments += table_segments
            total_size += table_size
        
        return {
            'total_segments': total_segments,
            'total_size_bytes': total_size,
            'tables': table_stats
        }
