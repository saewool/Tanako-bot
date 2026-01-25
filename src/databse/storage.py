"""
Binary Storage Manager for Columnar Database
Handles binary encoding/decoding and file I/O operations
"""

import struct
import os
import asyncio
import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Dict, Tuple, Union
from datetime import datetime
import json
import zlib


class DataType(Enum):
    INT32 = 1
    INT64 = 2
    FLOAT32 = 3
    FLOAT64 = 4
    STRING = 5
    BYTES = 6
    BOOL = 7
    TIMESTAMP = 8
    JSON = 9
    NULL = 10
    ARRAY = 11


@dataclass
class ColumnMetadata:
    name: str
    data_type: DataType
    nullable: bool = True
    default: Any = None
    indexed: bool = False
    compressed: bool = True


class BinaryEncoder:
    MAGIC_HEADER = b'COLDB'
    VERSION = 1
    
    TYPE_FORMATS = {
        DataType.INT32: ('i', 4),
        DataType.INT64: ('q', 8),
        DataType.FLOAT32: ('f', 4),
        DataType.FLOAT64: ('d', 8),
        DataType.BOOL: ('?', 1),
        DataType.TIMESTAMP: ('q', 8),
    }
    
    @classmethod
    def encode_header(cls, table_name: str, columns: List[ColumnMetadata], row_count: int) -> bytes:
        header = bytearray()
        header.extend(cls.MAGIC_HEADER)
        header.extend(struct.pack('B', cls.VERSION))
        name_bytes = table_name.encode('utf-8')
        header.extend(struct.pack('H', len(name_bytes)))
        header.extend(name_bytes)
        header.extend(struct.pack('I', len(columns)))
        header.extend(struct.pack('Q', row_count))
        
        for col in columns:
            col_name = col.name.encode('utf-8')
            header.extend(struct.pack('H', len(col_name)))
            header.extend(col_name)
            header.extend(struct.pack('B', col.data_type.value))
            flags = (col.nullable << 0) | (col.indexed << 1) | (col.compressed << 2)
            header.extend(struct.pack('B', flags))
            
            if col.default is not None:
                default_bytes = cls.encode_value(col.default, col.data_type)
                header.extend(struct.pack('I', len(default_bytes)))
                header.extend(default_bytes)
            else:
                header.extend(struct.pack('I', 0))
        
        return bytes(header)
    
    @classmethod
    def encode_value(cls, value: Any, data_type: DataType) -> bytes:
        if value is None:
            return struct.pack('B', 1)
        
        result = bytearray()
        result.extend(struct.pack('B', 0))
        
        if data_type in cls.TYPE_FORMATS:
            fmt, _ = cls.TYPE_FORMATS[data_type]
            if data_type == DataType.TIMESTAMP:
                if isinstance(value, datetime):
                    value = int(value.timestamp() * 1000)
                elif isinstance(value, (int, float)):
                    value = int(value)
            result.extend(struct.pack(fmt, value))
        elif data_type == DataType.STRING:
            encoded = value.encode('utf-8') if isinstance(value, str) else str(value).encode('utf-8')
            result.extend(struct.pack('I', len(encoded)))
            result.extend(encoded)
        elif data_type == DataType.BYTES:
            if isinstance(value, str):
                value = value.encode('utf-8')
            result.extend(struct.pack('I', len(value)))
            result.extend(value)
        elif data_type == DataType.JSON:
            json_str = json.dumps(value, ensure_ascii=False, default=str)
            encoded = json_str.encode('utf-8')
            result.extend(struct.pack('I', len(encoded)))
            result.extend(encoded)
        elif data_type == DataType.ARRAY:
            json_str = json.dumps(value, ensure_ascii=False, default=str)
            encoded = json_str.encode('utf-8')
            result.extend(struct.pack('I', len(encoded)))
            result.extend(encoded)
        elif data_type == DataType.NULL:
            pass
        
        return bytes(result)
    
    @classmethod
    def encode_column(cls, values: List[Any], data_type: DataType, compress: bool = True) -> bytes:
        column_data = bytearray()
        column_data.extend(struct.pack('I', len(values)))
        
        for value in values:
            encoded = cls.encode_value(value, data_type)
            column_data.extend(encoded)
        
        data = bytes(column_data)
        
        if compress and len(data) > 100:
            compressed = zlib.compress(data, level=6)
            if len(compressed) < len(data):
                return struct.pack('B', 1) + struct.pack('I', len(data)) + compressed
        
        return struct.pack('B', 0) + data
    
    @classmethod
    def compute_checksum(cls, data: bytes) -> bytes:
        return hashlib.sha256(data).digest()[:8]


class BinaryDecoder:
    @classmethod
    def decode_header(cls, data: bytes) -> Tuple[str, List[ColumnMetadata], int, int]:
        offset = 0
        
        magic = data[offset:offset+5]
        offset += 5
        if magic != BinaryEncoder.MAGIC_HEADER:
            raise ValueError("Invalid database file: wrong magic header")
        
        version = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        name_len = struct.unpack('H', data[offset:offset+2])[0]
        offset += 2
        table_name = data[offset:offset+name_len].decode('utf-8')
        offset += name_len
        
        col_count = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        row_count = struct.unpack('Q', data[offset:offset+8])[0]
        offset += 8
        
        columns = []
        for _ in range(col_count):
            col_name_len = struct.unpack('H', data[offset:offset+2])[0]
            offset += 2
            col_name = data[offset:offset+col_name_len].decode('utf-8')
            offset += col_name_len
            
            data_type_val = struct.unpack('B', data[offset:offset+1])[0]
            offset += 1
            data_type = DataType(data_type_val)
            
            flags = struct.unpack('B', data[offset:offset+1])[0]
            offset += 1
            nullable = bool(flags & 1)
            indexed = bool(flags & 2)
            compressed = bool(flags & 4)
            
            default_len = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            default = None
            if default_len > 0:
                default_data = data[offset:offset+default_len]
                offset += default_len
                default, _ = cls.decode_value(default_data, data_type, 0)
            
            columns.append(ColumnMetadata(
                name=col_name,
                data_type=data_type,
                nullable=nullable,
                default=default,
                indexed=indexed,
                compressed=compressed
            ))
        
        return table_name, columns, row_count, offset
    
    @classmethod
    def decode_value(cls, data: bytes, data_type: DataType, offset: int) -> Tuple[Any, int]:
        is_null = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        if is_null:
            return None, offset
        
        if data_type in BinaryEncoder.TYPE_FORMATS:
            fmt, size = BinaryEncoder.TYPE_FORMATS[data_type]
            value = struct.unpack(fmt, data[offset:offset+size])[0]
            if data_type == DataType.TIMESTAMP:
                value = datetime.fromtimestamp(value / 1000)
            return value, offset + size
        elif data_type == DataType.STRING:
            str_len = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            value = data[offset:offset+str_len].decode('utf-8')
            return value, offset + str_len
        elif data_type == DataType.BYTES:
            bytes_len = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            value = data[offset:offset+bytes_len]
            return value, offset + bytes_len
        elif data_type in (DataType.JSON, DataType.ARRAY):
            json_len = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            json_str = data[offset:offset+json_len].decode('utf-8')
            value = json.loads(json_str)
            return value, offset + json_len
        elif data_type == DataType.NULL:
            return None, offset
        
        return None, offset
    
    @classmethod
    def decode_column(cls, data: bytes, data_type: DataType, offset: int = 0) -> Tuple[List[Any], int]:
        is_compressed = struct.unpack('B', data[offset:offset+1])[0]
        offset += 1
        
        if is_compressed:
            original_size = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            remaining = data[offset:]
            decompressed = zlib.decompress(remaining)
            data = decompressed
            offset = 0
        
        count = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        values = []
        for _ in range(count):
            value, offset = cls.decode_value(data, data_type, offset)
            values.append(value)
        
        return values, offset


class StorageManager:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self._locks: Dict[str, asyncio.Lock] = {}
        os.makedirs(base_path, exist_ok=True)
    
    def _get_lock(self, table_name: str) -> asyncio.Lock:
        if table_name not in self._locks:
            self._locks[table_name] = asyncio.Lock()
        return self._locks[table_name]
    
    def _get_table_path(self, table_name: str) -> str:
        return os.path.join(self.base_path, f"{table_name}.coldb")
    
    def _get_index_path(self, table_name: str, column_name: str) -> str:
        return os.path.join(self.base_path, f"{table_name}_{column_name}.idx")
    
    def _get_wal_path(self, table_name: str) -> str:
        return os.path.join(self.base_path, f"{table_name}.wal")
    
    async def write_table(
        self,
        table_name: str,
        columns: List[ColumnMetadata],
        data: Dict[str, List[Any]]
    ) -> bool:
        lock = self._get_lock(table_name)
        async with lock:
            try:
                if not data or not columns:
                    row_count = 0
                else:
                    first_col = columns[0].name
                    row_count = len(data.get(first_col, []))
                
                header = BinaryEncoder.encode_header(table_name, columns, row_count)
                
                column_data = bytearray()
                for col in columns:
                    values = data.get(col.name, [])
                    encoded = BinaryEncoder.encode_column(values, col.data_type, col.compressed)
                    column_data.extend(struct.pack('I', len(encoded)))
                    column_data.extend(encoded)
                
                full_data = header + bytes(column_data)
                checksum = BinaryEncoder.compute_checksum(full_data)
                
                path = self._get_table_path(table_name)
                temp_path = path + '.tmp'
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._write_file, temp_path, checksum + full_data)
                await loop.run_in_executor(None, os.replace, temp_path, path)
                
                return True
            except Exception as e:
                print(f"Error writing table {table_name}: {e}")
                return False
    
    def _write_file(self, path: str, data: bytes):
        with open(path, 'wb') as f:
            f.write(data)
    
    def _read_file(self, path: str) -> bytes:
        with open(path, 'rb') as f:
            return f.read()
    
    async def read_table(self, table_name: str) -> Optional[Tuple[List[ColumnMetadata], Dict[str, List[Any]], int]]:
        lock = self._get_lock(table_name)
        async with lock:
            try:
                path = self._get_table_path(table_name)
                if not os.path.exists(path):
                    return None
                
                loop = asyncio.get_event_loop()
                data = await loop.run_in_executor(None, self._read_file, path)
                
                stored_checksum = data[:8]
                actual_data = data[8:]
                
                computed_checksum = BinaryEncoder.compute_checksum(actual_data)
                if stored_checksum != computed_checksum:
                    print(f"Checksum mismatch for table {table_name}")
                    return None
                
                table_name_read, columns, row_count, offset = BinaryDecoder.decode_header(actual_data)
                
                column_data: Dict[str, List[Any]] = {}
                for col in columns:
                    col_size = struct.unpack('I', actual_data[offset:offset+4])[0]
                    offset += 4
                    
                    col_data = actual_data[offset:offset+col_size]
                    values, _ = BinaryDecoder.decode_column(col_data, col.data_type)
                    column_data[col.name] = values
                    offset += col_size
                
                return columns, column_data, row_count
            except Exception as e:
                print(f"Error reading table {table_name}: {e}")
                return None
    
    async def table_exists(self, table_name: str) -> bool:
        path = self._get_table_path(table_name)
        return os.path.exists(path)
    
    async def delete_table(self, table_name: str) -> bool:
        lock = self._get_lock(table_name)
        async with lock:
            try:
                path = self._get_table_path(table_name)
                if os.path.exists(path):
                    os.remove(path)
                
                for f in os.listdir(self.base_path):
                    if f.startswith(f"{table_name}_") and f.endswith('.idx'):
                        os.remove(os.path.join(self.base_path, f))
                
                wal_path = self._get_wal_path(table_name)
                if os.path.exists(wal_path):
                    os.remove(wal_path)
                
                return True
            except Exception as e:
                print(f"Error deleting table {table_name}: {e}")
                return False
    
    async def list_tables(self) -> List[str]:
        tables = []
        for f in os.listdir(self.base_path):
            if f.endswith('.coldb'):
                tables.append(f[:-6])
        return tables
    
    async def get_table_size(self, table_name: str) -> int:
        path = self._get_table_path(table_name)
        if os.path.exists(path):
            return os.path.getsize(path)
        return 0
    
    async def backup_table(self, table_name: str, backup_path: str) -> bool:
        lock = self._get_lock(table_name)
        async with lock:
            try:
                src = self._get_table_path(table_name)
                if not os.path.exists(src):
                    return False
                
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                
                loop = asyncio.get_event_loop()
                data = await loop.run_in_executor(None, self._read_file, src)
                await loop.run_in_executor(None, self._write_file, backup_path, data)
                
                return True
            except Exception as e:
                print(f"Error backing up table {table_name}: {e}")
                return False
    
    async def restore_table(self, table_name: str, backup_path: str) -> bool:
        lock = self._get_lock(table_name)
        async with lock:
            try:
                if not os.path.exists(backup_path):
                    return False
                
                dst = self._get_table_path(table_name)
                
                loop = asyncio.get_event_loop()
                data = await loop.run_in_executor(None, self._read_file, backup_path)
                await loop.run_in_executor(None, self._write_file, dst, data)
                
                return True
            except Exception as e:
                print(f"Error restoring table {table_name}: {e}")
                return False
