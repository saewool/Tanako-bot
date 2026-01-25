"""
Transaction Manager for Columnar Database
Provides ACID transaction support with WAL (Write-Ahead Logging)
"""

import asyncio
import os
import struct
import time
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable
from datetime import datetime
import uuid


class TransactionState(Enum):
    ACTIVE = 1
    COMMITTED = 2
    ABORTED = 3
    PENDING = 4


class OperationType(Enum):
    INSERT = 1
    UPDATE = 2
    DELETE = 3
    CREATE_TABLE = 4
    DROP_TABLE = 5


@dataclass
class Operation:
    op_type: OperationType
    table_name: str
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    row_id: Optional[int] = None


@dataclass
class Transaction:
    id: str
    state: TransactionState
    operations: List[Operation] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    commit_time: Optional[float] = None
    
    def add_operation(self, operation: Operation):
        if self.state != TransactionState.ACTIVE:
            raise RuntimeError(f"Cannot add operation to {self.state.name} transaction")
        self.operations.append(operation)
    
    def is_active(self) -> bool:
        return self.state == TransactionState.ACTIVE
    
    def mark_committed(self):
        self.state = TransactionState.COMMITTED
        self.commit_time = time.time()
    
    def mark_aborted(self):
        self.state = TransactionState.ABORTED


class WriteAheadLog:
    MAGIC = b'WAL1'
    
    def __init__(self, wal_dir: str):
        self.wal_dir = wal_dir
        self._lock = asyncio.Lock()
        os.makedirs(wal_dir, exist_ok=True)
        self._current_file: Optional[str] = None
        self._file_handle = None
    
    def _get_wal_path(self, name: str) -> str:
        return os.path.join(self.wal_dir, f"{name}.wal")
    
    async def _ensure_file(self):
        if self._current_file is None:
            self._current_file = f"wal_{int(time.time() * 1000)}"
    
    async def write_operation(self, txn_id: str, operation: Operation):
        async with self._lock:
            await self._ensure_file()
            
            entry = {
                'txn_id': txn_id,
                'op_type': operation.op_type.value,
                'table_name': operation.table_name,
                'data': operation.data,
                'timestamp': operation.timestamp,
                'row_id': operation.row_id
            }
            
            entry_bytes = json.dumps(entry, default=str).encode('utf-8')
            
            record = bytearray()
            record.extend(self.MAGIC)
            record.extend(struct.pack('I', len(entry_bytes)))
            record.extend(entry_bytes)
            record.extend(struct.pack('I', self._crc32(entry_bytes)))
            
            path = self._get_wal_path(self._current_file)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._append_file, path, bytes(record))
    
    def _append_file(self, path: str, data: bytes):
        with open(path, 'ab') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    
    async def write_commit(self, txn_id: str):
        async with self._lock:
            await self._ensure_file()
            
            entry = {
                'txn_id': txn_id,
                'type': 'COMMIT',
                'timestamp': time.time()
            }
            
            entry_bytes = json.dumps(entry).encode('utf-8')
            
            record = bytearray()
            record.extend(self.MAGIC)
            record.extend(struct.pack('I', len(entry_bytes)))
            record.extend(entry_bytes)
            record.extend(struct.pack('I', self._crc32(entry_bytes)))
            
            path = self._get_wal_path(self._current_file)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._append_file, path, bytes(record))
    
    async def write_abort(self, txn_id: str):
        async with self._lock:
            await self._ensure_file()
            
            entry = {
                'txn_id': txn_id,
                'type': 'ABORT',
                'timestamp': time.time()
            }
            
            entry_bytes = json.dumps(entry).encode('utf-8')
            
            record = bytearray()
            record.extend(self.MAGIC)
            record.extend(struct.pack('I', len(entry_bytes)))
            record.extend(entry_bytes)
            record.extend(struct.pack('I', self._crc32(entry_bytes)))
            
            path = self._get_wal_path(self._current_file)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._append_file, path, bytes(record))
    
    def _crc32(self, data: bytes) -> int:
        import zlib
        return zlib.crc32(data) & 0xffffffff
    
    async def recover(self) -> List[Dict]:
        recovered = []
        
        wal_files = sorted([
            f for f in os.listdir(self.wal_dir) if f.endswith('.wal')
        ])
        
        for wal_file in wal_files:
            path = os.path.join(self.wal_dir, wal_file)
            
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(None, self._read_file, path)
            
            offset = 0
            while offset < len(data):
                if data[offset:offset+4] != self.MAGIC:
                    break
                offset += 4
                
                entry_len = struct.unpack('I', data[offset:offset+4])[0]
                offset += 4
                
                entry_bytes = data[offset:offset+entry_len]
                offset += entry_len
                
                stored_crc = struct.unpack('I', data[offset:offset+4])[0]
                offset += 4
                
                if self._crc32(entry_bytes) == stored_crc:
                    entry = json.loads(entry_bytes.decode('utf-8'))
                    recovered.append(entry)
        
        return recovered
    
    def _read_file(self, path: str) -> bytes:
        with open(path, 'rb') as f:
            return f.read()
    
    async def cleanup(self, before_timestamp: Optional[float] = None):
        if before_timestamp is None:
            before_timestamp = time.time() - 86400
        
        wal_files = [
            f for f in os.listdir(self.wal_dir) if f.endswith('.wal')
        ]
        
        for wal_file in wal_files:
            try:
                ts = int(wal_file.replace('wal_', '').replace('.wal', ''))
                if ts / 1000 < before_timestamp:
                    os.remove(os.path.join(self.wal_dir, wal_file))
            except (ValueError, OSError):
                pass


class TransactionManager:
    def __init__(self, wal_dir: str = "data/wal"):
        self.wal = WriteAheadLog(wal_dir)
        self._transactions: Dict[str, Transaction] = {}
        self._lock = asyncio.Lock()
        self._commit_callbacks: List[Callable[[Transaction], Awaitable[None]]] = []
        self._abort_callbacks: List[Callable[[Transaction], Awaitable[None]]] = []
    
    def on_commit(self, callback: Callable[[Transaction], Awaitable[None]]):
        self._commit_callbacks.append(callback)
    
    def on_abort(self, callback: Callable[[Transaction], Awaitable[None]]):
        self._abort_callbacks.append(callback)
    
    async def begin(self) -> Transaction:
        async with self._lock:
            txn_id = str(uuid.uuid4())
            txn = Transaction(id=txn_id, state=TransactionState.ACTIVE)
            self._transactions[txn_id] = txn
            return txn
    
    async def add_operation(self, txn: Transaction, operation: Operation):
        async with self._lock:
            if txn.id not in self._transactions:
                raise ValueError(f"Transaction {txn.id} not found")
            
            if txn.state != TransactionState.ACTIVE:
                raise RuntimeError(f"Transaction is {txn.state.name}")
            
            await self.wal.write_operation(txn.id, operation)
            txn.add_operation(operation)
    
    async def commit(self, txn: Transaction) -> bool:
        async with self._lock:
            if txn.id not in self._transactions:
                raise ValueError(f"Transaction {txn.id} not found")
            
            if txn.state != TransactionState.ACTIVE:
                raise RuntimeError(f"Cannot commit {txn.state.name} transaction")
            
            try:
                await self.wal.write_commit(txn.id)
                txn.mark_committed()
                
                for callback in self._commit_callbacks:
                    await callback(txn)
                
                return True
            except Exception as e:
                print(f"Error committing transaction {txn.id}: {e}")
                return False
    
    async def abort(self, txn: Transaction) -> bool:
        async with self._lock:
            if txn.id not in self._transactions:
                raise ValueError(f"Transaction {txn.id} not found")
            
            if txn.state != TransactionState.ACTIVE:
                return False
            
            try:
                await self.wal.write_abort(txn.id)
                txn.mark_aborted()
                
                for callback in self._abort_callbacks:
                    await callback(txn)
                
                return True
            except Exception as e:
                print(f"Error aborting transaction {txn.id}: {e}")
                return False
    
    async def recover(self) -> List[Transaction]:
        entries = await self.wal.recover()
        
        txn_entries: Dict[str, List[Dict]] = {}
        for entry in entries:
            txn_id = entry.get('txn_id')
            if txn_id:
                if txn_id not in txn_entries:
                    txn_entries[txn_id] = []
                txn_entries[txn_id].append(entry)
        
        recovered_txns = []
        for txn_id, ops in txn_entries.items():
            has_commit = any(e.get('type') == 'COMMIT' for e in ops)
            has_abort = any(e.get('type') == 'ABORT' for e in ops)
            
            if has_commit:
                state = TransactionState.COMMITTED
            elif has_abort:
                state = TransactionState.ABORTED
            else:
                state = TransactionState.PENDING
            
            txn = Transaction(id=txn_id, state=state)
            
            for entry in ops:
                if 'op_type' in entry:
                    op = Operation(
                        op_type=OperationType(entry['op_type']),
                        table_name=entry['table_name'],
                        data=entry['data'],
                        timestamp=entry['timestamp'],
                        row_id=entry.get('row_id')
                    )
                    txn.operations.append(op)
            
            recovered_txns.append(txn)
            self._transactions[txn_id] = txn
        
        return recovered_txns
    
    def get_transaction(self, txn_id: str) -> Optional[Transaction]:
        return self._transactions.get(txn_id)
    
    def get_active_transactions(self) -> List[Transaction]:
        return [
            txn for txn in self._transactions.values()
            if txn.state == TransactionState.ACTIVE
        ]
    
    async def cleanup(self, max_age_seconds: int = 3600):
        current_time = time.time()
        
        to_remove = []
        for txn_id, txn in self._transactions.items():
            if txn.state in (TransactionState.COMMITTED, TransactionState.ABORTED):
                if current_time - txn.start_time > max_age_seconds:
                    to_remove.append(txn_id)
        
        for txn_id in to_remove:
            del self._transactions[txn_id]
        
        await self.wal.cleanup(current_time - max_age_seconds)
