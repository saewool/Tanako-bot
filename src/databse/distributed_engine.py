"""
KotonexusTakako Distributed Database Engine
SSTable direct flush với encryption và horizontal scaling
"""

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Callable

from .storage import StorageManager, DataType, ColumnMetadata, BinaryEncoder, BinaryDecoder
from .index import IndexManager, IndexType
from .transaction import TransactionManager, Transaction, Operation, OperationType
from .cache import CacheManager, LRUCache, QueryCache
from .query import QueryBuilder, query
from .memtable import (
    MemTable, MemTableManager, SSTableWriter, SSTableReader, 
    FlushService, BloomFilter, SSTableMetadata
)
from .direct_flush import DirectFlushManager, DirectFlushWriter, DirectFlushReader
from .crypto import get_crypto_manager, encrypt_row, decrypt_row
from .cluster import (
    ClusterManager, NodeInfo, ConsistentHashRing, 
    NodeRegistry, DistributedCache, NodeClient
)


@dataclass
class Column:
    name: str
    data_type: DataType
    nullable: bool = True
    default: Any = None
    indexed: bool = False
    primary_key: bool = False
    unique: bool = False
    auto_increment: bool = False


@dataclass
class TableSchema:
    name: str
    columns: List[Column]
    primary_key: Optional[str] = None
    indexes: List[str] = field(default_factory=list)
    partition_key: Optional[str] = None  # For horizontal scaling (e.g., 'guild_id')
    created_at: datetime = field(default_factory=datetime.now)
    
    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def to_metadata(self) -> List[ColumnMetadata]:
        return [
            ColumnMetadata(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                default=col.default,
                indexed=col.indexed
            )
            for col in self.columns
        ]


@dataclass 
class Table:
    schema: TableSchema
    data: Dict[str, List[Any]] = field(default_factory=dict)
    row_count: int = 0
    _auto_increment_counter: int = 0
    
    def __post_init__(self):
        for col in self.schema.columns:
            if col.name not in self.data:
                self.data[col.name] = []


class DistributedColumnarDB:
    """
    KotonexusTakako Database Engine với:
    - SSTable direct flush (ghi trực tiếp xuống disk)
    - Fernet encryption cho data at rest
    - Horizontal scaling với fan-out reads across clusters
    - Partition by guild_id for distributed storage
    """
    
    def __init__(
        self,
        data_dir: str = "data/kotonexus_takako",
        node_id: Optional[str] = None,
        host: str = "0.0.0.0",
        port: int = 8080,
        cluster_enabled: bool = False,
        memtable_size_limit: int = 64 * 1024 * 1024,
        memtable_entry_limit: int = 100000,
        virtual_nodes: int = 150,
        node_weight: float = 1.0,
        use_direct_flush: bool = True
    ):
        self.data_dir = data_dir
        self.node_id = node_id or self._generate_node_id()
        self.host = host
        self.port = port
        self.cluster_enabled = cluster_enabled
        self.virtual_nodes = virtual_nodes
        self.node_weight = node_weight
        self.use_direct_flush = use_direct_flush
        
        self.storage = StorageManager(data_dir)
        self.index_manager = IndexManager()
        self.txn_manager = TransactionManager(os.path.join(data_dir, "wal"))
        self.cache_manager = CacheManager()
        self.query_cache = QueryCache(max_size=1000, ttl_seconds=60)
        
        self.crypto_manager = get_crypto_manager()
        
        if use_direct_flush:
            self.direct_flush_manager = DirectFlushManager(
                os.path.join(data_dir, "tables")
            )
            self.flush_service = None
            self.memtable_manager = None
        else:
            self.flush_service = FlushService(os.path.join(data_dir, "sstables"))
            self.memtable_manager = MemTableManager(
                os.path.join(data_dir, "sstables"),
                self.flush_service
            )
            self.direct_flush_manager = None
        
        self.memtable_size_limit = memtable_size_limit
        self.memtable_entry_limit = memtable_entry_limit
        
        if cluster_enabled:
            self.cluster_manager = ClusterManager(
                node_id=self.node_id,
                host=host,
                port=port,
                virtual_nodes=virtual_nodes,
                node_weight=node_weight
            )
        else:
            self.cluster_manager = None
        
        self._tables: Dict[str, Table] = {}
        self._schemas: Dict[str, TableSchema] = {}
        self._partition_keys: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        
        self._write_count = 0
        self._read_count = 0
        self._cache_hits = 0
        self._cache_misses = 0
    
    def _generate_node_id(self) -> str:
        import hashlib
        import socket
        hostname = socket.gethostname()
        return hashlib.md5(f"{hostname}:{time.time()}".encode()).hexdigest()[:12]
    
    async def initialize(self):
        """Initialize the database engine"""
        if self._initialized:
            return
        
        async with self._lock:
            os.makedirs(self.data_dir, exist_ok=True)
            
            if self.use_direct_flush:
                os.makedirs(os.path.join(self.data_dir, "tables"), exist_ok=True)
                await self.direct_flush_manager.initialize()
            else:
                os.makedirs(os.path.join(self.data_dir, "sstables"), exist_ok=True)
                await self.memtable_manager.initialize()
                await self.flush_service.start()
                await self.memtable_manager.start_compaction()
            
            table_names = await self.storage.list_tables()
            for table_name in table_names:
                if table_name.startswith('_'):
                    continue
                await self._load_table(table_name)
            
            await self._create_table_cache()
            
            if self.cluster_manager:
                await self.cluster_manager.start()
            
            self._initialized = True
    
    async def _create_table_cache(self):
        await self.cache_manager.create_cache(
            'tables',
            max_size=100,
            ttl_seconds=300
        )
        await self.cache_manager.create_cache(
            'rows',
            max_size=10000,
            max_memory_bytes=50 * 1024 * 1024,
            ttl_seconds=60
        )
    
    async def _load_table(self, table_name: str):
        result = await self.storage.read_table(table_name)
        if result is None:
            return
        
        columns, data, row_count = result
        
        schema_columns = [
            Column(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                default=col.default,
                indexed=col.indexed
            )
            for col in columns
        ]
        
        schema = TableSchema(name=table_name, columns=schema_columns)
        table = Table(schema=schema, data=data, row_count=row_count)
        
        self._schemas[table_name] = schema
        self._tables[table_name] = table
        
        col_metadata = [
            ColumnMetadata(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                default=col.default,
                indexed=col.indexed
            )
            for col in schema_columns
        ]
        
        if self.use_direct_flush and self.direct_flush_manager:
            await self.direct_flush_manager.register_table(table_name, col_metadata)
        elif self.memtable_manager:
            await self.memtable_manager.register_table(table_name, col_metadata)
        
        for col in columns:
            if col.indexed:
                self.index_manager.build_index(
                    table_name,
                    col.name,
                    data.get(col.name, []),
                    IndexType.BTREE
                )
    
    async def create_table(
        self,
        name: str,
        columns: List[Column],
        partition_key: Optional[str] = None,
        if_not_exists: bool = False
    ) -> bool:
        """Create a new table with optional partition key for horizontal scaling"""
        await self.initialize()
        
        async with self._lock:
            if name in self._schemas:
                if if_not_exists:
                    return True
                raise ValueError(f"Table {name} already exists")
            
            primary_key = None
            for col in columns:
                if col.primary_key:
                    primary_key = col.name
                    col.indexed = True
                    break
            
            schema = TableSchema(
                name=name,
                columns=columns,
                primary_key=primary_key,
                indexes=[col.name for col in columns if col.indexed],
                partition_key=partition_key
            )
            
            table = Table(schema=schema)
            
            self._schemas[name] = schema
            self._tables[name] = table
            
            if partition_key:
                self._partition_keys[name] = partition_key
            
            col_metadata = schema.to_metadata()
            
            if self.use_direct_flush and self.direct_flush_manager:
                await self.direct_flush_manager.register_table(name, col_metadata)
            elif self.memtable_manager:
                await self.memtable_manager.register_table(name, col_metadata)
            
            for col in columns:
                if col.indexed:
                    self.index_manager.create_index(name, col.name, IndexType.BTREE)
            
            await self._save_table(name)
            
            return True
    
    async def drop_table(self, name: str, if_exists: bool = False) -> bool:
        await self.initialize()
        
        async with self._lock:
            if name not in self._schemas:
                if if_exists:
                    return True
                raise ValueError(f"Table {name} does not exist")
            
            await self.storage.delete_table(name)
            self.index_manager.drop_table_indexes(name)
            await self.query_cache.invalidate_table(name)
            
            if name in self._partition_keys:
                del self._partition_keys[name]
            
            del self._schemas[name]
            del self._tables[name]
            
            return True
    
    async def _save_table(self, table_name: str):
        table = self._tables.get(table_name)
        if table is None:
            return
        
        metadata = table.schema.to_metadata()
        await self.storage.write_table(table_name, metadata, table.data)
    
    async def _get_partition_value(
        self,
        table_name: str,
        data: Dict[str, Any]
    ) -> Optional[int]:
        """Get partition key value (guild_id) from data"""
        partition_key = self._partition_keys.get(table_name)
        if partition_key and partition_key in data:
            val = data[partition_key]
            if isinstance(val, int):
                return val
        return None
    
    async def _is_local_owner(self, partition_value: Optional[int]) -> bool:
        """Check if this node owns the partition"""
        if not self.cluster_manager or partition_value is None:
            return True
        return await self.cluster_manager.is_owner(partition_value)
    
    async def insert(
        self,
        table_name: str,
        data: Dict[str, Any],
        transaction: Optional[Transaction] = None
    ) -> int:
        """Insert a row - uses SSTable direct flush or memtable depending on mode"""
        await self.initialize()
        
        partition_value = await self._get_partition_value(table_name, data)
        if not await self._is_local_owner(partition_value):
            if self.cluster_manager:
                result = await self.cluster_manager.write_data(
                    partition_value,  # type: ignore
                    table_name,
                    {'action': 'insert', 'data': data},
                    self._local_insert
                )
                return result if isinstance(result, int) else -1
        
        return await self._local_insert(table_name, data, transaction)
    
    async def _local_insert(
        self,
        table_name: str,
        data: Dict[str, Any],
        transaction: Optional[Transaction] = None
    ) -> int:
        """Insert locally using SSTable direct flush"""
        async with self._lock:
            if table_name not in self._tables:
                raise ValueError(f"Table {table_name} does not exist")
            
            table = self._tables[table_name]
            schema = table.schema
            
            row = {}
            for col in schema.columns:
                if col.name in data:
                    row[col.name] = data[col.name]
                elif col.auto_increment:
                    table._auto_increment_counter += 1
                    row[col.name] = table._auto_increment_counter
                elif col.default is not None:
                    row[col.name] = col.default
                elif not col.nullable:
                    raise ValueError(f"Column {col.name} cannot be null")
                else:
                    row[col.name] = None
            
            if self.use_direct_flush and self.direct_flush_manager:
                row_id = self.direct_flush_manager.get_next_row_id(table_name)
                await self.direct_flush_manager.insert(table_name, row_id, row)
            elif self.memtable_manager:
                memtable = await self.memtable_manager.get_memtable(table_name)
                if memtable:
                    row_id = memtable.get_next_row_id()
                    await memtable.insert(row_id, row)
                else:
                    row_id = table.row_count
                    for col_name, value in row.items():
                        if col_name not in table.data:
                            table.data[col_name] = []
                        table.data[col_name].append(value)
                    table.row_count += 1
            else:
                row_id = table.row_count
                for col_name, value in row.items():
                    if col_name not in table.data:
                        table.data[col_name] = []
                    table.data[col_name].append(value)
                table.row_count += 1
            
            for col in schema.columns:
                if col.indexed and col.name in row:
                    self.index_manager.insert_to_index(
                        table_name, col.name, row[col.name], row_id
                    )
            
            if transaction:
                op = Operation(
                    op_type=OperationType.INSERT,
                    table_name=table_name,
                    data=row,
                    row_id=row_id
                )
                await self.txn_manager.add_operation(transaction, op)
            
            await self.query_cache.invalidate_table(table_name)
            self._write_count += 1
            
            if self.cluster_manager:
                partition_value = await self._get_partition_value(table_name, row)
                if partition_value is not None:
                    await self.cluster_manager.broadcast_invalidation(
                        partition_value, table_name
                    )
            
            return row_id
    
    async def insert_many(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        transaction: Optional[Transaction] = None
    ) -> List[int]:
        """Batch insert rows"""
        row_ids = []
        for row in rows:
            row_id = await self.insert(table_name, row, transaction)
            row_ids.append(row_id)
        return row_ids
    
    async def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        condition: Optional[QueryBuilder] = None,
        transaction: Optional[Transaction] = None
    ) -> int:
        await self.initialize()
        
        async with self._lock:
            if table_name not in self._tables:
                raise ValueError(f"Table {table_name} does not exist")
            
            table = self._tables[table_name]
            schema = table.schema
            
            if condition is None:
                row_ids = list(range(table.row_count))
            else:
                row_ids = []
                for i in range(table.row_count):
                    row = {col: table.data[col][i] for col in table.data}
                    if condition._conditions.evaluate(row):
                        row_ids.append(i)
            
            updated = 0
            for row_id in row_ids:
                if self.use_direct_flush and self.direct_flush_manager:
                    await self.direct_flush_manager.update(table_name, row_id, data)
                elif self.memtable_manager:
                    memtable = await self.memtable_manager.get_memtable(table_name)
                    if memtable:
                        await memtable.update(row_id, data)
                    else:
                        for col_name, new_value in data.items():
                            col = schema.get_column(col_name)
                            if col is None:
                                continue
                            if col.indexed:
                                old_value = table.data[col_name][row_id]
                                self.index_manager.update_index(
                                    table_name, col_name, old_value, new_value, row_id
                                )
                            table.data[col_name][row_id] = new_value
                else:
                    for col_name, new_value in data.items():
                        col = schema.get_column(col_name)
                        if col is None:
                            continue
                        if col.indexed:
                            old_value = table.data[col_name][row_id]
                            self.index_manager.update_index(
                                table_name, col_name, old_value, new_value, row_id
                            )
                        table.data[col_name][row_id] = new_value
                updated += 1
            
            if transaction:
                op = Operation(
                    op_type=OperationType.UPDATE,
                    table_name=table_name,
                    data={'updates': data, 'row_ids': row_ids}
                )
                await self.txn_manager.add_operation(transaction, op)
            
            await self.query_cache.invalidate_table(table_name)
            self._write_count += 1
            
            return updated
    
    async def delete(
        self,
        table_name: str,
        condition: Optional[QueryBuilder] = None,
        transaction: Optional[Transaction] = None
    ) -> int:
        await self.initialize()
        
        async with self._lock:
            if table_name not in self._tables:
                raise ValueError(f"Table {table_name} does not exist")
            
            table = self._tables[table_name]
            schema = table.schema
            
            row_ids_to_delete: List[int] = []
            
            if condition is None:
                deleted = table.row_count
                if self.use_direct_flush and self.direct_flush_manager:
                    await self.direct_flush_manager.clear_table(table_name)
                else:
                    for col_name in table.data:
                        table.data[col_name] = []
                table.row_count = 0
                self.index_manager.clear_table_indexes(table_name)
            else:
                for i in range(table.row_count):
                    row = {col: table.data[col][i] for col in table.data}
                    if condition._conditions.evaluate(row):
                        row_ids_to_delete.append(i)
                
                for row_id in sorted(row_ids_to_delete, reverse=True):
                    if self.use_direct_flush and self.direct_flush_manager:
                        await self.direct_flush_manager.delete(table_name, row_id)
                    elif self.memtable_manager:
                        memtable = await self.memtable_manager.get_memtable(table_name)
                        if memtable:
                            await memtable.delete(row_id)
                        else:
                            for col_name in table.data:
                                col = schema.get_column(col_name)
                                if col and col.indexed:
                                    value = table.data[col_name][row_id]
                                    self.index_manager.delete_from_index(
                                        table_name, col_name, value, row_id
                                    )
                                del table.data[col_name][row_id]
                            table.row_count -= 1
                    else:
                        for col_name in table.data:
                            col = schema.get_column(col_name)
                            if col and col.indexed:
                                value = table.data[col_name][row_id]
                                self.index_manager.delete_from_index(
                                    table_name, col_name, value, row_id
                                )
                            del table.data[col_name][row_id]
                        table.row_count -= 1
                
                deleted = len(row_ids_to_delete)
            
            if transaction:
                op = Operation(
                    op_type=OperationType.DELETE,
                    table_name=table_name,
                    data={'row_ids': row_ids_to_delete if condition else 'all'}
                )
                await self.txn_manager.add_operation(transaction, op)
            
            await self.query_cache.invalidate_table(table_name)
            self._write_count += 1
            
            return deleted
    
    async def select(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        condition: Optional[QueryBuilder] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        partition_value: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Select rows - checks local data, memtable, SSTables, and remote nodes
        """
        await self.initialize()
        
        if partition_value and self.cluster_manager:
            if not await self._is_local_owner(partition_value):
                cached = await self.cluster_manager.distributed_cache.get(
                    partition_value, table_name
                )
                if cached:
                    self._cache_hits += 1
                    return self._filter_results(
                        cached.get('rows', []), columns, condition, 
                        order_by, limit, offset
                    )
                self._cache_misses += 1
        
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        cache_key = {
            'columns': columns,
            'condition': condition.to_dict() if condition else None,
            'order_by': order_by,
            'limit': limit,
            'offset': offset,
            'partition_value': partition_value
        }
        
        cached = await self.query_cache.get(table_name, cache_key)
        if cached is not None:
            self._cache_hits += 1
            return cached
        
        self._cache_misses += 1
        
        table = self._tables[table_name]
        schema = table.schema
        
        all_results: Dict[int, Dict[str, Any]] = {}
        
        if self.use_direct_flush and self.direct_flush_manager:
            direct_flush_results = await self.direct_flush_manager.scan(table_name)
            for row_id, row_data in direct_flush_results:
                all_results[row_id] = row_data
        elif self.memtable_manager:
            memtable_results = await self.memtable_manager.scan(table_name)
            for i in range(table.row_count):
                row = {col: table.data[col][i] for col in table.data}
                all_results[i] = row
            for row_id, row_data in memtable_results:
                all_results[row_id] = row_data
        else:
            for i in range(table.row_count):
                row = {col: table.data[col][i] for col in table.data}
                all_results[i] = row
        
        results = list(all_results.values())
        
        results = self._filter_results(
            results, columns, condition, order_by, limit, offset
        )
        
        await self.query_cache.set(table_name, cache_key, results)
        self._read_count += 1
        
        return results
    
    def _filter_results(
        self,
        results: List[Dict[str, Any]],
        columns: Optional[List[str]],
        condition: Optional[QueryBuilder],
        order_by: Optional[List[Tuple[str, str]]],
        limit: Optional[int],
        offset: int
    ) -> List[Dict[str, Any]]:
        """Apply filters to results"""
        if condition:
            results = [
                r for r in results
                if condition._conditions.evaluate(r)
            ]
        
        if order_by:
            for col, direction in reversed(order_by):
                reverse = direction.upper() == 'DESC'
                results.sort(key=lambda x: (x.get(col) is None, x.get(col)), reverse=reverse)
        
        if offset > 0:
            results = results[offset:]
        
        if limit is not None:
            results = results[:limit]
        
        if columns:
            results = [
                {k: v for k, v in r.items() if k in columns}
                for r in results
            ]
        
        return results
    
    async def find_one(
        self,
        table_name: str,
        condition: QueryBuilder
    ) -> Optional[Dict[str, Any]]:
        results = await self.select(table_name, condition=condition, limit=1)
        return results[0] if results else None
    
    async def find_by_id(
        self,
        table_name: str,
        id_column: str,
        id_value: Any
    ) -> Optional[Dict[str, Any]]:
        if table_name not in self._tables:
            return None
        
        table = self._tables[table_name]
        
        if self.use_direct_flush and self.direct_flush_manager:
            direct_result = await self.direct_flush_manager.find_by_column(
                table_name, id_column, id_value
            )
            if direct_result:
                return direct_result
        elif self.memtable_manager:
            memtable_result = await self.memtable_manager.find_by_column(
                table_name, id_column, id_value
            )
            if memtable_result:
                return memtable_result
        
        if self.index_manager.has_index(table_name, id_column):
            row_ids = self.index_manager.search_index(table_name, id_column, id_value)
            if row_ids:
                row_id = row_ids[0]
                if row_id < table.row_count:
                    return {col: table.data[col][row_id] for col in table.data if row_id < len(table.data.get(col, []))}
        
        q = query(table_name).where_eq(id_column, id_value)
        return await self.find_one(table_name, q)
    
    async def count(
        self,
        table_name: str,
        condition: Optional[QueryBuilder] = None
    ) -> int:
        await self.initialize()
        
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        table = self._tables[table_name]
        
        if condition is None:
            if self.use_direct_flush and self.direct_flush_manager:
                return self.direct_flush_manager.get_entry_count(table_name)
            elif self.memtable_manager:
                memtable = await self.memtable_manager.get_memtable(table_name)
                memtable_count = memtable.entry_count if memtable else 0
                return table.row_count + memtable_count
            else:
                return table.row_count
        
        results = await self.select(table_name, condition=condition)
        return len(results)
    
    async def exists(
        self,
        table_name: str,
        condition: QueryBuilder
    ) -> bool:
        return await self.count(table_name, condition) > 0
    
    def q(self, table_name: str) -> QueryBuilder:
        """Create a query builder for a table"""
        return query(table_name)
    
    async def begin_transaction(self) -> Transaction:
        return await self.txn_manager.begin()
    
    async def commit(self, transaction: Transaction):
        await self.txn_manager.commit(transaction)
        
        if not self.use_direct_flush and self.memtable_manager and self.flush_service:
            for op in transaction.operations:
                if op.op_type in (OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE):
                    memtable = await self.memtable_manager.get_memtable(op.table_name)
                    if memtable and memtable.should_flush():
                        await self.flush_service.schedule_flush(memtable)
    
    async def rollback(self, transaction: Transaction):
        await self.txn_manager.abort(transaction)
    
    async def flush_all(self):
        """Force flush all data to disk"""
        if self.use_direct_flush and self.direct_flush_manager:
            await self.direct_flush_manager.flush_all()
        elif self.memtable_manager and self.flush_service:
            for table_name in self._tables:
                memtable = await self.memtable_manager.get_memtable(table_name)
                if memtable and memtable.entry_count > 0:
                    await self.flush_service.flush_immediately(memtable)
        
        for table_name in self._tables:
            await self._save_table(table_name)
    
    async def list_tables(self) -> List[str]:
        await self.initialize()
        return list(self._schemas.keys())
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        await self.initialize()
        
        if table_name not in self._tables:
            return None
        
        table = self._tables[table_name]
        schema = table.schema
        
        if self.use_direct_flush and self.direct_flush_manager:
            direct_flush_entries = self.direct_flush_manager.get_entry_count(table_name)
            storage_entries = direct_flush_entries
        elif self.memtable_manager:
            memtable = await self.memtable_manager.get_memtable(table_name)
            storage_entries = memtable.entry_count if memtable else 0
        else:
            storage_entries = 0
        
        return {
            'name': table_name,
            'columns': [
                {
                    'name': col.name,
                    'type': col.data_type.name,
                    'nullable': col.nullable,
                    'indexed': col.indexed,
                    'primary_key': col.primary_key,
                    'auto_increment': col.auto_increment
                }
                for col in schema.columns
            ],
            'row_count': table.row_count,
            'storage_entries': storage_entries,
            'storage_mode': 'direct_flush' if self.use_direct_flush else 'memtable',
            'partition_key': schema.partition_key,
            'indexes': self.index_manager.list_indexes(table_name),
            'size_bytes': await self.storage.get_table_size(table_name),
            'created_at': schema.created_at.isoformat()
        }
    
    async def stats(self) -> Dict[str, Any]:
        await self.initialize()
        
        total_rows = sum(t.row_count for t in self._tables.values())
        sizes = [await self.storage.get_table_size(name) for name in self._tables]
        total_size = sum(sizes)
        
        stats = {
            'node_id': self.node_id,
            'cluster_enabled': self.cluster_enabled,
            'storage_mode': 'direct_flush' if self.use_direct_flush else 'memtable',
            'tables': len(self._tables),
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'write_count': self._write_count,
            'read_count': self._read_count,
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'cache_hit_rate': self._cache_hits / (self._cache_hits + self._cache_misses) 
                if (self._cache_hits + self._cache_misses) > 0 else 0,
            'cache_stats': self.cache_manager.stats(),
            'query_cache_stats': self.query_cache.stats(),
        }
        
        if self.use_direct_flush and self.direct_flush_manager:
            stats['direct_flush_stats'] = self.direct_flush_manager.stats()
        elif self.memtable_manager and self.flush_service:
            stats['memtable_stats'] = self.memtable_manager.stats()
            stats['flush_pending'] = self.flush_service.pending_count()
        
        if self.cluster_manager:
            stats['cluster'] = self.cluster_manager.stats()
        
        return stats
    
    async def close(self):
        """Shutdown the database engine"""
        await self.flush_all()
        
        if self.use_direct_flush and self.direct_flush_manager:
            await self.direct_flush_manager.close()
        elif self.memtable_manager and self.flush_service:
            await self.memtable_manager.wait_for_pending_flushes()
            await self.memtable_manager.stop_compaction()
            await self.flush_service.stop()
        
        if self.cluster_manager:
            await self.cluster_manager.stop()
        
        await self.cache_manager.clear_all()
        await self.query_cache.clear()
    
    async def join_cluster(self, seed_nodes: List[str]):
        """Join an existing cluster"""
        if self.cluster_manager:
            await self.cluster_manager.registry.join_cluster(seed_nodes)
    
    async def get_cluster_stats(self) -> Optional[Dict[str, Any]]:
        """Get cluster statistics"""
        if self.cluster_manager:
            return self.cluster_manager.stats()
        return None
