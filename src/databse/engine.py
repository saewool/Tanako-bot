"""
Main Columnar Database Engine
The core database system that ties all components together
"""

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import time

from .storage import StorageManager, DataType, ColumnMetadata, BinaryEncoder, BinaryDecoder
from .index import IndexManager, IndexType
from .transaction import TransactionManager, Transaction, Operation, OperationType
from .cache import CacheManager, LRUCache, QueryCache
from .query import QueryBuilder, query


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


class ColumnarDB:
    def __init__(self, data_dir: str = "data/db"):
        self.data_dir = data_dir
        self.storage = StorageManager(data_dir)
        self.index_manager = IndexManager()
        self.txn_manager = TransactionManager(os.path.join(data_dir, "wal"))
        self.cache_manager = CacheManager()
        self.query_cache = QueryCache(max_size=1000, ttl_seconds=60)
        
        self._tables: Dict[str, Table] = {}
        self._schemas: Dict[str, TableSchema] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
    
    async def initialize(self):
        if self._initialized:
            return
        
        async with self._lock:
            os.makedirs(self.data_dir, exist_ok=True)
            
            table_names = await self.storage.list_tables()
            for table_name in table_names:
                if table_name.startswith('_'):
                    continue
                await self._load_table(table_name)
            
            await self._create_table_cache()
            
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
        if_not_exists: bool = False
    ) -> bool:
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
                indexes=[col.name for col in columns if col.indexed]
            )
            
            table = Table(schema=schema)
            
            self._schemas[name] = schema
            self._tables[name] = table
            
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
            
            del self._schemas[name]
            del self._tables[name]
            
            return True
    
    async def _save_table(self, table_name: str):
        table = self._tables.get(table_name)
        if table is None:
            return
        
        metadata = table.schema.to_metadata()
        await self.storage.write_table(table_name, metadata, table.data)
    
    async def insert(
        self,
        table_name: str,
        data: Dict[str, Any],
        transaction: Optional[Transaction] = None
    ) -> int:
        await self.initialize()
        
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
            
            row_id = table.row_count
            
            for col_name, value in row.items():
                if col_name not in table.data:
                    table.data[col_name] = []
                table.data[col_name].append(value)
                
                col = schema.get_column(col_name)
                if col and col.indexed:
                    self.index_manager.insert_to_index(table_name, col_name, value, row_id)
            
            table.row_count += 1
            
            if transaction:
                op = Operation(
                    op_type=OperationType.INSERT,
                    table_name=table_name,
                    data=row,
                    row_id=row_id
                )
                await self.txn_manager.add_operation(transaction, op)
            else:
                await self._save_table(table_name)
            
            await self.query_cache.invalidate_table(table_name)
            
            return row_id
    
    async def insert_many(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        transaction: Optional[Transaction] = None
    ) -> List[int]:
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
                rows = condition.execute(table.data, [col.name for col in schema.columns])
                row_ids = []
                for i in range(table.row_count):
                    row = {col: table.data[col][i] for col in table.data}
                    if condition._conditions.evaluate(row):
                        row_ids.append(i)
            
            updated = 0
            for row_id in row_ids:
                for col_name, new_value in data.items():
                    col = schema.get_column(col_name)
                    if col is None:
                        continue
                    
                    if col.indexed:
                        old_value = table.data[col_name][row_id]
                        self.index_manager.update_index(table_name, col_name, old_value, new_value, row_id)
                    
                    table.data[col_name][row_id] = new_value
                updated += 1
            
            if transaction:
                op = Operation(
                    op_type=OperationType.UPDATE,
                    table_name=table_name,
                    data={'updates': data, 'row_ids': row_ids}
                )
                await self.txn_manager.add_operation(transaction, op)
            else:
                await self._save_table(table_name)
            
            await self.query_cache.invalidate_table(table_name)
            
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
            
            if condition is None:
                deleted = table.row_count
                for col_name in table.data:
                    table.data[col_name] = []
                table.row_count = 0
                self.index_manager.clear_table_indexes(table_name)
            else:
                row_ids_to_delete = []
                for i in range(table.row_count):
                    row = {col: table.data[col][i] for col in table.data}
                    if condition._conditions.evaluate(row):
                        row_ids_to_delete.append(i)
                
                for row_id in sorted(row_ids_to_delete, reverse=True):
                    for col_name in table.data:
                        col = schema.get_column(col_name)
                        if col and col.indexed:
                            value = table.data[col_name][row_id]
                            self.index_manager.delete_from_index(table_name, col_name, value, row_id)
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
            else:
                await self._save_table(table_name)
            
            await self.query_cache.invalidate_table(table_name)
            
            return deleted
    
    async def select(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        condition: Optional[QueryBuilder] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        await self.initialize()
        
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        cache_key = {
            'columns': columns,
            'condition': condition.to_dict() if condition else None,
            'order_by': order_by,
            'limit': limit,
            'offset': offset
        }
        
        cached = await self.query_cache.get(table_name, cache_key)
        if cached is not None:
            return cached
        
        table = self._tables[table_name]
        schema = table.schema
        
        q = query(table_name)
        
        if columns:
            q.select(*columns)
        else:
            q.select_all()
        
        if condition:
            q._conditions = condition._conditions
        
        if order_by:
            for col, direction in order_by:
                if direction.upper() == 'DESC':
                    q.order_by_desc(col)
                else:
                    q.order_by_asc(col)
        
        if offset > 0:
            q.offset(offset)
        
        if limit is not None:
            q.limit(limit)
        
        result = q.execute(table.data, [col.name for col in schema.columns])
        
        await self.query_cache.set(table_name, cache_key, result)
        
        return result
    
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
        
        if self.index_manager.has_index(table_name, id_column):
            row_ids = self.index_manager.search_index(table_name, id_column, id_value)
            if row_ids:
                row_id = row_ids[0]
                table = self._tables[table_name]
                return {col: table.data[col][row_id] for col in table.data}
        
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
            return table.row_count
        
        count = 0
        for i in range(table.row_count):
            row = {col: table.data[col][i] for col in table.data}
            if condition._conditions.evaluate(row):
                count += 1
        
        return count
    
    async def exists(
        self,
        table_name: str,
        condition: QueryBuilder
    ) -> bool:
        return await self.count(table_name, condition) > 0
    
    def query(self, table_name: str) -> QueryBuilder:
        return query(table_name)
    
    async def begin_transaction(self) -> Transaction:
        return await self.txn_manager.begin()
    
    async def commit(self, transaction: Transaction):
        await self.txn_manager.commit(transaction)
        
        for op in transaction.operations:
            if op.op_type in (OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE):
                await self._save_table(op.table_name)
    
    async def rollback(self, transaction: Transaction):
        await self.txn_manager.abort(transaction)
    
    async def create_index(
        self,
        table_name: str,
        column_name: str,
        index_type: IndexType = IndexType.BTREE
    ) -> bool:
        await self.initialize()
        
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        table = self._tables[table_name]
        schema = table.schema
        
        col = schema.get_column(column_name)
        if col is None:
            raise ValueError(f"Column {column_name} does not exist")
        
        values = table.data.get(column_name, [])
        self.index_manager.build_index(table_name, column_name, values, index_type)
        
        col.indexed = True
        await self._save_table(table_name)
        
        return True
    
    async def drop_index(self, table_name: str, column_name: str) -> bool:
        await self.initialize()
        
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} does not exist")
        
        self.index_manager.drop_index(table_name, column_name)
        
        col = self._schemas[table_name].get_column(column_name)
        if col:
            col.indexed = False
        
        await self._save_table(table_name)
        return True
    
    async def list_tables(self) -> List[str]:
        await self.initialize()
        return list(self._schemas.keys())
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        await self.initialize()
        
        if table_name not in self._tables:
            return None
        
        table = self._tables[table_name]
        schema = table.schema
        
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
            'indexes': self.index_manager.list_indexes(table_name),
            'size_bytes': await self.storage.get_table_size(table_name),
            'created_at': schema.created_at.isoformat()
        }
    
    async def backup(self, backup_dir: str) -> bool:
        await self.initialize()
        
        os.makedirs(backup_dir, exist_ok=True)
        
        for table_name in self._tables:
            backup_path = os.path.join(backup_dir, f"{table_name}.coldb")
            if not await self.storage.backup_table(table_name, backup_path):
                return False
        
        return True
    
    async def restore(self, backup_dir: str) -> bool:
        for f in os.listdir(backup_dir):
            if f.endswith('.coldb'):
                table_name = f[:-6]
                backup_path = os.path.join(backup_dir, f)
                if not await self.storage.restore_table(table_name, backup_path):
                    return False
                await self._load_table(table_name)
        
        return True
    
    async def vacuum(self):
        await self.initialize()
        
        for table_name in list(self._tables.keys()):
            await self._save_table(table_name)
        
        await self.query_cache.clear()
    
    async def stats(self) -> Dict[str, Any]:
        await self.initialize()
        
        total_rows = sum(t.row_count for t in self._tables.values())
        total_size = 0
        for name in self._tables:
            size = await self.storage.get_table_size(name)
            total_size += size
        
        return {
            'tables': len(self._tables),
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'cache_stats': self.cache_manager.stats(),
            'query_cache_stats': self.query_cache.stats()
        }
    
    async def close(self):
        for table_name in self._tables:
            await self._save_table(table_name)
        
        await self.cache_manager.clear_all()
        await self.query_cache.clear()
