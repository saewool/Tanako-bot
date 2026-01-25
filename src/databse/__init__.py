"""
Custom Columnar Database Engine
A production-ready, binary-based columnar database system
Built for high-performance Discord bot operations

Features:
- LSM-style writes with memtable for high write throughput
- SSTable format with Bloom filters for optimized disk I/O
- Horizontal scaling with CDN-style data distribution
- Consistent hashing for guild partitioning across nodes
"""

from .engine import ColumnarDB, Column, Table, DataType
from .query import QueryBuilder, Condition, OrderBy
from .storage import StorageManager, BinaryEncoder, BinaryDecoder, ColumnMetadata
from .index import IndexManager, BTreeIndex, HashIndex, IndexType
from .transaction import TransactionManager, Transaction
from .cache import CacheManager, LRUCache, QueryCache

from .memtable import (
    MemTable, MemTableManager, MemTableState,
    SSTableWriter, SSTableReader, SSTableMetadata,
    FlushService, BloomFilter, SkipList
)

from .cluster import (
    ClusterManager, NodeInfo, NodeState,
    ConsistentHashRing, NodeRegistry, NodeClient,
    DistributedCache
)

from .distributed_engine import DistributedColumnarDB, TableSchema

__all__ = [
    'ColumnarDB',
    'DistributedColumnarDB',
    'Column',
    'Table',
    'TableSchema',
    'DataType',
    'QueryBuilder',
    'Condition',
    'OrderBy',
    'StorageManager',
    'ColumnMetadata',
    'BinaryEncoder',
    'BinaryDecoder',
    'IndexManager',
    'IndexType',
    'BTreeIndex',
    'HashIndex',
    'TransactionManager',
    'Transaction',
    'CacheManager',
    'LRUCache',
    'QueryCache',
    'MemTable',
    'MemTableManager',
    'MemTableState',
    'SSTableWriter',
    'SSTableReader',
    'SSTableMetadata',
    'FlushService',
    'BloomFilter',
    'SkipList',
    'ClusterManager',
    'NodeInfo',
    'NodeState',
    'ConsistentHashRing',
    'NodeRegistry',
    'NodeClient',
    'DistributedCache',
]

__version__ = '2.0.0'
