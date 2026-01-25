"""
Index Manager for Columnar Database
Implements B-Tree and Hash indexes for fast lookups
"""

import struct
import os
import asyncio
from typing import Any, List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from enum import Enum
import bisect
import hashlib


class IndexType(Enum):
    BTREE = 1
    HASH = 2


@dataclass
class IndexEntry:
    value: Any
    row_ids: List[int]


class BTreeNode:
    def __init__(self, is_leaf: bool = True, order: int = 100):
        self.is_leaf = is_leaf
        self.order = order
        self.keys: List[Any] = []
        self.values: List[List[int]] = []
        self.children: List['BTreeNode'] = []
    
    def is_full(self) -> bool:
        return len(self.keys) >= 2 * self.order - 1


class BTreeIndex:
    def __init__(self, name: str, order: int = 100):
        self.name = name
        self.order = order
        self.root = BTreeNode(is_leaf=True, order=order)
        self._size = 0
    
    def insert(self, key: Any, row_id: int):
        if key is None:
            return
        
        existing = self._search_node(self.root, key)
        if existing is not None:
            if row_id not in existing:
                existing.append(row_id)
            return
        
        if self.root.is_full():
            new_root = BTreeNode(is_leaf=False, order=self.order)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        
        self._insert_non_full(self.root, key, row_id)
        self._size += 1
    
    def _insert_non_full(self, node: BTreeNode, key: Any, row_id: int):
        i = len(node.keys) - 1
        
        if node.is_leaf:
            pos = bisect.bisect_left(node.keys, key)
            if pos < len(node.keys) and node.keys[pos] == key:
                if row_id not in node.values[pos]:
                    node.values[pos].append(row_id)
            else:
                node.keys.insert(pos, key)
                node.values.insert(pos, [row_id])
        else:
            pos = bisect.bisect_right(node.keys, key)
            if node.children[pos].is_full():
                self._split_child(node, pos)
                if key > node.keys[pos]:
                    pos += 1
            self._insert_non_full(node.children[pos], key, row_id)
    
    def _split_child(self, parent: BTreeNode, index: int):
        order = self.order
        child = parent.children[index]
        new_node = BTreeNode(is_leaf=child.is_leaf, order=order)
        
        mid = order - 1
        
        parent.keys.insert(index, child.keys[mid])
        parent.values.insert(index, child.values[mid])
        parent.children.insert(index + 1, new_node)
        
        new_node.keys = child.keys[mid + 1:]
        new_node.values = child.values[mid + 1:]
        child.keys = child.keys[:mid]
        child.values = child.values[:mid]
        
        if not child.is_leaf:
            new_node.children = child.children[mid + 1:]
            child.children = child.children[:mid + 1]
    
    def _search_node(self, node: BTreeNode, key: Any) -> Optional[List[int]]:
        pos = bisect.bisect_left(node.keys, key)
        
        if pos < len(node.keys) and node.keys[pos] == key:
            return node.values[pos]
        
        if node.is_leaf:
            return None
        
        return self._search_node(node.children[pos], key)
    
    def search(self, key: Any) -> List[int]:
        result = self._search_node(self.root, key)
        return result if result else []
    
    def search_range(self, min_key: Any, max_key: Any, include_min: bool = True, include_max: bool = True) -> List[int]:
        results: Set[int] = set()
        self._range_search(self.root, min_key, max_key, include_min, include_max, results)
        return list(results)
    
    def _range_search(
        self,
        node: BTreeNode,
        min_key: Any,
        max_key: Any,
        include_min: bool,
        include_max: bool,
        results: Set[int]
    ):
        for i, key in enumerate(node.keys):
            in_range = True
            if min_key is not None:
                if include_min:
                    in_range = in_range and key >= min_key
                else:
                    in_range = in_range and key > min_key
            if max_key is not None:
                if include_max:
                    in_range = in_range and key <= max_key
                else:
                    in_range = in_range and key < max_key
            
            if in_range:
                results.update(node.values[i])
        
        if not node.is_leaf:
            for i, child in enumerate(node.children):
                should_search = True
                if i > 0 and max_key is not None:
                    should_search = node.keys[i-1] <= max_key
                if i < len(node.keys) and min_key is not None:
                    should_search = should_search and node.keys[i] >= min_key
                
                if should_search:
                    self._range_search(child, min_key, max_key, include_min, include_max, results)
    
    def delete(self, key: Any, row_id: Optional[int] = None):
        self._delete_from_node(self.root, key, row_id)
    
    def _delete_from_node(self, node: BTreeNode, key: Any, row_id: Optional[int]):
        pos = bisect.bisect_left(node.keys, key)
        
        if node.is_leaf:
            if pos < len(node.keys) and node.keys[pos] == key:
                if row_id is not None:
                    if row_id in node.values[pos]:
                        node.values[pos].remove(row_id)
                    if not node.values[pos]:
                        del node.keys[pos]
                        del node.values[pos]
                        self._size -= 1
                else:
                    del node.keys[pos]
                    del node.values[pos]
                    self._size -= 1
        else:
            if pos < len(node.keys) and node.keys[pos] == key:
                if row_id is not None:
                    if row_id in node.values[pos]:
                        node.values[pos].remove(row_id)
                else:
                    del node.keys[pos]
                    del node.values[pos]
                    self._size -= 1
            else:
                self._delete_from_node(node.children[pos], key, row_id)
    
    def clear(self):
        self.root = BTreeNode(is_leaf=True, order=self.order)
        self._size = 0
    
    def __len__(self) -> int:
        return self._size
    
    def all_entries(self) -> List[Tuple[Any, List[int]]]:
        entries = []
        self._collect_entries(self.root, entries)
        return entries
    
    def _collect_entries(self, node: BTreeNode, entries: List[Tuple[Any, List[int]]]):
        for i, key in enumerate(node.keys):
            entries.append((key, node.values[i].copy()))
        
        if not node.is_leaf:
            for child in node.children:
                self._collect_entries(child, entries)


class HashIndex:
    def __init__(self, name: str, bucket_count: int = 1024):
        self.name = name
        self.bucket_count = bucket_count
        self.buckets: List[Dict[Any, List[int]]] = [{} for _ in range(bucket_count)]
        self._size = 0
    
    def _hash(self, key: Any) -> int:
        if key is None:
            return 0
        
        if isinstance(key, (int, float)):
            key_bytes = str(key).encode()
        elif isinstance(key, str):
            key_bytes = key.encode()
        elif isinstance(key, bytes):
            key_bytes = key
        else:
            key_bytes = str(key).encode()
        
        return int(hashlib.md5(key_bytes).hexdigest(), 16) % self.bucket_count
    
    def insert(self, key: Any, row_id: int):
        if key is None:
            return
        
        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]
        
        if key in bucket:
            if row_id not in bucket[key]:
                bucket[key].append(row_id)
        else:
            bucket[key] = [row_id]
            self._size += 1
    
    def search(self, key: Any) -> List[int]:
        if key is None:
            return []
        
        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]
        
        return bucket.get(key, []).copy()
    
    def delete(self, key: Any, row_id: Optional[int] = None):
        if key is None:
            return
        
        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]
        
        if key in bucket:
            if row_id is not None:
                if row_id in bucket[key]:
                    bucket[key].remove(row_id)
                if not bucket[key]:
                    del bucket[key]
                    self._size -= 1
            else:
                del bucket[key]
                self._size -= 1
    
    def clear(self):
        self.buckets = [{} for _ in range(self.bucket_count)]
        self._size = 0
    
    def __len__(self) -> int:
        return self._size
    
    def all_entries(self) -> List[Tuple[Any, List[int]]]:
        entries = []
        for bucket in self.buckets:
            for key, row_ids in bucket.items():
                entries.append((key, row_ids.copy()))
        return entries


class IndexManager:
    def __init__(self):
        self.indexes: Dict[str, Dict[str, Any]] = {}
    
    def create_index(
        self,
        table_name: str,
        column_name: str,
        index_type: IndexType = IndexType.BTREE,
        **kwargs
    ) -> bool:
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        index_name = f"{table_name}_{column_name}"
        
        if index_type == IndexType.BTREE:
            order = kwargs.get('order', 100)
            self.indexes[table_name][column_name] = BTreeIndex(index_name, order)
        elif index_type == IndexType.HASH:
            bucket_count = kwargs.get('bucket_count', 1024)
            self.indexes[table_name][column_name] = HashIndex(index_name, bucket_count)
        
        return True
    
    def drop_index(self, table_name: str, column_name: str) -> bool:
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            del self.indexes[table_name][column_name]
            return True
        return False
    
    def get_index(self, table_name: str, column_name: str) -> Optional[Any]:
        if table_name in self.indexes:
            return self.indexes[table_name].get(column_name)
        return None
    
    def build_index(
        self,
        table_name: str,
        column_name: str,
        values: List[Any],
        index_type: IndexType = IndexType.BTREE
    ):
        self.create_index(table_name, column_name, index_type)
        index = self.get_index(table_name, column_name)
        
        if index:
            for row_id, value in enumerate(values):
                index.insert(value, row_id)
    
    def update_index(self, table_name: str, column_name: str, old_value: Any, new_value: Any, row_id: int):
        index = self.get_index(table_name, column_name)
        if index:
            index.delete(old_value, row_id)
            index.insert(new_value, row_id)
    
    def insert_to_index(self, table_name: str, column_name: str, value: Any, row_id: int):
        index = self.get_index(table_name, column_name)
        if index:
            index.insert(value, row_id)
    
    def delete_from_index(self, table_name: str, column_name: str, value: Any, row_id: int):
        index = self.get_index(table_name, column_name)
        if index:
            index.delete(value, row_id)
    
    def search_index(self, table_name: str, column_name: str, value: Any) -> List[int]:
        index = self.get_index(table_name, column_name)
        if index:
            return index.search(value)
        return []
    
    def range_search_index(
        self,
        table_name: str,
        column_name: str,
        min_value: Any,
        max_value: Any
    ) -> List[int]:
        index = self.get_index(table_name, column_name)
        if isinstance(index, BTreeIndex):
            return index.search_range(min_value, max_value)
        return []
    
    def clear_table_indexes(self, table_name: str):
        if table_name in self.indexes:
            for column_name in self.indexes[table_name]:
                self.indexes[table_name][column_name].clear()
    
    def drop_table_indexes(self, table_name: str):
        if table_name in self.indexes:
            del self.indexes[table_name]
    
    def list_indexes(self, table_name: str) -> List[str]:
        if table_name in self.indexes:
            return list(self.indexes[table_name].keys())
        return []
    
    def has_index(self, table_name: str, column_name: str) -> bool:
        return table_name in self.indexes and column_name in self.indexes[table_name]
