"""
Cluster Management for Distributed Database
Implements CDN-style architecture with:
- Consistent Hashing for data partitioning by guild_id
- Node Discovery & Registry
- Inter-node Communication
- Distributed Caching
"""

import asyncio
import hashlib
import json
import os
import struct
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Awaitable
from datetime import datetime
import bisect
import websockets
from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import serve as ws_serve


class SnowflakeHasher:
    """
    Specialized hasher for Discord Snowflake IDs.
    
    Discord Snowflake format (64-bit):
    - Bits 63-22: Timestamp (42 bits) - milliseconds since Discord Epoch
    - Bits 21-17: Internal worker ID (5 bits)
    - Bits 16-12: Internal process ID (5 bits)
    - Bits 11-0: Sequence/increment (12 bits)
    
    Problem: Servers created around the same time have very similar high bits,
    causing them to cluster on the same hash ring segments.
    
    Solution: Mix/whiten the bits before hashing to ensure uniform distribution.
    """
    
    DISCORD_EPOCH = 1420070400000
    
    @staticmethod
    def _mix64(x: int) -> int:
        """MurmurHash3 finalizer - excellent bit mixing for 64-bit integers"""
        x ^= x >> 33
        x = (x * 0xff51afd7ed558ccd) & 0xFFFFFFFFFFFFFFFF
        x ^= x >> 33
        x = (x * 0xc4ceb9fe1a85ec53) & 0xFFFFFFFFFFFFFFFF
        x ^= x >> 33
        return x
    
    @staticmethod
    def _rotate_left(x: int, r: int, bits: int = 64) -> int:
        """Rotate bits left"""
        return ((x << r) | (x >> (bits - r))) & ((1 << bits) - 1)
    
    @classmethod
    def whiten(cls, snowflake_id: int) -> int:
        """
        Whiten a Discord snowflake ID to ensure uniform distribution.
        
        1. Split the 64-bit ID into two 32-bit halves
        2. XOR-rotate to mix timestamp bits with sequence bits
        3. Apply MurmurHash3 finalizer for avalanche effect
        """
        high_bits = (snowflake_id >> 32) & 0xFFFFFFFF
        low_bits = snowflake_id & 0xFFFFFFFF
        
        mixed = high_bits ^ cls._rotate_left(low_bits, 17, 32)
        mixed = (mixed << 32) | (low_bits ^ cls._rotate_left(high_bits, 13, 32))
        
        return cls._mix64(mixed)
    
    @classmethod
    def hash_guild(cls, guild_id: int, seed: int = 0) -> int:
        """
        Generate a well-distributed hash for a guild ID.
        
        Args:
            guild_id: Discord guild/server ID (snowflake)
            seed: Optional seed for different hash variants
            
        Returns:
            128-bit hash value as integer
        """
        whitened = cls.whiten(guild_id)
        
        data = struct.pack('<QQ', whitened, seed)
        
        digest = hashlib.blake2b(data, digest_size=16).digest()
        return int.from_bytes(digest, 'big')
    
    @classmethod
    def analyze_distribution(cls, guild_ids: List[int], num_buckets: int = 100) -> Dict[str, Any]:
        """
        Analyze the distribution of guild IDs across buckets.
        
        Returns statistics about how evenly the IDs would be distributed.
        """
        if not guild_ids:
            return {'error': 'No guild IDs provided'}
        
        buckets = [0] * num_buckets
        max_hash = (1 << 128) - 1
        bucket_size = max_hash // num_buckets
        
        for guild_id in guild_ids:
            hash_val = cls.hash_guild(guild_id)
            bucket_idx = min(hash_val // bucket_size, num_buckets - 1)
            buckets[bucket_idx] += 1
        
        expected = len(guild_ids) / num_buckets
        variance = sum((count - expected) ** 2 for count in buckets) / num_buckets
        std_dev = variance ** 0.5
        
        max_count = max(buckets)
        min_count = min(buckets)
        
        return {
            'total_ids': len(guild_ids),
            'num_buckets': num_buckets,
            'expected_per_bucket': expected,
            'std_deviation': std_dev,
            'deviation_percent': (std_dev / expected * 100) if expected > 0 else 0,
            'max_bucket_count': max_count,
            'min_bucket_count': min_count,
            'max_deviation_from_expected': ((max_count - expected) / expected * 100) if expected > 0 else 0
        }


class NodeState(Enum):
    STARTING = 1
    ACTIVE = 2
    DRAINING = 3
    OFFLINE = 4


@dataclass
class NodeInfo:
    """Information about a cluster node"""
    node_id: str
    host: str
    port: int
    state: NodeState = NodeState.STARTING
    last_heartbeat: float = field(default_factory=time.time)
    guild_count: int = 0
    load_factor: float = 0.0
    version: str = "1.0.0"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"
    
    @property
    def ws_url(self) -> str:
        return f"ws://{self.host}:{self.port}"
    
    def is_healthy(self, timeout_seconds: float = 30.0) -> bool:
        return (
            self.state == NodeState.ACTIVE and
            time.time() - self.last_heartbeat < timeout_seconds
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'state': self.state.name,
            'last_heartbeat': self.last_heartbeat,
            'guild_count': self.guild_count,
            'load_factor': self.load_factor,
            'version': self.version,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeInfo':
        return cls(
            node_id=data['node_id'],
            host=data['host'],
            port=data['port'],
            state=NodeState[data.get('state', 'STARTING')],
            last_heartbeat=data.get('last_heartbeat', time.time()),
            guild_count=data.get('guild_count', 0),
            load_factor=data.get('load_factor', 0.0),
            version=data.get('version', '1.0.0'),
            metadata=data.get('metadata', {})
        )


class ConsistentHashRing:
    """
    Consistent Hashing Ring for distributing guild data across nodes.
    
    Features:
    - Snowflake-aware hashing for Discord guild IDs
    - Weighted virtual nodes for uneven node capacities
    - 128-bit hash space for minimal collisions
    - Deterministic seeding for consistent ring placement
    """
    
    DEFAULT_VIRTUAL_NODES = 150
    HASH_SPACE_BITS = 128
    
    def __init__(self, virtual_nodes: int = DEFAULT_VIRTUAL_NODES):
        self.default_virtual_nodes = virtual_nodes
        self._ring: List[Tuple[int, str]] = []  # (hash, node_id)
        self._nodes: Dict[str, NodeInfo] = {}
        self._node_weights: Dict[str, float] = {}  # node_id -> weight (1.0 = normal)
        self._node_vnodes: Dict[str, int] = {}  # node_id -> actual vnode count
        self._guild_cache: Dict[int, str] = {}  # guild_id -> node_id cache
        self._lock = asyncio.Lock()
    
    def _hash_vnode(self, node_id: str, vnode_index: int) -> int:
        """
        Generate deterministic hash for a virtual node.
        Uses HMAC-style construction for consistent placement.
        """
        data = f"{node_id}:vnode:{vnode_index}".encode()
        digest = hashlib.blake2b(data, digest_size=16).digest()
        return int.from_bytes(digest, 'big')
    
    def _hash_guild(self, guild_id: int) -> int:
        """
        Hash a Discord guild ID using snowflake-aware hashing.
        This ensures servers created at similar times are evenly distributed.
        """
        return SnowflakeHasher.hash_guild(guild_id)
    
    async def add_node(self, node: NodeInfo, weight: float = 1.0):
        """
        Add a node to the ring with weighted virtual nodes.
        
        Args:
            node: NodeInfo for the node to add
            weight: Capacity weight (2.0 = double capacity, gets 2x virtual nodes)
        """
        async with self._lock:
            if node.node_id in self._nodes:
                return
            
            self._nodes[node.node_id] = node
            self._node_weights[node.node_id] = weight
            
            vnode_count = int(self.default_virtual_nodes * weight)
            vnode_count = max(1, vnode_count)
            self._node_vnodes[node.node_id] = vnode_count
            
            for i in range(vnode_count):
                hash_value = self._hash_vnode(node.node_id, i)
                bisect.insort(self._ring, (hash_value, node.node_id))
            
            self._guild_cache.clear()
    
    async def update_node_weight(self, node_id: str, new_weight: float):
        """Update a node's weight and rebuild its virtual nodes."""
        async with self._lock:
            if node_id not in self._nodes:
                return
            
            node = self._nodes[node_id]
            old_vnode_count = self._node_vnodes.get(node_id, self.default_virtual_nodes)
            
            self._ring = [(h, nid) for h, nid in self._ring if nid != node_id]
            
            self._node_weights[node_id] = new_weight
            new_vnode_count = max(1, int(self.default_virtual_nodes * new_weight))
            self._node_vnodes[node_id] = new_vnode_count
            
            for i in range(new_vnode_count):
                hash_value = self._hash_vnode(node_id, i)
                bisect.insort(self._ring, (hash_value, node_id))
            
            self._guild_cache.clear()
    
    async def remove_node(self, node_id: str):
        """Remove a node from the ring"""
        async with self._lock:
            if node_id not in self._nodes:
                return
            
            del self._nodes[node_id]
            self._node_weights.pop(node_id, None)
            self._node_vnodes.pop(node_id, None)
            
            self._ring = [
                (h, nid) for h, nid in self._ring
                if nid != node_id
            ]
            
            self._guild_cache.clear()
    
    async def get_node_for_guild(self, guild_id: int) -> Optional[NodeInfo]:
        """Get the primary node responsible for a guild"""
        async with self._lock:
            if guild_id in self._guild_cache:
                node_id = self._guild_cache[guild_id]
                if node_id in self._nodes:
                    return self._nodes[node_id]
            
            if not self._ring:
                return None
            
            hash_value = self._hash_guild(guild_id)
            
            idx = bisect.bisect_left(self._ring, (hash_value, ""))
            if idx >= len(self._ring):
                idx = 0
            
            _, node_id = self._ring[idx]
            self._guild_cache[guild_id] = node_id
            
            return self._nodes.get(node_id)
    
    async def get_replica_nodes(
        self,
        guild_id: int,
        replica_count: int = 2
    ) -> List[NodeInfo]:
        """Get replica nodes for a guild (for redundancy)"""
        async with self._lock:
            if not self._ring:
                return []
            
            hash_value = self._hash_guild(guild_id)
            idx = bisect.bisect_left(self._ring, (hash_value, ""))
            
            nodes = []
            seen = set()
            
            for _ in range(len(self._ring)):
                if len(nodes) >= replica_count + 1:
                    break
                
                actual_idx = idx % len(self._ring)
                _, node_id = self._ring[actual_idx]
                
                if node_id not in seen:
                    seen.add(node_id)
                    if node_id in self._nodes:
                        nodes.append(self._nodes[node_id])
                
                idx += 1
            
            return nodes[1:] if len(nodes) > 1 else []
    
    async def get_all_nodes(self) -> List[NodeInfo]:
        """Get all nodes in the cluster"""
        async with self._lock:
            return list(self._nodes.values())
    
    async def get_healthy_nodes(self) -> List[NodeInfo]:
        """Get all healthy nodes"""
        async with self._lock:
            return [n for n in self._nodes.values() if n.is_healthy()]
    
    async def get_guilds_for_node(self, node_id: str) -> Set[int]:
        """Get all guilds that belong to a specific node"""
        async with self._lock:
            return {
                guild_id for guild_id, nid in self._guild_cache.items()
                if nid == node_id
            }
    
    def node_count(self) -> int:
        return len(self._nodes)
    
    async def analyze_distribution(self, sample_guild_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        """
        Analyze how guilds would be distributed across nodes.
        
        Args:
            sample_guild_ids: List of guild IDs to test, or None to use cached guilds
            
        Returns:
            Distribution statistics per node
        """
        async with self._lock:
            if not self._nodes:
                return {'error': 'No nodes in cluster'}
            
            guild_ids = sample_guild_ids or list(self._guild_cache.keys())
            
            if not guild_ids:
                import random
                base_id = 1000000000000000000
                guild_ids = [base_id + i * 1000 for i in range(1000)]
            
            node_counts: Dict[str, int] = {nid: 0 for nid in self._nodes}
            
            for guild_id in guild_ids:
                hash_value = self._hash_guild(guild_id)
                idx = bisect.bisect_left(self._ring, (hash_value, ""))
                if idx >= len(self._ring):
                    idx = 0
                _, node_id = self._ring[idx]
                node_counts[node_id] = node_counts.get(node_id, 0) + 1
            
            total = len(guild_ids)
            num_nodes = len(self._nodes)
            expected = total / num_nodes if num_nodes > 0 else 0
            
            per_node_stats = {}
            for nid, count in node_counts.items():
                weight = self._node_weights.get(nid, 1.0)
                expected_weighted = expected * weight
                deviation = ((count - expected_weighted) / expected_weighted * 100) if expected_weighted > 0 else 0
                per_node_stats[nid] = {
                    'count': count,
                    'percentage': (count / total * 100) if total > 0 else 0,
                    'weight': weight,
                    'expected': expected_weighted,
                    'deviation_percent': deviation,
                    'vnodes': self._node_vnodes.get(nid, self.default_virtual_nodes)
                }
            
            counts = list(node_counts.values())
            variance = sum((c - expected) ** 2 for c in counts) / num_nodes if num_nodes > 0 else 0
            std_dev = variance ** 0.5
            
            return {
                'total_guilds': total,
                'num_nodes': num_nodes,
                'expected_per_node': expected,
                'std_deviation': std_dev,
                'deviation_percent': (std_dev / expected * 100) if expected > 0 else 0,
                'max_count': max(counts) if counts else 0,
                'min_count': min(counts) if counts else 0,
                'nodes': per_node_stats
            }
    
    def stats(self) -> Dict[str, Any]:
        """Get current ring statistics."""
        node_info = []
        for nid, node in self._nodes.items():
            info = node.to_dict()
            info['weight'] = self._node_weights.get(nid, 1.0)
            info['vnodes'] = self._node_vnodes.get(nid, self.default_virtual_nodes)
            node_info.append(info)
        
        return {
            'node_count': len(self._nodes),
            'default_virtual_nodes': self.default_virtual_nodes,
            'ring_size': len(self._ring),
            'cache_size': len(self._guild_cache),
            'hash_algorithm': 'blake2b-128 with snowflake whitening',
            'nodes': node_info
        }


class NodeRegistry:
    """
    Node Discovery and Registry Service
    Handles node registration, heartbeats, and health monitoring
    """
    
    HEARTBEAT_INTERVAL = 5.0
    HEARTBEAT_TIMEOUT = 15.0
    CLEANUP_INTERVAL = 30.0
    
    def __init__(self, local_node: NodeInfo, hash_ring: ConsistentHashRing, default_weight: float = 1.0):
        self.local_node = local_node
        self.hash_ring = hash_ring
        self.default_weight = default_weight
        
        self._peers: Dict[str, NodeInfo] = {}
        self._peer_weights: Dict[str, float] = {}  # Track weights for peer nodes
        self._connections: Dict[str, Any] = {}  # WebSocket connections to peers
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._callbacks: List[Callable[[str, NodeInfo], Awaitable[None]]] = []
        self._lock = asyncio.Lock()
    
    def on_node_change(self, callback: Callable[[str, NodeInfo], Awaitable[None]]):
        """Register callback for node changes (join/leave)"""
        self._callbacks.append(callback)
    
    async def start(self):
        """Start the registry service"""
        if self._running:
            return
        
        self._running = True
        
        local_weight = self.local_node.metadata.get('weight', self.default_weight)
        await self.hash_ring.add_node(self.local_node, weight=local_weight)
        self.local_node.state = NodeState.ACTIVE
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        """Stop the registry service"""
        self._running = False
        self.local_node.state = NodeState.DRAINING
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        for conn in self._connections.values():
            try:
                await conn.close()
            except:
                pass
        
        self._connections.clear()
    
    async def join_cluster(self, seed_nodes: List[str]):
        """Join a cluster using seed node addresses"""
        for seed in seed_nodes:
            try:
                await self._connect_to_peer(seed)
                await self._sync_cluster_state(seed)
                break
            except Exception as e:
                print(f"Failed to connect to seed {seed}: {e}")
    
    async def _connect_to_peer(self, address: str):
        """Connect to a peer node"""
        try:
            if address in self._connections:
                return
            
            ws = await ws_connect(f"ws://{address}/cluster")
            self._connections[address] = ws
            
            await ws.send(json.dumps({
                'type': 'register',
                'node': self.local_node.to_dict()
            }))
            
            asyncio.create_task(self._handle_peer_messages(address, ws))
            
        except Exception as e:
            print(f"Error connecting to peer {address}: {e}")
    
    async def _handle_peer_messages(self, address: str, ws):
        """Handle messages from a peer node"""
        try:
            async for message in ws:
                data = json.loads(message)
                await self._process_peer_message(address, data)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            async with self._lock:
                if address in self._connections:
                    del self._connections[address]
    
    async def _process_peer_message(self, address: str, data: Dict[str, Any]):
        """Process a message from a peer"""
        msg_type = data.get('type')
        
        if msg_type == 'register':
            node = NodeInfo.from_dict(data['node'])
            await self._add_peer(node)
        
        elif msg_type == 'heartbeat':
            node_id = data.get('node_id')
            if node_id is not None and node_id in self._peers:
                self._peers[node_id].last_heartbeat = time.time()
                self._peers[node_id].load_factor = data.get('load_factor', 0.0)
                self._peers[node_id].guild_count = data.get('guild_count', 0)
        
        elif msg_type == 'cluster_state':
            nodes = data.get('nodes', [])
            for node_data in nodes:
                node = NodeInfo.from_dict(node_data)
                if node.node_id != self.local_node.node_id:
                    await self._add_peer(node)
        
        elif msg_type == 'node_leave':
            node_id = data.get('node_id')
            if node_id is not None:
                await self._remove_peer(node_id)
    
    async def _sync_cluster_state(self, address: str):
        """Request cluster state from a peer"""
        if address in self._connections:
            await self._connections[address].send(json.dumps({
                'type': 'request_cluster_state'
            }))
    
    async def _add_peer(self, node: NodeInfo):
        """Add a peer node with its configured weight"""
        async with self._lock:
            if node.node_id == self.local_node.node_id:
                return
            
            is_new = node.node_id not in self._peers
            self._peers[node.node_id] = node
            
            peer_weight = node.metadata.get('weight', self.default_weight)
            self._peer_weights[node.node_id] = peer_weight
            await self.hash_ring.add_node(node, weight=peer_weight)
            
            if is_new:
                for callback in self._callbacks:
                    try:
                        await callback('join', node)
                    except:
                        pass
    
    async def _remove_peer(self, node_id: str):
        """Remove a peer node"""
        async with self._lock:
            if node_id in self._peers:
                node = self._peers[node_id]
                del self._peers[node_id]
                await self.hash_ring.remove_node(node_id)
                
                for callback in self._callbacks:
                    try:
                        await callback('leave', node)
                    except:
                        pass
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats to peers"""
        while self._running:
            try:
                heartbeat = json.dumps({
                    'type': 'heartbeat',
                    'node_id': self.local_node.node_id,
                    'timestamp': time.time(),
                    'load_factor': self.local_node.load_factor,
                    'guild_count': self.local_node.guild_count
                })
                
                for address, ws in list(self._connections.items()):
                    try:
                        await ws.send(heartbeat)
                    except:
                        pass
                
                self.local_node.last_heartbeat = time.time()
                
            except Exception as e:
                print(f"Error in heartbeat loop: {e}")
            
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
    
    async def _cleanup_loop(self):
        """Clean up dead nodes"""
        while self._running:
            try:
                current_time = time.time()
                dead_nodes = []
                
                async with self._lock:
                    for node_id, node in self._peers.items():
                        if current_time - node.last_heartbeat > self.HEARTBEAT_TIMEOUT:
                            dead_nodes.append(node_id)
                
                for node_id in dead_nodes:
                    await self._remove_peer(node_id)
                
            except Exception as e:
                print(f"Error in cleanup loop: {e}")
            
            await asyncio.sleep(self.CLEANUP_INTERVAL)
    
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast a message to all peers"""
        msg_str = json.dumps(message)
        for address, ws in list(self._connections.items()):
            try:
                await ws.send(msg_str)
            except:
                pass
    
    def get_peer(self, node_id: str) -> Optional[NodeInfo]:
        return self._peers.get(node_id)
    
    def get_all_peers(self) -> List[NodeInfo]:
        return list(self._peers.values())
    
    def stats(self) -> Dict[str, Any]:
        return {
            'local_node': self.local_node.to_dict(),
            'peer_count': len(self._peers),
            'connection_count': len(self._connections),
            'peers': [n.to_dict() for n in self._peers.values()]
        }


class NodeClient:
    """
    Client for inter-node communication
    Handles fetching data from remote nodes with retry and timeout handling
    """
    
    REQUEST_TIMEOUT = 10.0
    CONNECT_TIMEOUT = 5.0
    MAX_RETRIES = 3
    RETRY_BACKOFF = 0.5
    
    BLACKLIST_DURATION = 30.0
    PROBE_INTERVAL = 10.0
    
    def __init__(self):
        self._connections: Dict[str, Any] = {}
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._connection_attempts: Dict[str, int] = {}
        self._failed_nodes: Dict[str, float] = {}
        self._known_nodes: Dict[str, NodeInfo] = {}
        self._probe_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self):
        """Start the node client with periodic probing"""
        if self._running:
            return
        self._running = True
        self._probe_task = asyncio.create_task(self._probe_loop())
    
    async def stop(self):
        """Stop the node client"""
        self._running = False
        if self._probe_task:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except asyncio.CancelledError:
                pass
        await self.close()
    
    async def _probe_loop(self):
        """Periodically probe failed nodes to check for recovery"""
        while self._running:
            try:
                await asyncio.sleep(self.PROBE_INTERVAL)
                
                failed_addresses = list(self._failed_nodes.keys())
                for address in failed_addresses:
                    if address in self._known_nodes:
                        node = self._known_nodes[address]
                        recovered = await self.probe_node(node)
                        if recovered:
                            print(f"Node {address} has recovered")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in probe loop: {e}")
    
    def register_node(self, node: NodeInfo):
        """Register a node for probing"""
        self._known_nodes[node.address] = node
    
    def _is_node_blacklisted(self, address: str) -> bool:
        """Check if a node is temporarily blacklisted due to failures"""
        if address not in self._failed_nodes:
            return False
        
        blacklist_time = self._failed_nodes[address]
        elapsed = time.time() - blacklist_time
        
        if elapsed > self.BLACKLIST_DURATION:
            del self._failed_nodes[address]
            return False
        return True
    
    async def probe_node(self, node: NodeInfo) -> bool:
        """Probe a potentially failed node to check if it's recovered"""
        address = node.address
        
        if address not in self._failed_nodes:
            return True
        
        try:
            ws = await asyncio.wait_for(
                ws_connect(f"{node.ws_url}/data"),
                timeout=self.CONNECT_TIMEOUT
            )
            await ws.close()
            
            if address in self._failed_nodes:
                del self._failed_nodes[address]
            return True
            
        except Exception:
            return False
    
    def get_failed_nodes(self) -> Dict[str, float]:
        """Get all currently blacklisted nodes with their blacklist times"""
        return dict(self._failed_nodes)
    
    async def _get_connection(self, node: NodeInfo):
        """Get or create connection to a node with timeout and retry"""
        address = node.address
        
        if self._is_node_blacklisted(address):
            raise ConnectionError(f"Node {address} is temporarily unavailable")
        
        async with self._lock:
            if address in self._connections:
                conn = self._connections[address]
                if conn.open:
                    return conn
                del self._connections[address]
        
        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                ws = await asyncio.wait_for(
                    ws_connect(f"{node.ws_url}/data"),
                    timeout=self.CONNECT_TIMEOUT
                )
                
                async with self._lock:
                    self._connections[address] = ws
                    if address in self._connection_attempts:
                        del self._connection_attempts[address]
                
                asyncio.create_task(self._handle_responses(address, ws))
                return ws
                
            except asyncio.TimeoutError:
                last_error = f"Connection to {address} timed out"
            except Exception as e:
                last_error = str(e)
            
            if attempt < self.MAX_RETRIES - 1:
                await asyncio.sleep(self.RETRY_BACKOFF * (2 ** attempt))
        
        self._failed_nodes[address] = time.time()
        raise ConnectionError(f"Failed to connect to {address}: {last_error}")
    
    async def _handle_responses(self, address: str, ws):
        """Handle responses from a node"""
        try:
            async for message in ws:
                data = json.loads(message)
                request_id = data.get('request_id')
                if request_id and request_id in self._pending:
                    if not self._pending[request_id].done():
                        self._pending[request_id].set_result(data)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            print(f"Error handling response from {address}: {e}")
        finally:
            async with self._lock:
                if address in self._connections:
                    del self._connections[address]
            
            async with self._lock:
                for req_id, future in list(self._pending.items()):
                    if not future.done():
                        future.set_exception(
                            ConnectionError(f"Connection to {address} closed")
                        )
    
    async def request(
        self,
        node: NodeInfo,
        action: str,
        data: Dict[str, Any],
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Send a request to a remote node with retry and timeout"""
        request_id = str(uuid.uuid4())
        timeout = timeout or self.REQUEST_TIMEOUT
        
        request = {
            'request_id': request_id,
            'action': action,
            'data': data
        }
        
        last_error = None
        for attempt in range(self.MAX_RETRIES):
            future: asyncio.Future = asyncio.Future()
            self._pending[request_id] = future
            
            try:
                ws = await self._get_connection(node)
                await ws.send(json.dumps(request))
                
                result = await asyncio.wait_for(future, timeout=timeout)
                
                if result.get('error'):
                    raise Exception(result['error'])
                
                return result
                
            except asyncio.TimeoutError:
                last_error = f"Request to {node.address} timed out"
            except ConnectionError as e:
                last_error = str(e)
                break
            except Exception as e:
                last_error = str(e)
            finally:
                if request_id in self._pending:
                    del self._pending[request_id]
            
            if attempt < self.MAX_RETRIES - 1:
                request_id = str(uuid.uuid4())
                await asyncio.sleep(self.RETRY_BACKOFF * (2 ** attempt))
        
        raise TimeoutError(f"Request failed after {self.MAX_RETRIES} attempts: {last_error}")
    
    async def fetch_guild_data(
        self,
        node: NodeInfo,
        guild_id: int,
        table_name: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch all data for a guild from a remote node"""
        response = await self.request(node, 'fetch_guild_data', {
            'guild_id': guild_id,
            'table_name': table_name
        })
        
        if response.get('success'):
            return response.get('data')
        return None
    
    async def query_remote(
        self,
        node: NodeInfo,
        table_name: str,
        query_params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute a query on a remote node"""
        response = await self.request(node, 'query', {
            'table_name': table_name,
            **query_params
        })
        
        if response.get('success'):
            return response.get('rows', [])
        return []
    
    async def invalidate_cache(
        self,
        node: NodeInfo,
        guild_id: int,
        table_name: Optional[str] = None
    ):
        """Tell a remote node to invalidate its cache for a guild"""
        await self.request(node, 'invalidate_cache', {
            'guild_id': guild_id,
            'table_name': table_name
        })
    
    async def close(self):
        """Close all connections"""
        for ws in self._connections.values():
            try:
                await ws.close()
            except:
                pass
        self._connections.clear()


@dataclass
class CachedData:
    """Cached data from remote nodes"""
    data: Dict[str, Any]
    source_node: str
    fetched_at: float
    ttl_seconds: float
    access_count: int = 0
    
    def is_expired(self) -> bool:
        return time.time() - self.fetched_at > self.ttl_seconds
    
    def access(self):
        self.access_count += 1


class DistributedCache:
    """
    CDN-style distributed cache
    Caches data fetched from owner nodes locally
    """
    
    DEFAULT_TTL = 60.0  # 1 minute default TTL
    MAX_CACHE_SIZE = 10000
    CLEANUP_INTERVAL = 30.0
    
    def __init__(
        self,
        node_client: NodeClient,
        hash_ring: ConsistentHashRing,
        local_node_id: str,
        ttl_seconds: float = DEFAULT_TTL
    ):
        self.node_client = node_client
        self.hash_ring = hash_ring
        self.local_node_id = local_node_id
        self.ttl_seconds = ttl_seconds
        
        self._cache: Dict[str, CachedData] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None
        
        self._hits = 0
        self._misses = 0
        self._fetches = 0
    
    def _make_key(self, guild_id: int, table_name: str, extra: str = "") -> str:
        return f"{guild_id}:{table_name}:{extra}"
    
    async def start(self):
        """Start the cache service"""
        if self._running:
            return
        
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
    
    async def stop(self):
        """Stop the cache service"""
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def get(
        self,
        guild_id: int,
        table_name: str,
        fetch_if_missing: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Get data from cache or fetch from owner node"""
        key = self._make_key(guild_id, table_name)
        
        async with self._lock:
            if key in self._cache:
                cached = self._cache[key]
                if not cached.is_expired():
                    cached.access()
                    self._hits += 1
                    return cached.data
                else:
                    del self._cache[key]
        
        self._misses += 1
        
        if not fetch_if_missing:
            return None
        
        owner = await self.hash_ring.get_node_for_guild(guild_id)
        if not owner:
            return None
        
        if owner.node_id == self.local_node_id:
            return None
        
        try:
            data = await self.node_client.fetch_guild_data(
                owner, guild_id, table_name
            )
            
            if data is not None:
                await self.set(guild_id, table_name, data, owner.node_id)
                self._fetches += 1
            
            return data
            
        except Exception as e:
            print(f"Error fetching from remote node: {e}")
            return None
    
    async def set(
        self,
        guild_id: int,
        table_name: str,
        data: Dict[str, Any],
        source_node: str,
        ttl: Optional[float] = None
    ):
        """Set data in cache"""
        key = self._make_key(guild_id, table_name)
        
        async with self._lock:
            if len(self._cache) >= self.MAX_CACHE_SIZE:
                await self._evict_oldest()
            
            self._cache[key] = CachedData(
                data=data,
                source_node=source_node,
                fetched_at=time.time(),
                ttl_seconds=ttl or self.ttl_seconds
            )
    
    async def invalidate(
        self,
        guild_id: int,
        table_name: Optional[str] = None
    ):
        """Invalidate cached data for a guild"""
        async with self._lock:
            if table_name:
                key = self._make_key(guild_id, table_name)
                if key in self._cache:
                    del self._cache[key]
            else:
                prefix = f"{guild_id}:"
                keys_to_delete = [
                    k for k in self._cache if k.startswith(prefix)
                ]
                for k in keys_to_delete:
                    del self._cache[k]
    
    async def invalidate_from_node(self, source_node: str):
        """Invalidate all data from a specific node"""
        async with self._lock:
            keys_to_delete = [
                k for k, v in self._cache.items()
                if v.source_node == source_node
            ]
            for k in keys_to_delete:
                del self._cache[k]
    
    async def _evict_oldest(self):
        """Evict oldest entries when cache is full"""
        if not self._cache:
            return
        
        sorted_entries = sorted(
            self._cache.items(),
            key=lambda x: x[1].fetched_at
        )
        
        evict_count = max(1, len(self._cache) // 10)
        for i in range(evict_count):
            if i < len(sorted_entries):
                del self._cache[sorted_entries[i][0]]
    
    async def _cleanup_loop(self):
        """Periodically clean up expired entries"""
        while self._running:
            try:
                async with self._lock:
                    expired = [
                        k for k, v in self._cache.items()
                        if v.is_expired()
                    ]
                    for k in expired:
                        del self._cache[k]
            except Exception as e:
                print(f"Error in cache cleanup: {e}")
            
            await asyncio.sleep(self.CLEANUP_INTERVAL)
    
    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            'size': len(self._cache),
            'max_size': self.MAX_CACHE_SIZE,
            'hits': self._hits,
            'misses': self._misses,
            'fetches': self._fetches,
            'hit_rate': self._hits / total if total > 0 else 0.0,
            'ttl_seconds': self.ttl_seconds
        }


class ClusterManager:
    """
    Main cluster management class
    Coordinates all cluster components
    
    Features:
    - Cluster-aware fan-out reads: queries broadcast to all clusters
    - Random server selection per cluster for responses
    - CDN-style data distribution
    """
    
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        data_port: int = 8081,
        virtual_nodes: int = 150,
        node_weight: float = 1.0
    ):
        self.local_node = NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            metadata={'data_port': data_port, 'weight': node_weight}
        )
        self.node_weight = node_weight
        
        self.hash_ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
        self.registry = NodeRegistry(self.local_node, self.hash_ring, default_weight=node_weight)
        self.node_client = NodeClient()
        self.distributed_cache = DistributedCache(
            self.node_client,
            self.hash_ring,
            node_id
        )
        
        self._running = False
        self._data_handlers: Dict[str, Callable] = {}
        
        self._cluster_groups: Dict[str, List[NodeInfo]] = {}
        
        import random
        self._random = random
    
    def register_data_handler(self, action: str, handler: Callable):
        """Register a handler for data requests"""
        self._data_handlers[action] = handler
    
    async def start(self, seed_nodes: Optional[List[str]] = None):
        """Start the cluster manager"""
        if self._running:
            return
        
        self._running = True
        
        await self.node_client.start()
        await self.registry.start()
        await self.distributed_cache.start()
        
        if seed_nodes:
            await self.registry.join_cluster(seed_nodes)
        
        self.registry.on_node_change(self._on_node_change)
    
    async def stop(self):
        """Stop the cluster manager"""
        self._running = False
        
        await self.distributed_cache.stop()
        await self.registry.stop()
        await self.node_client.stop()
    
    async def _on_node_change(self, event: str, node: NodeInfo):
        """Handle node join/leave events"""
        if event == 'leave':
            await self.distributed_cache.invalidate_from_node(node.node_id)
    
    async def is_owner(self, guild_id: int) -> bool:
        """Check if this node owns a guild"""
        owner = await self.hash_ring.get_node_for_guild(guild_id)
        return owner is not None and owner.node_id == self.local_node.node_id
    
    async def get_owner_node(self, guild_id: int) -> Optional[NodeInfo]:
        """Get the owner node for a guild"""
        return await self.hash_ring.get_node_for_guild(guild_id)
    
    async def get_data(
        self,
        guild_id: int,
        table_name: str,
        local_getter: Callable
    ) -> Optional[Dict[str, Any]]:
        """
        Get data for a guild - from local storage if owner, or from cache/remote
        """
        if await self.is_owner(guild_id):
            return await local_getter(guild_id, table_name)
        
        cached = await self.distributed_cache.get(guild_id, table_name)
        if cached is not None:
            return cached
        
        return None
    
    async def write_data(
        self,
        guild_id: int,
        table_name: str,
        data: Dict[str, Any],
        local_writer: Callable
    ) -> bool:
        """
        Write data for a guild - only if this node is the owner
        """
        if not await self.is_owner(guild_id):
            owner = await self.get_owner_node(guild_id)
            if owner:
                try:
                    response = await self.node_client.request(
                        owner, 'write_data', {
                            'guild_id': guild_id,
                            'table_name': table_name,
                            'data': data
                        }
                    )
                    return response.get('success', False)
                except Exception as e:
                    print(f"Error writing to remote node: {e}")
                    return False
            return False
        
        result = await local_writer(guild_id, table_name, data)
        
        peers = self.registry.get_all_peers()
        for peer in peers:
            try:
                await self.node_client.invalidate_cache(
                    peer, guild_id, table_name
                )
            except:
                pass
        
        return result
    
    async def broadcast_invalidation(self, guild_id: int, table_name: Optional[str] = None):
        """Broadcast cache invalidation to all nodes"""
        message = {
            'type': 'invalidate_cache',
            'guild_id': guild_id,
            'table_name': table_name
        }
        await self.registry.broadcast(message)
    
    def _group_nodes_by_cluster(self, nodes: List[NodeInfo]) -> Dict[str, List[NodeInfo]]:
        """
        Group nodes by their cluster ID.
        Cluster ID is determined by the 'cluster_id' field in node metadata,
        or falls back to using the host as a cluster identifier.
        """
        clusters: Dict[str, List[NodeInfo]] = {}
        for node in nodes:
            cluster_id = node.metadata.get('cluster_id', node.host)
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(node)
        return clusters
    
    def _select_random_node_per_cluster(
        self, 
        clusters: Dict[str, List[NodeInfo]]
    ) -> List[NodeInfo]:
        """
        Randomly select one healthy node from each cluster.
        Only selects nodes that are in ACTIVE state.
        """
        selected_nodes = []
        for cluster_id, nodes in clusters.items():
            healthy_nodes = [n for n in nodes if n.is_healthy()]
            if healthy_nodes:
                selected_nodes.append(self._random.choice(healthy_nodes))
        return selected_nodes
    
    async def fan_out_read(
        self,
        table_name: str,
        query_params: Dict[str, Any],
        local_reader: Callable,
        merge_strategy: str = 'union',
        timeout: float = 5.0
    ) -> List[Dict[str, Any]]:
        """
        Fan-out read: broadcasts query to all clusters.
        
        Queries are sent to one randomly selected server per cluster.
        Results are merged based on the specified strategy.
        
        Args:
            table_name: Name of the table to query
            query_params: Query parameters (conditions, columns, etc.)
            local_reader: Callback function to read local data
            merge_strategy: How to merge results:
                - 'union': Combine all results (default)
                - 'first_positive': Return first non-empty response
                - 'fastest': Return fastest response regardless of content
            timeout: Maximum wait time per query in seconds
        
        Returns:
            Merged list of rows from all clusters
        """
        all_peers = self.registry.get_all_peers()
        
        clusters = self._group_nodes_by_cluster(all_peers)
        
        selected_nodes = self._select_random_node_per_cluster(clusters)
        
        async def query_local_with_timeout():
            try:
                return await asyncio.wait_for(
                    local_reader(table_name, query_params),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                return []
            except Exception as e:
                return []
        
        async def query_remote_with_timeout(node: NodeInfo):
            try:
                return await asyncio.wait_for(
                    self.node_client.query_remote(node, table_name, query_params),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                return []
            except Exception as e:
                return []
        
        tasks = [asyncio.create_task(query_local_with_timeout())]
        
        for node in selected_nodes:
            tasks.append(asyncio.create_task(query_remote_with_timeout(node)))
        
        if merge_strategy == 'first_positive':
            while tasks:
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                tasks = list(pending)
                
                for task in done:
                    try:
                        result = task.result()
                        if result and len(result) > 0:
                            for t in pending:
                                t.cancel()
                            return result
                    except:
                        pass
            return []
        
        elif merge_strategy == 'fastest':
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            for task in done:
                try:
                    return task.result() or []
                except:
                    pass
            return []
        
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            merged = []
            seen_keys = set()
            
            for result in results:
                if isinstance(result, Exception):
                    continue
                if not result:
                    continue
                    
                for row in result:
                    if isinstance(row, dict):
                        row_key = self._generate_row_key(row)
                        if row_key not in seen_keys:
                            seen_keys.add(row_key)
                            merged.append(row)
            
            return merged
    
    def _generate_row_key(self, row: Dict[str, Any]) -> str:
        """Generate a unique key for a row to detect duplicates"""
        if 'id' in row:
            return f"id:{row['id']}"
        if 'row_id' in row:
            return f"row_id:{row['row_id']}"
        key_parts = sorted([(k, str(v)) for k, v in row.items()])
        return hashlib.md5(str(key_parts).encode()).hexdigest()
    
    async def fan_out_query(
        self,
        table_name: str,
        query_params: Dict[str, Any],
        local_reader: Callable,
        timeout: float = 5.0
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Fan-out query with detailed response metadata.
        
        Args:
            table_name: Name of the table to query
            query_params: Query parameters
            local_reader: Callback for local data
            timeout: Maximum time to wait for responses
        
        Returns:
            Tuple of (merged_results, metadata)
            metadata includes: responding_clusters, failed_clusters, response_times
        """
        all_peers = self.registry.get_all_peers()
        clusters = self._group_nodes_by_cluster(all_peers)
        selected_nodes = self._select_random_node_per_cluster(clusters)
        
        start_time = time.time()
        response_times: Dict[str, float] = {}
        failed_clusters: List[str] = []
        responding_clusters: List[str] = []
        
        async def timed_query(node: NodeInfo, cluster_id: str):
            node_start = time.time()
            try:
                result = await asyncio.wait_for(
                    self.node_client.query_remote(node, table_name, query_params),
                    timeout=timeout
                )
                response_times[cluster_id] = time.time() - node_start
                responding_clusters.append(cluster_id)
                return result
            except Exception:
                failed_clusters.append(cluster_id)
                return []
        
        tasks = []
        
        async def local_query():
            try:
                result = await local_reader(table_name, query_params)
                response_times['local'] = time.time() - start_time
                responding_clusters.append('local')
                return result
            except Exception:
                failed_clusters.append('local')
                return []
        
        tasks.append(asyncio.create_task(local_query()))
        
        for node in selected_nodes:
            cluster_id = node.metadata.get('cluster_id', node.host)
            tasks.append(asyncio.create_task(timed_query(node, cluster_id)))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        merged = []
        seen_keys = set()
        
        for result in results:
            if isinstance(result, Exception):
                continue
            if not result:
                continue
            for row in result:
                if isinstance(row, dict):
                    row_key = self._generate_row_key(row)
                    if row_key not in seen_keys:
                        seen_keys.add(row_key)
                        merged.append(row)
        
        metadata = {
            'total_time': time.time() - start_time,
            'responding_clusters': responding_clusters,
            'failed_clusters': failed_clusters,
            'response_times': response_times,
            'total_clusters': len(clusters) + 1,
            'total_rows': len(merged)
        }
        
        return merged, metadata
    
    def stats(self) -> Dict[str, Any]:
        return {
            'local_node': self.local_node.to_dict(),
            'hash_ring': self.hash_ring.stats(),
            'registry': self.registry.stats(),
            'cache': self.distributed_cache.stats(),
            'cluster_groups': len(self._cluster_groups)
        }
