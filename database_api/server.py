"""
WebSocket Database API Server
Handles all database operations via WebSocket protocol

Supports:
- Standard ColumnarDB operations
- Distributed mode with horizontal scaling
- Cluster operations for multi-node deployment
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, Set, Tuple
import websockets
from websockets.asyncio.server import ServerConnection
from websockets.http11 import Request, Response
from http import HTTPStatus

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.engine import ColumnarDB, Column as EngineColumn, DataType
from src.database.distributed_engine import DistributedColumnarDB, Column as DistributedColumn
from src.database.query import QueryBuilder, query

def Column(name, data_type, **kwargs):
    """Factory function to create the correct Column type"""
    return EngineColumn(name, data_type, **kwargs)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database_api')


class DatabaseAPIServer:
    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 8080, 
        data_dir: str = "data/kotonexus_takako",
        cluster_enabled: bool = False,
        node_id: Optional[str] = None,
        seed_nodes: Optional[str] = None,
        virtual_nodes: int = 150,
        node_weight: float = 1.0
    ):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.cluster_enabled = cluster_enabled
        self.node_id = node_id
        self.seed_nodes = seed_nodes.split(',') if seed_nodes else []
        self.virtual_nodes = virtual_nodes
        self.node_weight = node_weight
        self.db: Optional[ColumnarDB] = None
        self.distributed_db: Optional[DistributedColumnarDB] = None
        self.clients: Set[ServerConnection] = set()
        self._lock = asyncio.Lock()
        self._initialized = False
        
    async def initialize(self):
        if self._initialized:
            return
            
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs('data/logs', exist_ok=True)
        os.makedirs('data/backups', exist_ok=True)
        
        if self.cluster_enabled:
            self.distributed_db = DistributedColumnarDB(
                data_dir=self.data_dir,
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                cluster_enabled=True,
                virtual_nodes=self.virtual_nodes,
                node_weight=self.node_weight
            )
            await self.distributed_db.initialize()
            
            if self.seed_nodes:
                await self.distributed_db.join_cluster(self.seed_nodes)
            
            self.db = None
            logger.info(f"Distributed database initialized (node: {self.distributed_db.node_id})")
        else:
            self.db = ColumnarDB(self.data_dir)
            await self.db.initialize()
            self.distributed_db = None
            
        await self._create_additional_tables()
        self._initialized = True
        logger.info("Database initialized successfully")
    
    def _get_active_db(self):
        """Get the active database instance"""
        return self.distributed_db if self.cluster_enabled else self.db
    
    async def _create_additional_tables(self) -> None:
        db = self._get_active_db()
        if db is None:
            raise RuntimeError("Database not initialized")
        
        guilds_columns = [
            Column('guild_id', DataType.INT64, primary_key=True, indexed=True),
            Column('settings', DataType.JSON, default={}),
            Column('created_at', DataType.TIMESTAMP),
            Column('updated_at', DataType.TIMESTAMP),
            Column('case_counter', DataType.INT32, default=0),
            Column('ticket_counter', DataType.INT32, default=0),
            Column('filter_rules', DataType.JSON, default=[]),
            Column('custom_commands', DataType.JSON, default={})
        ]
        await db.create_table('guilds', guilds_columns, if_not_exists=True)
        
        users_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('user_id', DataType.INT64, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('data', DataType.JSON, default={}),
            Column('created_at', DataType.TIMESTAMP),
            Column('updated_at', DataType.TIMESTAMP)
        ]
        await db.create_table('users', users_columns, if_not_exists=True)
        
        tickets_columns = [
            Column('ticket_id', DataType.STRING, primary_key=True, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('channel_id', DataType.INT64, indexed=True),
            Column('creator_id', DataType.INT64, indexed=True),
            Column('category_id', DataType.STRING),
            Column('status', DataType.STRING, default='open'),
            Column('priority', DataType.INT32, default=2),
            Column('subject', DataType.STRING),
            Column('description', DataType.STRING),
            Column('created_at', DataType.TIMESTAMP),
            Column('updated_at', DataType.TIMESTAMP),
            Column('closed_at', DataType.TIMESTAMP, nullable=True),
            Column('claimed_by', DataType.INT64, nullable=True),
            Column('closed_by', DataType.INT64, nullable=True),
            Column('close_reason', DataType.STRING, nullable=True),
            Column('data', DataType.JSON, default={})
        ]
        await db.create_table('tickets', tickets_columns, if_not_exists=True)
        
        moderation_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('case_id', DataType.INT32, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('target_id', DataType.INT64, indexed=True),
            Column('moderator_id', DataType.INT64, indexed=True),
            Column('action', DataType.STRING, indexed=True),
            Column('reason', DataType.STRING),
            Column('created_at', DataType.TIMESTAMP),
            Column('expires_at', DataType.TIMESTAMP, nullable=True),
            Column('duration_seconds', DataType.INT64, nullable=True),
            Column('is_active', DataType.BOOL, default=True),
            Column('revoked', DataType.BOOL, default=False),
            Column('data', DataType.JSON, default={})
        ]
        await db.create_table('moderation_cases', moderation_columns, if_not_exists=True)
        
        filters_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('filter_type', DataType.STRING, indexed=True),
            Column('pattern', DataType.STRING),
            Column('action', DataType.STRING),
            Column('enabled', DataType.BOOL, default=True),
            Column('created_at', DataType.TIMESTAMP),
            Column('data', DataType.JSON, default={})
        ]
        await db.create_table('filters', filters_columns, if_not_exists=True)
        
        log_configs_columns = [
            Column('guild_id', DataType.INT64, primary_key=True, indexed=True),
            Column('enabled', DataType.BOOL, default=True),
            Column('channels', DataType.JSON, default={}),
            Column('events', DataType.JSON, default={}),
            Column('ignored', DataType.JSON, default={}),
            Column('updated_at', DataType.TIMESTAMP)
        ]
        await db.create_table('log_configs', log_configs_columns, if_not_exists=True)
        
        level_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('user_id', DataType.INT64, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('xp', DataType.INT32, default=0),
            Column('level', DataType.INT32, default=0),
            Column('total_xp', DataType.INT64, default=0),
            Column('daily_messages', DataType.INT32, default=0),
            Column('last_xp_date', DataType.STRING),
            Column('last_message_time', DataType.TIMESTAMP),
            Column('updated_at', DataType.TIMESTAMP)
        ]
        await db.create_table('user_levels', level_columns, if_not_exists=True)
        
        secret_users_columns = [
            Column('user_id', DataType.INT64, primary_key=True, indexed=True),
            Column('nickname', DataType.STRING, indexed=True),
            Column('created_at', DataType.TIMESTAMP),
            Column('is_active', DataType.BOOL, default=True)
        ]
        await db.create_table('secret_users', secret_users_columns, if_not_exists=True)
        
        secret_chats_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('sender_id', DataType.INT64, indexed=True),
            Column('receiver_id', DataType.INT64, indexed=True),
            Column('created_at', DataType.TIMESTAMP),
            Column('expires_at', DataType.TIMESTAMP, nullable=True)
        ]
        await db.create_table('secret_chats', secret_chats_columns, if_not_exists=True)
        
        host_checks_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('guild_id', DataType.INT64, indexed=True),
            Column('url', DataType.STRING),
            Column('name', DataType.STRING),
            Column('check_type', DataType.STRING),
            Column('check_interval', DataType.INT32, default=60),
            Column('last_status', DataType.STRING),
            Column('last_check', DataType.TIMESTAMP),
            Column('notify_channel_id', DataType.INT64, nullable=True),
            Column('created_at', DataType.TIMESTAMP),
            Column('is_active', DataType.BOOL, default=True)
        ]
        await db.create_table('host_checks', host_checks_columns, if_not_exists=True)
        
        node_status_columns = [
            Column('shard_id', DataType.INT32, primary_key=True, indexed=True),
            Column('status', DataType.STRING),
            Column('latency', DataType.FLOAT64),
            Column('guild_count', DataType.INT32, default=0),
            Column('member_count', DataType.INT64, default=0),
            Column('uptime_seconds', DataType.INT64, default=0),
            Column('memory_mb', DataType.FLOAT64),
            Column('cpu_percent', DataType.FLOAT64),
            Column('last_heartbeat', DataType.TIMESTAMP),
            Column('started_at', DataType.TIMESTAMP),
            Column('version', DataType.STRING)
        ]
        await db.create_table('node_status', node_status_columns, if_not_exists=True)
        
        metrics_columns = [
            Column('id', DataType.STRING, primary_key=True, indexed=True),
            Column('shard_id', DataType.INT32, indexed=True),
            Column('metric_type', DataType.STRING, indexed=True),
            Column('value', DataType.FLOAT64),
            Column('timestamp', DataType.TIMESTAMP, indexed=True),
            Column('data', DataType.JSON)
        ]
        await db.create_table('metrics', metrics_columns, if_not_exists=True)
        
        logger.info("Additional tables created")
    
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        action = request.get('action')
        data = request.get('data', {})
        request_id = request.get('request_id')
        
        db = self._get_active_db()
        if db is None:
            return {
                'request_id': request_id,
                'success': False,
                'error': 'Database not initialized'
            }
        
        try:
            async with self._lock:
                if action == 'ping':
                    result = {'pong': True, 'timestamp': datetime.now().isoformat()}
                
                elif action == 'insert':
                    table = data.get('table')
                    row_data = data.get('row')
                    row_id = await db.insert(table, row_data)
                    result = {'row_id': row_id, 'success': True}
                
                elif action == 'insert_many':
                    table = data.get('table')
                    rows = data.get('rows')
                    row_ids = await db.insert_many(table, rows)
                    result = {'row_ids': row_ids, 'success': True}
                
                elif action == 'update':
                    table = data.get('table')
                    update_data = data.get('data')
                    conditions = data.get('conditions', {})
                    
                    q = None
                    if conditions:
                        q = query(table)
                        for col, val in conditions.items():
                            q.where_eq(col, val)
                    
                    count = await db.update(table, update_data, q)
                    result = {'updated': count, 'success': True}
                
                elif action == 'delete':
                    table = data.get('table')
                    conditions = data.get('conditions', {})
                    
                    q = None
                    if conditions:
                        q = query(table)
                        for col, val in conditions.items():
                            q.where_eq(col, val)
                    
                    count = await db.delete(table, q)
                    result = {'deleted': count, 'success': True}
                
                elif action == 'select':
                    table = data.get('table')
                    columns = data.get('columns')
                    conditions = data.get('conditions', {})
                    order_by = data.get('order_by')
                    limit = data.get('limit')
                    offset = data.get('offset', 0)
                    
                    q = None
                    if conditions:
                        q = query(table)
                        for col, val in conditions.items():
                            q.where_eq(col, val)
                    
                    rows = await db.select(
                        table,
                        columns=columns,
                        condition=q,
                        order_by=order_by,
                        limit=limit,
                        offset=offset
                    )
                    result = {'rows': self._serialize_rows(rows), 'count': len(rows)}
                
                elif action == 'find_one':
                    table = data.get('table')
                    conditions = data.get('conditions', {})
                    
                    q = query(table)
                    for col, val in conditions.items():
                        q.where_eq(col, val)
                    
                    row = await db.find_one(table, q)
                    result = {'row': self._serialize_row(row) if row else None}
                
                elif action == 'find_by_id':
                    table = data.get('table')
                    id_column = data.get('id_column')
                    id_value = data.get('id_value')
                    
                    row = await db.find_by_id(table, id_column, id_value)
                    result = {'row': self._serialize_row(row) if row else None}
                
                elif action == 'count':
                    table = data.get('table')
                    conditions = data.get('conditions', {})
                    
                    q = None
                    if conditions:
                        q = query(table)
                        for col, val in conditions.items():
                            q.where_eq(col, val)
                    
                    count = await db.count(table, q)
                    result = {'count': count}
                
                elif action == 'exists':
                    table = data.get('table')
                    conditions = data.get('conditions', {})
                    
                    q = query(table)
                    for col, val in conditions.items():
                        q.where_eq(col, val)
                    
                    exists = await db.exists(table, q)
                    result = {'exists': exists}
                
                elif action == 'list_tables':
                    tables = await db.list_tables()
                    result = {'tables': tables}
                
                elif action == 'table_info':
                    table = data.get('table')
                    info = await db.get_table_info(table)
                    result = {'info': info}
                
                elif action == 'stats':
                    stats = await db.stats()
                    result = {'stats': stats}
                
                elif action == 'backup':
                    backup_dir = data.get('backup_dir', 'data/backups')
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_path = f"{backup_dir}/backup_{timestamp}"
                    if hasattr(db, 'backup'):
                        success = await db.backup(backup_path)
                    else:
                        success = False
                    result = {'success': success, 'path': backup_path}
                
                elif action == 'vacuum':
                    if hasattr(db, 'vacuum'):
                        await db.vacuum()
                    elif hasattr(db, 'flush_all'):
                        await db.flush_all()
                    result = {'success': True}
                
                elif action == 'cluster_stats':
                    if self.cluster_enabled and self.distributed_db:
                        cluster_stats = await self.distributed_db.get_cluster_stats()
                        result = {'cluster': cluster_stats}
                    else:
                        result = {'cluster': None, 'message': 'Cluster mode not enabled'}
                
                else:
                    result = {'error': f'Unknown action: {action}'}
                
            return {
                'request_id': request_id,
                'success': 'error' not in result,
                **result
            }
            
        except Exception as e:
            logger.error(f"Error handling request: {e}", exc_info=True)
            return {
                'request_id': request_id,
                'success': False,
                'error': str(e)
            }
    
    def _serialize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not row:
            return row
        
        serialized = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized
    
    def _serialize_rows(self, rows: list) -> list:
        return [self._serialize_row(row) for row in rows]
    
    async def handler(self, websocket: ServerConnection) -> None:
        self.clients.add(websocket)
        client_id = id(websocket)
        logger.info(f"Client connected: {client_id}")
        
        try:
            async for message in websocket:
                try:
                    if isinstance(message, bytes):
                        message = message.decode('utf-8')
                    request = json.loads(message)
                    response = await self.handle_request(request)
                    await websocket.send(json.dumps(response, default=str))
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'success': False,
                        'error': 'Invalid JSON'
                    }))
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await websocket.send(json.dumps({
                        'success': False,
                        'error': str(e)
                    }))
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {client_id}")
        finally:
            self.clients.discard(websocket)
    
    async def broadcast(self, message: Dict[str, Any]) -> None:
        if self.clients:
            await asyncio.gather(
                *[client.send(json.dumps(message, default=str)) for client in self.clients],
                return_exceptions=True
            )
    
    def process_request(self, connection: ServerConnection, request: Request) -> Optional[Response]:
        """Handle HTTP requests gracefully (health checks, probes, etc.)"""
        upgrade = request.headers.get("Upgrade", "").lower()
        
        if upgrade != "websocket":
            if request.path == "/health" or request.path == "/":
                return Response(
                    HTTPStatus.OK,
                    "OK\r\n",
                    websockets.Headers([
                        ("Content-Type", "text/plain"),
                        ("Content-Length", "4")
                    ])
                )
            return Response(
                HTTPStatus.BAD_REQUEST,
                "WebSocket endpoint only\r\n",
                websockets.Headers([
                    ("Content-Type", "text/plain"),
                    ("Content-Length", "23")
                ])
            )
        return None
    
    async def start(self) -> None:
        await self.initialize()
        
        logger.info(f"Starting WebSocket Database API on {self.host}:{self.port}")
        
        async with websockets.serve(
            self.handler,
            self.host,
            self.port,
            ping_interval=30,
            ping_timeout=10,
            max_size=10 * 1024 * 1024,
            process_request=self.process_request
        ):
            logger.info(f"Database API Server running on ws://{self.host}:{self.port}")
            await asyncio.Future()
    
    async def close(self):
        logger.info("Shutting down Database API Server...")
        
        for client in self.clients:
            await client.close()
        
        db = self._get_active_db()
        if db:
            await db.close()


async def main():
    host = os.getenv('DB_API_HOST', '0.0.0.0')
    port = int(os.getenv('DB_API_PORT', '8080'))
    data_dir = os.getenv('DB_DATA_DIR', 'data/kotonexus_takako')
    
    cluster_enabled = os.getenv('DB_CLUSTER_ENABLED', 'false').lower() == 'true'
    node_id = os.getenv('DB_NODE_ID')
    seed_nodes = os.getenv('DB_SEED_NODES')
    
    server = DatabaseAPIServer(
        host=host, 
        port=port, 
        data_dir=data_dir,
        cluster_enabled=cluster_enabled,
        node_id=node_id,
        seed_nodes=seed_nodes
    )
    
    if cluster_enabled:
        logger.info(f"Starting in CLUSTER mode (node_id: {node_id})")
        if seed_nodes:
            logger.info(f"Seed nodes: {seed_nodes}")
    else:
        logger.info("Starting in STANDALONE mode")
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await server.close()


if __name__ == '__main__':
    asyncio.run(main())
