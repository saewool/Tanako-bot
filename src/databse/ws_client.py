"""
WebSocket Database Client
Client for communicating with the Database API via WebSocket
"""

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
import websockets
from websockets.asyncio.client import ClientConnection

logger = logging.getLogger('db_client')


class DatabaseClient:
    def __init__(
        self,
        uri: str = "ws://localhost:8080",
        reconnect_interval: float = 5.0,
        max_reconnect_attempts: int = -1
    ):
        self.uri = uri
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        
        self._ws: Optional[ClientConnection] = None
        self._connected = False
        self._reconnecting = False
        self._pending_requests: Dict[str, asyncio.Future[Dict[str, Any]]] = {}
        self._receive_task: Optional[asyncio.Task[None]] = None
        self._reconnect_task: Optional[asyncio.Task[None]] = None
        self._lock = asyncio.Lock()
        self._event_handlers: Dict[str, List[Callable[..., Any]]] = {}
        self._reconnect_attempts = 0
    
    @property
    def is_connected(self) -> bool:
        return self._connected and self._ws is not None
    
    async def connect(self) -> bool:
        if self._connected:
            return True
        
        try:
            self._ws = await websockets.connect(
                self.uri,
                ping_interval=30,
                ping_timeout=10,
                max_size=10 * 1024 * 1024
            )
            self._connected = True
            self._reconnect_attempts = 0
            
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            logger.info(f"Connected to Database API at {self.uri}")
            await self._emit('connected')
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Database API: {e}")
            self._connected = False
            asyncio.create_task(self._reconnect())
            return False
    
    async def disconnect(self):
        self._connected = False
        
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        for future in self._pending_requests.values():
            if not future.done():
                future.set_exception(ConnectionError("Disconnected"))
        self._pending_requests.clear()
        
        logger.info("Disconnected from Database API")
        await self._emit('disconnected')
    
    async def _receive_loop(self) -> None:
        if self._ws is None:
            return
        
        try:
            async for message in self._ws:
                try:
                    if isinstance(message, bytes):
                        message = message.decode('utf-8')
                    response = json.loads(message)
                    request_id = response.get('request_id')
                    
                    if request_id and request_id in self._pending_requests:
                        future = self._pending_requests.pop(request_id)
                        if not future.done():
                            future.set_result(response)
                    else:
                        await self._emit('message', response)
                        
                except json.JSONDecodeError:
                    logger.warning("Received invalid JSON from server")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Connection to Database API closed")
            self._connected = False
            await self._emit('disconnected')
            asyncio.create_task(self._reconnect())
        except Exception as e:
            logger.error(f"Error in receive loop: {e}")
            self._connected = False
            asyncio.create_task(self._reconnect())
    
    async def _reconnect(self):
        if self._reconnecting:
            return
        
        self._reconnecting = True
        
        while not self._connected:
            if self.max_reconnect_attempts > 0 and self._reconnect_attempts >= self.max_reconnect_attempts:
                logger.error("Max reconnection attempts reached")
                self._reconnecting = False
                return
            
            self._reconnect_attempts += 1
            logger.info(f"Attempting to reconnect... (attempt {self._reconnect_attempts})")
            
            await asyncio.sleep(self.reconnect_interval)
            
            try:
                self._ws = await websockets.connect(
                    self.uri,
                    ping_interval=30,
                    ping_timeout=10
                )
                self._connected = True
                self._reconnect_attempts = 0
                
                self._receive_task = asyncio.create_task(self._receive_loop())
                
                logger.info("Reconnected to Database API")
                await self._emit('reconnected')
                break
                
            except Exception as e:
                logger.warning(f"Reconnection failed: {e}")
        
        self._reconnecting = False
    
    async def request(
        self,
        action: str,
        data: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        if not self.is_connected:
            await self.connect()
            if not self.is_connected:
                raise ConnectionError("Not connected to Database API")
        
        if self._ws is None:
            raise ConnectionError("WebSocket connection is not available")
        
        request_id = str(uuid.uuid4())
        request_payload = {
            'action': action,
            'data': data or {},
            'request_id': request_id
        }
        
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Dict[str, Any]] = loop.create_future()
        self._pending_requests[request_id] = future
        
        try:
            await self._ws.send(json.dumps(request_payload, default=str))
            response = await asyncio.wait_for(future, timeout=timeout)
            
            if not response.get('success', False) and 'error' in response:
                raise Exception(response['error'])
            
            return response
            
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            raise TimeoutError(f"Request timed out after {timeout}s")
        except Exception:
            self._pending_requests.pop(request_id, None)
            raise
    
    def on(self, event: str, handler: Callable):
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(handler)
    
    def off(self, event: str, handler: Callable):
        if event in self._event_handlers:
            self._event_handlers[event] = [h for h in self._event_handlers[event] if h != handler]
    
    async def _emit(self, event: str, *args):
        if event in self._event_handlers:
            for handler in self._event_handlers[event]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(*args)
                    else:
                        handler(*args)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")
    
    async def ping(self) -> bool:
        try:
            response = await self.request('ping', timeout=5.0)
            return response.get('pong', False)
        except:
            return False
    
    async def insert(self, table: str, row: Dict[str, Any]) -> int:
        response = await self.request('insert', {'table': table, 'row': row})
        return response.get('row_id', -1)
    
    async def insert_many(self, table: str, rows: List[Dict[str, Any]]) -> List[int]:
        response = await self.request('insert_many', {'table': table, 'rows': rows})
        return response.get('row_ids', [])
    
    async def update(
        self,
        table: str,
        data: Dict[str, Any],
        conditions: Optional[Dict[str, Any]] = None
    ) -> int:
        response = await self.request('update', {
            'table': table,
            'data': data,
            'conditions': conditions or {}
        })
        return response.get('updated', 0)
    
    async def delete(self, table: str, conditions: Optional[Dict[str, Any]] = None) -> int:
        response = await self.request('delete', {
            'table': table,
            'conditions': conditions or {}
        })
        return response.get('deleted', 0)
    
    async def select(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        conditions: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        response = await self.request('select', {
            'table': table,
            'columns': columns,
            'conditions': conditions or {},
            'order_by': order_by,
            'limit': limit,
            'offset': offset
        })
        return response.get('rows', [])
    
    async def find_one(
        self,
        table: str,
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        response = await self.request('find_one', {
            'table': table,
            'conditions': conditions
        })
        return response.get('row')
    
    async def find_by_id(
        self,
        table: str,
        id_column: str,
        id_value: Any
    ) -> Optional[Dict[str, Any]]:
        response = await self.request('find_by_id', {
            'table': table,
            'id_column': id_column,
            'id_value': id_value
        })
        return response.get('row')
    
    async def count(
        self,
        table: str,
        conditions: Optional[Dict[str, Any]] = None
    ) -> int:
        response = await self.request('count', {
            'table': table,
            'conditions': conditions or {}
        })
        return response.get('count', 0)
    
    async def exists(
        self,
        table: str,
        conditions: Dict[str, Any]
    ) -> bool:
        response = await self.request('exists', {
            'table': table,
            'conditions': conditions
        })
        return response.get('exists', False)
    
    async def list_tables(self) -> List[str]:
        response = await self.request('list_tables')
        return response.get('tables', [])
    
    async def table_info(self, table: str) -> Optional[Dict[str, Any]]:
        response = await self.request('table_info', {'table': table})
        return response.get('info')
    
    async def stats(self) -> Dict[str, Any]:
        response = await self.request('stats')
        return response.get('stats', {})
    
    async def backup(self, backup_dir: str = 'data/backups') -> Dict[str, Any]:
        response = await self.request('backup', {'backup_dir': backup_dir})
        return response
    
    async def vacuum(self) -> bool:
        response = await self.request('vacuum')
        return response.get('success', False)
