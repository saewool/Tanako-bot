"""
Database Manager (WebSocket Client Version)
Provides a high-level interface for database operations via WebSocket API
"""

import os
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, date

from src.database.ws_client import DatabaseClient
from src.models.guild import GuildConfig, GuildSettings
from src.models.moderation import ModerationCase, ModerationAction
from src.models.ticket import Ticket, TicketStatus
from src.models.user import UserData, GlobalUserData
from src.models.filter import FilterConfig, FilterRule
from src.models.logs import LogConfig


class DatabaseManager:
    def __init__(self, db_api_uri: Optional[str] = None):
        uri = db_api_uri or os.getenv('DB_API_URI', 'ws://localhost:8080')
        self.client = DatabaseClient(uri=uri)
        self._cache: Dict[str, Dict[Any, Any]] = {
            'guilds': {},
            'users': {},
            'tickets': {},
            'cases': {},
            'filters': {},
            'logs': {},
            'levels': {}
        }
    
    async def initialize(self):
        await self.client.connect()
    
    async def get_guild_config(self, guild_id: int) -> Optional[GuildConfig]:
        if guild_id in self._cache['guilds']:
            return self._cache['guilds'][guild_id]
        
        result = await self.client.find_by_id('guilds', 'guild_id', guild_id)
        if not result:
            return None
        
        config = GuildConfig.from_dict({
            'guild_id': result['guild_id'],
            'settings': result.get('settings', {}),
            'created_at': result.get('created_at'),
            'updated_at': result.get('updated_at'),
            'case_counter': result.get('case_counter', 0),
            'ticket_counter': result.get('ticket_counter', 0),
            'filter_rules': result.get('filter_rules', []),
            'custom_commands': result.get('custom_commands', {})
        })
        
        self._cache['guilds'][guild_id] = config
        return config
    
    async def get_or_create_guild_config(self, guild_id: int) -> GuildConfig:
        config = await self.get_guild_config(guild_id)
        if config:
            return config
        
        config = GuildConfig(guild_id=guild_id)
        await self.save_guild_config(config)
        return config
    
    async def save_guild_config(self, config: GuildConfig):
        config.updated_at = datetime.now()
        
        data = {
            'guild_id': config.guild_id,
            'settings': config.settings.to_dict(),
            'created_at': config.created_at.isoformat() if config.created_at else datetime.now().isoformat(),
            'updated_at': config.updated_at.isoformat(),
            'case_counter': config.case_counter,
            'ticket_counter': config.ticket_counter,
            'filter_rules': config.filter_rules,
            'custom_commands': config.custom_commands
        }
        
        existing = await self.client.find_by_id('guilds', 'guild_id', config.guild_id)
        
        if existing:
            await self.client.update('guilds', data, {'guild_id': config.guild_id})
        else:
            await self.client.insert('guilds', data)
        
        self._cache['guilds'][config.guild_id] = config
    
    async def get_moderation_case(self, guild_id: int, case_id: int) -> Optional[ModerationCase]:
        result = await self.client.find_one('moderation_cases', {
            'guild_id': guild_id,
            'case_id': case_id
        })
        
        if not result:
            return None
        
        case_data = result.get('data', {})
        case_data.update({
            'case_id': result['case_id'],
            'guild_id': result['guild_id'],
            'target_id': result['target_id'],
            'moderator_id': result['moderator_id'],
            'action': result['action'],
            'reason': result['reason'],
            'created_at': result.get('created_at'),
            'expires_at': result.get('expires_at'),
            'duration_seconds': result.get('duration_seconds'),
            'is_active': result.get('is_active', True),
            'revoked': result.get('revoked', False)
        })
        
        return ModerationCase.from_dict(case_data)
    
    async def get_user_cases(self, guild_id: int, user_id: int) -> List[ModerationCase]:
        results = await self.client.select('moderation_cases', conditions={
            'guild_id': guild_id,
            'target_id': user_id
        })
        
        cases = []
        for result in results:
            case_data = result.get('data', {})
            case_data.update({
                'case_id': result['case_id'],
                'guild_id': result['guild_id'],
                'target_id': result['target_id'],
                'moderator_id': result['moderator_id'],
                'action': result['action'],
                'reason': result['reason'],
                'created_at': result.get('created_at'),
                'is_active': result.get('is_active', True)
            })
            cases.append(ModerationCase.from_dict(case_data))
        
        return sorted(cases, key=lambda c: c.case_id, reverse=True)
    
    async def save_moderation_case(self, case: ModerationCase):
        data = {
            'id': f"{case.guild_id}_{case.case_id}",
            'case_id': case.case_id,
            'guild_id': case.guild_id,
            'target_id': case.target_id,
            'moderator_id': case.moderator_id,
            'action': case.action.value,
            'reason': case.reason,
            'created_at': case.created_at.isoformat() if case.created_at else datetime.now().isoformat(),
            'expires_at': case.expires_at.isoformat() if case.expires_at else None,
            'duration_seconds': case.duration_seconds,
            'is_active': case.is_active,
            'revoked': case.revoked,
            'data': case.to_dict()
        }
        
        existing = await self.client.find_by_id('moderation_cases', 'id', data['id'])
        
        if existing:
            await self.client.update('moderation_cases', data, {'id': data['id']})
        else:
            await self.client.insert('moderation_cases', data)
    
    async def get_ticket_by_channel(self, channel_id: int) -> Optional[Ticket]:
        result = await self.client.find_one('tickets', {'channel_id': channel_id})
        
        if not result:
            return None
        
        ticket_data = result.get('data')
        if ticket_data and isinstance(ticket_data, dict) and 'ticket_id' in ticket_data:
            return Ticket.from_dict(ticket_data)
        
        if 'ticket_id' in result:
            return Ticket.from_dict({
                'ticket_id': result.get('ticket_id'),
                'guild_id': result.get('guild_id'),
                'channel_id': result.get('channel_id'),
                'creator_id': result.get('creator_id'),
                'category_id': result.get('category_id', 'general'),
                'status': result.get('status', 'open'),
                'priority': result.get('priority', 2),
                'subject': result.get('subject', ''),
                'description': result.get('description', ''),
                'created_at': result.get('created_at'),
                'updated_at': result.get('updated_at'),
                'closed_at': result.get('closed_at'),
                'claimed_by': result.get('claimed_by'),
                'closed_by': result.get('closed_by'),
                'close_reason': result.get('close_reason'),
                'added_users': result.get('added_users', []),
                'tags': result.get('tags', []),
                'message_count': result.get('message_count', 0),
                'staff_message_count': result.get('staff_message_count', 0),
                'first_response_at': result.get('first_response_at'),
                'first_response_by': result.get('first_response_by'),
                'rating': result.get('rating'),
                'feedback': result.get('feedback'),
                'notes': result.get('notes', []),
                'transcript_url': result.get('transcript_url')
            })
        
        return None
    
    async def get_user_open_tickets(self, guild_id: int, user_id: int) -> List[Ticket]:
        results = await self.client.select('tickets', conditions={
            'guild_id': guild_id,
            'creator_id': user_id
        })
        
        tickets = []
        for result in results:
            ticket_data = result.get('data')
            if ticket_data and isinstance(ticket_data, dict) and 'ticket_id' in ticket_data:
                ticket = Ticket.from_dict(ticket_data)
            elif 'ticket_id' in result:
                ticket = Ticket.from_dict({
                    'ticket_id': result.get('ticket_id'),
                    'guild_id': result.get('guild_id'),
                    'channel_id': result.get('channel_id'),
                    'creator_id': result.get('creator_id'),
                    'category_id': result.get('category_id', 'general'),
                    'status': result.get('status', 'open'),
                    'priority': result.get('priority', 2),
                    'created_at': result.get('created_at'),
                    'closed_at': result.get('closed_at'),
                    'claimed_by': result.get('claimed_by'),
                })
            else:
                continue
            
            if ticket.is_open:
                tickets.append(ticket)
        
        return tickets
    
    async def save_ticket(self, ticket: Ticket):
        ticket.updated_at = datetime.now()
        
        data = {
            'ticket_id': ticket.ticket_id,
            'guild_id': ticket.guild_id,
            'channel_id': ticket.channel_id,
            'creator_id': ticket.creator_id,
            'status': ticket.status.value,
            'created_at': ticket.created_at.isoformat() if ticket.created_at else datetime.now().isoformat(),
            'closed_at': ticket.closed_at.isoformat() if ticket.closed_at else None,
            'data': ticket.to_dict()
        }
        
        existing = await self.client.find_by_id('tickets', 'ticket_id', ticket.ticket_id)
        
        if existing:
            await self.client.update('tickets', data, {'ticket_id': ticket.ticket_id})
        else:
            await self.client.insert('tickets', data)
    
    async def get_user_data(self, user_id: int, guild_id: int) -> Optional[UserData]:
        cache_key = f"{guild_id}_{user_id}"
        if cache_key in self._cache['users']:
            return self._cache['users'][cache_key]
        
        user_key = f"{guild_id}_{user_id}"
        result = await self.client.find_by_id('users', 'id', user_key)
        
        if not result:
            return None
        
        user_data = UserData.from_dict(result.get('data', {
            'user_id': user_id,
            'guild_id': guild_id
        }))
        
        self._cache['users'][cache_key] = user_data
        return user_data
    
    async def get_or_create_user_data(self, user_id: int, guild_id: int) -> UserData:
        data = await self.get_user_data(user_id, guild_id)
        if data:
            return data
        
        data = UserData(user_id=user_id, guild_id=guild_id)
        await self.save_user_data(data)
        return data
    
    async def save_user_data(self, user_data: UserData):
        user_key = f"{user_data.guild_id}_{user_data.user_id}"
        
        data = {
            'id': user_key,
            'user_id': user_data.user_id,
            'guild_id': user_data.guild_id,
            'data': user_data.to_dict(),
            'updated_at': datetime.now().isoformat()
        }
        
        existing = await self.client.find_by_id('users', 'id', user_key)
        
        if existing:
            await self.client.update('users', data, {'id': user_key})
        else:
            await self.client.insert('users', data)
        
        cache_key = f"{user_data.guild_id}_{user_data.user_id}"
        self._cache['users'][cache_key] = user_data
    
    async def get_filter_config(self, guild_id: int) -> Optional[FilterConfig]:
        if guild_id in self._cache['filters']:
            return self._cache['filters'][guild_id]
        
        result = await self.client.find_by_id('filters', 'guild_id', guild_id)
        
        if not result:
            return None
        
        config = FilterConfig.from_dict(result.get('data', {'guild_id': guild_id}))
        self._cache['filters'][guild_id] = config
        return config
    
    async def get_or_create_filter_config(self, guild_id: int) -> FilterConfig:
        config = await self.get_filter_config(guild_id)
        if config:
            return config
        
        config = FilterConfig(guild_id=guild_id)
        await self.save_filter_config(config)
        return config
    
    async def save_filter_config(self, config: FilterConfig):
        data = {
            'guild_id': config.guild_id,
            'data': config.to_dict(),
            'updated_at': datetime.now().isoformat()
        }
        
        existing = await self.client.find_by_id('filters', 'guild_id', config.guild_id)
        
        if existing:
            await self.client.update('filters', data, {'guild_id': config.guild_id})
        else:
            await self.client.insert('filters', data)
        
        self._cache['filters'][config.guild_id] = config
    
    async def get_log_config(self, guild_id: int) -> Optional[LogConfig]:
        if guild_id in self._cache['logs']:
            return self._cache['logs'][guild_id]
        
        result = await self.client.find_by_id('log_configs', 'guild_id', guild_id)
        
        if not result:
            return None
        
        config = LogConfig.from_dict(result.get('data', {'guild_id': guild_id}))
        self._cache['logs'][guild_id] = config
        return config
    
    async def get_or_create_log_config(self, guild_id: int) -> LogConfig:
        config = await self.get_log_config(guild_id)
        if config:
            return config
        
        config = LogConfig(guild_id=guild_id)
        await self.save_log_config(config)
        return config
    
    async def save_log_config(self, config: LogConfig):
        data = {
            'guild_id': config.guild_id,
            'data': config.to_dict(),
            'updated_at': datetime.now().isoformat()
        }
        
        existing = await self.client.find_by_id('log_configs', 'guild_id', config.guild_id)
        
        if existing:
            await self.client.update('log_configs', data, {'guild_id': config.guild_id})
        else:
            await self.client.insert('log_configs', data)
        
        self._cache['logs'][config.guild_id] = config
    
    async def get_user_level(self, user_id: int, guild_id: int) -> Optional[Dict[str, Any]]:
        level_key = f"{guild_id}_{user_id}"
        if level_key in self._cache['levels']:
            return self._cache['levels'][level_key]
        
        result = await self.client.find_by_id('user_levels', 'id', level_key)
        if result:
            self._cache['levels'][level_key] = result
        return result
    
    async def save_user_level(
        self,
        user_id: int,
        guild_id: int,
        xp: int,
        level: int,
        total_xp: int,
        daily_messages: int,
        last_xp_date: str
    ):
        level_key = f"{guild_id}_{user_id}"
        
        now = datetime.now()
        data = {
            'id': level_key,
            'user_id': user_id,
            'guild_id': guild_id,
            'xp': xp,
            'level': level,
            'total_xp': total_xp,
            'daily_messages': daily_messages,
            'last_xp_date': last_xp_date,
            'last_message_time': int(now.timestamp() * 1000),
            'updated_at': int(now.timestamp() * 1000)
        }
        
        existing = await self.client.find_by_id('user_levels', 'id', level_key)
        
        if existing:
            await self.client.update('user_levels', data, {'id': level_key})
        else:
            await self.client.insert('user_levels', data)
        
        self._cache['levels'][level_key] = data
    
    async def get_level_leaderboard(self, guild_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        results = await self.client.select(
            'user_levels',
            conditions={'guild_id': guild_id},
            order_by=[('total_xp', 'DESC')],
            limit=limit
        )
        return results
    
    async def get_secret_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        return await self.client.find_by_id('secret_users', 'user_id', user_id)
    
    async def get_secret_user_by_nickname(self, nickname: str) -> Optional[Dict[str, Any]]:
        return await self.client.find_one('secret_users', {'nickname': nickname})
    
    async def save_secret_user(self, user_id: int, nickname: str):
        existing = await self.get_secret_user(user_id)
        
        data = {
            'user_id': user_id,
            'nickname': nickname,
            'created_at': int(datetime.now().timestamp() * 1000),
            'is_active': True
        }
        
        if existing:
            await self.client.update('secret_users', data, {'user_id': user_id})
        else:
            await self.client.insert('secret_users', data)
    
    async def delete_secret_user(self, user_id: int):
        await self.client.delete('secret_users', {'user_id': user_id})
    
    async def create_secret_chat(self, sender_id: int, receiver_id: int) -> str:
        import uuid
        chat_id = str(uuid.uuid4())
        
        data = {
            'id': chat_id,
            'sender_id': sender_id,
            'receiver_id': receiver_id,
            'created_at': int(datetime.now().timestamp() * 1000),
            'expires_at': None
        }
        
        await self.client.insert('secret_chats', data)
        return chat_id
    
    async def get_active_secret_chat(self, user1_id: int, user2_id: int) -> Optional[Dict[str, Any]]:
        chat = await self.client.find_one('secret_chats', {
            'sender_id': user1_id,
            'receiver_id': user2_id
        })
        if chat:
            return chat
        
        return await self.client.find_one('secret_chats', {
            'sender_id': user2_id,
            'receiver_id': user1_id
        })
    
    async def get_host_checks(self, guild_id: int) -> List[Dict[str, Any]]:
        return await self.client.select('host_checks', conditions={
            'guild_id': guild_id,
            'is_active': True
        })
    
    async def get_all_active_host_checks(self) -> List[Dict[str, Any]]:
        return await self.client.select('host_checks', conditions={'is_active': True})
    
    async def save_host_check(
        self,
        guild_id: int,
        url: str,
        name: str,
        check_type: str = 'http',
        check_interval: int = 60,
        notify_channel_id: Optional[int] = None
    ) -> str:
        import uuid
        check_id = str(uuid.uuid4())
        
        now = datetime.now()
        data = {
            'id': check_id,
            'guild_id': guild_id,
            'url': url,
            'name': name,
            'check_type': check_type,
            'check_interval': check_interval,
            'last_status': 'pending',
            'last_check': int(now.timestamp() * 1000),
            'notify_channel_id': notify_channel_id,
            'created_at': int(now.timestamp() * 1000),
            'is_active': True
        }
        
        await self.client.insert('host_checks', data)
        return check_id
    
    async def update_host_check_status(self, check_id: str, status: str):
        await self.client.update('host_checks', {
            'last_status': status,
            'last_check': int(datetime.now().timestamp() * 1000)
        }, {'id': check_id})
    
    async def delete_host_check(self, check_id: str):
        await self.client.update('host_checks', {'is_active': False}, {'id': check_id})
    
    async def save_node_status(
        self,
        shard_id: int,
        status: str,
        latency: float,
        guild_count: int,
        member_count: int,
        uptime_seconds: int,
        memory_mb: float,
        cpu_percent: float,
        version: str
    ):
        now = datetime.now()
        data = {
            'shard_id': shard_id,
            'status': status,
            'latency': latency,
            'guild_count': guild_count,
            'member_count': member_count,
            'uptime_seconds': uptime_seconds,
            'memory_mb': memory_mb,
            'cpu_percent': cpu_percent,
            'last_heartbeat': int(now.timestamp() * 1000),
            'started_at': int(now.timestamp() * 1000),
            'version': version
        }
        
        existing = await self.client.find_by_id('node_status', 'shard_id', shard_id)
        
        if existing:
            await self.client.update('node_status', data, {'shard_id': shard_id})
        else:
            await self.client.insert('node_status', data)
    
    async def get_all_node_status(self) -> List[Dict[str, Any]]:
        return await self.client.select('node_status')
    
    async def save_metric(
        self,
        shard_id: int,
        metric_type: str,
        value: float,
        data: Optional[Dict[str, Any]] = None
    ):
        import uuid
        metric_id = str(uuid.uuid4())
        
        metric_data = {
            'id': metric_id,
            'shard_id': shard_id,
            'metric_type': metric_type,
            'value': value,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'data': data or {}
        }
        
        await self.client.insert('metrics', metric_data)
    
    async def get_metrics(
        self,
        shard_id: Optional[int] = None,
        metric_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        conditions = {}
        if shard_id is not None:
            conditions['shard_id'] = shard_id
        if metric_type:
            conditions['metric_type'] = metric_type
        
        return await self.client.select(
            'metrics',
            conditions=conditions,
            order_by=[('timestamp', 'DESC')],
            limit=limit
        )
    
    async def stats(self) -> Dict[str, Any]:
        return await self.client.stats()
    
    async def close(self):
        await self.client.disconnect()
