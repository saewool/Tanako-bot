"""
Logging Models
Data structures for logging configuration and entries
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class LogType(Enum):
    MESSAGE_DELETE = "message_delete"
    MESSAGE_EDIT = "message_edit"
    MESSAGE_BULK_DELETE = "message_bulk_delete"
    
    MEMBER_JOIN = "member_join"
    MEMBER_LEAVE = "member_leave"
    MEMBER_UPDATE = "member_update"
    MEMBER_BAN = "member_ban"
    MEMBER_UNBAN = "member_unban"
    
    ROLE_CREATE = "role_create"
    ROLE_DELETE = "role_delete"
    ROLE_UPDATE = "role_update"
    
    CHANNEL_CREATE = "channel_create"
    CHANNEL_DELETE = "channel_delete"
    CHANNEL_UPDATE = "channel_update"
    
    GUILD_UPDATE = "guild_update"
    
    VOICE_JOIN = "voice_join"
    VOICE_LEAVE = "voice_leave"
    VOICE_MOVE = "voice_move"
    VOICE_MUTE = "voice_mute"
    VOICE_DEAFEN = "voice_deafen"
    
    MOD_WARN = "mod_warn"
    MOD_MUTE = "mod_mute"
    MOD_UNMUTE = "mod_unmute"
    MOD_KICK = "mod_kick"
    MOD_BAN = "mod_ban"
    MOD_UNBAN = "mod_unban"
    MOD_TIMEOUT = "mod_timeout"
    
    AUTOMOD_ACTION = "automod_action"
    FILTER_MATCH = "filter_match"
    ANTIRAID_TRIGGER = "antiraid_trigger"
    ANTINUKE_TRIGGER = "antinuke_trigger"
    
    TICKET_CREATE = "ticket_create"
    TICKET_CLOSE = "ticket_close"
    TICKET_CLAIM = "ticket_claim"
    
    BOT_ERROR = "bot_error"
    BOT_COMMAND = "bot_command"
    
    @property
    def category(self) -> str:
        categories = {
            'message': ['message_delete', 'message_edit', 'message_bulk_delete'],
            'member': ['member_join', 'member_leave', 'member_update', 'member_ban', 'member_unban'],
            'role': ['role_create', 'role_delete', 'role_update'],
            'channel': ['channel_create', 'channel_delete', 'channel_update'],
            'guild': ['guild_update'],
            'voice': ['voice_join', 'voice_leave', 'voice_move', 'voice_mute', 'voice_deafen'],
            'moderation': ['mod_warn', 'mod_mute', 'mod_unmute', 'mod_kick', 'mod_ban', 'mod_unban', 'mod_timeout'],
            'automod': ['automod_action', 'filter_match', 'antiraid_trigger', 'antinuke_trigger'],
            'ticket': ['ticket_create', 'ticket_close', 'ticket_claim'],
            'bot': ['bot_error', 'bot_command']
        }
        
        for category, types in categories.items():
            if self.value in types:
                return category
        return 'other'
    
    @property
    def emoji(self) -> str:
        emojis = {
            LogType.MESSAGE_DELETE: "🗑️",
            LogType.MESSAGE_EDIT: "✏️",
            LogType.MESSAGE_BULK_DELETE: "🗑️",
            LogType.MEMBER_JOIN: "📥",
            LogType.MEMBER_LEAVE: "📤",
            LogType.MEMBER_UPDATE: "👤",
            LogType.MEMBER_BAN: "🔨",
            LogType.MEMBER_UNBAN: "🔓",
            LogType.ROLE_CREATE: "➕",
            LogType.ROLE_DELETE: "➖",
            LogType.ROLE_UPDATE: "🔧",
            LogType.CHANNEL_CREATE: "➕",
            LogType.CHANNEL_DELETE: "➖",
            LogType.CHANNEL_UPDATE: "🔧",
            LogType.GUILD_UPDATE: "⚙️",
            LogType.VOICE_JOIN: "🔊",
            LogType.VOICE_LEAVE: "🔇",
            LogType.VOICE_MOVE: "↔️",
            LogType.VOICE_MUTE: "🔇",
            LogType.VOICE_DEAFEN: "🔇",
            LogType.MOD_WARN: "⚠️",
            LogType.MOD_MUTE: "🔇",
            LogType.MOD_UNMUTE: "🔊",
            LogType.MOD_KICK: "👢",
            LogType.MOD_BAN: "🔨",
            LogType.MOD_UNBAN: "🔓",
            LogType.MOD_TIMEOUT: "⏱️",
            LogType.AUTOMOD_ACTION: "🤖",
            LogType.FILTER_MATCH: "🚫",
            LogType.ANTIRAID_TRIGGER: "🚨",
            LogType.ANTINUKE_TRIGGER: "🛡️",
            LogType.TICKET_CREATE: "🎫",
            LogType.TICKET_CLOSE: "📪",
            LogType.TICKET_CLAIM: "📝",
            LogType.BOT_ERROR: "❌",
            LogType.BOT_COMMAND: "⌨️"
        }
        return emojis.get(self, "📋")


@dataclass
class LogEntry:
    id: str
    guild_id: int
    log_type: LogType
    
    timestamp: datetime = field(default_factory=datetime.now)
    
    actor_id: Optional[int] = None
    target_id: Optional[int] = None
    
    channel_id: Optional[int] = None
    message_id: Optional[int] = None
    
    description: str = ""
    
    data: Dict[str, Any] = field(default_factory=dict)
    
    logged_to_channel: bool = False
    log_message_id: Optional[int] = None
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'guild_id': self.guild_id,
            'log_type': self.log_type.value,
            'timestamp': self.timestamp.isoformat(),
            'actor_id': self.actor_id,
            'target_id': self.target_id,
            'channel_id': self.channel_id,
            'message_id': self.message_id,
            'description': self.description,
            'data': self.data,
            'logged_to_channel': self.logged_to_channel,
            'log_message_id': self.log_message_id
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'LogEntry':
        entry = cls(
            id=data['id'],
            guild_id=data['guild_id'],
            log_type=LogType(data['log_type'])
        )
        
        if data.get('timestamp'):
            if isinstance(data['timestamp'], str):
                entry.timestamp = datetime.fromisoformat(data['timestamp'])
            else:
                entry.timestamp = data['timestamp']
        
        for key in ['actor_id', 'target_id', 'channel_id', 'message_id',
                    'description', 'data', 'logged_to_channel', 'log_message_id']:
            if key in data:
                setattr(entry, key, data[key])
        
        return entry


@dataclass
class LogConfig:
    guild_id: int
    
    enabled: bool = True
    
    message_log_channel: Optional[int] = None
    member_log_channel: Optional[int] = None
    moderation_log_channel: Optional[int] = None
    server_log_channel: Optional[int] = None
    voice_log_channel: Optional[int] = None
    automod_log_channel: Optional[int] = None
    ticket_log_channel: Optional[int] = None
    bot_log_channel: Optional[int] = None
    
    enabled_types: List[str] = field(default_factory=lambda: [t.value for t in LogType])
    
    ignored_channels: List[int] = field(default_factory=list)
    ignored_users: List[int] = field(default_factory=list)
    ignored_roles: List[int] = field(default_factory=list)
    
    compact_mode: bool = False
    include_bot_actions: bool = False
    show_moderator: bool = True
    
    max_entries: int = 10000
    
    def to_dict(self) -> dict:
        return {
            'guild_id': self.guild_id,
            'enabled': self.enabled,
            'message_log_channel': self.message_log_channel,
            'member_log_channel': self.member_log_channel,
            'moderation_log_channel': self.moderation_log_channel,
            'server_log_channel': self.server_log_channel,
            'voice_log_channel': self.voice_log_channel,
            'automod_log_channel': self.automod_log_channel,
            'ticket_log_channel': self.ticket_log_channel,
            'bot_log_channel': self.bot_log_channel,
            'enabled_types': self.enabled_types,
            'ignored_channels': self.ignored_channels,
            'ignored_users': self.ignored_users,
            'ignored_roles': self.ignored_roles,
            'compact_mode': self.compact_mode,
            'include_bot_actions': self.include_bot_actions,
            'show_moderator': self.show_moderator,
            'max_entries': self.max_entries
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'LogConfig':
        config = cls(guild_id=data['guild_id'])
        
        for key in ['enabled', 'message_log_channel', 'member_log_channel',
                    'moderation_log_channel', 'server_log_channel', 'voice_log_channel',
                    'automod_log_channel', 'ticket_log_channel', 'bot_log_channel',
                    'enabled_types', 'ignored_channels', 'ignored_users', 'ignored_roles',
                    'compact_mode', 'include_bot_actions', 'show_moderator', 'max_entries']:
            if key in data:
                setattr(config, key, data[key])
        
        return config
    
    def is_type_enabled(self, log_type: LogType) -> bool:
        return self.enabled and log_type.value in self.enabled_types
    
    def get_channel_for_type(self, log_type: LogType) -> Optional[int]:
        category = log_type.category
        
        channel_map = {
            'message': self.message_log_channel,
            'member': self.member_log_channel,
            'moderation': self.moderation_log_channel,
            'role': self.server_log_channel,
            'channel': self.server_log_channel,
            'guild': self.server_log_channel,
            'voice': self.voice_log_channel,
            'automod': self.automod_log_channel,
            'ticket': self.ticket_log_channel,
            'bot': self.bot_log_channel
        }
        
        return channel_map.get(category)
    
    def should_log(
        self,
        log_type: LogType,
        channel_id: Optional[int] = None,
        user_id: Optional[int] = None,
        user_roles: Optional[List[int]] = None
    ) -> bool:
        if not self.is_type_enabled(log_type):
            return False
        
        if channel_id and channel_id in self.ignored_channels:
            return False
        
        if user_id and user_id in self.ignored_users:
            return False
        
        if user_roles:
            if any(role in self.ignored_roles for role in user_roles):
                return False
        
        return True
    
    def enable_type(self, log_type: LogType):
        if log_type.value not in self.enabled_types:
            self.enabled_types.append(log_type.value)
    
    def disable_type(self, log_type: LogType):
        if log_type.value in self.enabled_types:
            self.enabled_types.remove(log_type.value)
    
    def set_channel(self, category: str, channel_id: int):
        channel_attrs = {
            'message': 'message_log_channel',
            'member': 'member_log_channel',
            'moderation': 'moderation_log_channel',
            'server': 'server_log_channel',
            'voice': 'voice_log_channel',
            'automod': 'automod_log_channel',
            'ticket': 'ticket_log_channel',
            'bot': 'bot_log_channel'
        }
        
        if category in channel_attrs:
            setattr(self, channel_attrs[category], channel_id)
