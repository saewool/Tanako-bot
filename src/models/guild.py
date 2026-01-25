"""
Guild Configuration Models
Stores per-guild settings and configurations
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from datetime import datetime


@dataclass
class WelcomeSettings:
    enabled: bool = False
    channel_id: Optional[int] = None
    message: str = "Welcome to {server}, {user}! You are member #{count}."
    dm_enabled: bool = False
    dm_message: str = "Welcome to {server}! Please read the rules."
    auto_role_ids: List[int] = field(default_factory=list)
    embed_enabled: bool = True
    embed_color: int = 0x2ECC71
    embed_thumbnail: bool = True
    embed_footer: str = "ID: {user_id}"


@dataclass
class GoodbyeSettings:
    enabled: bool = False
    channel_id: Optional[int] = None
    message: str = "Goodbye {user}! We'll miss you."
    embed_enabled: bool = True
    embed_color: int = 0x95A5A6


@dataclass
class LoggingSettings:
    enabled: bool = True
    message_log_channel: Optional[int] = None
    member_log_channel: Optional[int] = None
    moderation_log_channel: Optional[int] = None
    server_log_channel: Optional[int] = None
    voice_log_channel: Optional[int] = None
    
    log_message_delete: bool = True
    log_message_edit: bool = True
    log_message_bulk_delete: bool = True
    log_member_join: bool = True
    log_member_leave: bool = True
    log_member_update: bool = True
    log_member_ban: bool = True
    log_member_unban: bool = True
    log_role_create: bool = True
    log_role_delete: bool = True
    log_role_update: bool = True
    log_channel_create: bool = True
    log_channel_delete: bool = True
    log_channel_update: bool = True
    log_voice_join: bool = True
    log_voice_leave: bool = True
    log_voice_move: bool = True
    
    ignored_channels: List[int] = field(default_factory=list)
    ignored_users: List[int] = field(default_factory=list)
    ignored_roles: List[int] = field(default_factory=list)


@dataclass
class AntiRaidSettings:
    enabled: bool = False
    join_threshold: int = 10
    join_interval: int = 10
    action: str = "lockdown"
    lockdown_duration: int = 300
    new_account_threshold: int = 7
    alert_channel: Optional[int] = None
    
    auto_ban_new_accounts: bool = False
    auto_kick_no_avatar: bool = False
    
    whitelist_roles: List[int] = field(default_factory=list)


@dataclass
class AntiNukeSettings:
    enabled: bool = False
    
    max_bans_per_minute: int = 5
    max_kicks_per_minute: int = 5
    max_channel_deletes_per_minute: int = 3
    max_channel_creates_per_minute: int = 5
    max_role_deletes_per_minute: int = 3
    max_role_creates_per_minute: int = 5
    max_webhook_creates_per_minute: int = 3
    
    action: str = "strip_roles"
    alert_channel: Optional[int] = None
    
    protected_roles: List[int] = field(default_factory=list)
    trusted_users: List[int] = field(default_factory=list)
    
    log_all_actions: bool = True


@dataclass
class AutoModSettings:
    enabled: bool = False
    
    anti_spam: bool = True
    spam_threshold: int = 5
    spam_interval: int = 5
    
    anti_caps: bool = True
    caps_threshold: float = 0.7
    caps_min_length: int = 10
    
    anti_invite: bool = True
    allowed_invite_guilds: List[int] = field(default_factory=list)
    
    anti_link: bool = False
    allowed_domains: List[str] = field(default_factory=list)
    
    anti_emoji_spam: bool = True
    emoji_threshold: int = 10
    
    anti_mention_spam: bool = True
    mention_threshold: int = 5
    
    anti_newline_spam: bool = True
    newline_threshold: int = 10
    
    warn_on_violation: bool = True
    mute_on_repeated: bool = True
    mute_threshold: int = 3
    mute_duration: int = 300
    
    ignored_channels: List[int] = field(default_factory=list)
    ignored_roles: List[int] = field(default_factory=list)


@dataclass
class TicketSettings:
    enabled: bool = False
    category_id: Optional[int] = None
    log_channel_id: Optional[int] = None
    support_role_ids: List[int] = field(default_factory=list)
    
    max_open_tickets: int = 3
    auto_close_hours: int = 72
    
    create_message: str = "Click the button below to create a ticket."
    welcome_message: str = "Hello {user}! A staff member will assist you shortly."
    close_message: str = "This ticket has been closed. Thank you for contacting us."
    
    transcript_enabled: bool = True
    dm_transcript: bool = False
    
    categories: Dict[str, Dict] = field(default_factory=dict)


@dataclass
class GuildSettings:
    prefix: str = "!"
    language: str = "en"
    timezone: str = "UTC"
    
    welcome: WelcomeSettings = field(default_factory=WelcomeSettings)
    goodbye: GoodbyeSettings = field(default_factory=GoodbyeSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)
    anti_raid: AntiRaidSettings = field(default_factory=AntiRaidSettings)
    anti_nuke: AntiNukeSettings = field(default_factory=AntiNukeSettings)
    automod: AutoModSettings = field(default_factory=AutoModSettings)
    tickets: TicketSettings = field(default_factory=TicketSettings)
    
    moderator_roles: List[int] = field(default_factory=list)
    admin_roles: List[int] = field(default_factory=list)
    mute_role: Optional[int] = None
    
    disabled_commands: List[str] = field(default_factory=list)
    disabled_channels: List[int] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            'prefix': self.prefix,
            'language': self.language,
            'timezone': self.timezone,
            'welcome': self._dataclass_to_dict(self.welcome),
            'goodbye': self._dataclass_to_dict(self.goodbye),
            'logging': self._dataclass_to_dict(self.logging),
            'anti_raid': self._dataclass_to_dict(self.anti_raid),
            'anti_nuke': self._dataclass_to_dict(self.anti_nuke),
            'automod': self._dataclass_to_dict(self.automod),
            'tickets': self._dataclass_to_dict(self.tickets),
            'moderator_roles': self.moderator_roles,
            'admin_roles': self.admin_roles,
            'mute_role': self.mute_role,
            'disabled_commands': self.disabled_commands,
            'disabled_channels': self.disabled_channels
        }
    
    def _dataclass_to_dict(self, obj) -> dict:
        if hasattr(obj, '__dataclass_fields__'):
            return {k: self._dataclass_to_dict(v) if hasattr(v, '__dataclass_fields__') else v
                    for k, v in obj.__dict__.items()}
        return obj
    
    @classmethod
    def from_dict(cls, data: dict) -> 'GuildSettings':
        settings = cls()
        
        for key in ['prefix', 'language', 'timezone', 'moderator_roles', 
                    'admin_roles', 'mute_role', 'disabled_commands', 'disabled_channels']:
            if key in data:
                setattr(settings, key, data[key])
        
        if 'welcome' in data:
            settings.welcome = cls._dict_to_dataclass(WelcomeSettings, data['welcome'])
        if 'goodbye' in data:
            settings.goodbye = cls._dict_to_dataclass(GoodbyeSettings, data['goodbye'])
        if 'logging' in data:
            settings.logging = cls._dict_to_dataclass(LoggingSettings, data['logging'])
        if 'anti_raid' in data:
            settings.anti_raid = cls._dict_to_dataclass(AntiRaidSettings, data['anti_raid'])
        if 'anti_nuke' in data:
            settings.anti_nuke = cls._dict_to_dataclass(AntiNukeSettings, data['anti_nuke'])
        if 'automod' in data:
            settings.automod = cls._dict_to_dataclass(AutoModSettings, data['automod'])
        if 'tickets' in data:
            settings.tickets = cls._dict_to_dataclass(TicketSettings, data['tickets'])
        
        return settings
    
    @staticmethod
    def _dict_to_dataclass(cls, data: dict):
        if not data:
            return cls()
        
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class GuildConfig:
    guild_id: int
    settings: GuildSettings = field(default_factory=GuildSettings)
    
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    case_counter: int = 0
    ticket_counter: int = 0
    
    filter_rules: List[dict] = field(default_factory=list)
    custom_commands: Dict[str, dict] = field(default_factory=dict)
    
    def get_next_case_id(self) -> int:
        self.case_counter += 1
        return self.case_counter
    
    def get_next_ticket_id(self) -> int:
        self.ticket_counter += 1
        return self.ticket_counter
    
    def to_dict(self) -> dict:
        return {
            'guild_id': self.guild_id,
            'settings': self.settings.to_dict(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'case_counter': self.case_counter,
            'ticket_counter': self.ticket_counter,
            'filter_rules': self.filter_rules,
            'custom_commands': self.custom_commands
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'GuildConfig':
        config = cls(guild_id=data['guild_id'])
        
        if 'settings' in data:
            config.settings = GuildSettings.from_dict(data['settings'])
        
        if 'created_at' in data:
            if isinstance(data['created_at'], str):
                config.created_at = datetime.fromisoformat(data['created_at'])
            else:
                config.created_at = data['created_at']
        
        if 'updated_at' in data:
            if isinstance(data['updated_at'], str):
                config.updated_at = datetime.fromisoformat(data['updated_at'])
            else:
                config.updated_at = data['updated_at']
        
        config.case_counter = data.get('case_counter', 0)
        config.ticket_counter = data.get('ticket_counter', 0)
        config.filter_rules = data.get('filter_rules', [])
        config.custom_commands = data.get('custom_commands', {})
        
        return config
