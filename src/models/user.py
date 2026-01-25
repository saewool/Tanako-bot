"""
User Data Models
Data structures for user information and statistics
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set


@dataclass
class UserStats:
    messages_sent: int = 0
    commands_used: int = 0
    
    warnings_received: int = 0
    mutes_received: int = 0
    kicks_received: int = 0
    bans_received: int = 0
    
    tickets_created: int = 0
    tickets_closed: int = 0
    
    invites_created: int = 0
    invites_used: int = 0
    
    voice_time_seconds: int = 0
    
    last_message_at: Optional[datetime] = None
    last_command_at: Optional[datetime] = None
    last_voice_join_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            'messages_sent': self.messages_sent,
            'commands_used': self.commands_used,
            'warnings_received': self.warnings_received,
            'mutes_received': self.mutes_received,
            'kicks_received': self.kicks_received,
            'bans_received': self.bans_received,
            'tickets_created': self.tickets_created,
            'tickets_closed': self.tickets_closed,
            'invites_created': self.invites_created,
            'invites_used': self.invites_used,
            'voice_time_seconds': self.voice_time_seconds,
            'last_message_at': self.last_message_at.isoformat() if self.last_message_at else None,
            'last_command_at': self.last_command_at.isoformat() if self.last_command_at else None,
            'last_voice_join_at': self.last_voice_join_at.isoformat() if self.last_voice_join_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'UserStats':
        stats = cls()
        
        for key in ['messages_sent', 'commands_used', 'warnings_received', 'mutes_received',
                    'kicks_received', 'bans_received', 'tickets_created', 'tickets_closed',
                    'invites_created', 'invites_used', 'voice_time_seconds']:
            if key in data:
                setattr(stats, key, data[key])
        
        for key in ['last_message_at', 'last_command_at', 'last_voice_join_at']:
            if data.get(key):
                if isinstance(data[key], str):
                    setattr(stats, key, datetime.fromisoformat(data[key]))
                else:
                    setattr(stats, key, data[key])
        
        return stats


@dataclass
class UserData:
    user_id: int
    guild_id: int
    
    joined_at: Optional[datetime] = None
    first_seen: datetime = field(default_factory=datetime.now)
    last_seen: datetime = field(default_factory=datetime.now)
    
    stats: UserStats = field(default_factory=UserStats)
    
    notes: List[dict] = field(default_factory=list)
    
    is_muted: bool = False
    mute_expires_at: Optional[datetime] = None
    mute_role_id: Optional[int] = None
    
    is_banned: bool = False
    ban_expires_at: Optional[datetime] = None
    
    active_warnings: int = 0
    total_warnings: int = 0
    warning_points: int = 0
    
    level: int = 0
    xp: int = 0
    
    reputation: int = 0
    
    afk: bool = False
    afk_message: Optional[str] = None
    afk_since: Optional[datetime] = None
    
    timezone: Optional[str] = None
    
    custom_data: Dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'guild_id': self.guild_id,
            'joined_at': self.joined_at.isoformat() if self.joined_at else None,
            'first_seen': self.first_seen.isoformat(),
            'last_seen': self.last_seen.isoformat(),
            'stats': self.stats.to_dict(),
            'notes': self.notes,
            'is_muted': self.is_muted,
            'mute_expires_at': self.mute_expires_at.isoformat() if self.mute_expires_at else None,
            'mute_role_id': self.mute_role_id,
            'is_banned': self.is_banned,
            'ban_expires_at': self.ban_expires_at.isoformat() if self.ban_expires_at else None,
            'active_warnings': self.active_warnings,
            'total_warnings': self.total_warnings,
            'warning_points': self.warning_points,
            'level': self.level,
            'xp': self.xp,
            'reputation': self.reputation,
            'afk': self.afk,
            'afk_message': self.afk_message,
            'afk_since': self.afk_since.isoformat() if self.afk_since else None,
            'timezone': self.timezone,
            'custom_data': self.custom_data
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'UserData':
        user_data = cls(
            user_id=data['user_id'],
            guild_id=data['guild_id']
        )
        
        for key in ['joined_at', 'first_seen', 'last_seen', 'mute_expires_at', 
                    'ban_expires_at', 'afk_since']:
            if data.get(key):
                if isinstance(data[key], str):
                    setattr(user_data, key, datetime.fromisoformat(data[key]))
                else:
                    setattr(user_data, key, data[key])
        
        if 'stats' in data:
            user_data.stats = UserStats.from_dict(data['stats'])
        
        for key in ['notes', 'is_muted', 'mute_role_id', 'is_banned', 'active_warnings',
                    'total_warnings', 'warning_points', 'level', 'xp', 'reputation',
                    'afk', 'afk_message', 'timezone', 'custom_data']:
            if key in data:
                setattr(user_data, key, data[key])
        
        return user_data
    
    def add_note(self, author_id: int, content: str):
        self.notes.append({
            'author_id': author_id,
            'content': content,
            'timestamp': datetime.now().isoformat()
        })
    
    def set_afk(self, message: str = "AFK"):
        self.afk = True
        self.afk_message = message
        self.afk_since = datetime.now()
    
    def clear_afk(self):
        self.afk = False
        self.afk_message = None
        self.afk_since = None
    
    def mute(self, duration_seconds: Optional[int] = None, role_id: Optional[int] = None):
        self.is_muted = True
        self.mute_role_id = role_id
        if duration_seconds:
            from datetime import timedelta
            self.mute_expires_at = datetime.now() + timedelta(seconds=duration_seconds)
        self.stats.mutes_received += 1
    
    def unmute(self):
        self.is_muted = False
        self.mute_expires_at = None
        self.mute_role_id = None
    
    def add_warning(self, points: int = 1):
        self.active_warnings += 1
        self.total_warnings += 1
        self.warning_points += points
        self.stats.warnings_received += 1
    
    def remove_warning(self, points: int = 1):
        self.active_warnings = max(0, self.active_warnings - 1)
        self.warning_points = max(0, self.warning_points - points)
    
    def add_xp(self, amount: int) -> bool:
        self.xp += amount
        xp_needed = self._xp_for_level(self.level + 1)
        
        if self.xp >= xp_needed:
            self.xp -= xp_needed
            self.level += 1
            return True
        return False
    
    def _xp_for_level(self, level: int) -> int:
        return int(5 * (level ** 2) + 50 * level + 100)
    
    def update_last_seen(self):
        self.last_seen = datetime.now()


@dataclass 
class GlobalUserData:
    user_id: int
    
    first_seen: datetime = field(default_factory=datetime.now)
    
    blacklisted: bool = False
    blacklist_reason: Optional[str] = None
    blacklisted_at: Optional[datetime] = None
    
    premium: bool = False
    premium_since: Optional[datetime] = None
    premium_tier: int = 0
    
    badges: List[str] = field(default_factory=list)
    
    guilds: List[int] = field(default_factory=list)
    
    total_commands_used: int = 0
    
    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'first_seen': self.first_seen.isoformat(),
            'blacklisted': self.blacklisted,
            'blacklist_reason': self.blacklist_reason,
            'blacklisted_at': self.blacklisted_at.isoformat() if self.blacklisted_at else None,
            'premium': self.premium,
            'premium_since': self.premium_since.isoformat() if self.premium_since else None,
            'premium_tier': self.premium_tier,
            'badges': self.badges,
            'guilds': self.guilds,
            'total_commands_used': self.total_commands_used
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'GlobalUserData':
        user_data = cls(user_id=data['user_id'])
        
        for key in ['first_seen', 'blacklisted_at', 'premium_since']:
            if data.get(key):
                if isinstance(data[key], str):
                    setattr(user_data, key, datetime.fromisoformat(data[key]))
                else:
                    setattr(user_data, key, data[key])
        
        for key in ['blacklisted', 'blacklist_reason', 'premium', 'premium_tier',
                    'badges', 'guilds', 'total_commands_used']:
            if key in data:
                setattr(user_data, key, data[key])
        
        return user_data
    
    def blacklist(self, reason: str = "No reason provided"):
        self.blacklisted = True
        self.blacklist_reason = reason
        self.blacklisted_at = datetime.now()
    
    def unblacklist(self):
        self.blacklisted = False
        self.blacklist_reason = None
        self.blacklisted_at = None
    
    def grant_premium(self, tier: int = 1):
        self.premium = True
        self.premium_since = datetime.now()
        self.premium_tier = tier
    
    def revoke_premium(self):
        self.premium = False
        self.premium_since = None
        self.premium_tier = 0
    
    def add_badge(self, badge: str):
        if badge not in self.badges:
            self.badges.append(badge)
    
    def remove_badge(self, badge: str):
        if badge in self.badges:
            self.badges.remove(badge)
