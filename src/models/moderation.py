"""
Moderation Models
Data structures for moderation actions and cases
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class ModerationAction(Enum):
    WARN = "warn"
    MUTE = "mute"
    UNMUTE = "unmute"
    KICK = "kick"
    BAN = "ban"
    UNBAN = "unban"
    TIMEOUT = "timeout"
    UNTIMEOUT = "untimeout"
    SOFTBAN = "softban"
    TEMPBAN = "tempban"
    NOTE = "note"
    
    @property
    def emoji(self) -> str:
        emojis = {
            ModerationAction.WARN: "⚠️",
            ModerationAction.MUTE: "🔇",
            ModerationAction.UNMUTE: "🔊",
            ModerationAction.KICK: "👢",
            ModerationAction.BAN: "🔨",
            ModerationAction.UNBAN: "🔓",
            ModerationAction.TIMEOUT: "⏱️",
            ModerationAction.UNTIMEOUT: "⏰",
            ModerationAction.SOFTBAN: "🧹",
            ModerationAction.TEMPBAN: "⏳",
            ModerationAction.NOTE: "📝"
        }
        return emojis.get(self, "❓")
    
    @property
    def past_tense(self) -> str:
        past = {
            ModerationAction.WARN: "warned",
            ModerationAction.MUTE: "muted",
            ModerationAction.UNMUTE: "unmuted",
            ModerationAction.KICK: "kicked",
            ModerationAction.BAN: "banned",
            ModerationAction.UNBAN: "unbanned",
            ModerationAction.TIMEOUT: "timed out",
            ModerationAction.UNTIMEOUT: "timeout removed",
            ModerationAction.SOFTBAN: "softbanned",
            ModerationAction.TEMPBAN: "temporarily banned",
            ModerationAction.NOTE: "noted"
        }
        return past.get(self, self.value)


@dataclass
class ModerationCase:
    case_id: int
    guild_id: int
    target_id: int
    moderator_id: int
    action: ModerationAction
    reason: str = "No reason provided"
    
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    
    duration_seconds: Optional[int] = None
    
    message_id: Optional[int] = None
    channel_id: Optional[int] = None
    
    reference_case_id: Optional[int] = None
    
    is_active: bool = True
    revoked: bool = False
    revoked_by: Optional[int] = None
    revoked_at: Optional[datetime] = None
    revoke_reason: Optional[str] = None
    
    dm_sent: bool = False
    dm_failed: bool = False
    
    auto_mod: bool = False
    
    def to_dict(self) -> dict:
        return {
            'case_id': self.case_id,
            'guild_id': self.guild_id,
            'target_id': self.target_id,
            'moderator_id': self.moderator_id,
            'action': self.action.value,
            'reason': self.reason,
            'created_at': self.created_at.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'duration_seconds': self.duration_seconds,
            'message_id': self.message_id,
            'channel_id': self.channel_id,
            'reference_case_id': self.reference_case_id,
            'is_active': self.is_active,
            'revoked': self.revoked,
            'revoked_by': self.revoked_by,
            'revoked_at': self.revoked_at.isoformat() if self.revoked_at else None,
            'revoke_reason': self.revoke_reason,
            'dm_sent': self.dm_sent,
            'dm_failed': self.dm_failed,
            'auto_mod': self.auto_mod
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ModerationCase':
        case = cls(
            case_id=data['case_id'],
            guild_id=data['guild_id'],
            target_id=data['target_id'],
            moderator_id=data['moderator_id'],
            action=ModerationAction(data['action']),
            reason=data.get('reason', 'No reason provided')
        )
        
        if data.get('created_at'):
            if isinstance(data['created_at'], str):
                case.created_at = datetime.fromisoformat(data['created_at'])
            else:
                case.created_at = data['created_at']
        
        if data.get('expires_at'):
            if isinstance(data['expires_at'], str):
                case.expires_at = datetime.fromisoformat(data['expires_at'])
            else:
                case.expires_at = data['expires_at']
        
        case.duration_seconds = data.get('duration_seconds')
        case.message_id = data.get('message_id')
        case.channel_id = data.get('channel_id')
        case.reference_case_id = data.get('reference_case_id')
        case.is_active = data.get('is_active', True)
        case.revoked = data.get('revoked', False)
        case.revoked_by = data.get('revoked_by')
        
        if data.get('revoked_at'):
            if isinstance(data['revoked_at'], str):
                case.revoked_at = datetime.fromisoformat(data['revoked_at'])
            else:
                case.revoked_at = data['revoked_at']
        
        case.revoke_reason = data.get('revoke_reason')
        case.dm_sent = data.get('dm_sent', False)
        case.dm_failed = data.get('dm_failed', False)
        case.auto_mod = data.get('auto_mod', False)
        
        return case
    
    @property
    def is_expired(self) -> bool:
        if not self.expires_at:
            return False
        return datetime.now() >= self.expires_at
    
    def revoke(self, revoked_by: int, reason: str = "No reason provided"):
        self.revoked = True
        self.revoked_by = revoked_by
        self.revoked_at = datetime.now()
        self.revoke_reason = reason
        self.is_active = False


@dataclass
class Warning:
    warning_id: int
    guild_id: int
    user_id: int
    moderator_id: int
    reason: str
    
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    
    severity: int = 1
    points: int = 1
    
    pardoned: bool = False
    pardoned_by: Optional[int] = None
    pardoned_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            'warning_id': self.warning_id,
            'guild_id': self.guild_id,
            'user_id': self.user_id,
            'moderator_id': self.moderator_id,
            'reason': self.reason,
            'created_at': self.created_at.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'severity': self.severity,
            'points': self.points,
            'pardoned': self.pardoned,
            'pardoned_by': self.pardoned_by,
            'pardoned_at': self.pardoned_at.isoformat() if self.pardoned_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Warning':
        warning = cls(
            warning_id=data['warning_id'],
            guild_id=data['guild_id'],
            user_id=data['user_id'],
            moderator_id=data['moderator_id'],
            reason=data['reason']
        )
        
        if data.get('created_at'):
            if isinstance(data['created_at'], str):
                warning.created_at = datetime.fromisoformat(data['created_at'])
            else:
                warning.created_at = data['created_at']
        
        if data.get('expires_at'):
            if isinstance(data['expires_at'], str):
                warning.expires_at = datetime.fromisoformat(data['expires_at'])
            else:
                warning.expires_at = data['expires_at']
        
        warning.severity = data.get('severity', 1)
        warning.points = data.get('points', 1)
        warning.pardoned = data.get('pardoned', False)
        warning.pardoned_by = data.get('pardoned_by')
        
        if data.get('pardoned_at'):
            if isinstance(data['pardoned_at'], str):
                warning.pardoned_at = datetime.fromisoformat(data['pardoned_at'])
            else:
                warning.pardoned_at = data['pardoned_at']
        
        return warning
    
    @property
    def is_active(self) -> bool:
        if self.pardoned:
            return False
        if self.expires_at and datetime.now() >= self.expires_at:
            return False
        return True
    
    def pardon(self, pardoned_by: int):
        self.pardoned = True
        self.pardoned_by = pardoned_by
        self.pardoned_at = datetime.now()


@dataclass
class PunishmentEscalation:
    guild_id: int
    thresholds: List[dict] = field(default_factory=list)
    
    def get_punishment(self, warning_count: int, total_points: int) -> Optional[dict]:
        for threshold in sorted(self.thresholds, key=lambda x: x.get('points', x.get('count', 0)), reverse=True):
            check_value = total_points if 'points' in threshold else warning_count
            threshold_value = threshold.get('points', threshold.get('count', 0))
            
            if check_value >= threshold_value:
                return threshold
        
        return None
    
    def to_dict(self) -> dict:
        return {
            'guild_id': self.guild_id,
            'thresholds': self.thresholds
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PunishmentEscalation':
        return cls(
            guild_id=data['guild_id'],
            thresholds=data.get('thresholds', [])
        )
    
    @classmethod
    def default(cls, guild_id: int) -> 'PunishmentEscalation':
        return cls(
            guild_id=guild_id,
            thresholds=[
                {'count': 3, 'action': 'mute', 'duration': 3600},
                {'count': 5, 'action': 'mute', 'duration': 86400},
                {'count': 7, 'action': 'kick'},
                {'count': 10, 'action': 'ban'}
            ]
        )
