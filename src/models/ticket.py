"""
Ticket System Models
Data structures for the ticket management system
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict


class TicketStatus(Enum):
    OPEN = "open"
    CLAIMED = "claimed"
    ON_HOLD = "on_hold"
    CLOSED = "closed"
    DELETED = "deleted"
    
    @property
    def emoji(self) -> str:
        emojis = {
            TicketStatus.OPEN: "📬",
            TicketStatus.CLAIMED: "📝",
            TicketStatus.ON_HOLD: "⏸️",
            TicketStatus.CLOSED: "📪",
            TicketStatus.DELETED: "🗑️"
        }
        return emojis.get(self, "❓")
    
    @property
    def color(self) -> int:
        colors = {
            TicketStatus.OPEN: 0x2ECC71,
            TicketStatus.CLAIMED: 0x3498DB,
            TicketStatus.ON_HOLD: 0xF1C40F,
            TicketStatus.CLOSED: 0x95A5A6,
            TicketStatus.DELETED: 0xE74C3C
        }
        return colors.get(self, 0x99AAB5)


class TicketPriority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4
    
    @property
    def emoji(self) -> str:
        emojis = {
            TicketPriority.LOW: "🟢",
            TicketPriority.MEDIUM: "🟡",
            TicketPriority.HIGH: "🟠",
            TicketPriority.URGENT: "🔴"
        }
        return emojis.get(self, "⚪")


@dataclass
class TicketCategory:
    id: str
    name: str
    description: str = ""
    emoji: str = "🎫"
    
    support_roles: List[int] = field(default_factory=list)
    
    naming_scheme: str = "ticket-{number}"
    
    welcome_message: Optional[str] = None
    
    auto_close_hours: Optional[int] = None
    
    require_reason: bool = False
    
    max_open: int = 1
    
    enabled: bool = True
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'emoji': self.emoji,
            'support_roles': self.support_roles,
            'naming_scheme': self.naming_scheme,
            'welcome_message': self.welcome_message,
            'auto_close_hours': self.auto_close_hours,
            'require_reason': self.require_reason,
            'max_open': self.max_open,
            'enabled': self.enabled
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TicketCategory':
        return cls(
            id=data['id'],
            name=data['name'],
            description=data.get('description', ''),
            emoji=data.get('emoji', '🎫'),
            support_roles=data.get('support_roles', []),
            naming_scheme=data.get('naming_scheme', 'ticket-{number}'),
            welcome_message=data.get('welcome_message'),
            auto_close_hours=data.get('auto_close_hours'),
            require_reason=data.get('require_reason', False),
            max_open=data.get('max_open', 1),
            enabled=data.get('enabled', True)
        )


@dataclass
class TicketMessage:
    message_id: int
    author_id: int
    content: str
    timestamp: datetime
    
    attachments: List[str] = field(default_factory=list)
    embeds: List[dict] = field(default_factory=list)
    
    edited: bool = False
    deleted: bool = False
    
    is_staff: bool = False
    
    def to_dict(self) -> dict:
        return {
            'message_id': self.message_id,
            'author_id': self.author_id,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'attachments': self.attachments,
            'embeds': self.embeds,
            'edited': self.edited,
            'deleted': self.deleted,
            'is_staff': self.is_staff
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TicketMessage':
        return cls(
            message_id=data['message_id'],
            author_id=data['author_id'],
            content=data['content'],
            timestamp=datetime.fromisoformat(data['timestamp']) if isinstance(data['timestamp'], str) else data['timestamp'],
            attachments=data.get('attachments', []),
            embeds=data.get('embeds', []),
            edited=data.get('edited', False),
            deleted=data.get('deleted', False),
            is_staff=data.get('is_staff', False)
        )


@dataclass
class Ticket:
    ticket_id: str
    guild_id: int
    channel_id: int
    creator_id: int
    
    category_id: str = "general"
    status: TicketStatus = TicketStatus.OPEN
    priority: TicketPriority = TicketPriority.MEDIUM
    
    subject: str = ""
    description: str = ""
    
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    closed_at: Optional[datetime] = None
    
    claimed_by: Optional[int] = None
    claimed_at: Optional[datetime] = None
    
    closed_by: Optional[int] = None
    close_reason: Optional[str] = None
    
    added_users: List[int] = field(default_factory=list)
    
    tags: List[str] = field(default_factory=list)
    
    message_count: int = 0
    staff_message_count: int = 0
    
    first_response_at: Optional[datetime] = None
    first_response_by: Optional[int] = None
    
    rating: Optional[int] = None
    feedback: Optional[str] = None
    
    notes: List[dict] = field(default_factory=list)
    
    transcript_url: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'ticket_id': self.ticket_id,
            'guild_id': self.guild_id,
            'channel_id': self.channel_id,
            'creator_id': self.creator_id,
            'category_id': self.category_id,
            'status': self.status.value,
            'priority': self.priority.value,
            'subject': self.subject,
            'description': self.description,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'closed_at': self.closed_at.isoformat() if self.closed_at else None,
            'claimed_by': self.claimed_by,
            'claimed_at': self.claimed_at.isoformat() if self.claimed_at else None,
            'closed_by': self.closed_by,
            'close_reason': self.close_reason,
            'added_users': self.added_users,
            'tags': self.tags,
            'message_count': self.message_count,
            'staff_message_count': self.staff_message_count,
            'first_response_at': self.first_response_at.isoformat() if self.first_response_at else None,
            'first_response_by': self.first_response_by,
            'rating': self.rating,
            'feedback': self.feedback,
            'notes': self.notes,
            'transcript_url': self.transcript_url
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Ticket':
        ticket = cls(
            ticket_id=data['ticket_id'],
            guild_id=data['guild_id'],
            channel_id=data['channel_id'],
            creator_id=data['creator_id']
        )
        
        ticket.category_id = data.get('category_id', 'general')
        ticket.status = TicketStatus(data.get('status', 'open'))
        ticket.priority = TicketPriority(data.get('priority', 2))
        ticket.subject = data.get('subject', '')
        ticket.description = data.get('description', '')
        
        for field_name in ['created_at', 'updated_at', 'closed_at', 'claimed_at', 'first_response_at']:
            if data.get(field_name):
                value = data[field_name]
                if isinstance(value, str):
                    setattr(ticket, field_name, datetime.fromisoformat(value))
                else:
                    setattr(ticket, field_name, value)
        
        ticket.claimed_by = data.get('claimed_by')
        ticket.closed_by = data.get('closed_by')
        ticket.close_reason = data.get('close_reason')
        ticket.added_users = data.get('added_users', [])
        ticket.tags = data.get('tags', [])
        ticket.message_count = data.get('message_count', 0)
        ticket.staff_message_count = data.get('staff_message_count', 0)
        ticket.first_response_by = data.get('first_response_by')
        ticket.rating = data.get('rating')
        ticket.feedback = data.get('feedback')
        ticket.notes = data.get('notes', [])
        ticket.transcript_url = data.get('transcript_url')
        
        return ticket
    
    @property
    def is_open(self) -> bool:
        return self.status in (TicketStatus.OPEN, TicketStatus.CLAIMED, TicketStatus.ON_HOLD)
    
    @property
    def response_time(self) -> Optional[int]:
        if self.first_response_at and self.created_at:
            return int((self.first_response_at - self.created_at).total_seconds())
        return None
    
    @property
    def resolution_time(self) -> Optional[int]:
        if self.closed_at and self.created_at:
            return int((self.closed_at - self.created_at).total_seconds())
        return None
    
    def claim(self, user_id: int):
        self.claimed_by = user_id
        self.claimed_at = datetime.now()
        self.status = TicketStatus.CLAIMED
        self.updated_at = datetime.now()
    
    def unclaim(self):
        self.claimed_by = None
        self.claimed_at = None
        self.status = TicketStatus.OPEN
        self.updated_at = datetime.now()
    
    def close(self, closed_by: int, reason: Optional[str] = None):
        self.closed_by = closed_by
        self.close_reason = reason
        self.closed_at = datetime.now()
        self.status = TicketStatus.CLOSED
        self.updated_at = datetime.now()
    
    def reopen(self):
        self.closed_by = None
        self.close_reason = None
        self.closed_at = None
        self.status = TicketStatus.OPEN if not self.claimed_by else TicketStatus.CLAIMED
        self.updated_at = datetime.now()
    
    def add_note(self, author_id: int, content: str):
        self.notes.append({
            'author_id': author_id,
            'content': content,
            'timestamp': datetime.now().isoformat()
        })
        self.updated_at = datetime.now()
    
    def set_first_response(self, staff_id: int):
        if not self.first_response_at:
            self.first_response_at = datetime.now()
            self.first_response_by = staff_id
