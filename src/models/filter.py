"""
Filter Models
Data structures for word filtering and content moderation
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Pattern, Set
import re
import unicodedata


class FilterAction(Enum):
    DELETE = "delete"
    WARN = "warn"
    MUTE = "mute"
    KICK = "kick"
    BAN = "ban"
    LOG = "log"
    
    @property
    def severity(self) -> int:
        severities = {
            FilterAction.LOG: 1,
            FilterAction.DELETE: 2,
            FilterAction.WARN: 3,
            FilterAction.MUTE: 4,
            FilterAction.KICK: 5,
            FilterAction.BAN: 6
        }
        return severities.get(self, 0)


class FilterType(Enum):
    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"
    FUZZY = "fuzzy"
    WILDCARD = "wildcard"


class FilterBypassType(Enum):
    NONE = "none"
    ZALGO = "zalgo"
    SPACING = "spacing"
    UNICODE = "unicode"
    LEET = "leet"
    REVERSED = "reversed"
    REPEATED = "repeated"
    MIXED = "mixed"


@dataclass
class FilterRule:
    id: str
    guild_id: int
    
    pattern: str
    filter_type: FilterType = FilterType.CONTAINS
    
    action: FilterAction = FilterAction.DELETE
    secondary_action: Optional[FilterAction] = None
    
    enabled: bool = True
    
    case_sensitive: bool = False
    
    check_bypass: bool = True
    bypass_types: List[FilterBypassType] = field(default_factory=lambda: [
        FilterBypassType.ZALGO,
        FilterBypassType.SPACING,
        FilterBypassType.UNICODE,
        FilterBypassType.LEET
    ])
    
    exempt_roles: List[int] = field(default_factory=list)
    exempt_channels: List[int] = field(default_factory=list)
    exempt_users: List[int] = field(default_factory=list)
    
    punishment_duration: Optional[int] = None
    
    strikes_before_action: int = 1
    strike_expiry_hours: int = 24
    
    custom_message: Optional[str] = None
    dm_user: bool = False
    
    log_matches: bool = True
    
    created_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[int] = None
    
    match_count: int = 0
    last_match_at: Optional[datetime] = None
    
    _compiled_pattern: Optional[Pattern] = field(default=None, repr=False)
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'guild_id': self.guild_id,
            'pattern': self.pattern,
            'filter_type': self.filter_type.value,
            'action': self.action.value,
            'secondary_action': self.secondary_action.value if self.secondary_action else None,
            'enabled': self.enabled,
            'case_sensitive': self.case_sensitive,
            'check_bypass': self.check_bypass,
            'bypass_types': [bt.value for bt in self.bypass_types],
            'exempt_roles': self.exempt_roles,
            'exempt_channels': self.exempt_channels,
            'exempt_users': self.exempt_users,
            'punishment_duration': self.punishment_duration,
            'strikes_before_action': self.strikes_before_action,
            'strike_expiry_hours': self.strike_expiry_hours,
            'custom_message': self.custom_message,
            'dm_user': self.dm_user,
            'log_matches': self.log_matches,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by,
            'match_count': self.match_count,
            'last_match_at': self.last_match_at.isoformat() if self.last_match_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FilterRule':
        rule = cls(
            id=data['id'],
            guild_id=data['guild_id'],
            pattern=data['pattern']
        )
        
        rule.filter_type = FilterType(data.get('filter_type', 'contains'))
        rule.action = FilterAction(data.get('action', 'delete'))
        if data.get('secondary_action'):
            rule.secondary_action = FilterAction(data['secondary_action'])
        
        for key in ['enabled', 'case_sensitive', 'check_bypass', 'dm_user', 'log_matches']:
            if key in data:
                setattr(rule, key, data[key])
        
        if 'bypass_types' in data:
            rule.bypass_types = [FilterBypassType(bt) for bt in data['bypass_types']]
        
        for key in ['exempt_roles', 'exempt_channels', 'exempt_users']:
            if key in data:
                setattr(rule, key, data[key])
        
        for key in ['punishment_duration', 'strikes_before_action', 'strike_expiry_hours',
                    'custom_message', 'created_by', 'match_count']:
            if key in data:
                setattr(rule, key, data[key])
        
        for key in ['created_at', 'last_match_at']:
            if data.get(key):
                if isinstance(data[key], str):
                    setattr(rule, key, datetime.fromisoformat(data[key]))
                else:
                    setattr(rule, key, data[key])
        
        return rule
    
    def compile_pattern(self) -> Pattern:
        if self._compiled_pattern:
            return self._compiled_pattern
        
        flags = 0 if self.case_sensitive else re.IGNORECASE
        
        if self.filter_type == FilterType.REGEX:
            self._compiled_pattern = re.compile(self.pattern, flags)
        elif self.filter_type == FilterType.EXACT:
            self._compiled_pattern = re.compile(rf'^{re.escape(self.pattern)}$', flags)
        elif self.filter_type == FilterType.WILDCARD:
            wildcard_pattern = re.escape(self.pattern).replace(r'\*', '.*').replace(r'\?', '.')
            self._compiled_pattern = re.compile(wildcard_pattern, flags)
        else:
            self._compiled_pattern = re.compile(re.escape(self.pattern), flags)
        
        return self._compiled_pattern
    
    def matches(self, content: str, check_bypass: bool = True) -> tuple[bool, Optional[str]]:
        if not self.enabled:
            return False, None
        
        pattern = self.compile_pattern()
        
        if pattern.search(content):
            return True, content
        
        if check_bypass and self.check_bypass:
            normalized = self._normalize_for_bypass(content)
            if normalized != content and pattern.search(normalized):
                return True, normalized
        
        return False, None
    
    def _normalize_for_bypass(self, text: str) -> str:
        if FilterBypassType.ZALGO in self.bypass_types:
            text = self._remove_zalgo(text)
        
        if FilterBypassType.SPACING in self.bypass_types:
            text = self._remove_extra_spacing(text)
        
        if FilterBypassType.UNICODE in self.bypass_types:
            text = self._normalize_unicode(text)
        
        if FilterBypassType.LEET in self.bypass_types:
            text = self._decode_leet(text)
        
        return text
    
    def _remove_zalgo(self, text: str) -> str:
        return ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    def _remove_extra_spacing(self, text: str) -> str:
        cleaned = re.sub(r'\s+', ' ', text)
        cleaned = re.sub(r'(\w)\s+(\w)', r'\1\2', cleaned)
        return cleaned
    
    def _normalize_unicode(self, text: str) -> str:
        char_map = {
            '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f',
            '𝐠': 'g', '𝐡': 'h', '𝐢': 'i', '𝐣': 'j', '𝐤': 'k', '𝐥': 'l',
            '𝐦': 'm', '𝐧': 'n', '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r',
            '𝐬': 's', '𝐭': 't', '𝐮': 'u', '𝐯': 'v', '𝐰': 'w', '𝐱': 'x',
            '𝐲': 'y', '𝐳': 'z',
            'ａ': 'a', 'ｂ': 'b', 'ｃ': 'c', 'ｄ': 'd', 'ｅ': 'e', 'ｆ': 'f',
            'ｇ': 'g', 'ｈ': 'h', 'ｉ': 'i', 'ｊ': 'j', 'ｋ': 'k', 'ｌ': 'l',
            'ｍ': 'm', 'ｎ': 'n', 'ｏ': 'o', 'ｐ': 'p', 'ｑ': 'q', 'ｒ': 'r',
            'ｓ': 's', 'ｔ': 't', 'ｕ': 'u', 'ｖ': 'v', 'ｗ': 'w', 'ｘ': 'x',
            'ｙ': 'y', 'ｚ': 'z',
            'α': 'a', 'β': 'b', 'ε': 'e', 'η': 'n', 'ι': 'i', 'κ': 'k',
            'μ': 'm', 'ν': 'n', 'ο': 'o', 'ρ': 'r', 'σ': 's', 'τ': 't',
            'υ': 'u', 'χ': 'x', 'ψ': 'y',
        }
        
        text = unicodedata.normalize('NFKC', text)
        return ''.join(char_map.get(c, c) for c in text)
    
    def _decode_leet(self, text: str) -> str:
        leet_map = {
            '0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's',
            '6': 'g', '7': 't', '8': 'b', '9': 'g',
            '@': 'a', '$': 's', '!': 'i', '+': 't',
            '|': 'l', '(': 'c', ')': 'd', '/\\': 'a',
            '\\/': 'v', '|_|': 'u', '|\\|': 'n', '|\\/|': 'm',
        }
        
        result = text
        for leet, letter in sorted(leet_map.items(), key=lambda x: -len(x[0])):
            result = result.replace(leet, letter)
        
        return result
    
    def record_match(self):
        self.match_count += 1
        self.last_match_at = datetime.now()


@dataclass
class FilterConfig:
    guild_id: int
    
    enabled: bool = True
    
    rules: List[FilterRule] = field(default_factory=list)
    
    global_exempt_roles: List[int] = field(default_factory=list)
    global_exempt_channels: List[int] = field(default_factory=list)
    global_exempt_users: List[int] = field(default_factory=list)
    
    log_channel: Optional[int] = None
    alert_channel: Optional[int] = None
    
    default_action: FilterAction = FilterAction.DELETE
    
    user_strikes: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'guild_id': self.guild_id,
            'enabled': self.enabled,
            'rules': [r.to_dict() for r in self.rules],
            'global_exempt_roles': self.global_exempt_roles,
            'global_exempt_channels': self.global_exempt_channels,
            'global_exempt_users': self.global_exempt_users,
            'log_channel': self.log_channel,
            'alert_channel': self.alert_channel,
            'default_action': self.default_action.value,
            'user_strikes': self.user_strikes
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FilterConfig':
        config = cls(guild_id=data['guild_id'])
        
        config.enabled = data.get('enabled', True)
        
        if 'rules' in data:
            config.rules = [FilterRule.from_dict(r) for r in data['rules']]
        
        for key in ['global_exempt_roles', 'global_exempt_channels', 'global_exempt_users']:
            if key in data:
                setattr(config, key, data[key])
        
        config.log_channel = data.get('log_channel')
        config.alert_channel = data.get('alert_channel')
        
        if 'default_action' in data:
            config.default_action = FilterAction(data['default_action'])
        
        config.user_strikes = data.get('user_strikes', {})
        
        return config
    
    def add_rule(self, rule: FilterRule):
        self.rules.append(rule)
    
    def remove_rule(self, rule_id: str) -> bool:
        for i, rule in enumerate(self.rules):
            if rule.id == rule_id:
                del self.rules[i]
                return True
        return False
    
    def get_rule(self, rule_id: str) -> Optional[FilterRule]:
        for rule in self.rules:
            if rule.id == rule_id:
                return rule
        return None
    
    def check_content(
        self,
        content: str,
        user_id: int,
        channel_id: int,
        user_roles: List[int]
    ) -> List[tuple[FilterRule, str]]:
        if not self.enabled:
            return []
        
        if user_id in self.global_exempt_users:
            return []
        if channel_id in self.global_exempt_channels:
            return []
        if any(role in self.global_exempt_roles for role in user_roles):
            return []
        
        matches = []
        
        for rule in self.rules:
            if not rule.enabled:
                continue
            
            if user_id in rule.exempt_users:
                continue
            if channel_id in rule.exempt_channels:
                continue
            if any(role in rule.exempt_roles for role in user_roles):
                continue
            
            matched, normalized = rule.matches(content)
            if matched:
                matches.append((rule, normalized or content))
        
        return matches
    
    def add_strike(self, user_id: int, rule_id: str) -> int:
        user_key = str(user_id)
        if user_key not in self.user_strikes:
            self.user_strikes[user_key] = {}
        
        if rule_id not in self.user_strikes[user_key]:
            self.user_strikes[user_key][rule_id] = {
                'count': 0,
                'timestamps': []
            }
        
        self.user_strikes[user_key][rule_id]['count'] += 1
        self.user_strikes[user_key][rule_id]['timestamps'].append(
            datetime.now().isoformat()
        )
        
        return self.user_strikes[user_key][rule_id]['count']
    
    def get_strikes(self, user_id: int, rule_id: str) -> int:
        user_key = str(user_id)
        if user_key not in self.user_strikes:
            return 0
        if rule_id not in self.user_strikes[user_key]:
            return 0
        return self.user_strikes[user_key][rule_id]['count']
    
    def clear_strikes(self, user_id: int, rule_id: Optional[str] = None):
        user_key = str(user_id)
        if user_key not in self.user_strikes:
            return
        
        if rule_id:
            if rule_id in self.user_strikes[user_key]:
                del self.user_strikes[user_key][rule_id]
        else:
            del self.user_strikes[user_key]
