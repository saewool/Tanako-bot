"""
Cooldown Manager for Discord Bot
Handles command cooldowns and rate limiting
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
from enum import Enum


class BucketType(Enum):
    DEFAULT = 0
    USER = 1
    GUILD = 2
    CHANNEL = 3
    MEMBER = 4
    CATEGORY = 5
    ROLE = 6


@dataclass
class CooldownBucket:
    rate: int
    per: float
    tokens: float = field(default=0.0)
    last_update: float = field(default_factory=time.time)
    
    def get_tokens(self) -> float:
        current = time.time()
        time_passed = current - self.last_update
        self.last_update = current
        self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per))
        return self.tokens
    
    def update_rate_limit(self) -> Optional[float]:
        tokens = self.get_tokens()
        
        if tokens >= 1.0:
            self.tokens -= 1.0
            return None
        
        return self.per - (tokens * self.per / self.rate)
    
    def reset(self):
        self.tokens = self.rate
        self.last_update = time.time()
    
    def get_retry_after(self) -> float:
        tokens = self.get_tokens()
        if tokens >= 1.0:
            return 0.0
        return (1.0 - tokens) * (self.per / self.rate)


@dataclass
class CommandCooldown:
    rate: int
    per: float
    bucket_type: BucketType = BucketType.USER
    
    def get_bucket_key(
        self,
        user_id: int,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None
    ) -> str:
        if self.bucket_type == BucketType.USER:
            return f"user:{user_id}"
        elif self.bucket_type == BucketType.GUILD:
            return f"guild:{guild_id}"
        elif self.bucket_type == BucketType.CHANNEL:
            return f"channel:{channel_id}"
        elif self.bucket_type == BucketType.MEMBER:
            return f"member:{guild_id}:{user_id}"
        else:
            return f"default:{user_id}"


class CooldownManager:
    def __init__(self):
        self._cooldowns: Dict[str, CommandCooldown] = {}
        self._buckets: Dict[str, Dict[str, CooldownBucket]] = {}
        self._global_cooldowns: Dict[int, float] = {}
        self._lock = asyncio.Lock()
    
    def register_cooldown(
        self,
        command_name: str,
        rate: int,
        per: float,
        bucket_type: BucketType = BucketType.USER
    ):
        self._cooldowns[command_name] = CommandCooldown(
            rate=rate,
            per=per,
            bucket_type=bucket_type
        )
        self._buckets[command_name] = {}
    
    def unregister_cooldown(self, command_name: str):
        if command_name in self._cooldowns:
            del self._cooldowns[command_name]
        if command_name in self._buckets:
            del self._buckets[command_name]
    
    async def check_cooldown(
        self,
        command_name: str,
        user_id: int,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None
    ) -> Tuple[bool, float]:
        async with self._lock:
            if command_name not in self._cooldowns:
                return True, 0.0
            
            cooldown = self._cooldowns[command_name]
            bucket_key = cooldown.get_bucket_key(user_id, guild_id, channel_id)
            
            if bucket_key not in self._buckets[command_name]:
                self._buckets[command_name][bucket_key] = CooldownBucket(
                    rate=cooldown.rate,
                    per=cooldown.per,
                    tokens=cooldown.rate
                )
            
            bucket = self._buckets[command_name][bucket_key]
            retry_after = bucket.update_rate_limit()
            
            if retry_after is not None:
                return False, retry_after
            
            return True, 0.0
    
    async def reset_cooldown(
        self,
        command_name: str,
        user_id: int,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None
    ):
        async with self._lock:
            if command_name not in self._cooldowns:
                return
            
            cooldown = self._cooldowns[command_name]
            bucket_key = cooldown.get_bucket_key(user_id, guild_id, channel_id)
            
            if bucket_key in self._buckets[command_name]:
                self._buckets[command_name][bucket_key].reset()
    
    async def get_remaining_cooldown(
        self,
        command_name: str,
        user_id: int,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None
    ) -> float:
        async with self._lock:
            if command_name not in self._cooldowns:
                return 0.0
            
            cooldown = self._cooldowns[command_name]
            bucket_key = cooldown.get_bucket_key(user_id, guild_id, channel_id)
            
            if bucket_key not in self._buckets[command_name]:
                return 0.0
            
            bucket = self._buckets[command_name][bucket_key]
            return bucket.get_retry_after()
    
    def set_global_cooldown(self, user_id: int, until: float):
        self._global_cooldowns[user_id] = until
    
    def check_global_cooldown(self, user_id: int) -> Tuple[bool, float]:
        if user_id not in self._global_cooldowns:
            return True, 0.0
        
        until = self._global_cooldowns[user_id]
        current = time.time()
        
        if current >= until:
            del self._global_cooldowns[user_id]
            return True, 0.0
        
        return False, until - current
    
    def clear_global_cooldown(self, user_id: int):
        if user_id in self._global_cooldowns:
            del self._global_cooldowns[user_id]
    
    async def cleanup_expired(self):
        async with self._lock:
            current = time.time()
            
            for command_name in list(self._buckets.keys()):
                for bucket_key in list(self._buckets[command_name].keys()):
                    bucket = self._buckets[command_name][bucket_key]
                    if current - bucket.last_update > bucket.per * 2:
                        del self._buckets[command_name][bucket_key]
            
            for user_id in list(self._global_cooldowns.keys()):
                if current >= self._global_cooldowns[user_id]:
                    del self._global_cooldowns[user_id]
    
    def get_cooldown_info(self, command_name: str) -> Optional[Dict]:
        if command_name not in self._cooldowns:
            return None
        
        cooldown = self._cooldowns[command_name]
        return {
            'rate': cooldown.rate,
            'per': cooldown.per,
            'bucket_type': cooldown.bucket_type.name,
            'active_buckets': len(self._buckets.get(command_name, {}))
        }
    
    def list_cooldowns(self) -> Dict[str, Dict]:
        return {
            name: self.get_cooldown_info(name)
            for name in self._cooldowns
        }


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, list] = {}
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, key: str) -> Tuple[bool, float]:
        async with self._lock:
            current = time.time()
            
            if key not in self._requests:
                self._requests[key] = []
            
            self._requests[key] = [
                ts for ts in self._requests[key]
                if current - ts < self.window_seconds
            ]
            
            if len(self._requests[key]) < self.max_requests:
                self._requests[key].append(current)
                return True, 0.0
            
            oldest = self._requests[key][0]
            retry_after = self.window_seconds - (current - oldest)
            return False, max(0, retry_after)
    
    async def reset(self, key: str):
        async with self._lock:
            if key in self._requests:
                del self._requests[key]
    
    async def cleanup(self):
        async with self._lock:
            current = time.time()
            for key in list(self._requests.keys()):
                self._requests[key] = [
                    ts for ts in self._requests[key]
                    if current - ts < self.window_seconds
                ]
                if not self._requests[key]:
                    del self._requests[key]


class AdaptiveCooldown:
    def __init__(
        self,
        base_rate: int,
        base_per: float,
        min_rate: int = 1,
        max_per: float = 300.0,
        increase_factor: float = 1.5,
        decrease_factor: float = 0.8
    ):
        self.base_rate = base_rate
        self.base_per = base_per
        self.min_rate = min_rate
        self.max_per = max_per
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        
        self._user_state: Dict[int, Dict] = {}
    
    def _get_user_state(self, user_id: int) -> Dict:
        if user_id not in self._user_state:
            self._user_state[user_id] = {
                'rate': self.base_rate,
                'per': self.base_per,
                'violations': 0,
                'last_violation': 0.0
            }
        return self._user_state[user_id]
    
    def record_violation(self, user_id: int):
        state = self._get_user_state(user_id)
        current = time.time()
        
        if current - state['last_violation'] > 3600:
            state['violations'] = 0
        
        state['violations'] += 1
        state['last_violation'] = current
        
        state['per'] = min(
            self.max_per,
            state['per'] * self.increase_factor
        )
    
    def record_success(self, user_id: int):
        state = self._get_user_state(user_id)
        
        state['per'] = max(
            self.base_per,
            state['per'] * self.decrease_factor
        )
        
        if state['per'] <= self.base_per:
            state['violations'] = max(0, state['violations'] - 1)
    
    def get_cooldown(self, user_id: int) -> Tuple[int, float]:
        state = self._get_user_state(user_id)
        return state['rate'], state['per']
    
    def reset_user(self, user_id: int):
        if user_id in self._user_state:
            del self._user_state[user_id]
