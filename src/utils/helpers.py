"""
Helper Functions for Discord Bot
Common utility functions used throughout the bot
"""

import re
import uuid
import hashlib
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple, Union
import asyncio


def format_timestamp(
    dt: datetime,
    style: str = 'f'
) -> str:
    timestamp = int(dt.timestamp())
    return f"<t:{timestamp}:{style}>"


def parse_duration(duration_str: str) -> Optional[timedelta]:
    patterns = {
        r'(\d+)\s*s(?:ec(?:ond)?s?)?': 'seconds',
        r'(\d+)\s*m(?:in(?:ute)?s?)?': 'minutes',
        r'(\d+)\s*h(?:(?:ou)?rs?)?': 'hours',
        r'(\d+)\s*d(?:ays?)?': 'days',
        r'(\d+)\s*w(?:eeks?)?': 'weeks',
        r'(\d+)\s*mo(?:nths?)?': 'months',
    }
    
    total_seconds = 0
    remaining = duration_str.lower().strip()
    
    for pattern, unit in patterns.items():
        match = re.search(pattern, remaining)
        if match:
            value = int(match.group(1))
            if unit == 'seconds':
                total_seconds += value
            elif unit == 'minutes':
                total_seconds += value * 60
            elif unit == 'hours':
                total_seconds += value * 3600
            elif unit == 'days':
                total_seconds += value * 86400
            elif unit == 'weeks':
                total_seconds += value * 604800
            elif unit == 'months':
                total_seconds += value * 2592000
    
    if total_seconds == 0:
        try:
            total_seconds = int(duration_str)
        except ValueError:
            return None
    
    return timedelta(seconds=total_seconds)


def format_duration(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    
    if total_seconds < 0:
        return "0 seconds"
    
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
    
    if len(parts) > 1:
        return ", ".join(parts[:-1]) + " and " + parts[-1]
    return parts[0]


def truncate_string(
    text: str,
    max_length: int,
    suffix: str = "..."
) -> str:
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def sanitize_input(text: str, allow_newlines: bool = True) -> str:
    text = text.replace('@everyone', '@\u200beveryone')
    text = text.replace('@here', '@\u200bhere')
    
    if not allow_newlines:
        text = text.replace('\n', ' ')
    
    text = re.sub(r'<@!?(\d+)>', r'<@\u200b\1>', text)
    text = re.sub(r'<@&(\d+)>', r'<@&\u200b\1>', text)
    
    return text


def generate_id(prefix: str = "", length: int = 8) -> str:
    unique_id = uuid.uuid4().hex[:length].upper()
    if prefix:
        return f"{prefix}-{unique_id}"
    return unique_id


def format_number(number: Union[int, float], decimals: int = 2) -> str:
    if abs(number) >= 1_000_000_000:
        return f"{number / 1_000_000_000:.{decimals}f}B"
    elif abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.{decimals}f}M"
    elif abs(number) >= 1_000:
        return f"{number / 1_000:.{decimals}f}K"
    elif isinstance(number, float):
        return f"{number:.{decimals}f}"
    return str(number)


def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def paginate_list(
    items: List[Any],
    page: int = 1,
    per_page: int = 10
) -> Tuple[List[Any], int, int]:
    total_items = len(items)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    
    page = max(1, min(page, total_pages))
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    return items[start_idx:end_idx], page, total_pages


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def escape_markdown(text: str) -> str:
    markdown_chars = ['*', '_', '`', '~', '|', '>', '#', '-', '+', '=', '[', ']', '(', ')', '!']
    for char in markdown_chars:
        text = text.replace(char, f'\\{char}')
    return text


def extract_ids(text: str) -> List[int]:
    ids = re.findall(r'\d{17,20}', text)
    return [int(id_str) for id_str in ids]


def human_join(items: List[str], conjunction: str = "and") -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} {conjunction} {items[1]}"
    return f"{', '.join(items[:-1])}, {conjunction} {items[-1]}"


def generate_hash(data: str, length: int = 16) -> str:
    return hashlib.sha256(data.encode()).hexdigest()[:length]


def is_valid_snowflake(value: Any) -> bool:
    try:
        snowflake = int(value)
        return 17 <= len(str(snowflake)) <= 20 and snowflake > 0
    except (ValueError, TypeError):
        return False


def get_ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return f"{n}{suffix}"


def pluralize(word: str, count: int, plural_form: Optional[str] = None) -> str:
    if count == 1:
        return word
    return plural_form or (word + 's')


def levenshtein_distance(s1: str, s2: str) -> int:
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]


def find_similar(query: str, options: List[str], threshold: float = 0.6) -> List[Tuple[str, float]]:
    results = []
    query_lower = query.lower()
    
    for option in options:
        option_lower = option.lower()
        
        if query_lower in option_lower:
            similarity = len(query_lower) / len(option_lower)
            results.append((option, similarity))
            continue
        
        max_len = max(len(query_lower), len(option_lower))
        if max_len == 0:
            continue
        
        distance = levenshtein_distance(query_lower, option_lower)
        similarity = 1 - (distance / max_len)
        
        if similarity >= threshold:
            results.append((option, similarity))
    
    return sorted(results, key=lambda x: x[1], reverse=True)


def merge_dicts(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


async def async_retry(
    func,
    max_attempts: int = 3,
    delay: float = 1.0,
    exponential: bool = True,
    exceptions: Tuple = (Exception,)
):
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return await func()
        except exceptions as e:
            last_exception = e
            if attempt < max_attempts - 1:
                wait_time = delay * (2 ** attempt if exponential else 1)
                await asyncio.sleep(wait_time)
    
    raise last_exception


class Singleton:
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class Registry:
    def __init__(self):
        self._items = {}
    
    def register(self, name: str, item: Any):
        self._items[name] = item
    
    def unregister(self, name: str):
        if name in self._items:
            del self._items[name]
    
    def get(self, name: str) -> Optional[Any]:
        return self._items.get(name)
    
    def list_all(self) -> List[str]:
        return list(self._items.keys())
    
    def __contains__(self, name: str) -> bool:
        return name in self._items
