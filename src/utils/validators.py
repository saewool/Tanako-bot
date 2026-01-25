"""
Input Validators for Discord Bot
Validates various types of user input
"""

import re
from typing import Optional, Tuple
from urllib.parse import urlparse


DISCORD_INVITE_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?'
    r'(?:discord\.(?:gg|io|me|li)|discordapp\.com/invite|discord\.com/invite)/'
    r'([a-zA-Z0-9\-]+)',
    re.IGNORECASE
)

DISCORD_MENTION_PATTERN = re.compile(r'<@!?(\d{17,20})>')

DISCORD_ROLE_MENTION_PATTERN = re.compile(r'<@&(\d{17,20})>')

DISCORD_CHANNEL_MENTION_PATTERN = re.compile(r'<#(\d{17,20})>')

DISCORD_EMOJI_PATTERN = re.compile(r'<a?:(\w+):(\d{17,20})>')

UNICODE_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U0001f926-\U0001f937"
    "\U00010000-\U0010ffff"
    "\u2640-\u2642"
    "\u2600-\u2B55"
    "\u200d"
    "\u23cf"
    "\u23e9"
    "\u231a"
    "\ufe0f"
    "\u3030"
    "]+",
    re.UNICODE
)

URL_PATTERN = re.compile(
    r'https?://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)',
    re.IGNORECASE
)

HEX_COLOR_PATTERN = re.compile(r'^#?([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$')

EMAIL_PATTERN = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
)


def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme in ('http', 'https'), result.netloc])
    except Exception:
        return False


def is_valid_image_url(url: str) -> bool:
    if not is_valid_url(url):
        return False
    
    image_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp')
    parsed = urlparse(url)
    path = parsed.path.lower()
    
    return any(path.endswith(ext) for ext in image_extensions)


def is_valid_invite(text: str) -> Tuple[bool, Optional[str]]:
    match = DISCORD_INVITE_PATTERN.search(text)
    if match:
        return True, match.group(1)
    return False, None


def extract_invites(text: str) -> list:
    return DISCORD_INVITE_PATTERN.findall(text)


def is_valid_mention(text: str) -> Tuple[bool, Optional[int]]:
    match = DISCORD_MENTION_PATTERN.match(text)
    if match:
        return True, int(match.group(1))
    return False, None


def extract_mentions(text: str) -> list:
    return [int(id_str) for id_str in DISCORD_MENTION_PATTERN.findall(text)]


def is_valid_role_mention(text: str) -> Tuple[bool, Optional[int]]:
    match = DISCORD_ROLE_MENTION_PATTERN.match(text)
    if match:
        return True, int(match.group(1))
    return False, None


def extract_role_mentions(text: str) -> list:
    return [int(id_str) for id_str in DISCORD_ROLE_MENTION_PATTERN.findall(text)]


def is_valid_channel_mention(text: str) -> Tuple[bool, Optional[int]]:
    match = DISCORD_CHANNEL_MENTION_PATTERN.match(text)
    if match:
        return True, int(match.group(1))
    return False, None


def extract_channel_mentions(text: str) -> list:
    return [int(id_str) for id_str in DISCORD_CHANNEL_MENTION_PATTERN.findall(text)]


def is_valid_emoji(text: str) -> Tuple[bool, Optional[Tuple[str, int]]]:
    match = DISCORD_EMOJI_PATTERN.match(text)
    if match:
        return True, (match.group(1), int(match.group(2)))
    
    if UNICODE_EMOJI_PATTERN.match(text):
        return True, None
    
    return False, None


def extract_custom_emojis(text: str) -> list:
    return [(name, int(id_str)) for name, id_str in DISCORD_EMOJI_PATTERN.findall(text)]


def count_emojis(text: str) -> int:
    custom_count = len(DISCORD_EMOJI_PATTERN.findall(text))
    unicode_count = len(UNICODE_EMOJI_PATTERN.findall(text))
    return custom_count + unicode_count


def validate_hex_color(color: str) -> Tuple[bool, Optional[int]]:
    match = HEX_COLOR_PATTERN.match(color)
    if not match:
        return False, None
    
    hex_value = match.group(1)
    if len(hex_value) == 3:
        hex_value = ''.join(c * 2 for c in hex_value)
    
    return True, int(hex_value, 16)


def is_valid_snowflake(value: str) -> bool:
    try:
        snowflake = int(value)
        return 17 <= len(value) <= 20 and snowflake > 0
    except ValueError:
        return False


def is_valid_email(email: str) -> bool:
    return bool(EMAIL_PATTERN.match(email))


def contains_mass_mentions(text: str, threshold: int = 5) -> bool:
    if '@everyone' in text or '@here' in text:
        return True
    
    mentions = extract_mentions(text)
    return len(mentions) >= threshold


def contains_invite(text: str) -> bool:
    return bool(DISCORD_INVITE_PATTERN.search(text))


def contains_url(text: str) -> bool:
    return bool(URL_PATTERN.search(text))


def extract_urls(text: str) -> list:
    return URL_PATTERN.findall(text)


def is_excessive_caps(text: str, threshold: float = 0.7, min_length: int = 10) -> bool:
    letters = [c for c in text if c.isalpha()]
    
    if len(letters) < min_length:
        return False
    
    uppercase_count = sum(1 for c in letters if c.isupper())
    ratio = uppercase_count / len(letters)
    
    return ratio > threshold


def is_excessive_emojis(text: str, threshold: int = 10) -> bool:
    return count_emojis(text) > threshold


def is_spam_like(text: str) -> Tuple[bool, Optional[str]]:
    if len(text) > 2000:
        return True, "Message too long"
    
    if is_excessive_caps(text):
        return True, "Excessive caps"
    
    if is_excessive_emojis(text):
        return True, "Too many emojis"
    
    if contains_mass_mentions(text):
        return True, "Mass mentions"
    
    repeating_pattern = re.compile(r'(.{3,})\1{5,}')
    if repeating_pattern.search(text):
        return True, "Repeating pattern"
    
    words = text.split()
    if len(words) > 5:
        unique_words = set(words)
        if len(unique_words) / len(words) < 0.2:
            return True, "Repetitive words"
    
    return False, None


def sanitize_username(username: str) -> str:
    sanitized = re.sub(r'[^\w\s-]', '', username)
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()
    return sanitized[:32] if sanitized else "User"


def validate_command_name(name: str) -> bool:
    return bool(re.match(r'^[a-z][a-z0-9_-]{0,31}$', name))


def validate_prefix(prefix: str) -> Tuple[bool, Optional[str]]:
    if not prefix:
        return False, "Prefix cannot be empty"
    
    if len(prefix) > 5:
        return False, "Prefix must be 5 characters or less"
    
    if prefix.isspace():
        return False, "Prefix cannot be only whitespace"
    
    if '@' in prefix:
        return False, "Prefix cannot contain @"
    
    return True, None


def validate_reason(reason: str, max_length: int = 512) -> Tuple[bool, str]:
    if not reason or not reason.strip():
        return True, "No reason provided"
    
    reason = reason.strip()
    
    if len(reason) > max_length:
        reason = reason[:max_length - 3] + "..."
    
    return True, reason


def parse_user_input(
    text: str,
    allow_mentions: bool = True,
    allow_ids: bool = True
) -> Optional[int]:
    if allow_mentions:
        match = DISCORD_MENTION_PATTERN.match(text)
        if match:
            return int(match.group(1))
    
    if allow_ids:
        try:
            user_id = int(text.strip())
            if 17 <= len(str(user_id)) <= 20:
                return user_id
        except ValueError:
            pass
    
    return None
