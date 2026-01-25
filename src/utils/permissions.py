"""
Permission System for Discord Bot
Role-based access control for bot commands
"""

import discord
from discord.ext import commands
from enum import IntEnum
from typing import Callable, List, Optional, Set, Union
from functools import wraps


class PermissionLevel(IntEnum):
    EVERYONE = 0
    TRUSTED = 1
    MODERATOR = 2
    ADMIN = 3
    SERVER_OWNER = 4
    BOT_OWNER = 5


class PermissionChecker:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._bot_owners: Set[int] = set()
        self._trusted_users: dict[int, Set[int]] = {}
        self._moderator_roles: dict[int, Set[int]] = {}
        self._admin_roles: dict[int, Set[int]] = {}
    
    def set_bot_owners(self, owner_ids: List[int]):
        self._bot_owners = set(owner_ids)
    
    def add_bot_owner(self, user_id: int):
        self._bot_owners.add(user_id)
    
    def remove_bot_owner(self, user_id: int):
        self._bot_owners.discard(user_id)
    
    def is_bot_owner(self, user_id: int) -> bool:
        return user_id in self._bot_owners
    
    def set_trusted_users(self, guild_id: int, user_ids: List[int]):
        self._trusted_users[guild_id] = set(user_ids)
    
    def add_trusted_user(self, guild_id: int, user_id: int):
        if guild_id not in self._trusted_users:
            self._trusted_users[guild_id] = set()
        self._trusted_users[guild_id].add(user_id)
    
    def remove_trusted_user(self, guild_id: int, user_id: int):
        if guild_id in self._trusted_users:
            self._trusted_users[guild_id].discard(user_id)
    
    def set_moderator_roles(self, guild_id: int, role_ids: List[int]):
        self._moderator_roles[guild_id] = set(role_ids)
    
    def add_moderator_role(self, guild_id: int, role_id: int):
        if guild_id not in self._moderator_roles:
            self._moderator_roles[guild_id] = set()
        self._moderator_roles[guild_id].add(role_id)
    
    def remove_moderator_role(self, guild_id: int, role_id: int):
        if guild_id in self._moderator_roles:
            self._moderator_roles[guild_id].discard(role_id)
    
    def set_admin_roles(self, guild_id: int, role_ids: List[int]):
        self._admin_roles[guild_id] = set(role_ids)
    
    def add_admin_role(self, guild_id: int, role_id: int):
        if guild_id not in self._admin_roles:
            self._admin_roles[guild_id] = set()
        self._admin_roles[guild_id].add(role_id)
    
    def remove_admin_role(self, guild_id: int, role_id: int):
        if guild_id in self._admin_roles:
            self._admin_roles[guild_id].discard(role_id)
    
    def get_permission_level(self, member: discord.Member) -> PermissionLevel:
        if member.id in self._bot_owners:
            return PermissionLevel.BOT_OWNER
        
        if member.id == member.guild.owner_id:
            return PermissionLevel.SERVER_OWNER
        
        if member.guild_permissions.administrator:
            return PermissionLevel.ADMIN
        
        guild_id = member.guild.id
        member_role_ids = {r.id for r in member.roles}
        
        if guild_id in self._admin_roles:
            if member_role_ids & self._admin_roles[guild_id]:
                return PermissionLevel.ADMIN
        
        if guild_id in self._moderator_roles:
            if member_role_ids & self._moderator_roles[guild_id]:
                return PermissionLevel.MODERATOR
        
        mod_perms = [
            member.guild_permissions.manage_messages,
            member.guild_permissions.kick_members,
            member.guild_permissions.ban_members,
            member.guild_permissions.manage_roles,
            member.guild_permissions.manage_channels
        ]
        if any(mod_perms):
            return PermissionLevel.MODERATOR
        
        if guild_id in self._trusted_users:
            if member.id in self._trusted_users[guild_id]:
                return PermissionLevel.TRUSTED
        
        return PermissionLevel.EVERYONE
    
    def has_permission(self, member: discord.Member, required_level: PermissionLevel) -> bool:
        return self.get_permission_level(member) >= required_level
    
    def can_moderate(self, moderator: discord.Member, target: discord.Member) -> bool:
        if moderator.id == target.id:
            return False
        
        if target.id == target.guild.owner_id:
            return False
        
        if moderator.id == moderator.guild.owner_id:
            return True
        
        if moderator.id in self._bot_owners:
            return True
        
        if moderator.top_role <= target.top_role:
            return False
        
        return True
    
    def can_assign_role(self, member: discord.Member, role: discord.Role) -> bool:
        if member.id == member.guild.owner_id:
            return True
        
        if not member.guild_permissions.manage_roles:
            return False
        
        return member.top_role > role


def require_permission(level: PermissionLevel):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, ctx: commands.Context, *args, **kwargs):
            if not hasattr(self, 'bot') or not hasattr(self.bot, 'permission_checker'):
                return await func(self, ctx, *args, **kwargs)
            
            checker: PermissionChecker = self.bot.permission_checker
            
            if not checker.has_permission(ctx.author, level):
                embed = discord.Embed(
                    title="❌ Permission Denied",
                    description=f"You need **{level.name}** permission level or higher to use this command.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


def require_permissions(*perms: str):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, ctx: commands.Context, *args, **kwargs):
            missing = []
            for perm in perms:
                if not getattr(ctx.author.guild_permissions, perm, False):
                    missing.append(perm.replace('_', ' ').title())
            
            if missing:
                embed = discord.Embed(
                    title="❌ Missing Permissions",
                    description=f"You need the following permissions:\n" + "\n".join(f"• {p}" for p in missing),
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


def bot_owner_only():
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, ctx: commands.Context, *args, **kwargs):
            if hasattr(self, 'bot') and hasattr(self.bot, 'permission_checker'):
                checker: PermissionChecker = self.bot.permission_checker
                if not checker.is_bot_owner(ctx.author.id):
                    embed = discord.Embed(
                        title="❌ Access Denied",
                        description="This command is only available to bot owners.",
                        color=discord.Color.red()
                    )
                    await ctx.send(embed=embed)
                    return
            
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


def guild_only():
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, ctx: commands.Context, *args, **kwargs):
            if ctx.guild is None:
                embed = discord.Embed(
                    title="❌ Server Only",
                    description="This command can only be used in a server.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


def dm_only():
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, ctx: commands.Context, *args, **kwargs):
            if ctx.guild is not None:
                embed = discord.Embed(
                    title="❌ DM Only",
                    description="This command can only be used in DMs.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            
            return await func(self, ctx, *args, **kwargs)
        return wrapper
    return decorator


class PermissionConfig:
    def __init__(self):
        self.command_permissions: dict[str, PermissionLevel] = {}
        self.command_role_whitelist: dict[str, Set[int]] = {}
        self.command_role_blacklist: dict[str, Set[int]] = {}
        self.command_user_whitelist: dict[str, Set[int]] = {}
        self.command_user_blacklist: dict[str, Set[int]] = {}
        self.command_channel_whitelist: dict[str, Set[int]] = {}
        self.command_channel_blacklist: dict[str, Set[int]] = {}
    
    def set_command_permission(self, command: str, level: PermissionLevel):
        self.command_permissions[command] = level
    
    def get_command_permission(self, command: str) -> PermissionLevel:
        return self.command_permissions.get(command, PermissionLevel.EVERYONE)
    
    def whitelist_role(self, command: str, role_id: int):
        if command not in self.command_role_whitelist:
            self.command_role_whitelist[command] = set()
        self.command_role_whitelist[command].add(role_id)
    
    def blacklist_role(self, command: str, role_id: int):
        if command not in self.command_role_blacklist:
            self.command_role_blacklist[command] = set()
        self.command_role_blacklist[command].add(role_id)
    
    def whitelist_user(self, command: str, user_id: int):
        if command not in self.command_user_whitelist:
            self.command_user_whitelist[command] = set()
        self.command_user_whitelist[command].add(user_id)
    
    def blacklist_user(self, command: str, user_id: int):
        if command not in self.command_user_blacklist:
            self.command_user_blacklist[command] = set()
        self.command_user_blacklist[command].add(user_id)
    
    def whitelist_channel(self, command: str, channel_id: int):
        if command not in self.command_channel_whitelist:
            self.command_channel_whitelist[command] = set()
        self.command_channel_whitelist[command].add(channel_id)
    
    def blacklist_channel(self, command: str, channel_id: int):
        if command not in self.command_channel_blacklist:
            self.command_channel_blacklist[command] = set()
        self.command_channel_blacklist[command].add(channel_id)
    
    def can_use_command(
        self,
        command: str,
        member: discord.Member,
        channel: discord.abc.GuildChannel
    ) -> tuple[bool, Optional[str]]:
        if member.id in self.command_user_blacklist.get(command, set()):
            return False, "You are blacklisted from using this command."
        
        if channel.id in self.command_channel_blacklist.get(command, set()):
            return False, "This command cannot be used in this channel."
        
        member_role_ids = {r.id for r in member.roles}
        if member_role_ids & self.command_role_blacklist.get(command, set()):
            return False, "Your role is blacklisted from using this command."
        
        if command in self.command_user_whitelist:
            if member.id not in self.command_user_whitelist[command]:
                return False, "You are not whitelisted for this command."
        
        if command in self.command_channel_whitelist:
            if channel.id not in self.command_channel_whitelist[command]:
                return False, "This command cannot be used in this channel."
        
        if command in self.command_role_whitelist:
            if not (member_role_ids & self.command_role_whitelist[command]):
                return False, "You don't have a whitelisted role for this command."
        
        return True, None
    
    def to_dict(self) -> dict:
        return {
            'command_permissions': {k: v.value for k, v in self.command_permissions.items()},
            'command_role_whitelist': {k: list(v) for k, v in self.command_role_whitelist.items()},
            'command_role_blacklist': {k: list(v) for k, v in self.command_role_blacklist.items()},
            'command_user_whitelist': {k: list(v) for k, v in self.command_user_whitelist.items()},
            'command_user_blacklist': {k: list(v) for k, v in self.command_user_blacklist.items()},
            'command_channel_whitelist': {k: list(v) for k, v in self.command_channel_whitelist.items()},
            'command_channel_blacklist': {k: list(v) for k, v in self.command_channel_blacklist.items()}
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PermissionConfig':
        config = cls()
        
        for cmd, level in data.get('command_permissions', {}).items():
            config.command_permissions[cmd] = PermissionLevel(level)
        
        for cmd, roles in data.get('command_role_whitelist', {}).items():
            config.command_role_whitelist[cmd] = set(roles)
        
        for cmd, roles in data.get('command_role_blacklist', {}).items():
            config.command_role_blacklist[cmd] = set(roles)
        
        for cmd, users in data.get('command_user_whitelist', {}).items():
            config.command_user_whitelist[cmd] = set(users)
        
        for cmd, users in data.get('command_user_blacklist', {}).items():
            config.command_user_blacklist[cmd] = set(users)
        
        for cmd, channels in data.get('command_channel_whitelist', {}).items():
            config.command_channel_whitelist[cmd] = set(channels)
        
        for cmd, channels in data.get('command_channel_blacklist', {}).items():
            config.command_channel_blacklist[cmd] = set(channels)
        
        return config
