"""
Rich Embed Builder for Discord Bot
Provides a fluent interface for creating beautiful Discord embeds
"""

import discord
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union


class EmbedColor(Enum):
    PRIMARY = 0x5865F2
    SUCCESS = 0x57F287
    WARNING = 0xFEE75C
    ERROR = 0xED4245
    INFO = 0x5865F2
    MODERATION = 0xEB459E
    LOGGING = 0x3498DB
    WELCOME = 0x2ECC71
    TICKET = 0xF1C40F
    ANTI_RAID = 0xE74C3C
    ANTI_NUKE = 0x9B59B6
    FILTER = 0xE67E22
    CUSTOM = 0x99AAB5


class EmbedBuilder:
    def __init__(self, title: Optional[str] = None, description: Optional[str] = None):
        self._embed = discord.Embed()
        if title:
            self._embed.title = title
        if description:
            self._embed.description = description
        self._embed.timestamp = datetime.now()
    
    def title(self, title: str) -> 'EmbedBuilder':
        self._embed.title = title
        return self
    
    def description(self, description: str) -> 'EmbedBuilder':
        self._embed.description = description
        return self
    
    def color(self, color: Union[EmbedColor, int, discord.Color]) -> 'EmbedBuilder':
        if isinstance(color, EmbedColor):
            self._embed.color = color.value
        elif isinstance(color, int):
            self._embed.color = color
        else:
            self._embed.color = color
        return self
    
    def url(self, url: str) -> 'EmbedBuilder':
        self._embed.url = url
        return self
    
    def author(
        self,
        name: str,
        url: Optional[str] = None,
        icon_url: Optional[str] = None
    ) -> 'EmbedBuilder':
        self._embed.set_author(name=name, url=url, icon_url=icon_url)
        return self
    
    def footer(
        self,
        text: str,
        icon_url: Optional[str] = None
    ) -> 'EmbedBuilder':
        self._embed.set_footer(text=text, icon_url=icon_url)
        return self
    
    def thumbnail(self, url: str) -> 'EmbedBuilder':
        self._embed.set_thumbnail(url=url)
        return self
    
    def image(self, url: str) -> 'EmbedBuilder':
        self._embed.set_image(url=url)
        return self
    
    def timestamp(self, timestamp: Optional[datetime] = None) -> 'EmbedBuilder':
        self._embed.timestamp = timestamp or datetime.now()
        return self
    
    def no_timestamp(self) -> 'EmbedBuilder':
        self._embed.timestamp = None
        return self
    
    def field(
        self,
        name: str,
        value: str,
        inline: bool = False
    ) -> 'EmbedBuilder':
        self._embed.add_field(name=name, value=value, inline=inline)
        return self
    
    def fields(self, *fields: Tuple[str, str, bool]) -> 'EmbedBuilder':
        for field_data in fields:
            if len(field_data) == 2:
                name, value = field_data
                inline = False
            else:
                name, value, inline = field_data
            self._embed.add_field(name=name, value=value, inline=inline)
        return self
    
    def blank_field(self, inline: bool = False) -> 'EmbedBuilder':
        self._embed.add_field(name='\u200b', value='\u200b', inline=inline)
        return self
    
    def clear_fields(self) -> 'EmbedBuilder':
        self._embed.clear_fields()
        return self
    
    def build(self) -> discord.Embed:
        return self._embed
    
    @classmethod
    def success(cls, title: str, description: str) -> discord.Embed:
        return (
            cls(title=f"✅ {title}", description=description)
            .color(EmbedColor.SUCCESS)
            .build()
        )
    
    @classmethod
    def error(cls, title: str, description: str) -> discord.Embed:
        return (
            cls(title=f"❌ {title}", description=description)
            .color(EmbedColor.ERROR)
            .build()
        )
    
    @classmethod
    def warning(cls, title: str, description: str) -> discord.Embed:
        return (
            cls(title=f"⚠️ {title}", description=description)
            .color(EmbedColor.WARNING)
            .build()
        )
    
    @classmethod
    def info(cls, title: str, description: str) -> discord.Embed:
        return (
            cls(title=f"ℹ️ {title}", description=description)
            .color(EmbedColor.INFO)
            .build()
        )
    
    @classmethod
    def moderation(
        cls,
        action: str,
        moderator: discord.Member,
        target: Union[discord.Member, discord.User],
        reason: Optional[str] = None,
        duration: Optional[str] = None,
        case_id: Optional[int] = None
    ) -> discord.Embed:
        embed = (
            cls(title=f"🔨 Moderation Action: {action}")
            .color(EmbedColor.MODERATION)
            .field("Moderator", f"{moderator.mention} ({moderator.id})", True)
            .field("Target", f"{target.mention} ({target.id})", True)
        )
        
        if case_id:
            embed.field("Case ID", f"#{case_id}", True)
        
        if duration:
            embed.field("Duration", duration, True)
        
        if reason:
            embed.field("Reason", reason, False)
        
        return embed.footer(f"User ID: {target.id}").build()
    
    @classmethod
    def welcome(
        cls,
        member: discord.Member,
        guild: discord.Guild,
        message: Optional[str] = None
    ) -> discord.Embed:
        description = message or f"Welcome to **{guild.name}**, {member.mention}! You are member #{guild.member_count}."
        
        return (
            cls(title=f"👋 Welcome, {member.display_name}!", description=description)
            .color(EmbedColor.WELCOME)
            .thumbnail(member.display_avatar.url)
            .field("Account Created", f"<t:{int(member.created_at.timestamp())}:R>", True)
            .field("Member Count", str(guild.member_count), True)
            .footer(f"ID: {member.id}")
            .build()
        )
    
    @classmethod
    def goodbye(
        cls,
        member: Union[discord.Member, discord.User],
        guild: discord.Guild
    ) -> discord.Embed:
        return (
            cls(
                title=f"👋 Goodbye, {member.display_name}!",
                description=f"**{member.display_name}** has left **{guild.name}**."
            )
            .color(0x95A5A6)
            .thumbnail(member.display_avatar.url)
            .footer(f"ID: {member.id} | Members: {guild.member_count}")
            .build()
        )
    
    @classmethod
    def ticket_create(
        cls,
        user: discord.Member,
        category: str,
        ticket_id: str
    ) -> discord.Embed:
        return (
            cls(
                title=f"🎫 Ticket Created",
                description=f"Hello {user.mention}! A staff member will assist you shortly.\n\nPlease describe your issue in detail."
            )
            .color(EmbedColor.TICKET)
            .field("Ticket ID", ticket_id, True)
            .field("Category", category, True)
            .field("Created By", user.mention, True)
            .footer("Use the buttons below to manage this ticket")
            .build()
        )
    
    @classmethod
    def ticket_close(
        cls,
        closed_by: discord.Member,
        ticket_id: str,
        reason: Optional[str] = None
    ) -> discord.Embed:
        embed = (
            cls(
                title="🔒 Ticket Closed",
                description=f"This ticket has been closed by {closed_by.mention}."
            )
            .color(EmbedColor.ERROR)
            .field("Ticket ID", ticket_id, True)
        )
        
        if reason:
            embed.field("Reason", reason, False)
        
        return embed.build()
    
    @classmethod
    def anti_raid_alert(
        cls,
        guild: discord.Guild,
        action_taken: str,
        affected_users: int,
        reason: str
    ) -> discord.Embed:
        return (
            cls(
                title="🚨 Anti-Raid Alert",
                description=f"Suspicious activity detected in **{guild.name}**!"
            )
            .color(EmbedColor.ANTI_RAID)
            .field("Reason", reason, False)
            .field("Action Taken", action_taken, True)
            .field("Users Affected", str(affected_users), True)
            .footer("Anti-Raid Protection")
            .build()
        )
    
    @classmethod
    def anti_nuke_alert(
        cls,
        guild: discord.Guild,
        action_taken: str,
        perpetrator: Optional[discord.Member],
        reason: str
    ) -> discord.Embed:
        embed = (
            cls(
                title="🛡️ Anti-Nuke Alert",
                description=f"Potential nuke attempt detected in **{guild.name}**!"
            )
            .color(EmbedColor.ANTI_NUKE)
            .field("Reason", reason, False)
            .field("Action Taken", action_taken, True)
        )
        
        if perpetrator:
            embed.field("Perpetrator", f"{perpetrator.mention} ({perpetrator.id})", True)
        
        return embed.footer("Anti-Nuke Protection").build()
    
    @classmethod
    def filter_violation(
        cls,
        user: discord.Member,
        message_content: str,
        filter_type: str,
        action_taken: str
    ) -> discord.Embed:
        truncated = message_content[:500] + "..." if len(message_content) > 500 else message_content
        
        return (
            cls(
                title="🚫 Filter Violation",
                description=f"A message from {user.mention} was flagged."
            )
            .color(EmbedColor.FILTER)
            .field("User", f"{user.mention} ({user.id})", True)
            .field("Filter Type", filter_type, True)
            .field("Action Taken", action_taken, True)
            .field("Message Content", f"```{truncated}```", False)
            .footer(f"User ID: {user.id}")
            .build()
        )
    
    @classmethod
    def log_message_delete(
        cls,
        message: discord.Message,
        deleted_by: Optional[discord.Member] = None
    ) -> discord.Embed:
        content = message.content[:1000] + "..." if len(message.content) > 1000 else message.content
        
        embed = (
            cls(title="🗑️ Message Deleted")
            .color(EmbedColor.LOGGING)
            .field("Author", f"{message.author.mention} ({message.author.id})", True)
            .field("Channel", message.channel.mention, True)
        )
        
        if deleted_by:
            embed.field("Deleted By", deleted_by.mention, True)
        
        if content:
            embed.field("Content", f"```{content}```", False)
        
        if message.attachments:
            embed.field("Attachments", "\n".join(a.url for a in message.attachments), False)
        
        return embed.footer(f"Message ID: {message.id}").build()
    
    @classmethod
    def log_message_edit(
        cls,
        before: discord.Message,
        after: discord.Message
    ) -> discord.Embed:
        before_content = before.content[:500] + "..." if len(before.content) > 500 else before.content
        after_content = after.content[:500] + "..." if len(after.content) > 500 else after.content
        
        return (
            cls(title="✏️ Message Edited")
            .color(EmbedColor.LOGGING)
            .field("Author", f"{after.author.mention} ({after.author.id})", True)
            .field("Channel", after.channel.mention, True)
            .field("Jump to Message", f"[Click Here]({after.jump_url})", True)
            .field("Before", f"```{before_content or 'No content'}```", False)
            .field("After", f"```{after_content or 'No content'}```", False)
            .footer(f"Message ID: {after.id}")
            .build()
        )
    
    @classmethod
    def log_member_join(cls, member: discord.Member) -> discord.Embed:
        account_age = datetime.now() - member.created_at.replace(tzinfo=None)
        is_new = account_age.days < 7
        
        embed = (
            cls(
                title="📥 Member Joined",
                description=f"{member.mention} joined the server"
            )
            .color(EmbedColor.SUCCESS)
            .thumbnail(member.display_avatar.url)
            .field("Username", str(member), True)
            .field("ID", str(member.id), True)
            .field("Account Created", f"<t:{int(member.created_at.timestamp())}:R>", True)
        )
        
        if is_new:
            embed.field("⚠️ Warning", "New account (less than 7 days old)", False)
        
        return embed.footer(f"Member #{member.guild.member_count}").build()
    
    @classmethod
    def log_member_leave(cls, member: discord.Member) -> discord.Embed:
        roles = [r.mention for r in member.roles if r.name != "@everyone"]
        roles_str = ", ".join(roles[:10]) if roles else "None"
        if len(roles) > 10:
            roles_str += f" and {len(roles) - 10} more..."
        
        return (
            cls(
                title="📤 Member Left",
                description=f"{member.mention} left the server"
            )
            .color(EmbedColor.ERROR)
            .thumbnail(member.display_avatar.url)
            .field("Username", str(member), True)
            .field("ID", str(member.id), True)
            .field("Joined", f"<t:{int(member.joined_at.timestamp())}:R>" if member.joined_at else "Unknown", True)
            .field("Roles", roles_str, False)
            .footer(f"Members: {member.guild.member_count}")
            .build()
        )
    
    @classmethod
    def log_role_update(
        cls,
        before: discord.Role,
        after: discord.Role,
        changed_by: Optional[discord.Member] = None
    ) -> discord.Embed:
        changes = []
        
        if before.name != after.name:
            changes.append(f"Name: `{before.name}` → `{after.name}`")
        if before.color != after.color:
            changes.append(f"Color: `{before.color}` → `{after.color}`")
        if before.hoist != after.hoist:
            changes.append(f"Hoisted: `{before.hoist}` → `{after.hoist}`")
        if before.mentionable != after.mentionable:
            changes.append(f"Mentionable: `{before.mentionable}` → `{after.mentionable}`")
        if before.permissions != after.permissions:
            changes.append("Permissions changed")
        
        embed = (
            cls(title="🔧 Role Updated", description=f"Role {after.mention} was modified")
            .color(after.color if after.color.value else EmbedColor.LOGGING.value)
        )
        
        if changes:
            embed.field("Changes", "\n".join(changes), False)
        
        if changed_by:
            embed.field("Modified By", changed_by.mention, True)
        
        return embed.footer(f"Role ID: {after.id}").build()
    
    @classmethod
    def log_channel_create(
        cls,
        channel: discord.abc.GuildChannel,
        created_by: Optional[discord.Member] = None
    ) -> discord.Embed:
        channel_type = type(channel).__name__.replace("Channel", "")
        
        embed = (
            cls(
                title="➕ Channel Created",
                description=f"{channel.mention} was created"
            )
            .color(EmbedColor.SUCCESS)
            .field("Name", channel.name, True)
            .field("Type", channel_type, True)
            .field("Category", channel.category.name if hasattr(channel, 'category') and channel.category else "None", True)
        )
        
        if created_by:
            embed.field("Created By", created_by.mention, True)
        
        return embed.footer(f"Channel ID: {channel.id}").build()
    
    @classmethod
    def log_channel_delete(
        cls,
        channel: discord.abc.GuildChannel,
        deleted_by: Optional[discord.Member] = None
    ) -> discord.Embed:
        channel_type = type(channel).__name__.replace("Channel", "")
        
        embed = (
            cls(
                title="➖ Channel Deleted",
                description=f"#{channel.name} was deleted"
            )
            .color(EmbedColor.ERROR)
            .field("Name", channel.name, True)
            .field("Type", channel_type, True)
        )
        
        if deleted_by:
            embed.field("Deleted By", deleted_by.mention, True)
        
        return embed.footer(f"Channel ID: {channel.id}").build()
    
    @classmethod
    def pagination_embed(
        cls,
        title: str,
        items: List[str],
        page: int,
        total_pages: int,
        color: EmbedColor = EmbedColor.PRIMARY
    ) -> discord.Embed:
        return (
            cls(title=title, description="\n".join(items))
            .color(color)
            .footer(f"Page {page}/{total_pages}")
            .build()
        )
    
    @classmethod
    def stats_embed(
        cls,
        title: str,
        stats: dict,
        thumbnail_url: Optional[str] = None
    ) -> discord.Embed:
        embed = cls(title=title).color(EmbedColor.INFO)
        
        if thumbnail_url:
            embed.thumbnail(thumbnail_url)
        
        for key, value in stats.items():
            embed.field(key, str(value), True)
        
        return embed.build()
