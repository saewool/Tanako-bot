"""
Logging Cog
Comprehensive server logging system
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
from datetime import datetime

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.models.logs import LogType, LogEntry, LogConfig


class LoggingCog(commands.Cog, name="Logging"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._message_cache = {}
    
    async def _get_log_channel(self, guild_id: int, log_type: LogType) -> Optional[discord.TextChannel]:
        log_config = await self.bot.db.get_log_config(guild_id)
        if not log_config or not log_config.enabled:
            return None
        
        if not log_config.is_type_enabled(log_type):
            return None
        
        channel_id = log_config.get_channel_for_type(log_type)
        if not channel_id:
            return None
        
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return None
        
        return guild.get_channel(channel_id)
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        
        self._message_cache[message.id] = {
            'content': message.content,
            'author_id': message.author.id,
            'channel_id': message.channel.id,
            'attachments': [a.url for a in message.attachments]
        }
        
        if len(self._message_cache) > 10000:
            oldest = list(self._message_cache.keys())[:1000]
            for key in oldest:
                del self._message_cache[key]
    
    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        
        channel = await self._get_log_channel(message.guild.id, LogType.MESSAGE_DELETE)
        if not channel:
            return
        
        embed = EmbedBuilder.log_message_delete(message)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if not after.guild or after.author.bot:
            return
        
        if before.content == after.content:
            return
        
        channel = await self._get_log_channel(after.guild.id, LogType.MESSAGE_EDIT)
        if not channel:
            return
        
        embed = EmbedBuilder.log_message_edit(before, after)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_bulk_message_delete(self, messages: list):
        if not messages:
            return
        
        guild = messages[0].guild
        if not guild:
            return
        
        channel = await self._get_log_channel(guild.id, LogType.MESSAGE_BULK_DELETE)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(
                title="🗑️ Bulk Message Delete",
                description=f"**{len(messages)}** messages were deleted."
            )
            .color(EmbedColor.LOGGING)
            .field("Channel", messages[0].channel.mention, True)
            .build()
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        channel = await self._get_log_channel(member.guild.id, LogType.MEMBER_JOIN)
        if not channel:
            return
        
        embed = EmbedBuilder.log_member_join(member)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        channel = await self._get_log_channel(member.guild.id, LogType.MEMBER_LEAVE)
        if not channel:
            return
        
        embed = EmbedBuilder.log_member_leave(member)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if before.roles == after.roles and before.nick == after.nick:
            return
        
        channel = await self._get_log_channel(after.guild.id, LogType.MEMBER_UPDATE)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(title="👤 Member Updated")
            .color(EmbedColor.LOGGING)
            .field("Member", f"{after.mention} ({after.id})", True)
        )
        
        if before.nick != after.nick:
            embed.field("Nickname", f"`{before.nick}` → `{after.nick}`", False)
        
        if before.roles != after.roles:
            added = [r.mention for r in after.roles if r not in before.roles]
            removed = [r.mention for r in before.roles if r not in after.roles]
            
            if added:
                embed.field("Roles Added", " ".join(added), True)
            if removed:
                embed.field("Roles Removed", " ".join(removed), True)
        
        try:
            await channel.send(embed=embed.build())
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        channel = await self._get_log_channel(guild.id, LogType.MEMBER_BAN)
        if not channel:
            return
        
        reason = "Unknown"
        moderator = None
        
        try:
            async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
                if entry.target.id == user.id:
                    reason = entry.reason or "No reason provided"
                    moderator = entry.user
                    break
        except:
            pass
        
        embed = (
            EmbedBuilder(
                title="🔨 Member Banned",
                description=f"{user.mention} was banned from the server."
            )
            .color(EmbedColor.ERROR)
            .field("User", f"{user} ({user.id})", True)
            .field("Reason", reason, False)
            .thumbnail(user.display_avatar.url)
        )
        
        if moderator:
            embed.field("Moderator", moderator.mention, True)
        
        try:
            await channel.send(embed=embed.build())
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        channel = await self._get_log_channel(guild.id, LogType.MEMBER_UNBAN)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(
                title="🔓 Member Unbanned",
                description=f"{user.mention} was unbanned from the server."
            )
            .color(EmbedColor.SUCCESS)
            .field("User", f"{user} ({user.id})", True)
            .thumbnail(user.display_avatar.url)
            .build()
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        channel = await self._get_log_channel(role.guild.id, LogType.ROLE_CREATE)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(
                title="➕ Role Created",
                description=f"A new role was created: {role.mention}"
            )
            .color(role.color if role.color.value else EmbedColor.SUCCESS.value)
            .field("Name", role.name, True)
            .field("Color", str(role.color), True)
            .field("Position", str(role.position), True)
            .field("Mentionable", "Yes" if role.mentionable else "No", True)
            .field("Hoisted", "Yes" if role.hoist else "No", True)
            .footer(f"Role ID: {role.id}")
            .build()
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        channel = await self._get_log_channel(role.guild.id, LogType.ROLE_DELETE)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(
                title="➖ Role Deleted",
                description=f"A role was deleted: **{role.name}**"
            )
            .color(EmbedColor.ERROR)
            .field("Name", role.name, True)
            .field("Color", str(role.color), True)
            .footer(f"Role ID: {role.id}")
            .build()
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        if before.name == after.name and before.color == after.color and before.permissions == after.permissions:
            return
        
        channel = await self._get_log_channel(after.guild.id, LogType.ROLE_UPDATE)
        if not channel:
            return
        
        embed = EmbedBuilder.log_role_update(before, after)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        log_channel = await self._get_log_channel(channel.guild.id, LogType.CHANNEL_CREATE)
        if not log_channel:
            return
        
        embed = EmbedBuilder.log_channel_create(channel)
        
        try:
            await log_channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        log_channel = await self._get_log_channel(channel.guild.id, LogType.CHANNEL_DELETE)
        if not log_channel:
            return
        
        embed = EmbedBuilder.log_channel_delete(channel)
        
        try:
            await log_channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState
    ):
        if before.channel == after.channel:
            return
        
        if before.channel is None and after.channel is not None:
            log_type = LogType.VOICE_JOIN
            description = f"{member.mention} joined voice channel {after.channel.mention}"
        elif before.channel is not None and after.channel is None:
            log_type = LogType.VOICE_LEAVE
            description = f"{member.mention} left voice channel {before.channel.mention}"
        else:
            log_type = LogType.VOICE_MOVE
            description = f"{member.mention} moved from {before.channel.mention} to {after.channel.mention}"
        
        channel = await self._get_log_channel(member.guild.id, log_type)
        if not channel:
            return
        
        embed = (
            EmbedBuilder(title=f"🔊 Voice {log_type.name.split('_')[1].title()}", description=description)
            .color(EmbedColor.LOGGING)
            .thumbnail(member.display_avatar.url)
            .footer(f"User ID: {member.id}")
            .build()
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.hybrid_group(name="logs", description="Logging configuration")
    @commands.has_permissions(manage_guild=True)
    async def logs(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            log_config = await self.bot.db.get_log_config(ctx.guild.id)
            
            embed = (
                EmbedBuilder(
                    title="📋 Logging Configuration",
                    description="Server logging settings"
                )
                .color(EmbedColor.LOGGING)
                .field("Status", "✅ Enabled" if (log_config and log_config.enabled) else "❌ Disabled", True)
            )
            
            if log_config:
                channels = [
                    ("Message Logs", log_config.message_log_channel),
                    ("Member Logs", log_config.member_log_channel),
                    ("Moderation Logs", log_config.moderation_log_channel),
                    ("Server Logs", log_config.server_log_channel),
                    ("Voice Logs", log_config.voice_log_channel),
                ]
                
                for name, channel_id in channels:
                    embed.field(name, f"<#{channel_id}>" if channel_id else "Not set", True)
            
            await ctx.send(embed=embed.build())
    
    @logs.command(name="enable", description="Enable logging")
    @commands.has_permissions(manage_guild=True)
    async def logs_enable(self, ctx: commands.Context):
        log_config = await self.bot.db.get_or_create_log_config(ctx.guild.id)
        log_config.enabled = True
        await self.bot.db.save_log_config(log_config)
        
        await ctx.send(embed=EmbedBuilder.success("Logging", "Logging has been enabled."))
    
    @logs.command(name="disable", description="Disable logging")
    @commands.has_permissions(manage_guild=True)
    async def logs_disable(self, ctx: commands.Context):
        log_config = await self.bot.db.get_or_create_log_config(ctx.guild.id)
        log_config.enabled = False
        await self.bot.db.save_log_config(log_config)
        
        await ctx.send(embed=EmbedBuilder.success("Logging", "Logging has been disabled."))
    
    @logs.command(name="channel", description="Set a log channel")
    @commands.has_permissions(manage_guild=True)
    async def logs_channel(self, ctx: commands.Context, category: str, channel: discord.TextChannel):
        valid_categories = ['message', 'member', 'moderation', 'server', 'voice', 'automod', 'ticket', 'bot']
        
        if category.lower() not in valid_categories:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Category",
                f"Valid categories: {', '.join(valid_categories)}"
            ))
        
        log_config = await self.bot.db.get_or_create_log_config(ctx.guild.id)
        log_config.set_channel(category.lower(), channel.id)
        await self.bot.db.save_log_config(log_config)
        
        await ctx.send(embed=EmbedBuilder.success(
            "Log Channel Set",
            f"{category.title()} logs will be sent to {channel.mention}"
        ))
    
    @logs.command(name="ignore", description="Ignore a channel or role from logging")
    @commands.has_permissions(manage_guild=True)
    async def logs_ignore(self, ctx: commands.Context, action: str, target_type: str, target: str):
        log_config = await self.bot.db.get_or_create_log_config(ctx.guild.id)
        
        if target_type.lower() == "channel":
            try:
                channel = await commands.TextChannelConverter().convert(ctx, target)
                target_list = log_config.ignored_channels
                target_id = channel.id
                target_name = channel.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Channel not found."))
        
        elif target_type.lower() == "role":
            try:
                role = await commands.RoleConverter().convert(ctx, target)
                target_list = log_config.ignored_roles
                target_id = role.id
                target_name = role.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Role not found."))
        
        elif target_type.lower() == "user":
            try:
                user = await commands.MemberConverter().convert(ctx, target)
                target_list = log_config.ignored_users
                target_id = user.id
                target_name = user.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "User not found."))
        
        else:
            return await ctx.send(embed=EmbedBuilder.error("Invalid Type", "Use `channel`, `role`, or `user`."))
        
        if action.lower() == "add":
            if target_id not in target_list:
                target_list.append(target_id)
                await self.bot.db.save_log_config(log_config)
                await ctx.send(embed=EmbedBuilder.success("Ignored", f"{target_name} will be ignored in logs."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Ignored", "This target is already ignored."))
        
        elif action.lower() == "remove":
            if target_id in target_list:
                target_list.remove(target_id)
                await self.bot.db.save_log_config(log_config)
                await ctx.send(embed=EmbedBuilder.success("Removed", f"{target_name} will no longer be ignored."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Ignored", "This target is not in the ignore list."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))


async def setup(bot: commands.Bot):
    await bot.add_cog(LoggingCog(bot))
