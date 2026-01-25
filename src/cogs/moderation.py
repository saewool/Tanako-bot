"""
Moderation Cog
Handles all moderation commands and actions
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, Union
from datetime import datetime, timedelta
import asyncio

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.helpers import parse_duration, format_duration
from src.utils.permissions import require_permission, PermissionLevel
from src.models.moderation import ModerationCase, ModerationAction, Warning


class ModerationCog(commands.Cog, name="Moderation"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    async def _create_case(
        self,
        guild_id: int,
        target_id: int,
        moderator_id: int,
        action: ModerationAction,
        reason: str,
        duration_seconds: Optional[int] = None,
        auto_mod: bool = False
    ) -> ModerationCase:
        guild_config = await self.bot.db.get_or_create_guild_config(guild_id)
        case_id = guild_config.get_next_case_id()
        
        expires_at = None
        if duration_seconds:
            expires_at = datetime.now() + timedelta(seconds=duration_seconds)
        
        case = ModerationCase(
            case_id=case_id,
            guild_id=guild_id,
            target_id=target_id,
            moderator_id=moderator_id,
            action=action,
            reason=reason,
            duration_seconds=duration_seconds,
            expires_at=expires_at,
            auto_mod=auto_mod
        )
        
        await self.bot.db.save_moderation_case(case)
        await self.bot.db.save_guild_config(guild_config)
        
        return case
    
    async def _dm_user(
        self,
        user: Union[discord.Member, discord.User],
        guild: discord.Guild,
        action: ModerationAction,
        reason: str,
        duration: Optional[str] = None,
        case_id: Optional[int] = None
    ) -> bool:
        try:
            embed = (
                EmbedBuilder(
                    title=f"{action.emoji} You have been {action.past_tense}",
                    description=f"You have been {action.past_tense} in **{guild.name}**."
                )
                .color(EmbedColor.MODERATION)
                .field("Reason", reason, False)
            )
            
            if duration:
                embed.field("Duration", duration, True)
            
            if case_id:
                embed.field("Case ID", f"#{case_id}", True)
            
            await user.send(embed=embed.build())
            return True
        except discord.Forbidden:
            return False
    
    async def _log_moderation(
        self,
        guild: discord.Guild,
        case: ModerationCase,
        moderator: discord.Member,
        target: Union[discord.Member, discord.User]
    ):
        guild_config = await self.bot.db.get_guild_config(guild.id)
        if not guild_config:
            return
        
        log_channel_id = guild_config.settings.logging.moderation_log_channel
        if not log_channel_id:
            return
        
        channel = guild.get_channel(log_channel_id)
        if not channel or not isinstance(channel, discord.TextChannel):
            return
        
        duration_str = None
        if case.duration_seconds:
            duration_str = format_duration(timedelta(seconds=case.duration_seconds))
        
        embed = EmbedBuilder.moderation(
            action=case.action.value.upper(),
            moderator=moderator,
            target=target,
            reason=case.reason,
            duration=duration_str,
            case_id=case.case_id
        )
        
        try:
            msg = await channel.send(embed=embed)
            case.message_id = msg.id
            case.channel_id = channel.id
            await self.bot.db.save_moderation_case(case)
        except discord.Forbidden:
            pass
    
    @commands.hybrid_command(name="warn", description="Warn a member")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(
        member="The member to warn",
        reason="Reason for the warning"
    )
    async def warn(
        self,
        ctx: commands.Context,
        member: discord.Member,
        *,
        reason: str = "No reason provided"
    ):
        if member.id == ctx.author.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot warn yourself."))
        
        if member.id == self.bot.user.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot warn myself."))
        
        if member.top_role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot warn someone with a higher or equal role."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=member.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.WARN,
            reason=reason
        )
        
        dm_sent = await self._dm_user(member, ctx.guild, ModerationAction.WARN, reason, case_id=case.case_id)
        case.dm_sent = dm_sent
        case.dm_failed = not dm_sent
        await self.bot.db.save_moderation_case(case)
        
        await self._log_moderation(ctx.guild, case, ctx.author, member)
        
        user_data = await self.bot.db.get_or_create_user_data(member.id, ctx.guild.id)
        user_data.add_warning()
        await self.bot.db.save_user_data(user_data)
        
        embed = (
            EmbedBuilder(
                title="⚠️ Member Warned",
                description=f"{member.mention} has been warned."
            )
            .color(EmbedColor.WARNING)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .field("DM Sent", "✅" if dm_sent else "❌", True)
            .field("Total Warnings", str(user_data.active_warnings), True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="kick", description="Kick a member from the server")
    @commands.has_permissions(kick_members=True)
    @commands.bot_has_permissions(kick_members=True)
    @app_commands.describe(
        member="The member to kick",
        reason="Reason for the kick"
    )
    async def kick(
        self,
        ctx: commands.Context,
        member: discord.Member,
        *,
        reason: str = "No reason provided"
    ):
        if member.id == ctx.author.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot kick yourself."))
        
        if member.id == self.bot.user.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot kick myself."))
        
        if member.top_role >= ctx.guild.me.top_role:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot kick someone with a higher or equal role than mine."))
        
        if member.top_role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot kick someone with a higher or equal role."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=member.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.KICK,
            reason=reason
        )
        
        dm_sent = await self._dm_user(member, ctx.guild, ModerationAction.KICK, reason, case_id=case.case_id)
        case.dm_sent = dm_sent
        case.dm_failed = not dm_sent
        
        try:
            await member.kick(reason=f"[Case #{case.case_id}] {reason}")
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to kick this member."))
        
        await self.bot.db.save_moderation_case(case)
        await self._log_moderation(ctx.guild, case, ctx.author, member)
        
        embed = (
            EmbedBuilder(
                title="👢 Member Kicked",
                description=f"{member.mention} has been kicked from the server."
            )
            .color(EmbedColor.MODERATION)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .field("DM Sent", "✅" if dm_sent else "❌", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="ban", description="Ban a member from the server")
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    @app_commands.describe(
        user="The user to ban (can be ID for non-members)",
        reason="Reason for the ban",
        delete_days="Number of days of messages to delete (0-7)"
    )
    async def ban(
        self,
        ctx: commands.Context,
        user: Union[discord.Member, discord.User],
        delete_days: int = 0,
        *,
        reason: str = "No reason provided"
    ):
        if user.id == ctx.author.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot ban yourself."))
        
        if user.id == self.bot.user.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot ban myself."))
        
        delete_days = max(0, min(7, delete_days))
        
        if isinstance(user, discord.Member):
            if user.top_role >= ctx.guild.me.top_role:
                return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot ban someone with a higher or equal role than mine."))
            
            if user.top_role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
                return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot ban someone with a higher or equal role."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=user.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.BAN,
            reason=reason
        )
        
        dm_sent = await self._dm_user(user, ctx.guild, ModerationAction.BAN, reason, case_id=case.case_id)
        case.dm_sent = dm_sent
        case.dm_failed = not dm_sent
        
        try:
            await ctx.guild.ban(user, reason=f"[Case #{case.case_id}] {reason}", delete_message_days=delete_days)
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to ban this user."))
        except discord.NotFound:
            return await ctx.send(embed=EmbedBuilder.error("Error", "User not found."))
        
        await self.bot.db.save_moderation_case(case)
        await self._log_moderation(ctx.guild, case, ctx.author, user)
        
        embed = (
            EmbedBuilder(
                title="🔨 Member Banned",
                description=f"{user.mention} has been banned from the server."
            )
            .color(EmbedColor.MODERATION)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .field("Messages Deleted", f"{delete_days} days", True)
            .field("DM Sent", "✅" if dm_sent else "❌", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="unban", description="Unban a user from the server")
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    @app_commands.describe(
        user_id="The ID of the user to unban",
        reason="Reason for the unban"
    )
    async def unban(
        self,
        ctx: commands.Context,
        user_id: str,
        *,
        reason: str = "No reason provided"
    ):
        try:
            user_id_int = int(user_id)
        except ValueError:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Invalid user ID."))
        
        try:
            user = await self.bot.fetch_user(user_id_int)
        except discord.NotFound:
            return await ctx.send(embed=EmbedBuilder.error("Error", "User not found."))
        
        try:
            await ctx.guild.fetch_ban(user)
        except discord.NotFound:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This user is not banned."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=user.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.UNBAN,
            reason=reason
        )
        
        try:
            await ctx.guild.unban(user, reason=f"[Case #{case.case_id}] {reason}")
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to unban this user."))
        
        await self._log_moderation(ctx.guild, case, ctx.author, user)
        
        embed = (
            EmbedBuilder(
                title="🔓 Member Unbanned",
                description=f"{user.mention} has been unbanned."
            )
            .color(EmbedColor.SUCCESS)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="timeout", description="Timeout a member")
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    @app_commands.describe(
        member="The member to timeout",
        duration="Duration of the timeout (e.g., 1h, 30m, 1d)",
        reason="Reason for the timeout"
    )
    async def timeout(
        self,
        ctx: commands.Context,
        member: discord.Member,
        duration: str,
        *,
        reason: str = "No reason provided"
    ):
        if member.id == ctx.author.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot timeout yourself."))
        
        if member.id == self.bot.user.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot timeout myself."))
        
        if member.top_role >= ctx.guild.me.top_role:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot timeout someone with a higher or equal role than mine."))
        
        if member.top_role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot timeout someone with a higher or equal role."))
        
        duration_delta = parse_duration(duration)
        if not duration_delta:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Invalid duration format. Use formats like: 1h, 30m, 1d"))
        
        max_timeout = timedelta(days=28)
        if duration_delta > max_timeout:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Timeout duration cannot exceed 28 days."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=member.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.TIMEOUT,
            reason=reason,
            duration_seconds=int(duration_delta.total_seconds())
        )
        
        duration_str = format_duration(duration_delta)
        dm_sent = await self._dm_user(member, ctx.guild, ModerationAction.TIMEOUT, reason, duration_str, case.case_id)
        case.dm_sent = dm_sent
        case.dm_failed = not dm_sent
        
        try:
            await member.timeout(duration_delta, reason=f"[Case #{case.case_id}] {reason}")
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to timeout this member."))
        
        await self.bot.db.save_moderation_case(case)
        await self._log_moderation(ctx.guild, case, ctx.author, member)
        
        embed = (
            EmbedBuilder(
                title="⏱️ Member Timed Out",
                description=f"{member.mention} has been timed out."
            )
            .color(EmbedColor.MODERATION)
            .field("Duration", duration_str, True)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .field("DM Sent", "✅" if dm_sent else "❌", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="untimeout", description="Remove timeout from a member")
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    @app_commands.describe(
        member="The member to remove timeout from",
        reason="Reason for removing the timeout"
    )
    async def untimeout(
        self,
        ctx: commands.Context,
        member: discord.Member,
        *,
        reason: str = "No reason provided"
    ):
        if not member.is_timed_out():
            return await ctx.send(embed=EmbedBuilder.error("Error", "This member is not timed out."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=member.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.UNTIMEOUT,
            reason=reason
        )
        
        try:
            await member.timeout(None, reason=f"[Case #{case.case_id}] {reason}")
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to remove timeout from this member."))
        
        await self._log_moderation(ctx.guild, case, ctx.author, member)
        
        embed = (
            EmbedBuilder(
                title="⏰ Timeout Removed",
                description=f"Timeout has been removed from {member.mention}."
            )
            .color(EmbedColor.SUCCESS)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="softban", description="Ban and immediately unban to delete messages")
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    @app_commands.describe(
        member="The member to softban",
        reason="Reason for the softban"
    )
    async def softban(
        self,
        ctx: commands.Context,
        member: discord.Member,
        *,
        reason: str = "No reason provided"
    ):
        if member.id == ctx.author.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot softban yourself."))
        
        if member.id == self.bot.user.id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot softban myself."))
        
        if member.top_role >= ctx.guild.me.top_role:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot softban someone with a higher or equal role than mine."))
        
        if member.top_role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot softban someone with a higher or equal role."))
        
        case = await self._create_case(
            guild_id=ctx.guild.id,
            target_id=member.id,
            moderator_id=ctx.author.id,
            action=ModerationAction.SOFTBAN,
            reason=reason
        )
        
        dm_sent = await self._dm_user(member, ctx.guild, ModerationAction.SOFTBAN, reason, case_id=case.case_id)
        case.dm_sent = dm_sent
        case.dm_failed = not dm_sent
        
        try:
            await ctx.guild.ban(member, reason=f"[Case #{case.case_id}] Softban: {reason}", delete_message_days=7)
            await ctx.guild.unban(member, reason="Softban unban")
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to softban this member."))
        
        await self.bot.db.save_moderation_case(case)
        await self._log_moderation(ctx.guild, case, ctx.author, member)
        
        embed = (
            EmbedBuilder(
                title="🧹 Member Softbanned",
                description=f"{member.mention} has been softbanned (kicked + messages deleted)."
            )
            .color(EmbedColor.MODERATION)
            .field("Reason", reason, False)
            .field("Case ID", f"#{case.case_id}", True)
            .field("DM Sent", "✅" if dm_sent else "❌", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="purge", description="Delete messages in bulk")
    @commands.has_permissions(manage_messages=True)
    @commands.bot_has_permissions(manage_messages=True)
    @app_commands.describe(
        amount="Number of messages to delete (1-1000)",
        user="Only delete messages from this user"
    )
    async def purge(
        self,
        ctx: commands.Context,
        amount: int,
        user: Optional[discord.Member] = None
    ):
        if amount < 1 or amount > 1000:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Amount must be between 1 and 1000."))
        
        await ctx.defer()
        
        def check(message):
            if user:
                return message.author.id == user.id
            return True
        
        try:
            deleted = await ctx.channel.purge(limit=amount + 1, check=check, bulk=True)
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to delete messages."))
        except discord.HTTPException as e:
            return await ctx.send(embed=EmbedBuilder.error("Error", f"Failed to delete messages: {e}"))
        
        deleted_count = len(deleted) - 1
        
        embed = (
            EmbedBuilder(
                title="🗑️ Messages Purged",
                description=f"Successfully deleted **{deleted_count}** messages."
            )
            .color(EmbedColor.SUCCESS)
        )
        
        if user:
            embed.field("From User", user.mention, True)
        
        msg = await ctx.send(embed=embed.build())
        await asyncio.sleep(5)
        try:
            await msg.delete()
        except:
            pass
    
    @commands.hybrid_command(name="case", description="View a moderation case")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(case_id="The case ID to view")
    async def case(self, ctx: commands.Context, case_id: int):
        mod_case = await self.bot.db.get_moderation_case(ctx.guild.id, case_id)
        
        if not mod_case:
            return await ctx.send(embed=EmbedBuilder.error("Error", f"Case #{case_id} not found."))
        
        try:
            target = await self.bot.fetch_user(mod_case.target_id)
            target_str = f"{target.mention} ({target.id})"
        except:
            target_str = f"<@{mod_case.target_id}> ({mod_case.target_id})"
        
        try:
            moderator = await self.bot.fetch_user(mod_case.moderator_id)
            mod_str = f"{moderator.mention} ({moderator.id})"
        except:
            mod_str = f"<@{mod_case.moderator_id}> ({mod_case.moderator_id})"
        
        embed = (
            EmbedBuilder(
                title=f"Case #{mod_case.case_id}",
                description=f"{mod_case.action.emoji} **{mod_case.action.value.upper()}**"
            )
            .color(EmbedColor.INFO)
            .field("Target", target_str, True)
            .field("Moderator", mod_str, True)
            .field("Reason", mod_case.reason, False)
            .field("Date", f"<t:{int(mod_case.created_at.timestamp())}:F>", True)
        )
        
        if mod_case.duration_seconds:
            embed.field("Duration", format_duration(timedelta(seconds=mod_case.duration_seconds)), True)
        
        if mod_case.expires_at:
            embed.field("Expires", f"<t:{int(mod_case.expires_at.timestamp())}:R>", True)
        
        status = "Active" if mod_case.is_active else "Expired/Revoked"
        embed.field("Status", status, True)
        
        if mod_case.revoked:
            embed.field("Revoked By", f"<@{mod_case.revoked_by}>", True)
            embed.field("Revoke Reason", mod_case.revoke_reason or "No reason", False)
        
        await ctx.send(embed=embed.build())
    
    @commands.hybrid_command(name="cases", description="View moderation cases for a user")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(user="The user to view cases for")
    async def cases(self, ctx: commands.Context, user: discord.User):
        cases = await self.bot.db.get_user_cases(ctx.guild.id, user.id)
        
        if not cases:
            return await ctx.send(embed=EmbedBuilder.info("No Cases", f"{user.mention} has no moderation cases."))
        
        embed = (
            EmbedBuilder(
                title=f"📋 Cases for {user.display_name}",
                description=f"Found **{len(cases)}** case(s)"
            )
            .color(EmbedColor.INFO)
            .thumbnail(user.display_avatar.url)
        )
        
        for case in cases[:10]:
            status = "✅" if case.is_active else "❌"
            embed.field(
                f"{status} Case #{case.case_id} - {case.action.value.upper()}",
                f"{case.reason[:50]}...\n<t:{int(case.created_at.timestamp())}:R>",
                False
            )
        
        if len(cases) > 10:
            embed.footer(f"Showing 10 of {len(cases)} cases")
        
        await ctx.send(embed=embed.build())
    
    @commands.hybrid_command(name="warnings", description="View warnings for a user")
    @commands.has_permissions(manage_messages=True)
    @app_commands.describe(user="The user to view warnings for")
    async def warnings(self, ctx: commands.Context, user: discord.Member):
        user_data = await self.bot.db.get_user_data(user.id, ctx.guild.id)
        
        if not user_data or user_data.active_warnings == 0:
            return await ctx.send(embed=EmbedBuilder.info("No Warnings", f"{user.mention} has no active warnings."))
        
        embed = (
            EmbedBuilder(
                title=f"⚠️ Warnings for {user.display_name}",
                description=f"Active warnings: **{user_data.active_warnings}**\nTotal warnings: **{user_data.total_warnings}**"
            )
            .color(EmbedColor.WARNING)
            .thumbnail(user.display_avatar.url)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="clearwarnings", description="Clear all warnings for a user")
    @commands.has_permissions(administrator=True)
    @app_commands.describe(
        user="The user to clear warnings for",
        reason="Reason for clearing warnings"
    )
    async def clearwarnings(
        self,
        ctx: commands.Context,
        user: discord.Member,
        *,
        reason: str = "No reason provided"
    ):
        user_data = await self.bot.db.get_user_data(user.id, ctx.guild.id)
        
        if not user_data or user_data.active_warnings == 0:
            return await ctx.send(embed=EmbedBuilder.info("No Warnings", f"{user.mention} has no active warnings."))
        
        cleared = user_data.active_warnings
        user_data.active_warnings = 0
        user_data.warning_points = 0
        await self.bot.db.save_user_data(user_data)
        
        embed = (
            EmbedBuilder(
                title="✅ Warnings Cleared",
                description=f"Cleared **{cleared}** warning(s) for {user.mention}."
            )
            .color(EmbedColor.SUCCESS)
            .field("Reason", reason, False)
            .field("Cleared By", ctx.author.mention, True)
            .build()
        )
        
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(ModerationCog(bot))
