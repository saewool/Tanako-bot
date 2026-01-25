"""
Anti-Nuke Cog
Protects the server from nuke attacks (mass bans, channel deletions, etc.)
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class ActionTracker:
    def __init__(self):
        self.bans: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.kicks: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.channel_deletes: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.channel_creates: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.role_deletes: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.role_creates: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.webhook_creates: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
    
    def add_action(self, action_type: str, guild_id: int, user_id: int):
        now = datetime.now()
        tracker = getattr(self, action_type, None)
        if tracker is not None:
            tracker[guild_id][user_id].append(now)
    
    def get_action_count(self, action_type: str, guild_id: int, user_id: int, window_seconds: int = 60) -> int:
        tracker = getattr(self, action_type, None)
        if tracker is None:
            return 0
        
        cutoff = datetime.now() - timedelta(seconds=window_seconds)
        actions = tracker[guild_id][user_id]
        return sum(1 for dt in actions if dt > cutoff)
    
    def cleanup(self, max_age_seconds: int = 300):
        cutoff = datetime.now() - timedelta(seconds=max_age_seconds)
        
        for attr in ['bans', 'kicks', 'channel_deletes', 'channel_creates', 
                     'role_deletes', 'role_creates', 'webhook_creates']:
            tracker = getattr(self, attr)
            for guild_id in list(tracker.keys()):
                for user_id in list(tracker[guild_id].keys()):
                    tracker[guild_id][user_id] = [
                        dt for dt in tracker[guild_id][user_id] if dt > cutoff
                    ]
                    if not tracker[guild_id][user_id]:
                        del tracker[guild_id][user_id]
                if not tracker[guild_id]:
                    del tracker[guild_id]


class AntiNukeCog(commands.Cog, name="Anti-Nuke"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.tracker = ActionTracker()
        self.cleanup_tracker.start()
    
    def cog_unload(self):
        self.cleanup_tracker.cancel()
    
    @tasks.loop(minutes=5)
    async def cleanup_tracker(self):
        self.tracker.cleanup()
    
    async def _check_and_punish(
        self,
        guild: discord.Guild,
        user_id: int,
        action_type: str,
        threshold_attr: str
    ) -> bool:
        guild_config = await self.bot.db.get_guild_config(guild.id)
        if not guild_config:
            return False
        
        settings = guild_config.settings.anti_nuke
        if not settings.enabled:
            return False
        
        if user_id in settings.trusted_users:
            return False
        
        threshold = getattr(settings, threshold_attr, 5)
        count = self.tracker.get_action_count(action_type, guild.id, user_id, 60)
        
        if count >= threshold:
            member = guild.get_member(user_id)
            if not member:
                return False
            
            if member.id == guild.owner_id:
                return False
            
            if member.top_role >= guild.me.top_role:
                return False
            
            await self._punish_user(guild, member, settings.action, action_type, count)
            await self._send_alert(guild, member, action_type, count, settings.alert_channel)
            return True
        
        return False
    
    async def _punish_user(self, guild: discord.Guild, member: discord.Member, action: str, violation: str, count: int):
        reason = f"Anti-Nuke: {violation} abuse ({count} actions in 60 seconds)"
        
        if action == "ban":
            try:
                await member.ban(reason=reason)
            except:
                pass
        elif action == "kick":
            try:
                await member.kick(reason=reason)
            except:
                pass
        elif action == "strip_roles":
            try:
                roles_to_remove = [r for r in member.roles if r != guild.default_role and r < guild.me.top_role]
                if roles_to_remove:
                    await member.remove_roles(*roles_to_remove, reason=reason)
            except:
                pass
        elif action == "timeout":
            try:
                await member.timeout(timedelta(hours=24), reason=reason)
            except:
                pass
    
    async def _send_alert(
        self,
        guild: discord.Guild,
        perpetrator: discord.Member,
        action_type: str,
        count: int,
        alert_channel_id: Optional[int]
    ):
        if not alert_channel_id:
            return
        
        channel = guild.get_channel(alert_channel_id)
        if not channel:
            return
        
        action_names = {
            'bans': 'Mass Ban',
            'kicks': 'Mass Kick',
            'channel_deletes': 'Mass Channel Delete',
            'channel_creates': 'Mass Channel Create',
            'role_deletes': 'Mass Role Delete',
            'role_creates': 'Mass Role Create',
            'webhook_creates': 'Mass Webhook Create'
        }
        
        embed = EmbedBuilder.anti_nuke_alert(
            guild=guild,
            action_taken="Roles stripped / Action taken",
            perpetrator=perpetrator,
            reason=f"{action_names.get(action_type, action_type)}: {count} actions in 60 seconds"
        )
        
        try:
            await channel.send(embed=embed)
        except:
            pass
    
    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.ban):
            if entry.target.id == user.id:
                self.tracker.add_action('bans', guild.id, entry.user.id)
                await self._check_and_punish(guild, entry.user.id, 'bans', 'max_bans_per_minute')
                break
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        async for entry in member.guild.audit_logs(limit=1, action=discord.AuditLogAction.kick):
            if entry.target.id == member.id:
                time_diff = datetime.now(entry.created_at.tzinfo) - entry.created_at
                if time_diff.total_seconds() < 5:
                    self.tracker.add_action('kicks', member.guild.id, entry.user.id)
                    await self._check_and_punish(member.guild, entry.user.id, 'kicks', 'max_kicks_per_minute')
                break
    
    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
            if entry.target.id == channel.id:
                self.tracker.add_action('channel_deletes', channel.guild.id, entry.user.id)
                await self._check_and_punish(channel.guild, entry.user.id, 'channel_deletes', 'max_channel_deletes_per_minute')
                break
    
    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
            if entry.target.id == channel.id:
                self.tracker.add_action('channel_creates', channel.guild.id, entry.user.id)
                await self._check_and_punish(channel.guild, entry.user.id, 'channel_creates', 'max_channel_creates_per_minute')
                break
    
    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
            self.tracker.add_action('role_deletes', role.guild.id, entry.user.id)
            await self._check_and_punish(role.guild, entry.user.id, 'role_deletes', 'max_role_deletes_per_minute')
            break
    
    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
            if entry.target.id == role.id:
                self.tracker.add_action('role_creates', role.guild.id, entry.user.id)
                await self._check_and_punish(role.guild, entry.user.id, 'role_creates', 'max_role_creates_per_minute')
                break
    
    @commands.Cog.listener()
    async def on_webhooks_update(self, channel: discord.TextChannel):
        async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.webhook_create):
            time_diff = datetime.now(entry.created_at.tzinfo) - entry.created_at
            if time_diff.total_seconds() < 5:
                self.tracker.add_action('webhook_creates', channel.guild.id, entry.user.id)
                await self._check_and_punish(channel.guild, entry.user.id, 'webhook_creates', 'max_webhook_creates_per_minute')
            break
    
    @commands.hybrid_group(name="antinuke", description="Anti-nuke configuration")
    @commands.has_permissions(administrator=True)
    async def antinuke(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found.")
            
            settings = guild_config.settings.anti_nuke
            
            embed = (
                EmbedBuilder(
                    title="🛡️ Anti-Nuke Configuration",
                    description="Current anti-nuke protection settings"
                )
                .color(EmbedColor.ANTI_NUKE)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Action", settings.action, True)
                .field("Trusted Users", str(len(settings.trusted_users)), True)
                .field("Max Bans/min", str(settings.max_bans_per_minute), True)
                .field("Max Kicks/min", str(settings.max_kicks_per_minute), True)
                .field("Max Channel Deletes/min", str(settings.max_channel_deletes_per_minute), True)
                .field("Max Role Deletes/min", str(settings.max_role_deletes_per_minute), True)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @antinuke.command(name="enable", description="Enable anti-nuke protection")
    @commands.has_permissions(administrator=True)
    async def antinuke_enable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_nuke.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Anti-Nuke", "Anti-nuke protection has been enabled."))
    
    @antinuke.command(name="disable", description="Disable anti-nuke protection")
    @commands.has_permissions(administrator=True)
    async def antinuke_disable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_nuke.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Anti-Nuke", "Anti-nuke protection has been disabled."))
    
    @antinuke.command(name="action", description="Set the action to take on nuke detection")
    @commands.has_permissions(administrator=True)
    async def antinuke_action(self, ctx: commands.Context, action: str):
        valid_actions = ['ban', 'kick', 'strip_roles', 'timeout']
        if action.lower() not in valid_actions:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Action",
                f"Valid actions: {', '.join(valid_actions)}"
            ))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_nuke.action = action.lower()
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Action Updated", f"Anti-nuke action set to: {action}"))
    
    @antinuke.command(name="trust", description="Add or remove a trusted user")
    @commands.has_permissions(administrator=True)
    async def antinuke_trust(self, ctx: commands.Context, action: str, user: discord.Member):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if action.lower() == "add":
            if user.id not in guild_config.settings.anti_nuke.trusted_users:
                guild_config.settings.anti_nuke.trusted_users.append(user.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Trusted User", f"{user.mention} is now a trusted user."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Trusted", "This user is already trusted."))
        
        elif action.lower() == "remove":
            if user.id in guild_config.settings.anti_nuke.trusted_users:
                guild_config.settings.anti_nuke.trusted_users.remove(user.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Trusted User", f"{user.mention} is no longer a trusted user."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Trusted", "This user is not in the trusted list."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
    
    @antinuke.command(name="alertchannel", description="Set the alert channel for nuke notifications")
    @commands.has_permissions(administrator=True)
    async def antinuke_alertchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_nuke.alert_channel = channel.id
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Alert Channel", f"Nuke alerts will be sent to {channel.mention}"))
    
    @antinuke.command(name="threshold", description="Set action thresholds")
    @commands.has_permissions(administrator=True)
    async def antinuke_threshold(self, ctx: commands.Context, action_type: str, limit: int):
        valid_types = {
            'bans': 'max_bans_per_minute',
            'kicks': 'max_kicks_per_minute',
            'channel_deletes': 'max_channel_deletes_per_minute',
            'channel_creates': 'max_channel_creates_per_minute',
            'role_deletes': 'max_role_deletes_per_minute',
            'role_creates': 'max_role_creates_per_minute',
            'webhooks': 'max_webhook_creates_per_minute'
        }
        
        if action_type.lower() not in valid_types:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Type",
                f"Valid types: {', '.join(valid_types.keys())}"
            ))
        
        if limit < 1 or limit > 50:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Limit must be between 1 and 50."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        setattr(guild_config.settings.anti_nuke, valid_types[action_type.lower()], limit)
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Threshold Updated", f"{action_type} threshold set to {limit} per minute."))


async def setup(bot: commands.Bot):
    await bot.add_cog(AntiNukeCog(bot))
