"""
Anti-Raid Cog
Protects the server from raid attacks
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class AntiRaidCog(commands.Cog, name="Anti-Raid"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.join_cache: Dict[int, List[datetime]] = defaultdict(list)
        self.lockdown_guilds: Dict[int, datetime] = {}
        self.cleanup_cache.start()
    
    def cog_unload(self):
        self.cleanup_cache.cancel()
    
    @tasks.loop(minutes=5)
    async def cleanup_cache(self):
        cutoff = datetime.now() - timedelta(minutes=10)
        
        for guild_id in list(self.join_cache.keys()):
            self.join_cache[guild_id] = [
                dt for dt in self.join_cache[guild_id] if dt > cutoff
            ]
            if not self.join_cache[guild_id]:
                del self.join_cache[guild_id]
        
        for guild_id in list(self.lockdown_guilds.keys()):
            if datetime.now() > self.lockdown_guilds[guild_id]:
                await self._end_lockdown(guild_id)
    
    async def _end_lockdown(self, guild_id: int):
        if guild_id not in self.lockdown_guilds:
            return
        
        del self.lockdown_guilds[guild_id]
        
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        
        guild_config = await self.bot.db.get_guild_config(guild_id)
        if guild_config and guild_config.settings.anti_raid.alert_channel:
            channel = guild.get_channel(guild_config.settings.anti_raid.alert_channel)
            if channel:
                embed = (
                    EmbedBuilder(
                        title="🔓 Lockdown Ended",
                        description="The raid lockdown has automatically ended."
                    )
                    .color(EmbedColor.SUCCESS)
                    .build()
                )
                try:
                    await channel.send(embed=embed)
                except:
                    pass
    
    async def _trigger_lockdown(self, guild: discord.Guild, reason: str, affected: int):
        guild_config = await self.bot.db.get_guild_config(guild.id)
        if not guild_config:
            return
        
        settings = guild_config.settings.anti_raid
        
        lockdown_end = datetime.now() + timedelta(seconds=settings.lockdown_duration)
        self.lockdown_guilds[guild.id] = lockdown_end
        
        if settings.alert_channel:
            channel = guild.get_channel(settings.alert_channel)
            if channel:
                embed = EmbedBuilder.anti_raid_alert(
                    guild=guild,
                    action_taken="Server Lockdown",
                    affected_users=affected,
                    reason=reason
                )
                try:
                    await channel.send(embed=embed)
                except:
                    pass
    
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        
        guild_config = await self.bot.db.get_guild_config(member.guild.id)
        if not guild_config:
            return
        
        settings = guild_config.settings.anti_raid
        if not settings.enabled:
            return
        
        if member.guild.id in self.lockdown_guilds:
            try:
                await member.kick(reason="Anti-Raid: Server is in lockdown mode")
            except:
                pass
            return
        
        user_roles = [r.id for r in member.roles]
        if any(role_id in user_roles for role_id in settings.whitelist_roles):
            return
        
        now = datetime.now()
        self.join_cache[member.guild.id].append(now)
        
        cutoff = now - timedelta(seconds=settings.join_interval)
        recent_joins = [dt for dt in self.join_cache[member.guild.id] if dt > cutoff]
        self.join_cache[member.guild.id] = recent_joins
        
        if len(recent_joins) >= settings.join_threshold:
            await self._trigger_lockdown(
                member.guild,
                f"Mass join detected: {len(recent_joins)} joins in {settings.join_interval} seconds",
                len(recent_joins)
            )
            
            if settings.action == "kick_all":
                for join_time in recent_joins:
                    pass
        
        account_age = (now - member.created_at.replace(tzinfo=None)).days
        if account_age < settings.new_account_threshold:
            if settings.auto_ban_new_accounts:
                try:
                    await member.ban(reason=f"Anti-Raid: Account too new ({account_age} days old)")
                except:
                    pass
            elif settings.alert_channel:
                channel = member.guild.get_channel(settings.alert_channel)
                if channel:
                    embed = (
                        EmbedBuilder(
                            title="⚠️ New Account Alert",
                            description=f"{member.mention} joined with a very new account."
                        )
                        .color(EmbedColor.WARNING)
                        .field("Account Age", f"{account_age} days", True)
                        .field("Created", f"<t:{int(member.created_at.timestamp())}:R>", True)
                        .thumbnail(member.display_avatar.url)
                        .build()
                    )
                    try:
                        await channel.send(embed=embed)
                    except:
                        pass
        
        if settings.auto_kick_no_avatar and member.default_avatar == member.display_avatar:
            try:
                await member.kick(reason="Anti-Raid: No avatar (default avatar)")
            except:
                pass
    
    @commands.hybrid_group(name="antiraid", description="Anti-raid configuration")
    @commands.has_permissions(administrator=True)
    async def antiraid(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found.")
            
            settings = guild_config.settings.anti_raid
            
            embed = (
                EmbedBuilder(
                    title="🛡️ Anti-Raid Configuration",
                    description="Current anti-raid protection settings"
                )
                .color(EmbedColor.ANTI_RAID)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Join Threshold", f"{settings.join_threshold} joins", True)
                .field("Interval", f"{settings.join_interval} seconds", True)
                .field("Action", settings.action, True)
                .field("Lockdown Duration", f"{settings.lockdown_duration} seconds", True)
                .field("New Account Threshold", f"{settings.new_account_threshold} days", True)
                .field("Auto-ban New Accounts", "✅" if settings.auto_ban_new_accounts else "❌", True)
                .field("Auto-kick No Avatar", "✅" if settings.auto_kick_no_avatar else "❌", True)
                .build()
            )
            
            if ctx.guild.id in self.lockdown_guilds:
                embed.add_field(
                    name="⚠️ LOCKDOWN ACTIVE",
                    value=f"Ends <t:{int(self.lockdown_guilds[ctx.guild.id].timestamp())}:R>",
                    inline=False
                )
            
            await ctx.send(embed=embed)
    
    @antiraid.command(name="enable", description="Enable anti-raid protection")
    @commands.has_permissions(administrator=True)
    async def antiraid_enable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_raid.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Anti-Raid", "Anti-raid protection has been enabled."))
    
    @antiraid.command(name="disable", description="Disable anti-raid protection")
    @commands.has_permissions(administrator=True)
    async def antiraid_disable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_raid.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Anti-Raid", "Anti-raid protection has been disabled."))
    
    @antiraid.command(name="threshold", description="Set the join threshold for raid detection")
    @commands.has_permissions(administrator=True)
    async def antiraid_threshold(self, ctx: commands.Context, joins: int, seconds: int):
        if joins < 3 or joins > 50:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Join threshold must be between 3 and 50."))
        if seconds < 5 or seconds > 60:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Interval must be between 5 and 60 seconds."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_raid.join_threshold = joins
        guild_config.settings.anti_raid.join_interval = seconds
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success(
            "Threshold Updated",
            f"Raid will be detected if {joins} members join within {seconds} seconds."
        ))
    
    @antiraid.command(name="alertchannel", description="Set the alert channel for raid notifications")
    @commands.has_permissions(administrator=True)
    async def antiraid_alertchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_raid.alert_channel = channel.id
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Alert Channel", f"Raid alerts will be sent to {channel.mention}"))
    
    @antiraid.command(name="lockdown", description="Manually trigger or end a lockdown")
    @commands.has_permissions(administrator=True)
    async def antiraid_lockdown(self, ctx: commands.Context, action: str):
        if action.lower() == "start":
            if ctx.guild.id in self.lockdown_guilds:
                return await ctx.send(embed=EmbedBuilder.warning("Already Active", "Lockdown is already active."))
            
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            duration = guild_config.settings.anti_raid.lockdown_duration if guild_config else 300
            
            self.lockdown_guilds[ctx.guild.id] = datetime.now() + timedelta(seconds=duration)
            
            await ctx.send(embed=EmbedBuilder.success(
                "Lockdown Started",
                f"Server is now in lockdown mode. New members will be kicked. Ends in {duration} seconds."
            ))
        
        elif action.lower() == "end":
            if ctx.guild.id not in self.lockdown_guilds:
                return await ctx.send(embed=EmbedBuilder.warning("Not Active", "Lockdown is not currently active."))
            
            del self.lockdown_guilds[ctx.guild.id]
            
            await ctx.send(embed=EmbedBuilder.success("Lockdown Ended", "Server lockdown has been manually ended."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `start` or `end`."))
    
    @antiraid.command(name="newaccount", description="Set the new account age threshold")
    @commands.has_permissions(administrator=True)
    async def antiraid_newaccount(self, ctx: commands.Context, days: int):
        if days < 0 or days > 365:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Days must be between 0 and 365."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.anti_raid.new_account_threshold = days
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success(
            "Threshold Updated",
            f"Accounts younger than {days} days will be flagged."
        ))


async def setup(bot: commands.Bot):
    await bot.add_cog(AntiRaidCog(bot))
