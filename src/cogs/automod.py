"""
AutoMod Cog
Automatic moderation system for spam, caps, invites, etc.
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import re

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.validators import (
    is_excessive_caps, is_excessive_emojis, contains_invite,
    contains_url, contains_mass_mentions
)


class SpamTracker:
    def __init__(self):
        self.messages: Dict[int, Dict[int, List[datetime]]] = defaultdict(lambda: defaultdict(list))
        self.violations: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    
    def add_message(self, guild_id: int, user_id: int):
        now = datetime.now()
        self.messages[guild_id][user_id].append(now)
    
    def get_message_count(self, guild_id: int, user_id: int, window_seconds: int = 5) -> int:
        cutoff = datetime.now() - timedelta(seconds=window_seconds)
        messages = self.messages[guild_id][user_id]
        return sum(1 for dt in messages if dt > cutoff)
    
    def add_violation(self, guild_id: int, user_id: int):
        self.violations[guild_id][user_id] += 1
    
    def get_violations(self, guild_id: int, user_id: int) -> int:
        return self.violations[guild_id][user_id]
    
    def reset_violations(self, guild_id: int, user_id: int):
        self.violations[guild_id][user_id] = 0
    
    def cleanup(self, max_age_seconds: int = 60):
        cutoff = datetime.now() - timedelta(seconds=max_age_seconds)
        
        for guild_id in list(self.messages.keys()):
            for user_id in list(self.messages[guild_id].keys()):
                self.messages[guild_id][user_id] = [
                    dt for dt in self.messages[guild_id][user_id] if dt > cutoff
                ]
                if not self.messages[guild_id][user_id]:
                    del self.messages[guild_id][user_id]
            if not self.messages[guild_id]:
                del self.messages[guild_id]


class AutoModCog(commands.Cog, name="AutoMod"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.spam_tracker = SpamTracker()
        self.cleanup_task.start()
    
    def cog_unload(self):
        self.cleanup_task.cancel()
    
    @tasks.loop(minutes=1)
    async def cleanup_task(self):
        self.spam_tracker.cleanup()
    
    async def _is_exempt(self, message: discord.Message) -> bool:
        guild_config = await self.bot.db.get_guild_config(message.guild.id)
        if not guild_config:
            return True
        
        settings = guild_config.settings.automod
        
        if message.channel.id in settings.ignored_channels:
            return True
        
        user_roles = [r.id for r in message.author.roles]
        if any(role_id in user_roles for role_id in settings.ignored_roles):
            return True
        
        if message.author.guild_permissions.manage_messages:
            return True
        
        return False
    
    async def _take_action(
        self,
        message: discord.Message,
        violation_type: str,
        settings
    ):
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass
        
        self.spam_tracker.add_violation(message.guild.id, message.author.id)
        violations = self.spam_tracker.get_violations(message.guild.id, message.author.id)
        
        if settings.warn_on_violation:
            user_data = await self.bot.db.get_or_create_user_data(message.author.id, message.guild.id)
            user_data.add_warning()
            await self.bot.db.save_user_data(user_data)
        
        if settings.mute_on_repeated and violations >= settings.mute_threshold:
            if isinstance(message.author, discord.Member):
                try:
                    await message.author.timeout(
                        timedelta(seconds=settings.mute_duration),
                        reason=f"AutoMod: Repeated {violation_type} violations"
                    )
                    self.spam_tracker.reset_violations(message.guild.id, message.author.id)
                except discord.Forbidden:
                    pass
        
        try:
            warning_msg = await message.channel.send(
                f"{message.author.mention}, your message was removed for **{violation_type}**.",
                delete_after=5
            )
        except discord.Forbidden:
            pass
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        
        if not message.content:
            return
        
        guild_config = await self.bot.db.get_guild_config(message.guild.id)
        if not guild_config:
            return
        
        settings = guild_config.settings.automod
        if not settings.enabled:
            return
        
        if await self._is_exempt(message):
            return
        
        if settings.anti_spam:
            self.spam_tracker.add_message(message.guild.id, message.author.id)
            count = self.spam_tracker.get_message_count(
                message.guild.id, message.author.id, settings.spam_interval
            )
            
            if count >= settings.spam_threshold:
                await self._take_action(message, "spam", settings)
                return
        
        if settings.anti_caps:
            if is_excessive_caps(message.content, settings.caps_threshold, settings.caps_min_length):
                await self._take_action(message, "excessive caps", settings)
                return
        
        if settings.anti_invite:
            if contains_invite(message.content):
                await self._take_action(message, "invite link", settings)
                return
        
        if settings.anti_link:
            if contains_url(message.content):
                from src.utils.validators import extract_urls
                urls = extract_urls(message.content)
                
                allowed = False
                for url in urls:
                    for domain in settings.allowed_domains:
                        if domain in url:
                            allowed = True
                            break
                
                if not allowed and urls:
                    await self._take_action(message, "unauthorized link", settings)
                    return
        
        if settings.anti_emoji_spam:
            if is_excessive_emojis(message.content, settings.emoji_threshold):
                await self._take_action(message, "emoji spam", settings)
                return
        
        if settings.anti_mention_spam:
            if contains_mass_mentions(message.content, settings.mention_threshold):
                await self._take_action(message, "mass mentions", settings)
                return
        
        if settings.anti_newline_spam:
            newlines = message.content.count('\n')
            if newlines > settings.newline_threshold:
                await self._take_action(message, "newline spam", settings)
                return
    
    @commands.hybrid_group(name="automod", description="AutoMod configuration")
    @commands.has_permissions(manage_messages=True)
    async def automod(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found.")
            
            settings = guild_config.settings.automod
            
            embed = (
                EmbedBuilder(
                    title="🤖 AutoMod Configuration",
                    description="Automatic moderation settings"
                )
                .color(EmbedColor.INFO)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Anti-Spam", "✅" if settings.anti_spam else "❌", True)
                .field("Anti-Caps", "✅" if settings.anti_caps else "❌", True)
                .field("Anti-Invite", "✅" if settings.anti_invite else "❌", True)
                .field("Anti-Link", "✅" if settings.anti_link else "❌", True)
                .field("Anti-Emoji Spam", "✅" if settings.anti_emoji_spam else "❌", True)
                .field("Anti-Mention Spam", "✅" if settings.anti_mention_spam else "❌", True)
                .field("Warn on Violation", "✅" if settings.warn_on_violation else "❌", True)
                .field("Auto-Mute", "✅" if settings.mute_on_repeated else "❌", True)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @automod.command(name="enable", description="Enable AutoMod")
    @commands.has_permissions(manage_messages=True)
    async def automod_enable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.automod.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("AutoMod", "AutoMod has been enabled."))
    
    @automod.command(name="disable", description="Disable AutoMod")
    @commands.has_permissions(manage_messages=True)
    async def automod_disable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.automod.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("AutoMod", "AutoMod has been disabled."))
    
    @automod.command(name="toggle", description="Toggle an AutoMod feature")
    @commands.has_permissions(manage_messages=True)
    async def automod_toggle(self, ctx: commands.Context, feature: str, enabled: bool):
        feature_map = {
            'spam': 'anti_spam',
            'caps': 'anti_caps',
            'invite': 'anti_invite',
            'link': 'anti_link',
            'emoji': 'anti_emoji_spam',
            'mention': 'anti_mention_spam',
            'newline': 'anti_newline_spam',
            'warn': 'warn_on_violation',
            'mute': 'mute_on_repeated'
        }
        
        if feature.lower() not in feature_map:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Feature",
                f"Valid features: {', '.join(feature_map.keys())}"
            ))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        setattr(guild_config.settings.automod, feature_map[feature.lower()], enabled)
        await self.bot.db.save_guild_config(guild_config)
        
        status = "enabled" if enabled else "disabled"
        await ctx.send(embed=EmbedBuilder.success("AutoMod", f"Anti-{feature} has been {status}."))
    
    @automod.command(name="threshold", description="Set AutoMod thresholds")
    @commands.has_permissions(manage_messages=True)
    async def automod_threshold(self, ctx: commands.Context, setting: str, value: int):
        threshold_map = {
            'spam': 'spam_threshold',
            'spam_interval': 'spam_interval',
            'caps': 'caps_min_length',
            'emoji': 'emoji_threshold',
            'mention': 'mention_threshold',
            'newline': 'newline_threshold',
            'mute_threshold': 'mute_threshold',
            'mute_duration': 'mute_duration'
        }
        
        if setting.lower() not in threshold_map:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Setting",
                f"Valid settings: {', '.join(threshold_map.keys())}"
            ))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        setattr(guild_config.settings.automod, threshold_map[setting.lower()], value)
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Threshold Updated", f"{setting} set to {value}"))
    
    @automod.command(name="ignore", description="Ignore a channel or role from AutoMod")
    @commands.has_permissions(manage_messages=True)
    async def automod_ignore(self, ctx: commands.Context, action: str, target_type: str, target: str):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if target_type.lower() == "channel":
            try:
                channel = await commands.TextChannelConverter().convert(ctx, target)
                target_list = guild_config.settings.automod.ignored_channels
                target_id = channel.id
                target_name = channel.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Channel not found."))
        
        elif target_type.lower() == "role":
            try:
                role = await commands.RoleConverter().convert(ctx, target)
                target_list = guild_config.settings.automod.ignored_roles
                target_id = role.id
                target_name = role.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Role not found."))
        
        else:
            return await ctx.send(embed=EmbedBuilder.error("Invalid Type", "Use `channel` or `role`."))
        
        if action.lower() == "add":
            if target_id not in target_list:
                target_list.append(target_id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Ignored", f"{target_name} will be ignored by AutoMod."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Ignored", "This target is already ignored."))
        
        elif action.lower() == "remove":
            if target_id in target_list:
                target_list.remove(target_id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Removed", f"{target_name} will no longer be ignored."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Ignored", "This target is not in the ignore list."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))


async def setup(bot: commands.Bot):
    await bot.add_cog(AutoModCog(bot))
