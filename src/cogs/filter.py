"""
Word Filter Cog
Advanced word filtering with bypass detection
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, List
import re

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.helpers import generate_id
from src.models.filter import FilterRule, FilterType, FilterAction, FilterConfig


class FilterCog(commands.Cog, name="Filter"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return
        if message.author.bot:
            return
        if not message.content:
            return
        
        filter_config = await self.bot.db.get_filter_config(message.guild.id)
        if not filter_config or not filter_config.enabled:
            return
        
        user_roles = [r.id for r in message.author.roles]
        
        matches = filter_config.check_content(
            message.content,
            message.author.id,
            message.channel.id,
            user_roles
        )
        
        if not matches:
            return
        
        rule, matched_content = matches[0]
        
        rule.record_match()
        await self.bot.db.save_filter_config(filter_config)
        
        strikes = filter_config.add_strike(message.author.id, rule.id)
        
        action = rule.action
        if strikes >= rule.strikes_before_action and rule.secondary_action:
            action = rule.secondary_action
        
        await self._execute_action(message, rule, action, matched_content)
        
        if rule.log_matches:
            await self._log_match(message, rule, matched_content, action)
    
    async def _execute_action(
        self,
        message: discord.Message,
        rule: FilterRule,
        action: FilterAction,
        matched_content: str
    ):
        try:
            await message.delete()
        except discord.Forbidden:
            pass
        except discord.NotFound:
            pass
        
        if action == FilterAction.LOG:
            return
        
        if rule.dm_user:
            try:
                dm_message = rule.custom_message or f"Your message was removed for violating server rules."
                await message.author.send(dm_message)
            except discord.Forbidden:
                pass
        
        if action == FilterAction.WARN:
            user_data = await self.bot.db.get_or_create_user_data(message.author.id, message.guild.id)
            user_data.add_warning()
            await self.bot.db.save_user_data(user_data)
        
        elif action == FilterAction.MUTE:
            if isinstance(message.author, discord.Member):
                duration = rule.punishment_duration or 300
                try:
                    from datetime import timedelta
                    await message.author.timeout(timedelta(seconds=duration), reason=f"Filter: {rule.pattern}")
                except discord.Forbidden:
                    pass
        
        elif action == FilterAction.KICK:
            if isinstance(message.author, discord.Member):
                try:
                    await message.author.kick(reason=f"Filter violation: {rule.pattern}")
                except discord.Forbidden:
                    pass
        
        elif action == FilterAction.BAN:
            if isinstance(message.author, discord.Member):
                try:
                    await message.author.ban(reason=f"Filter violation: {rule.pattern}", delete_message_days=1)
                except discord.Forbidden:
                    pass
    
    async def _log_match(
        self,
        message: discord.Message,
        rule: FilterRule,
        matched_content: str,
        action: FilterAction
    ):
        filter_config = await self.bot.db.get_filter_config(message.guild.id)
        if not filter_config or not filter_config.log_channel:
            return
        
        channel = message.guild.get_channel(filter_config.log_channel)
        if not channel:
            return
        
        embed = EmbedBuilder.filter_violation(
            user=message.author,
            message_content=message.content[:500],
            filter_type=rule.filter_type.value,
            action_taken=action.value
        )
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            pass
    
    @commands.hybrid_group(name="filter", description="Word filter configuration")
    @commands.has_permissions(manage_messages=True)
    async def filter(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            filter_config = await self.bot.db.get_filter_config(ctx.guild.id)
            
            embed = (
                EmbedBuilder(
                    title="🚫 Word Filter",
                    description="Advanced word filtering system"
                )
                .color(EmbedColor.FILTER)
                .field("Status", "✅ Enabled" if (filter_config and filter_config.enabled) else "❌ Disabled", True)
                .field("Rules", str(len(filter_config.rules)) if filter_config else "0", True)
                .field("Log Channel", f"<#{filter_config.log_channel}>" if (filter_config and filter_config.log_channel) else "Not set", True)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @filter.command(name="enable", description="Enable the word filter")
    @commands.has_permissions(manage_messages=True)
    async def filter_enable(self, ctx: commands.Context):
        filter_config = await self.bot.db.get_or_create_filter_config(ctx.guild.id)
        filter_config.enabled = True
        await self.bot.db.save_filter_config(filter_config)
        
        await ctx.send(embed=EmbedBuilder.success("Word Filter", "Word filter has been enabled."))
    
    @filter.command(name="disable", description="Disable the word filter")
    @commands.has_permissions(manage_messages=True)
    async def filter_disable(self, ctx: commands.Context):
        filter_config = await self.bot.db.get_or_create_filter_config(ctx.guild.id)
        filter_config.enabled = False
        await self.bot.db.save_filter_config(filter_config)
        
        await ctx.send(embed=EmbedBuilder.success("Word Filter", "Word filter has been disabled."))
    
    @filter.command(name="add", description="Add a filter rule")
    @commands.has_permissions(manage_messages=True)
    async def filter_add(
        self,
        ctx: commands.Context,
        pattern: str,
        filter_type: str = "contains",
        action: str = "delete"
    ):
        try:
            f_type = FilterType(filter_type.lower())
        except ValueError:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Type",
                f"Valid types: {', '.join(t.value for t in FilterType)}"
            ))
        
        try:
            f_action = FilterAction(action.lower())
        except ValueError:
            return await ctx.send(embed=EmbedBuilder.error(
                "Invalid Action",
                f"Valid actions: {', '.join(a.value for a in FilterAction)}"
            ))
        
        if f_type == FilterType.REGEX:
            try:
                re.compile(pattern)
            except re.error as e:
                return await ctx.send(embed=EmbedBuilder.error("Invalid Regex", str(e)))
        
        filter_config = await self.bot.db.get_or_create_filter_config(ctx.guild.id)
        
        rule = FilterRule(
            id=generate_id("RULE"),
            guild_id=ctx.guild.id,
            pattern=pattern,
            filter_type=f_type,
            action=f_action,
            created_by=ctx.author.id
        )
        
        filter_config.add_rule(rule)
        await self.bot.db.save_filter_config(filter_config)
        
        embed = (
            EmbedBuilder(
                title="✅ Filter Rule Added",
                description=f"New filter rule created."
            )
            .color(EmbedColor.SUCCESS)
            .field("Rule ID", rule.id, True)
            .field("Pattern", f"`{pattern}`", True)
            .field("Type", f_type.value, True)
            .field("Action", f_action.value, True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @filter.command(name="remove", description="Remove a filter rule")
    @commands.has_permissions(manage_messages=True)
    async def filter_remove(self, ctx: commands.Context, rule_id: str):
        filter_config = await self.bot.db.get_filter_config(ctx.guild.id)
        if not filter_config:
            return await ctx.send(embed=EmbedBuilder.error("Error", "No filter configuration found."))
        
        if filter_config.remove_rule(rule_id):
            await self.bot.db.save_filter_config(filter_config)
            await ctx.send(embed=EmbedBuilder.success("Rule Removed", f"Filter rule `{rule_id}` has been removed."))
        else:
            await ctx.send(embed=EmbedBuilder.error("Not Found", f"Rule `{rule_id}` not found."))
    
    @filter.command(name="list", description="List all filter rules")
    @commands.has_permissions(manage_messages=True)
    async def filter_list(self, ctx: commands.Context):
        filter_config = await self.bot.db.get_filter_config(ctx.guild.id)
        if not filter_config or not filter_config.rules:
            return await ctx.send(embed=EmbedBuilder.info("No Rules", "No filter rules configured."))
        
        embed = (
            EmbedBuilder(
                title="📋 Filter Rules",
                description=f"Total: {len(filter_config.rules)} rule(s)"
            )
            .color(EmbedColor.INFO)
        )
        
        for rule in filter_config.rules[:10]:
            status = "✅" if rule.enabled else "❌"
            embed.field(
                f"{status} {rule.id}",
                f"Pattern: `{rule.pattern[:30]}...` if len(rule.pattern) > 30 else `{rule.pattern}`\n"
                f"Type: {rule.filter_type.value} | Action: {rule.action.value}\n"
                f"Matches: {rule.match_count}",
                False
            )
        
        if len(filter_config.rules) > 10:
            embed.footer(f"Showing 10 of {len(filter_config.rules)} rules")
        
        await ctx.send(embed=embed.build())
    
    @filter.command(name="logchannel", description="Set the filter log channel")
    @commands.has_permissions(manage_messages=True)
    async def filter_logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        filter_config = await self.bot.db.get_or_create_filter_config(ctx.guild.id)
        filter_config.log_channel = channel.id
        await self.bot.db.save_filter_config(filter_config)
        
        await ctx.send(embed=EmbedBuilder.success("Log Channel", f"Filter logs will be sent to {channel.mention}"))
    
    @filter.command(name="exempt", description="Exempt a role or channel from filtering")
    @commands.has_permissions(manage_messages=True)
    async def filter_exempt(
        self,
        ctx: commands.Context,
        action: str,
        target_type: str,
        target: str
    ):
        filter_config = await self.bot.db.get_or_create_filter_config(ctx.guild.id)
        
        if target_type.lower() == "role":
            try:
                role = await commands.RoleConverter().convert(ctx, target)
                target_id = role.id
                target_list = filter_config.global_exempt_roles
                target_name = role.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Role not found."))
        
        elif target_type.lower() == "channel":
            try:
                channel = await commands.TextChannelConverter().convert(ctx, target)
                target_id = channel.id
                target_list = filter_config.global_exempt_channels
                target_name = channel.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Channel not found."))
        
        elif target_type.lower() == "user":
            try:
                user = await commands.MemberConverter().convert(ctx, target)
                target_id = user.id
                target_list = filter_config.global_exempt_users
                target_name = user.mention
            except:
                return await ctx.send(embed=EmbedBuilder.error("Error", "User not found."))
        
        else:
            return await ctx.send(embed=EmbedBuilder.error("Invalid Type", "Use `role`, `channel`, or `user`."))
        
        if action.lower() == "add":
            if target_id not in target_list:
                target_list.append(target_id)
                await self.bot.db.save_filter_config(filter_config)
                await ctx.send(embed=EmbedBuilder.success("Exemption Added", f"{target_name} is now exempt from filtering."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Exempt", "This target is already exempt."))
        
        elif action.lower() == "remove":
            if target_id in target_list:
                target_list.remove(target_id)
                await self.bot.db.save_filter_config(filter_config)
                await ctx.send(embed=EmbedBuilder.success("Exemption Removed", f"{target_name} is no longer exempt from filtering."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Exempt", "This target is not in the exemption list."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
    
    @filter.command(name="test", description="Test if a message would be filtered")
    @commands.has_permissions(manage_messages=True)
    async def filter_test(self, ctx: commands.Context, *, text: str):
        filter_config = await self.bot.db.get_filter_config(ctx.guild.id)
        if not filter_config:
            return await ctx.send(embed=EmbedBuilder.info("No Filter", "No filter configuration found."))
        
        user_roles = [r.id for r in ctx.author.roles]
        matches = filter_config.check_content(text, ctx.author.id, ctx.channel.id, user_roles)
        
        if matches:
            rule, matched = matches[0]
            embed = (
                EmbedBuilder(
                    title="🚫 Would Be Filtered",
                    description="This message would trigger the filter."
                )
                .color(EmbedColor.ERROR)
                .field("Rule ID", rule.id, True)
                .field("Pattern", f"`{rule.pattern}`", True)
                .field("Action", rule.action.value, True)
                .build()
            )
        else:
            embed = (
                EmbedBuilder(
                    title="✅ Would Not Be Filtered",
                    description="This message would pass through the filter."
                )
                .color(EmbedColor.SUCCESS)
                .build()
            )
        
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(FilterCog(bot))
