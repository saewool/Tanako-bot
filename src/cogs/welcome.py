"""
Welcome System Cog
Handles welcome/goodbye messages and auto-roles
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import asyncio

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.permissions import require_permission, PermissionLevel


class WelcomeCog(commands.Cog, name="Welcome"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    def _format_welcome_message(
        self,
        template: str,
        member: discord.Member,
        guild: discord.Guild
    ) -> str:
        replacements = {
            '{user}': member.mention,
            '{username}': member.name,
            '{displayname}': member.display_name,
            '{server}': guild.name,
            '{count}': str(guild.member_count),
            '{user_id}': str(member.id),
            '{guild_id}': str(guild.id),
            '{created_at}': f"<t:{int(member.created_at.timestamp())}:R>"
        }
        
        result = template
        for key, value in replacements.items():
            result = result.replace(key, value)
        
        return result
    
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        
        guild_config = await self.bot.db.get_guild_config(member.guild.id)
        if not guild_config:
            return
        
        settings = guild_config.settings.welcome
        
        if settings.auto_role_ids:
            roles_to_add = []
            for role_id in settings.auto_role_ids:
                role = member.guild.get_role(role_id)
                if role and role < member.guild.me.top_role:
                    roles_to_add.append(role)
            
            if roles_to_add:
                try:
                    await member.add_roles(*roles_to_add, reason="Auto-role on join")
                except discord.Forbidden:
                    pass
        
        if settings.enabled and settings.channel_id:
            channel = member.guild.get_channel(settings.channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                message = self._format_welcome_message(
                    settings.message, member, member.guild
                )
                
                if settings.embed_enabled:
                    embed = EmbedBuilder.welcome(member, member.guild, message)
                    try:
                        await channel.send(embed=embed)
                    except discord.Forbidden:
                        pass
                else:
                    try:
                        await channel.send(message)
                    except discord.Forbidden:
                        pass
        
        if settings.dm_enabled:
            dm_message = self._format_welcome_message(
                settings.dm_message, member, member.guild
            )
            
            try:
                embed = (
                    EmbedBuilder(
                        title=f"Welcome to {member.guild.name}!",
                        description=dm_message
                    )
                    .color(EmbedColor.WELCOME)
                    .thumbnail(member.guild.icon.url if member.guild.icon else None)
                    .build()
                )
                await member.send(embed=embed)
            except discord.Forbidden:
                pass
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        if member.bot:
            return
        
        guild_config = await self.bot.db.get_guild_config(member.guild.id)
        if not guild_config:
            return
        
        settings = guild_config.settings.goodbye
        
        if settings.enabled and settings.channel_id:
            channel = member.guild.get_channel(settings.channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                message = self._format_welcome_message(
                    settings.message, member, member.guild
                )
                
                if settings.embed_enabled:
                    embed = EmbedBuilder.goodbye(member, member.guild)
                    try:
                        await channel.send(embed=embed)
                    except discord.Forbidden:
                        pass
                else:
                    try:
                        await channel.send(message)
                    except discord.Forbidden:
                        pass
    
    @commands.hybrid_group(name="welcome", description="Welcome system configuration")
    @commands.has_permissions(manage_guild=True)
    async def welcome(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found for this server.")
            
            settings = guild_config.settings.welcome
            
            embed = (
                EmbedBuilder(
                    title="👋 Welcome System Configuration",
                    description="Current welcome system settings"
                )
                .color(EmbedColor.INFO)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Channel", f"<#{settings.channel_id}>" if settings.channel_id else "Not set", True)
                .field("DM Welcome", "✅ Enabled" if settings.dm_enabled else "❌ Disabled", True)
                .field("Embed Mode", "✅ Enabled" if settings.embed_enabled else "❌ Disabled", True)
                .field("Auto Roles", str(len(settings.auto_role_ids)) + " roles", True)
                .field("Welcome Message", f"```{settings.message[:100]}...```" if len(settings.message) > 100 else f"```{settings.message}```", False)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @welcome.command(name="enable", description="Enable the welcome system")
    @commands.has_permissions(manage_guild=True)
    async def welcome_enable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Welcome System", "Welcome system has been enabled!"))
    
    @welcome.command(name="disable", description="Disable the welcome system")
    @commands.has_permissions(manage_guild=True)
    async def welcome_disable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Welcome System", "Welcome system has been disabled."))
    
    @welcome.command(name="channel", description="Set the welcome channel")
    @commands.has_permissions(manage_guild=True)
    async def welcome_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.channel_id = channel.id
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Welcome Channel", f"Welcome channel set to {channel.mention}"))
    
    @welcome.command(name="message", description="Set the welcome message")
    @commands.has_permissions(manage_guild=True)
    async def welcome_message(self, ctx: commands.Context, *, message: str):
        if len(message) > 2000:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Message must be 2000 characters or less."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.message = message
        await self.bot.db.save_guild_config(guild_config)
        
        embed = (
            EmbedBuilder(
                title="✅ Welcome Message Updated",
                description="The welcome message has been updated."
            )
            .color(EmbedColor.SUCCESS)
            .field("New Message", f"```{message}```", False)
            .field("Variables", "`{user}` - mention\n`{username}` - name\n`{server}` - server name\n`{count}` - member count", False)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @welcome.command(name="dm", description="Toggle DM welcome messages")
    @commands.has_permissions(manage_guild=True)
    async def welcome_dm(self, ctx: commands.Context, enabled: bool):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.dm_enabled = enabled
        await self.bot.db.save_guild_config(guild_config)
        
        status = "enabled" if enabled else "disabled"
        await ctx.send(embed=EmbedBuilder.success("DM Welcome", f"DM welcome messages have been {status}."))
    
    @welcome.command(name="dmmessage", description="Set the DM welcome message")
    @commands.has_permissions(manage_guild=True)
    async def welcome_dm_message(self, ctx: commands.Context, *, message: str):
        if len(message) > 2000:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Message must be 2000 characters or less."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.dm_message = message
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("DM Message Updated", "The DM welcome message has been updated."))
    
    @welcome.command(name="embed", description="Toggle embed mode for welcome messages")
    @commands.has_permissions(manage_guild=True)
    async def welcome_embed(self, ctx: commands.Context, enabled: bool):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.welcome.embed_enabled = enabled
        await self.bot.db.save_guild_config(guild_config)
        
        status = "enabled" if enabled else "disabled"
        await ctx.send(embed=EmbedBuilder.success("Embed Mode", f"Embed mode has been {status}."))
    
    @welcome.command(name="autorole", description="Manage auto-roles for new members")
    @commands.has_permissions(manage_guild=True)
    async def welcome_autorole(self, ctx: commands.Context, action: str, role: discord.Role = None):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if action.lower() == "add":
            if not role:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Please specify a role to add."))
            
            if role >= ctx.guild.me.top_role:
                return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot assign roles higher than my highest role."))
            
            if role.id not in guild_config.settings.welcome.auto_role_ids:
                guild_config.settings.welcome.auto_role_ids.append(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Auto Role", f"{role.mention} will be given to new members."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Added", "This role is already an auto-role."))
        
        elif action.lower() == "remove":
            if not role:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Please specify a role to remove."))
            
            if role.id in guild_config.settings.welcome.auto_role_ids:
                guild_config.settings.welcome.auto_role_ids.remove(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Auto Role", f"{role.mention} removed from auto-roles."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Found", "This role is not an auto-role."))
        
        elif action.lower() == "list":
            roles = []
            for role_id in guild_config.settings.welcome.auto_role_ids:
                r = ctx.guild.get_role(role_id)
                if r:
                    roles.append(r.mention)
            
            if roles:
                embed = (
                    EmbedBuilder(title="🎭 Auto Roles", description="\n".join(roles))
                    .color(EmbedColor.INFO)
                    .build()
                )
            else:
                embed = EmbedBuilder.info("Auto Roles", "No auto-roles configured.")
            
            await ctx.send(embed=embed)
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add`, `remove`, or `list`."))
    
    @welcome.command(name="test", description="Test the welcome message")
    @commands.has_permissions(manage_guild=True)
    async def welcome_test(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
        if not guild_config:
            return await ctx.send(embed=EmbedBuilder.error("Error", "No configuration found."))
        
        settings = guild_config.settings.welcome
        
        message = self._format_welcome_message(
            settings.message, ctx.author, ctx.guild
        )
        
        if settings.embed_enabled:
            embed = EmbedBuilder.welcome(ctx.author, ctx.guild, message)
            await ctx.send(content="**Test Welcome Message:**", embed=embed)
        else:
            await ctx.send(f"**Test Welcome Message:**\n{message}")
    
    @commands.hybrid_group(name="goodbye", description="Goodbye system configuration")
    @commands.has_permissions(manage_guild=True)
    async def goodbye(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found for this server.")
            
            settings = guild_config.settings.goodbye
            
            embed = (
                EmbedBuilder(
                    title="👋 Goodbye System Configuration",
                    description="Current goodbye system settings"
                )
                .color(EmbedColor.INFO)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Channel", f"<#{settings.channel_id}>" if settings.channel_id else "Not set", True)
                .field("Embed Mode", "✅ Enabled" if settings.embed_enabled else "❌ Disabled", True)
                .field("Goodbye Message", f"```{settings.message}```", False)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @goodbye.command(name="enable", description="Enable the goodbye system")
    @commands.has_permissions(manage_guild=True)
    async def goodbye_enable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.goodbye.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Goodbye System", "Goodbye system has been enabled!"))
    
    @goodbye.command(name="disable", description="Disable the goodbye system")
    @commands.has_permissions(manage_guild=True)
    async def goodbye_disable(self, ctx: commands.Context):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.goodbye.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Goodbye System", "Goodbye system has been disabled."))
    
    @goodbye.command(name="channel", description="Set the goodbye channel")
    @commands.has_permissions(manage_guild=True)
    async def goodbye_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.goodbye.channel_id = channel.id
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Goodbye Channel", f"Goodbye channel set to {channel.mention}"))
    
    @goodbye.command(name="message", description="Set the goodbye message")
    @commands.has_permissions(manage_guild=True)
    async def goodbye_message(self, ctx: commands.Context, *, message: str):
        if len(message) > 2000:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Message must be 2000 characters or less."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.goodbye.message = message
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Goodbye Message", "The goodbye message has been updated."))


async def setup(bot: commands.Bot):
    await bot.add_cog(WelcomeCog(bot))
