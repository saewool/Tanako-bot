"""
Admin Cog
Administrative commands for bot and server management
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import asyncio
import sys
import os
from datetime import datetime

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.helpers import format_bytes, format_duration
from src.utils.permissions import bot_owner_only


class AdminCog(commands.Cog, name="Admin"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.start_time = datetime.now()
    
    @commands.hybrid_group(name="config", description="Server configuration")
    @commands.has_permissions(administrator=True)
    async def config(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await ctx.defer()
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found.")
            
            settings = guild_config.settings
            
            embed = (
                EmbedBuilder(
                    title="⚙️ Server Configuration",
                    description=f"Configuration for **{ctx.guild.name}**"
                )
                .color(EmbedColor.INFO)
                .field("Prefix", f"`{settings.prefix}`", True)
                .field("Language", settings.language, True)
                .field("Timezone", settings.timezone, True)
                .field("Moderator Roles", str(len(settings.moderator_roles)), True)
                .field("Admin Roles", str(len(settings.admin_roles)), True)
                .field("Disabled Commands", str(len(settings.disabled_commands)), True)
                .thumbnail(ctx.guild.icon.url if ctx.guild.icon else None)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @config.command(name="prefix", description="Set the command prefix")
    @commands.has_permissions(administrator=True)
    async def config_prefix(self, ctx: commands.Context, prefix: str):
        await ctx.defer()
        if len(prefix) > 5:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Prefix must be 5 characters or less."))
        
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.prefix = prefix
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Prefix Updated", f"Command prefix set to `{prefix}`"))
    
    @config.command(name="modrole", description="Add or remove a moderator role")
    @commands.has_permissions(administrator=True)
    async def config_modrole(self, ctx: commands.Context, action: str, role: discord.Role):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if action.lower() == "add":
            if role.id not in guild_config.settings.moderator_roles:
                guild_config.settings.moderator_roles.append(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Moderator Role", f"{role.mention} added as moderator role."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Added", "This role is already a moderator role."))
        
        elif action.lower() == "remove":
            if role.id in guild_config.settings.moderator_roles:
                guild_config.settings.moderator_roles.remove(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Moderator Role", f"{role.mention} removed from moderator roles."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Found", "This role is not a moderator role."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
    
    @config.command(name="adminrole", description="Add or remove an admin role")
    @commands.has_permissions(administrator=True)
    async def config_adminrole(self, ctx: commands.Context, action: str, role: discord.Role):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if action.lower() == "add":
            if role.id not in guild_config.settings.admin_roles:
                guild_config.settings.admin_roles.append(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Admin Role", f"{role.mention} added as admin role."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Added", "This role is already an admin role."))
        
        elif action.lower() == "remove":
            if role.id in guild_config.settings.admin_roles:
                guild_config.settings.admin_roles.remove(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Admin Role", f"{role.mention} removed from admin roles."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Found", "This role is not an admin role."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
    
    @commands.hybrid_command(name="botinfo", description="Get information about the bot")
    async def botinfo(self, ctx: commands.Context):
        await ctx.defer()
        
        uptime = datetime.now() - self.start_time
        
        total_members = sum(g.member_count for g in self.bot.guilds)
        total_channels = sum(len(g.channels) for g in self.bot.guilds)
        
        embed = (
            EmbedBuilder(
                title=f"🤖 {self.bot.user.name}",
                description="A comprehensive Discord moderation bot"
            )
            .color(EmbedColor.PRIMARY)
            .thumbnail(self.bot.user.display_avatar.url)
            .field("Servers", str(len(self.bot.guilds)), True)
            .field("Users", str(total_members), True)
            .field("Channels", str(total_channels), True)
            .field("Uptime", format_duration(uptime), True)
            .field("Latency", f"{round(self.bot.latency * 1000)}ms", True)
            .field("Python", sys.version.split()[0], True)
            .field("Discord.py", discord.__version__, True)
            .field("Commands", str(len(self.bot.commands)), True)
            .field("Cogs", str(len(self.bot.cogs)), True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="backup", description="Create a backup of server settings")
    @commands.has_permissions(administrator=True)
    async def backup(self, ctx: commands.Context):
        await ctx.defer()
        
        guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
        if not guild_config:
            return await ctx.send(embed=EmbedBuilder.error("Error", "No configuration to backup."))
        
        backup_id = f"{ctx.guild.id}_{int(datetime.now().timestamp())}"
        backup_path = f"data/backups/{backup_id}.json"
        
        import json
        os.makedirs("data/backups", exist_ok=True)
        with open(backup_path, 'w') as f:
            json.dump(guild_config.to_dict(), f, indent=2)
        
        embed = (
            EmbedBuilder(
                title="💾 Backup Created",
                description="Server configuration has been backed up."
            )
            .color(EmbedColor.SUCCESS)
            .field("Backup ID", backup_id, False)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="restore", description="Restore server settings from a backup")
    @commands.has_permissions(administrator=True)
    async def restore(self, ctx: commands.Context, backup_id: str):
        await ctx.defer()
        
        backup_path = f"data/backups/{backup_id}.json"
        
        if not os.path.exists(backup_path):
            backups_dir = "data/backups"
            if os.path.exists(backups_dir):
                available = [f.replace('.json', '') for f in os.listdir(backups_dir) 
                           if f.startswith(str(ctx.guild.id)) and f.endswith('.json')]
                if available:
                    return await ctx.send(embed=EmbedBuilder.error(
                        "Backup Not Found", 
                        f"Available backups:\n" + "\n".join(f"`{b}`" for b in available[-5:])
                    ))
            return await ctx.send(embed=EmbedBuilder.error("Error", "Backup file not found."))
        
        import json
        try:
            with open(backup_path, 'r') as f:
                data = json.load(f)
            
            from src.models.guild import GuildConfig
            config = GuildConfig.from_dict(data)
            config.guild_id = ctx.guild.id
            
            await self.bot.db.save_guild_config(config)
            
            embed = (
                EmbedBuilder(
                    title="✅ Restore Complete",
                    description="Server configuration has been restored from backup."
                )
                .color(EmbedColor.SUCCESS)
                .field("Backup ID", backup_id, False)
                .build()
            )
            
            await ctx.send(embed=embed)
        except Exception as e:
            await ctx.send(embed=EmbedBuilder.error("Error", f"Failed to restore: {str(e)}"))
    
    @commands.hybrid_command(name="roleall", description="Add or remove a role from all members")
    @commands.has_permissions(administrator=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def roleall(self, ctx: commands.Context, action: str, role: discord.Role):
        if role >= ctx.guild.me.top_role:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I cannot manage this role."))
        
        if role >= ctx.author.top_role and ctx.author.id != ctx.guild.owner_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You cannot manage this role."))
        
        await ctx.defer()
        
        count = 0
        errors = 0
        
        if action.lower() == "add":
            for member in ctx.guild.members:
                if role not in member.roles and not member.bot:
                    try:
                        await member.add_roles(role, reason=f"Role all by {ctx.author}")
                        count += 1
                    except:
                        errors += 1
        
        elif action.lower() == "remove":
            for member in ctx.guild.members:
                if role in member.roles and not member.bot:
                    try:
                        await member.remove_roles(role, reason=f"Role all by {ctx.author}")
                        count += 1
                    except:
                        errors += 1
        
        else:
            return await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
        
        embed = (
            EmbedBuilder(
                title="✅ Role All Complete",
                description=f"Role {action} operation completed."
            )
            .color(EmbedColor.SUCCESS)
            .field("Role", role.mention, True)
            .field("Affected", str(count), True)
            .field("Errors", str(errors), True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="slowmode", description="Set channel slowmode")
    @commands.has_permissions(manage_channels=True)
    async def slowmode(self, ctx: commands.Context, seconds: int, channel: Optional[discord.TextChannel] = None):
        channel = channel or ctx.channel
        
        if seconds < 0 or seconds > 21600:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Slowmode must be between 0 and 21600 seconds."))
        
        await channel.edit(slowmode_delay=seconds)
        
        if seconds == 0:
            await ctx.send(embed=EmbedBuilder.success("Slowmode", f"Slowmode disabled in {channel.mention}"))
        else:
            await ctx.send(embed=EmbedBuilder.success("Slowmode", f"Slowmode set to {seconds} seconds in {channel.mention}"))
    
    @commands.hybrid_command(name="resetchannel", description="Reset a channel by cloning and deleting")
    @commands.has_permissions(administrator=True)
    @commands.bot_has_permissions(manage_channels=True)
    async def resetchannel(self, ctx: commands.Context):
        confirm_msg = await ctx.send(
            embed=EmbedBuilder.warning(
                "Confirm Reset",
                f"Are you sure you want to reset {ctx.channel.mention}? This will delete and recreate the channel.\n\nReact with ✅ to confirm or ❌ to cancel."
            )
        )
        
        await confirm_msg.add_reaction("✅")
        await confirm_msg.add_reaction("❌")
        
        def check(reaction, user):
            return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
        
        try:
            reaction, user = await self.bot.wait_for("reaction_add", timeout=30, check=check)
        except asyncio.TimeoutError:
            return await confirm_msg.edit(embed=EmbedBuilder.error("Timeout", "Reset cancelled due to timeout."))
        
        if str(reaction.emoji) == "❌":
            return await confirm_msg.edit(embed=EmbedBuilder.error("Cancelled", "Channel reset cancelled."))
        
        position = ctx.channel.position
        new_channel = await ctx.channel.clone(reason=f"Channel reset by {ctx.author}")
        await new_channel.edit(position=position)
        await ctx.channel.delete(reason=f"Channel reset by {ctx.author}")
        
        await new_channel.send(
            embed=EmbedBuilder.success("Channel Reset", f"This channel was reset by {ctx.author.mention}")
        )
    
    @commands.hybrid_command(name="dbstats", description="View database statistics")
    async def dbstats(self, ctx: commands.Context):
        db_stats = await self.bot.db.stats()
        
        embed = (
            EmbedBuilder(
                title="📊 Bot Statistics",
                description="Database and performance statistics"
            )
            .color(EmbedColor.INFO)
            .field("Tables", str(db_stats.get('tables', 0)), True)
            .field("Total Rows", str(db_stats.get('total_rows', 0)), True)
            .field("Database Size", format_bytes(db_stats.get('total_size_bytes', 0)), True)
            .build()
        )
        
        await ctx.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
