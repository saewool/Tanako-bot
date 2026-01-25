"""
Utility Cog
General utility commands for users
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import asyncio
from datetime import datetime

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.helpers import format_timestamp


class UtilityCog(commands.Cog, name="Utility"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    @commands.hybrid_command(name="avatar", description="Get a user's avatar")
    async def avatar(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        user = user or ctx.author
        
        embed = (
            EmbedBuilder(
                title=f"🖼️ {user.display_name}'s Avatar"
            )
            .color(user.color if user.color.value else EmbedColor.INFO.value)
            .image(user.display_avatar.url)
            .field("Links", f"[PNG]({user.display_avatar.with_format('png')}) | [JPG]({user.display_avatar.with_format('jpg')}) | [WEBP]({user.display_avatar.with_format('webp')})", False)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="banner", description="Get a user's banner")
    async def banner(self, ctx: commands.Context, user: Optional[discord.User] = None):
        user = user or ctx.author
        
        user = await self.bot.fetch_user(user.id)
        
        if not user.banner:
            return await ctx.send(embed=EmbedBuilder.error("No Banner", "This user doesn't have a banner."))
        
        embed = (
            EmbedBuilder(
                title=f"🖼️ {user.display_name}'s Banner"
            )
            .color(EmbedColor.INFO)
            .image(user.banner.url)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="servericon", description="Get the server's icon")
    async def servericon(self, ctx: commands.Context):
        if not ctx.guild.icon:
            return await ctx.send(embed=EmbedBuilder.error("No Icon", "This server doesn't have an icon."))
        
        embed = (
            EmbedBuilder(
                title=f"🖼️ {ctx.guild.name}'s Icon"
            )
            .color(EmbedColor.INFO)
            .image(ctx.guild.icon.url)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="afk", description="Set yourself as AFK")
    async def afk(self, ctx: commands.Context, *, reason: str = "AFK"):
        user_data = await self.bot.db.get_or_create_user_data(ctx.author.id, ctx.guild.id)
        user_data.set_afk(reason)
        await self.bot.db.save_user_data(user_data)
        
        await ctx.send(embed=EmbedBuilder.success("AFK Set", f"You are now AFK: {reason}"))
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        
        user_data = await self.bot.db.get_user_data(message.author.id, message.guild.id)
        if user_data and user_data.afk:
            user_data.clear_afk()
            await self.bot.db.save_user_data(user_data)
            
            try:
                await message.channel.send(
                    f"Welcome back, {message.author.mention}! I've removed your AFK status.",
                    delete_after=5
                )
            except:
                pass
        
        for user in message.mentions:
            if user.bot:
                continue
            
            afk_user_data = await self.bot.db.get_user_data(user.id, message.guild.id)
            if afk_user_data and afk_user_data.afk:
                try:
                    await message.channel.send(
                        f"{user.display_name} is AFK: {afk_user_data.afk_message} (since <t:{int(afk_user_data.afk_since.timestamp())}:R>)",
                        delete_after=10
                    )
                except:
                    pass
    
    @commands.hybrid_command(name="remind", description="Set a reminder")
    async def remind(self, ctx: commands.Context, time: str, *, message: str):
        from src.utils.helpers import parse_duration
        
        duration = parse_duration(time)
        if not duration:
            return await ctx.send(embed=EmbedBuilder.error("Invalid Time", "Use formats like: 1h, 30m, 1d"))
        
        if duration.total_seconds() > 604800:
            return await ctx.send(embed=EmbedBuilder.error("Too Long", "Reminders cannot be longer than 7 days."))
        
        await ctx.send(embed=EmbedBuilder.success(
            "Reminder Set",
            f"I'll remind you in {time}: {message}"
        ))
        
        await asyncio.sleep(duration.total_seconds())
        
        try:
            await ctx.author.send(
                embed=EmbedBuilder(
                    title="⏰ Reminder",
                    description=message
                )
                .color(EmbedColor.INFO)
                .field("Set", f"<t:{int((datetime.now() - duration).timestamp())}:R>", True)
                .field("Server", ctx.guild.name, True)
                .build()
            )
        except discord.Forbidden:
            await ctx.channel.send(
                f"{ctx.author.mention}",
                embed=EmbedBuilder(
                    title="⏰ Reminder",
                    description=message
                )
                .color(EmbedColor.INFO)
                .build()
            )
    
    @commands.hybrid_command(name="poll", description="Create a poll")
    async def poll(self, ctx: commands.Context, question: str, *, options: str = None):
        if options:
            option_list = [o.strip() for o in options.split("|")]
            if len(option_list) < 2:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Provide at least 2 options separated by |"))
            if len(option_list) > 10:
                return await ctx.send(embed=EmbedBuilder.error("Error", "Maximum 10 options allowed."))
            
            emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
            
            description = "\n".join(f"{emojis[i]} {option}" for i, option in enumerate(option_list))
            
            embed = (
                EmbedBuilder(
                    title=f"📊 {question}",
                    description=description
                )
                .color(EmbedColor.INFO)
                .footer(f"Poll by {ctx.author.display_name}")
                .build()
            )
            
            msg = await ctx.send(embed=embed)
            
            for i in range(len(option_list)):
                await msg.add_reaction(emojis[i])
        else:
            embed = (
                EmbedBuilder(
                    title=f"📊 {question}",
                    description="React with ✅ for yes or ❌ for no"
                )
                .color(EmbedColor.INFO)
                .footer(f"Poll by {ctx.author.display_name}")
                .build()
            )
            
            msg = await ctx.send(embed=embed)
            await msg.add_reaction("✅")
            await msg.add_reaction("❌")
    
    @commands.hybrid_command(name="embed", description="Create a custom embed")
    @commands.has_permissions(manage_messages=True)
    async def embed(
        self,
        ctx: commands.Context,
        title: str,
        *,
        description: str
    ):
        embed = (
            EmbedBuilder(title=title, description=description)
            .color(EmbedColor.PRIMARY)
            .footer(f"Created by {ctx.author.display_name}")
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="say", description="Make the bot say something")
    @commands.has_permissions(manage_messages=True)
    async def say(self, ctx: commands.Context, *, message: str):
        try:
            await ctx.message.delete()
        except:
            pass
        
        await ctx.send(message)
    
    @commands.hybrid_command(name="roleinfo", description="Get information about a role")
    async def roleinfo(self, ctx: commands.Context, role: discord.Role):
        members_with_role = len([m for m in ctx.guild.members if role in m.roles])
        
        permissions = [perm.replace('_', ' ').title() for perm, value in role.permissions if value]
        perms_str = ", ".join(permissions[:10]) if permissions else "None"
        if len(permissions) > 10:
            perms_str += f" and {len(permissions) - 10} more..."
        
        embed = (
            EmbedBuilder(
                title=f"🎭 {role.name}",
                description=f"Information about {role.mention}"
            )
            .color(role.color if role.color.value else EmbedColor.INFO.value)
            .field("ID", str(role.id), True)
            .field("Color", str(role.color), True)
            .field("Position", str(role.position), True)
            .field("Members", str(members_with_role), True)
            .field("Mentionable", "Yes" if role.mentionable else "No", True)
            .field("Hoisted", "Yes" if role.hoist else "No", True)
            .field("Created", f"<t:{int(role.created_at.timestamp())}:R>", True)
            .field("Managed", "Yes" if role.managed else "No", True)
            .field("Key Permissions", perms_str, False)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="channelinfo", description="Get information about a channel")
    async def channelinfo(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        channel = channel or ctx.channel
        
        embed = (
            EmbedBuilder(
                title=f"#️⃣ {channel.name}",
                description=channel.topic or "No topic set"
            )
            .color(EmbedColor.INFO)
            .field("ID", str(channel.id), True)
            .field("Category", channel.category.name if channel.category else "None", True)
            .field("Position", str(channel.position), True)
            .field("NSFW", "Yes" if channel.nsfw else "No", True)
            .field("Slowmode", f"{channel.slowmode_delay}s" if channel.slowmode_delay else "None", True)
            .field("Created", f"<t:{int(channel.created_at.timestamp())}:R>", True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="firstmessage", description="Get the first message in a channel")
    async def firstmessage(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        channel = channel or ctx.channel
        
        async for message in channel.history(limit=1, oldest_first=True):
            embed = (
                EmbedBuilder(
                    title="📜 First Message",
                    description=f"[Jump to message]({message.jump_url})"
                )
                .color(EmbedColor.INFO)
                .field("Author", message.author.mention, True)
                .field("Sent", f"<t:{int(message.created_at.timestamp())}:F>", True)
                .field("Content", message.content[:500] if message.content else "No text content", False)
                .build()
            )
            
            return await ctx.send(embed=embed)
        
        await ctx.send(embed=EmbedBuilder.error("Not Found", "No messages found in this channel."))
    
    @commands.hybrid_command(name="snipe", description="Snipe the last deleted message")
    async def snipe(self, ctx: commands.Context):
        await ctx.send(embed=EmbedBuilder.info("Snipe", "Message snipe feature is disabled for privacy."))
    
    @commands.hybrid_command(name="membercount", description="Get the server member count")
    async def membercount(self, ctx: commands.Context):
        guild = ctx.guild
        
        total = guild.member_count
        humans = len([m for m in guild.members if not m.bot])
        bots = len([m for m in guild.members if m.bot])
        online = len([m for m in guild.members if m.status != discord.Status.offline])
        
        embed = (
            EmbedBuilder(
                title=f"👥 {guild.name} Members",
                description=f"Total: **{total}** members"
            )
            .color(EmbedColor.INFO)
            .field("Humans", str(humans), True)
            .field("Bots", str(bots), True)
            .field("Online", str(online), True)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="help", description="Show help for commands")
    async def help_command(self, ctx: commands.Context, command: Optional[str] = None):
        await ctx.defer()
        
        if command:
            cmd = self.bot.get_command(command)
            if not cmd:
                return await ctx.send(embed=EmbedBuilder.error("Not Found", f"Command `{command}` not found."))
            
            embed = (
                EmbedBuilder(
                    title=f"📖 Help: {cmd.name}",
                    description=cmd.description or cmd.help or "No description available."
                )
                .color(EmbedColor.INFO)
                .field("Usage", f"`{ctx.prefix}{cmd.name} {cmd.signature}`", False)
            )
            
            if cmd.aliases:
                embed.field("Aliases", ", ".join(f"`{a}`" for a in cmd.aliases), True)
            
            await ctx.send(embed=embed.build())
        else:
            embed = (
                EmbedBuilder(
                    title="📖 Bot Help",
                    description=f"Use `{ctx.prefix}help <command>` for more info on a command."
                )
                .color(EmbedColor.INFO)
            )
            
            for cog_name, cog in self.bot.cogs.items():
                commands_list = [cmd.name for cmd in cog.get_commands() if not cmd.hidden]
                if commands_list:
                    embed.field(cog_name, ", ".join(f"`{c}`" for c in commands_list[:10]), False)
            
            await ctx.send(embed=embed.build())


async def setup(bot: commands.Bot):
    await bot.add_cog(UtilityCog(bot))
