"""
Metrics Cog
Bot statistics and monitoring
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional
import psutil
import platform
from datetime import datetime

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class MetricsCog(commands.Cog, name="Metrics"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._message_count: int = 0
        self._command_count: int = 0
        self._metrics_task_started = False
    
    async def cog_load(self):
        if not self._metrics_task_started:
            self.collect_metrics_loop.start()
            self._metrics_task_started = True
    
    async def cog_unload(self):
        if self.collect_metrics_loop.is_running():
            self.collect_metrics_loop.cancel()
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.author.bot:
            self._message_count += 1
    
    @commands.Cog.listener()
    async def on_command(self, ctx: commands.Context):
        self._command_count += 1
    
    @tasks.loop(minutes=5)
    async def collect_metrics_loop(self):
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            shard_id = self.bot.shard_id if self.bot.shard_id is not None else 0
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='messages',
                value=float(self._message_count),
                data={'period': '5m'}
            )
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='commands',
                value=float(self._command_count),
                data={'period': '5m'}
            )
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='memory',
                value=memory_mb,
                data={'unit': 'MB'}
            )
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='cpu',
                value=cpu_percent,
                data={'unit': '%'}
            )
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='guilds',
                value=float(len(self.bot.guilds)),
                data={}
            )
            
            await self.bot.db.save_metric(
                shard_id=shard_id,
                metric_type='latency',
                value=self.bot.latency * 1000 if self.bot.latency else 0,
                data={'unit': 'ms'}
            )
            
            self._message_count = 0
            self._command_count = 0
            
        except Exception as e:
            pass
    
    @collect_metrics_loop.before_loop
    async def before_collect_metrics_loop(self):
        await self.bot.wait_until_ready()
    
    @commands.hybrid_command(name="stats", aliases=["botstats", "info"], description="View bot statistics")
    async def stats(self, ctx: commands.Context):
        process = psutil.Process()
        memory = process.memory_info()
        memory_mb = memory.rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        uptime = datetime.now() - self.bot.start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
        
        total_members = sum(g.member_count or 0 for g in self.bot.guilds)
        total_channels = sum(len(g.channels) for g in self.bot.guilds)
        total_commands = len(self.bot.commands)
        
        shard_info = ""
        if self.bot.shard_count:
            shard_info = f"Shards: {self.bot.shard_count}"
            if self.bot.shard_id is not None:
                shard_info += f" (Current: {self.bot.shard_id})"
        
        embed = (
            EmbedBuilder(
                title="Bot Statistics",
                description=f"**{self.bot.user.name}** v{self.bot.version}"
            )
            .color(EmbedColor.PRIMARY)
            .thumbnail(self.bot.user.display_avatar.url)
            .field("Uptime", uptime_str, True)
            .field("Latency", f"{self.bot.latency * 1000:.0f}ms", True)
            .field("Servers", str(len(self.bot.guilds)), True)
            .field("Users", f"{total_members:,}", True)
            .field("Channels", str(total_channels), True)
            .field("Commands", str(total_commands), True)
            .field("Memory", f"{memory_mb:.1f} MB", True)
            .field("CPU", f"{cpu_percent:.1f}%", True)
            .field("Python", platform.python_version(), True)
        )
        
        if shard_info:
            embed.field("Sharding", shard_info, False)
        
        embed.field("Discord.py", discord.__version__, True)
        embed.field("Platform", platform.system(), True)
        
        await ctx.send(embed=embed.build())
    
    @commands.hybrid_command(name="ping", description="Check bot latency")
    async def ping(self, ctx: commands.Context):
        start = datetime.now()
        msg = await ctx.send("Pinging...")
        end = datetime.now()
        
        api_latency = (end - start).total_seconds() * 1000
        ws_latency = self.bot.latency * 1000 if self.bot.latency else 0
        
        embed = (
            EmbedBuilder(
                title="Pong!",
                description="Bot latency information"
            )
            .color(EmbedColor.SUCCESS if ws_latency < 200 else EmbedColor.WARNING)
            .field("API Latency", f"{api_latency:.0f}ms", True)
            .field("WebSocket Latency", f"{ws_latency:.0f}ms", True)
        )
        
        if self.bot.shard_id is not None:
            embed.field("Shard", str(self.bot.shard_id), True)
        
        await msg.edit(content=None, embed=embed.build())
    
    @commands.hybrid_command(name="serverinfo", aliases=["guildinfo"], description="View server information")
    async def serverinfo(self, ctx: commands.Context):
        guild = ctx.guild
        
        text_channels = len(guild.text_channels)
        voice_channels = len(guild.voice_channels)
        categories = len(guild.categories)
        
        roles = len(guild.roles) - 1
        emojis = len(guild.emojis)
        stickers = len(guild.stickers)
        
        humans = len([m for m in guild.members if not m.bot])
        bots = len([m for m in guild.members if m.bot])
        online = len([m for m in guild.members if m.status != discord.Status.offline])
        
        boost_level = guild.premium_tier
        boost_count = guild.premium_subscription_count or 0
        
        verification_levels = {
            discord.VerificationLevel.none: "None",
            discord.VerificationLevel.low: "Low",
            discord.VerificationLevel.medium: "Medium",
            discord.VerificationLevel.high: "High",
            discord.VerificationLevel.highest: "Highest"
        }
        
        embed = (
            EmbedBuilder(
                title=guild.name,
                description=guild.description or "No description set"
            )
            .color(EmbedColor.INFO)
        )
        
        if guild.icon:
            embed.thumbnail(guild.icon.url)
        
        embed.field("Owner", guild.owner.mention if guild.owner else "Unknown", True)
        embed.field("Created", f"<t:{int(guild.created_at.timestamp())}:R>", True)
        embed.field("ID", str(guild.id), True)
        
        embed.field("Members", f"Total: {guild.member_count}\nHumans: {humans}\nBots: {bots}\nOnline: {online}", True)
        embed.field("Channels", f"Text: {text_channels}\nVoice: {voice_channels}\nCategories: {categories}", True)
        embed.field("Other", f"Roles: {roles}\nEmojis: {emojis}\nStickers: {stickers}", True)
        
        embed.field("Boost", f"Level {boost_level}\n{boost_count} boosts", True)
        embed.field("Verification", verification_levels.get(guild.verification_level, "Unknown"), True)
        
        if guild.banner:
            embed.image(guild.banner.url)
        
        await ctx.send(embed=embed.build())
    
    @commands.hybrid_command(name="userinfo", aliases=["whois", "user"], description="View user information")
    async def userinfo(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        user = user or ctx.author
        
        roles = [r.mention for r in user.roles[1:]][:10]
        roles_str = ", ".join(roles) if roles else "None"
        if len(user.roles) > 11:
            roles_str += f" and {len(user.roles) - 11} more..."
        
        status_emoji = {
            discord.Status.online: "🟢",
            discord.Status.idle: "🟡",
            discord.Status.dnd: "🔴",
            discord.Status.offline: "⚫"
        }
        
        embed = (
            EmbedBuilder(
                title=f"{status_emoji.get(user.status, '')} {user.display_name}"
            )
            .color(user.color if user.color.value else EmbedColor.INFO.value)
            .thumbnail(user.display_avatar.url)
            .field("Username", str(user), True)
            .field("ID", str(user.id), True)
            .field("Status", str(user.status).title(), True)
            .field("Created", f"<t:{int(user.created_at.timestamp())}:R>", True)
            .field("Joined", f"<t:{int(user.joined_at.timestamp())}:R>" if user.joined_at else "Unknown", True)
            .field("Top Role", user.top_role.mention if user.top_role.name != "@everyone" else "None", True)
            .field(f"Roles ({len(user.roles) - 1})", roles_str, False)
        )
        
        if user.premium_since:
            embed.field("Boosting Since", f"<t:{int(user.premium_since.timestamp())}:R>", True)
        
        await ctx.send(embed=embed.build())
    
    @commands.hybrid_command(name="metrics", description="View bot metrics")
    @commands.has_permissions(manage_guild=True)
    async def view_metrics(self, ctx: commands.Context, limit: int = 10):
        shard_id = self.bot.shard_id if self.bot.shard_id is not None else 0
        
        metrics = await self.bot.db.get_metrics(shard_id=shard_id, limit=limit * 6)
        
        if not metrics:
            return await ctx.send(
                embed=EmbedBuilder.info("No Metrics", "No metrics have been collected yet.")
            )
        
        messages_metrics = [m for m in metrics if m.get('metric_type') == 'messages'][:5]
        commands_metrics = [m for m in metrics if m.get('metric_type') == 'commands'][:5]
        memory_metrics = [m for m in metrics if m.get('metric_type') == 'memory'][:5]
        
        embed = (
            EmbedBuilder(
                title="Bot Metrics",
                description=f"Recent metrics for Shard {shard_id}"
            )
            .color(EmbedColor.INFO)
        )
        
        if messages_metrics:
            total_messages = sum(m.get('value', 0) for m in messages_metrics)
            embed.field("Messages (last 25m)", str(int(total_messages)), True)
        
        if commands_metrics:
            total_commands = sum(m.get('value', 0) for m in commands_metrics)
            embed.field("Commands (last 25m)", str(int(total_commands)), True)
        
        if memory_metrics:
            avg_memory = sum(m.get('value', 0) for m in memory_metrics) / len(memory_metrics)
            embed.field("Avg Memory", f"{avg_memory:.1f} MB", True)
        
        await ctx.send(embed=embed.build())


async def setup(bot: commands.Bot):
    await bot.add_cog(MetricsCog(bot))
