"""
Host Checking Cog
Monitor external websites and internal bot nodes
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
from typing import Optional, List
import aiohttp
import asyncio
from datetime import datetime

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class HostCheckCog(commands.Cog, name="HostCheck"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None
        self._check_task_started = False
    
    async def cog_load(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        if not self._check_task_started:
            self.host_check_loop.start()
            self._check_task_started = True
    
    async def cog_unload(self):
        if self._session:
            await self._session.close()
        if self.host_check_loop.is_running():
            self.host_check_loop.cancel()
    
    @tasks.loop(minutes=1)
    async def host_check_loop(self):
        try:
            checks = await self.bot.db.get_all_active_host_checks()
            
            for check in checks:
                check_id = check.get('id')
                url = check.get('url')
                check_type = check.get('check_type', 'http')
                notify_channel_id = check.get('notify_channel_id')
                last_status = check.get('last_status')
                
                new_status = await self._perform_check(url, check_type)
                
                await self.bot.db.update_host_check_status(check_id, new_status)
                
                if notify_channel_id and last_status != new_status and last_status != 'pending':
                    await self._send_notification(
                        notify_channel_id,
                        check.get('name', url),
                        url,
                        last_status,
                        new_status
                    )
        except Exception as e:
            pass
    
    @host_check_loop.before_loop
    async def before_host_check_loop(self):
        await self.bot.wait_until_ready()
    
    async def _perform_check(self, url: str, check_type: str) -> str:
        try:
            if check_type == 'http':
                async with self._session.get(url) as response:
                    if 200 <= response.status < 400:
                        return 'online'
                    else:
                        return f'error:{response.status}'
            
            elif check_type == 'ping':
                async with self._session.head(url) as response:
                    return 'online' if response.status < 400 else 'offline'
            
            else:
                async with self._session.get(url) as response:
                    return 'online' if response.status < 400 else 'offline'
        
        except asyncio.TimeoutError:
            return 'timeout'
        except aiohttp.ClientConnectorError:
            return 'unreachable'
        except Exception as e:
            return f'error:{str(e)[:50]}'
    
    async def _send_notification(
        self,
        channel_id: int,
        name: str,
        url: str,
        old_status: str,
        new_status: str
    ):
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return
            
            if new_status == 'online':
                embed = (
                    EmbedBuilder(
                        title="Host Back Online",
                        description=f"**{name}** is now online!"
                    )
                    .color(EmbedColor.SUCCESS)
                    .field("URL", url, False)
                    .field("Previous Status", old_status, True)
                    .field("Current Status", new_status, True)
                    .timestamp()
                    .build()
                )
            else:
                embed = (
                    EmbedBuilder(
                        title="Host Down!",
                        description=f"**{name}** appears to be down!"
                    )
                    .color(EmbedColor.ERROR)
                    .field("URL", url, False)
                    .field("Previous Status", old_status, True)
                    .field("Current Status", new_status, True)
                    .timestamp()
                    .build()
                )
            
            await channel.send(embed=embed)
        except Exception as e:
            pass
    
    @commands.group(name="host", aliases=["uptime", "monitor"], description="Host monitoring commands")
    async def host(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            embed = (
                EmbedBuilder(
                    title="Host Monitoring",
                    description="Monitor websites and services!"
                )
                .color(EmbedColor.INFO)
                .field("Add", "`!host add <name> <url> [notify_channel]` - Add a host to monitor", False)
                .field("Remove", "`!host remove <name>` - Remove a monitored host", False)
                .field("List", "`!host list` - View all monitored hosts", False)
                .field("Check", "`!host check <url>` - Quick one-time check", False)
                .field("Status", "`!host status` - View all host statuses", False)
                .field("Nodes", "`!host nodes` - View bot node/shard status", False)
                .build()
            )
            await ctx.send(embed=embed)
    
    @host.command(name="add", description="Add a host to monitor")
    @commands.has_permissions(manage_guild=True)
    async def add_host(
        self,
        ctx: commands.Context,
        name: str,
        url: str,
        notify_channel: Optional[discord.TextChannel] = None
    ):
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        check_id = await self.bot.db.save_host_check(
            guild_id=ctx.guild.id,
            url=url,
            name=name,
            check_type='http',
            check_interval=60,
            notify_channel_id=notify_channel.id if notify_channel else None
        )
        
        await ctx.send(
            embed=EmbedBuilder.success(
                "Host Added",
                f"Now monitoring **{name}** ({url})\n"
                f"Notifications: {notify_channel.mention if notify_channel else 'Disabled'}"
            )
        )
    
    @host.command(name="remove", aliases=["delete"], description="Remove a monitored host")
    @commands.has_permissions(manage_guild=True)
    async def remove_host(self, ctx: commands.Context, *, name: str):
        checks = await self.bot.db.get_host_checks(ctx.guild.id)
        
        for check in checks:
            if check.get('name', '').lower() == name.lower():
                await self.bot.db.delete_host_check(check.get('id'))
                return await ctx.send(
                    embed=EmbedBuilder.success("Host Removed", f"Stopped monitoring **{name}**")
                )
        
        await ctx.send(
            embed=EmbedBuilder.error("Not Found", f"No host named **{name}** found.")
        )
    
    @host.command(name="list", description="List all monitored hosts")
    async def list_hosts(self, ctx: commands.Context):
        checks = await self.bot.db.get_host_checks(ctx.guild.id)
        
        if not checks:
            return await ctx.send(
                embed=EmbedBuilder.info("No Hosts", "No hosts are being monitored in this server.")
            )
        
        description_lines = []
        for check in checks:
            status = check.get('last_status', 'unknown')
            status_emoji = "🟢" if status == 'online' else "🟡" if status == 'pending' else "🔴"
            
            description_lines.append(
                f"{status_emoji} **{check.get('name')}**\n"
                f"└ {check.get('url')}"
            )
        
        embed = (
            EmbedBuilder(
                title=f"Monitored Hosts ({len(checks)})",
                description="\n\n".join(description_lines)
            )
            .color(EmbedColor.INFO)
            .footer("Hosts are checked every minute")
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @host.command(name="check", description="Quick check a URL")
    async def quick_check(self, ctx: commands.Context, url: str):
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        msg = await ctx.send(
            embed=EmbedBuilder.info("Checking...", f"Checking {url}...")
        )
        
        start_time = datetime.now()
        status = await self._perform_check(url, 'http')
        response_time = (datetime.now() - start_time).total_seconds() * 1000
        
        if status == 'online':
            embed = (
                EmbedBuilder(
                    title="Host Online",
                    description=f"**{url}** is responding!"
                )
                .color(EmbedColor.SUCCESS)
                .field("Status", status, True)
                .field("Response Time", f"{response_time:.0f}ms", True)
                .build()
            )
        else:
            embed = (
                EmbedBuilder(
                    title="Host Down",
                    description=f"**{url}** is not responding."
                )
                .color(EmbedColor.ERROR)
                .field("Status", status, True)
                .field("Response Time", f"{response_time:.0f}ms", True)
                .build()
            )
        
        await msg.edit(embed=embed)
    
    @host.command(name="status", description="View all host statuses")
    async def host_status(self, ctx: commands.Context):
        checks = await self.bot.db.get_host_checks(ctx.guild.id)
        
        if not checks:
            return await ctx.send(
                embed=EmbedBuilder.info("No Hosts", "No hosts are being monitored in this server.")
            )
        
        online = sum(1 for c in checks if c.get('last_status') == 'online')
        offline = len(checks) - online
        
        embed = (
            EmbedBuilder(
                title="Host Status Overview"
            )
            .color(EmbedColor.SUCCESS if offline == 0 else EmbedColor.WARNING)
            .field("Total Hosts", str(len(checks)), True)
            .field("Online", f" {online}", True)
            .field("Offline", f" {offline}", True)
        )
        
        for check in checks[:10]:
            status = check.get('last_status', 'unknown')
            status_emoji = "🟢" if status == 'online' else "🔴"
            last_check = check.get('last_check', 'Never')
            
            embed.field(
                f"{status_emoji} {check.get('name')}",
                f"Status: {status}\nLast Check: {last_check}",
                True
            )
        
        await ctx.send(embed=embed.build())
    
    @host.command(name="nodes", aliases=["shards"], description="View bot node status")
    async def node_status(self, ctx: commands.Context):
        nodes = await self.bot.db.get_all_node_status()
        
        if not nodes:
            if self.bot.shard_id is not None:
                nodes = [{
                    'shard_id': self.bot.shard_id,
                    'status': 'online',
                    'latency': self.bot.latency * 1000 if self.bot.latency else 0,
                    'guild_count': len(self.bot.guilds),
                    'memory_mb': 0
                }]
            else:
                nodes = [{
                    'shard_id': 0,
                    'status': 'online',
                    'latency': self.bot.latency * 1000 if self.bot.latency else 0,
                    'guild_count': len(self.bot.guilds),
                    'memory_mb': 0
                }]
        
        embed = (
            EmbedBuilder(
                title="Bot Node Status"
            )
            .color(EmbedColor.INFO)
        )
        
        total_guilds = 0
        total_members = 0
        
        for node in nodes:
            shard_id = node.get('shard_id', 0)
            status = node.get('status', 'unknown')
            latency = node.get('latency', 0)
            guild_count = node.get('guild_count', 0)
            member_count = node.get('member_count', 0)
            memory_mb = node.get('memory_mb', 0)
            
            total_guilds += guild_count
            total_members += member_count
            
            status_emoji = "🟢" if status in ['online', 'ready'] else "🔴"
            
            embed.field(
                f"{status_emoji} Shard {shard_id}",
                f"Status: {status}\n"
                f"Latency: {latency:.0f}ms\n"
                f"Guilds: {guild_count}\n"
                f"Memory: {memory_mb:.1f}MB",
                True
            )
        
        embed.field("Total", f"Guilds: {total_guilds}\nMembers: {total_members}", False)
        
        await ctx.send(embed=embed.build())


async def setup(bot: commands.Bot):
    await bot.add_cog(HostCheckCog(bot))
