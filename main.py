"""
Discord Security Bot with Sharding Support
Main entry point for the Discord bot
"""

import os
import sys
import asyncio
import logging
import psutil
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from src.db_manager import DatabaseManager
from src.cogs import (
    WelcomeCog,
    ModerationCog,
    TicketsCog,
    AntiRaidCog,
    AntiNukeCog,
    FilterCog,
    LoggingCog,
    AutoModCog,
    AdminCog,
    UtilityCog
)

load_dotenv()

os.makedirs('data/logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data/logs/bot.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger('discord_bot')


class SecurityBot(commands.AutoShardedBot):
    def __init__(self, shard_count: Optional[int] = None, shard_ids: Optional[list] = None):
        intents = discord.Intents.all()
        
        super().__init__(
            command_prefix=commands.when_mentioned_or('!'),
            intents=intents,
            help_command=None,
            case_insensitive=True,
            shard_count=shard_count,
            shard_ids=shard_ids
        )
        
        self.db: DatabaseManager = DatabaseManager()
        self.start_time: datetime = datetime.now()
        self.version: str = "2.0.0"
        self._metrics_task: Optional[asyncio.Task] = None
    
    async def setup_hook(self):
        logger.info("Initializing database connection...")
        await self.db.initialize()
        logger.info("Database connection established")
        
        logger.info("Loading cogs...")
        cogs = [
            WelcomeCog(self),
            ModerationCog(self),
            TicketsCog(self),
            AntiRaidCog(self),
            AntiNukeCog(self),
            FilterCog(self),
            LoggingCog(self),
            AutoModCog(self),
            AdminCog(self),
            UtilityCog(self)
        ]
        
        for cog in cogs:
            await self.add_cog(cog)
            logger.info(f"Loaded cog: {cog.__class__.__name__}")
        
        try:
            from src.cogs.leveling import LevelingCog
            await self.add_cog(LevelingCog(self))
            logger.info("Loaded cog: LevelingCog")
        except ImportError:
            logger.warning("LevelingCog not found, skipping...")
        
        try:
            from src.cogs.secret_chat import SecretChatCog
            await self.add_cog(SecretChatCog(self))
            logger.info("Loaded cog: SecretChatCog")
        except ImportError:
            logger.warning("SecretChatCog not found, skipping...")
        
        try:
            from src.cogs.host_check import HostCheckCog
            await self.add_cog(HostCheckCog(self))
            logger.info("Loaded cog: HostCheckCog")
        except ImportError:
            logger.warning("HostCheckCog not found, skipping...")
        
        try:
            from src.cogs.metrics import MetricsCog
            await self.add_cog(MetricsCog(self))
            logger.info("Loaded cog: MetricsCog")
        except ImportError:
            logger.warning("MetricsCog not found, skipping...")
        
        logger.info(f"Loaded {len(self.cogs)} cogs successfully")
        
        logger.info("Syncing slash commands...")
        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} slash command(s) globally")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}")
    
    async def on_ready(self):
        logger.info(f"Bot is ready!")
        if self.user:
            logger.info(f"Logged in as: {self.user} (ID: {self.user.id})")
        
        if self.shard_count:
            logger.info(f"Running with {self.shard_count} shard(s)")
            if self.shard_ids:
                logger.info(f"Shard IDs: {self.shard_ids}")
        
        logger.info(f"Connected to {len(self.guilds)} guild(s)")
        logger.info(f"Discord.py version: {discord.__version__}")
        
        shard_info = ""
        if self.shard_id is not None:
            shard_info = f" | Shard {self.shard_id}"
        
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"{len(self.guilds)} servers{shard_info}"
            ),
            status=discord.Status.online
        )
        
        self._start_heartbeat()
    
    async def on_shard_ready(self, shard_id: int):
        logger.info(f"Shard {shard_id} is ready")
        await self._report_shard_status(shard_id, "ready")
    
    async def on_shard_connect(self, shard_id: int):
        logger.info(f"Shard {shard_id} connected")
        await self._report_shard_status(shard_id, "connected")
    
    async def on_shard_disconnect(self, shard_id: int):
        logger.warning(f"Shard {shard_id} disconnected")
        await self._report_shard_status(shard_id, "disconnected")
    
    async def on_shard_resumed(self, shard_id: int):
        logger.info(f"Shard {shard_id} resumed")
        await self._report_shard_status(shard_id, "resumed")
    
    async def _report_shard_status(self, shard_id: int, status: str):
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent()
            
            shard_guilds = [g for g in self.guilds if g.shard_id == shard_id]
            guild_count = len(shard_guilds)
            member_count = sum(g.member_count or 0 for g in shard_guilds)
            
            uptime = (datetime.now() - self.start_time).total_seconds()
            
            latency = 0.0
            if self.shards and shard_id in self.shards:
                shard = self.shards[shard_id]
                latency = shard.latency * 1000 if shard.latency else 0.0
            
            await self.db.save_node_status(
                shard_id=shard_id,
                status=status,
                latency=latency,
                guild_count=guild_count,
                member_count=member_count,
                uptime_seconds=int(uptime),
                memory_mb=memory_mb,
                cpu_percent=cpu_percent,
                version=self.version
            )
        except Exception as e:
            logger.error(f"Failed to report shard status: {e}")
    
    def _start_heartbeat(self):
        @tasks.loop(seconds=30)
        async def heartbeat():
            if self.shard_ids:
                for shard_id in self.shard_ids:
                    await self._report_shard_status(shard_id, "online")
            elif self.shard_id is not None:
                await self._report_shard_status(self.shard_id, "online")
            else:
                await self._report_shard_status(0, "online")
        
        heartbeat.start()
    
    async def on_guild_join(self, guild: discord.Guild):
        logger.info(f"Joined guild: {guild.name} (ID: {guild.id})")
        await self.db.get_or_create_guild_config(guild.id)
    
    async def on_guild_remove(self, guild: discord.Guild):
        logger.info(f"Left guild: {guild.name} (ID: {guild.id})")
    
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if isinstance(error, commands.CommandNotFound):
            return
        
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have permission to use this command.", delete_after=10)
            return
        
        if isinstance(error, commands.BotMissingPermissions):
            await ctx.send("I don't have the required permissions to execute this command.", delete_after=10)
            return
        
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: `{error.param.name}`", delete_after=10)
            return
        
        if isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument provided.", delete_after=10)
            return
        
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"Command on cooldown. Try again in {error.retry_after:.1f}s", delete_after=5)
            return
        
        logger.error(f"Command error in {ctx.command}: {error}", exc_info=error)
    
    async def close(self):
        logger.info("Shutting down bot...")
        await self.db.close()
        await super().close()


async def main():
    os.makedirs('data/logs', exist_ok=True)
    os.makedirs('data/db', exist_ok=True)
    os.makedirs('data/backups', exist_ok=True)
    os.makedirs('data/guilds', exist_ok=True)
    
    token = os.getenv('DISCORD_TOKEN')
    
    if not token:
        logger.error("DISCORD_TOKEN not found in environment variables!")
        logger.error("Please set your Discord bot token in the Secrets tab.")
        sys.exit(1)
    
    shard_count = os.getenv('SHARD_COUNT')
    shard_ids_str = os.getenv('SHARD_IDS')
    
    shard_count_int = int(shard_count) if shard_count else None
    shard_ids = None
    if shard_ids_str:
        shard_ids = [int(x.strip()) for x in shard_ids_str.split(',')]
    
    if shard_count_int:
        logger.info(f"Starting bot with {shard_count_int} shards")
        if shard_ids:
            logger.info(f"Running shards: {shard_ids}")
    else:
        logger.info("Starting bot with automatic sharding")
    
    bot = SecurityBot(shard_count=shard_count_int, shard_ids=shard_ids)
    
    try:
        logger.info("Starting bot...")
        await bot.start(token)
    except discord.LoginFailure:
        logger.error("Invalid Discord token! Please check your DISCORD_TOKEN.")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if not bot.is_closed():
            await bot.close()


if __name__ == '__main__':
    asyncio.run(main())
