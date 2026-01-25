"""
Leveling System Cog
XP and level progression based on chat activity
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import random
from datetime import datetime, date

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class LevelingCog(commands.Cog, name="Leveling"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.xp_min = 15
        self.xp_max = 25
        self.daily_message_limit = 15
        self._cooldowns: dict = {}
    
    def _xp_for_level(self, level: int) -> int:
        return int(5 * (level ** 2) + 50 * level + 100)
    
    def _get_random_xp(self) -> int:
        return random.randint(self.xp_min, self.xp_max)
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        
        if message.content.startswith('!') or message.content.startswith('/'):
            return
        
        user_id = message.author.id
        guild_id = message.guild.id
        today = date.today().isoformat()
        
        try:
            level_data = await self.bot.db.get_user_level(user_id, guild_id)
            
            if level_data is None:
                level_data = {
                    'xp': 0,
                    'level': 0,
                    'total_xp': 0,
                    'daily_messages': 0,
                    'last_xp_date': today
                }
            
            if level_data.get('last_xp_date') != today:
                level_data['daily_messages'] = 0
                level_data['last_xp_date'] = today
            
            if level_data['daily_messages'] >= self.daily_message_limit:
                return
            
            level_data['daily_messages'] += 1
            
            xp_gained = self._get_random_xp()
            level_data['xp'] += xp_gained
            level_data['total_xp'] += xp_gained
            
            current_level = level_data['level']
            xp_needed = self._xp_for_level(current_level + 1)
            
            leveled_up = False
            while level_data['xp'] >= xp_needed:
                level_data['xp'] -= xp_needed
                level_data['level'] += 1
                leveled_up = True
                xp_needed = self._xp_for_level(level_data['level'] + 1)
            
            await self.bot.db.save_user_level(
                user_id=user_id,
                guild_id=guild_id,
                xp=level_data['xp'],
                level=level_data['level'],
                total_xp=level_data['total_xp'],
                daily_messages=level_data['daily_messages'],
                last_xp_date=level_data['last_xp_date']
            )
            
            if leveled_up:
                embed = (
                    EmbedBuilder(
                        title="Level Up!",
                        description=f"Congratulations {message.author.mention}! You reached **Level {level_data['level']}**!"
                    )
                    .color(EmbedColor.SUCCESS)
                    .thumbnail(message.author.display_avatar.url)
                    .field("Total XP", f"{level_data['total_xp']:,}", True)
                    .field("Next Level", f"{level_data['xp']:,}/{xp_needed:,} XP", True)
                    .build()
                )
                
                try:
                    await message.channel.send(embed=embed, delete_after=30)
                except:
                    pass
        except Exception as e:
            pass
    
    @commands.hybrid_command(name="level", description="Check your or someone's level")
    async def level(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        user = user or ctx.author
        
        level_data = await self.bot.db.get_user_level(user.id, ctx.guild.id)
        
        if not level_data:
            return await ctx.send(
                embed=EmbedBuilder.info("No Data", f"{user.display_name} hasn't earned any XP yet.")
            )
        
        current_level = level_data.get('level', 0)
        current_xp = level_data.get('xp', 0)
        total_xp = level_data.get('total_xp', 0)
        daily_messages = level_data.get('daily_messages', 0)
        xp_needed = self._xp_for_level(current_level + 1)
        
        progress = (current_xp / xp_needed) * 100 if xp_needed > 0 else 0
        progress_bar = self._create_progress_bar(progress)
        
        embed = (
            EmbedBuilder(
                title=f"Level Stats: {user.display_name}",
                description=f"**Level {current_level}**\n{progress_bar}"
            )
            .color(user.color if user.color.value else EmbedColor.INFO.value)
            .thumbnail(user.display_avatar.url)
            .field("Current XP", f"{current_xp:,}/{xp_needed:,}", True)
            .field("Total XP", f"{total_xp:,}", True)
            .field("Daily Messages", f"{daily_messages}/{self.daily_message_limit}", True)
            .footer(f"Keep chatting to level up! (Max {self.daily_message_limit} messages/day count)")
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="rank", description="Alias for level command")
    async def rank(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        await self.level(ctx, user)
    
    @commands.hybrid_command(name="leaderboard", aliases=["lb", "top"], description="View the XP leaderboard")
    async def leaderboard(self, ctx: commands.Context, page: int = 1):
        page = max(1, page)
        per_page = 10
        
        leaderboard = await self.bot.db.get_level_leaderboard(ctx.guild.id, limit=100)
        
        if not leaderboard:
            return await ctx.send(
                embed=EmbedBuilder.info("No Data", "No one has earned XP yet in this server.")
            )
        
        total_pages = (len(leaderboard) + per_page - 1) // per_page
        page = min(page, total_pages)
        
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_entries = leaderboard[start_idx:end_idx]
        
        description_lines = []
        medals = ["🥇", "🥈", "🥉"]
        
        for i, entry in enumerate(page_entries, start=start_idx + 1):
            user_id = entry.get('user_id')
            level = entry.get('level', 0)
            total_xp = entry.get('total_xp', 0)
            
            try:
                member = ctx.guild.get_member(user_id) or await ctx.guild.fetch_member(user_id)
                name = member.display_name if member else f"User#{user_id}"
            except:
                name = f"User#{user_id}"
            
            if i <= 3:
                rank_display = medals[i-1]
            else:
                rank_display = f"**#{i}**"
            
            description_lines.append(
                f"{rank_display} {name}\n└ Level {level} | {total_xp:,} XP"
            )
        
        author_rank = None
        for i, entry in enumerate(leaderboard, 1):
            if entry.get('user_id') == ctx.author.id:
                author_rank = i
                break
        
        footer_text = f"Page {page}/{total_pages}"
        if author_rank:
            footer_text += f" | Your rank: #{author_rank}"
        
        embed = (
            EmbedBuilder(
                title=f"XP Leaderboard - {ctx.guild.name}",
                description="\n\n".join(description_lines)
            )
            .color(EmbedColor.PRIMARY)
            .footer(footer_text)
            .build()
        )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="xpinfo", description="Get information about the XP system")
    async def xpinfo(self, ctx: commands.Context):
        embed = (
            EmbedBuilder(
                title="XP System Information",
                description="Earn XP by chatting in the server!"
            )
            .color(EmbedColor.INFO)
            .field("XP per Message", f"{self.xp_min}-{self.xp_max} XP (random)", True)
            .field("Daily Limit", f"{self.daily_message_limit} messages/day", True)
            .field("Level Formula", "5 * level^2 + 50 * level + 100", False)
            .field("Example Levels", 
                   "Level 1: 155 XP\n"
                   "Level 5: 475 XP\n"
                   "Level 10: 1,100 XP\n"
                   "Level 20: 3,100 XP\n"
                   "Level 50: 15,100 XP", False)
            .footer("XP resets daily, but your total XP and level are permanent!")
            .build()
        )
        
        await ctx.send(embed=embed)
    
    def _create_progress_bar(self, percentage: float, length: int = 10) -> str:
        filled = int((percentage / 100) * length)
        empty = length - filled
        bar = "█" * filled + "░" * empty
        return f"`[{bar}]` {percentage:.1f}%"


async def setup(bot: commands.Bot):
    await bot.add_cog(LevelingCog(bot))
