"""
Secret Chat Cog
Private anonymous messaging through the bot
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import re

from src.utils.embed_builder import EmbedBuilder, EmbedColor


class SecretChatCog(commands.Cog, name="SecretChat"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._active_conversations: dict = {}
        self._auto_delete_keywords = ['already', 'xóa', 'delete', 'destroy']
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        
        if not isinstance(message.channel, discord.DMChannel):
            return
        
        if message.content.startswith('!'):
            return
        
        content_lower = message.content.lower()
        for keyword in self._auto_delete_keywords:
            if keyword in content_lower:
                try:
                    await message.delete()
                except:
                    pass
                return
        
        user_id = message.author.id
        
        if user_id in self._active_conversations:
            target_id = self._active_conversations[user_id]
            await self._relay_message(message, target_id)
    
    async def _relay_message(self, message: discord.Message, target_id: int):
        try:
            sender_data = await self.bot.db.get_secret_user(message.author.id)
            if not sender_data:
                await message.channel.send(
                    embed=EmbedBuilder.error(
                        "Not Registered",
                        "You need to register a nickname first using `!secret register <nickname>`"
                    )
                )
                return
            
            sender_nickname = sender_data.get('nickname', 'Anonymous')
            
            target_user = self.bot.get_user(target_id)
            if not target_user:
                try:
                    target_user = await self.bot.fetch_user(target_id)
                except:
                    await message.channel.send(
                        embed=EmbedBuilder.error("Error", "Could not find the user to send message to.")
                    )
                    return
            
            embed = (
                EmbedBuilder(
                    title="Secret Message",
                    description=message.content
                )
                .color(0x2F3136)
                .field("From", f"**{sender_nickname}**", True)
                .footer("Reply in this DM to respond. Type 'already' to delete messages.")
                .build()
            )
            
            try:
                await target_user.send(embed=embed)
                
                self._active_conversations[target_id] = message.author.id
                
                try:
                    await message.add_reaction("✅")
                except Exception:
                    pass
            except discord.Forbidden:
                await message.channel.send(
                    embed=EmbedBuilder.error(
                        "Cannot Send",
                        "The user has DMs disabled or has blocked the bot."
                    )
                )
        except Exception as e:
            await message.channel.send(
                embed=EmbedBuilder.error("Error", f"Failed to relay message: {str(e)}")
            )
    
    @commands.group(name="secret", description="Secret chat commands")
    async def secret(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            embed = (
                EmbedBuilder(
                    title="Secret Chat System",
                    description="Send anonymous messages through the bot!"
                )
                .color(0x2F3136)
                .field("Register", "`!secret register <nickname>` - Create your secret identity", False)
                .field("Connect", "`!secret connect <nickname>` - Start chatting with someone", False)
                .field("Disconnect", "`!secret disconnect` - End the current conversation", False)
                .field("Profile", "`!secret profile` - View your secret profile", False)
                .field("Delete", "`!secret delete` - Delete your secret account", False)
                .field("Auto-Delete", "Type 'already' in any message to auto-delete it", False)
                .footer("All messages are NOT logged for your privacy!")
                .build()
            )
            await ctx.send(embed=embed)
    
    @secret.command(name="register", description="Register a secret nickname")
    async def register(self, ctx: commands.Context, *, nickname: str):
        if isinstance(ctx.channel, discord.DMChannel):
            pass
        else:
            try:
                await ctx.message.delete()
            except:
                pass
        
        nickname = nickname.strip()
        
        if len(nickname) < 3 or len(nickname) > 20:
            return await ctx.author.send(
                embed=EmbedBuilder.error("Invalid Nickname", "Nickname must be 3-20 characters long.")
            )
        
        if not re.match(r'^[a-zA-Z0-9_]+$', nickname):
            return await ctx.author.send(
                embed=EmbedBuilder.error("Invalid Nickname", "Nickname can only contain letters, numbers, and underscores.")
            )
        
        existing = await self.bot.db.get_secret_user_by_nickname(nickname)
        if existing and existing.get('user_id') != ctx.author.id:
            return await ctx.author.send(
                embed=EmbedBuilder.error("Nickname Taken", "This nickname is already in use. Please choose another.")
            )
        
        await self.bot.db.save_secret_user(ctx.author.id, nickname)
        
        await ctx.author.send(
            embed=EmbedBuilder.success(
                "Registered!",
                f"Your secret nickname is now **{nickname}**.\n\n"
                "Use `!secret connect <target_nickname>` to start chatting with someone.\n"
                "Your real Discord username will never be shown!"
            )
        )
        
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("Check your DMs!", delete_after=5)
    
    @secret.command(name="connect", description="Connect to another secret user")
    async def connect(self, ctx: commands.Context, *, target_nickname: str):
        if not isinstance(ctx.channel, discord.DMChannel):
            try:
                await ctx.message.delete()
            except:
                pass
            await ctx.author.send(
                embed=EmbedBuilder.info("Use DMs", "Please use this command in DMs for privacy!")
            )
            return
        
        sender_data = await self.bot.db.get_secret_user(ctx.author.id)
        if not sender_data:
            return await ctx.send(
                embed=EmbedBuilder.error(
                    "Not Registered",
                    "You need to register first! Use `!secret register <nickname>`"
                )
            )
        
        target_data = await self.bot.db.get_secret_user_by_nickname(target_nickname)
        if not target_data:
            return await ctx.send(
                embed=EmbedBuilder.error(
                    "User Not Found",
                    f"No user with nickname **{target_nickname}** found."
                )
            )
        
        target_id = target_data.get('user_id')
        
        if target_id == ctx.author.id:
            return await ctx.send(
                embed=EmbedBuilder.error("Error", "You cannot connect to yourself!")
            )
        
        self._active_conversations[ctx.author.id] = target_id
        
        await ctx.send(
            embed=EmbedBuilder.success(
                "Connected!",
                f"You are now connected to **{target_nickname}**.\n\n"
                "Simply type your message here and it will be relayed anonymously.\n"
                "Type `!secret disconnect` to end the conversation.\n"
                "Type 'already' to auto-delete any message."
            )
        )
    
    @secret.command(name="disconnect", description="End the current conversation")
    async def disconnect(self, ctx: commands.Context):
        if ctx.author.id in self._active_conversations:
            del self._active_conversations[ctx.author.id]
            await ctx.send(
                embed=EmbedBuilder.success("Disconnected", "Your secret conversation has ended.")
            )
        else:
            await ctx.send(
                embed=EmbedBuilder.info("No Connection", "You are not connected to anyone.")
            )
    
    @secret.command(name="profile", description="View your secret profile")
    async def profile(self, ctx: commands.Context):
        if not isinstance(ctx.channel, discord.DMChannel):
            try:
                await ctx.message.delete()
            except:
                pass
        
        user_data = await self.bot.db.get_secret_user(ctx.author.id)
        
        if not user_data:
            return await ctx.author.send(
                embed=EmbedBuilder.info(
                    "Not Registered",
                    "You don't have a secret profile yet.\nUse `!secret register <nickname>` to create one!"
                )
            )
        
        connected_to = "No one"
        if ctx.author.id in self._active_conversations:
            target_id = self._active_conversations[ctx.author.id]
            target_data = await self.bot.db.get_secret_user(target_id)
            if target_data:
                connected_to = target_data.get('nickname', 'Unknown')
        
        embed = (
            EmbedBuilder(
                title="Your Secret Profile"
            )
            .color(0x2F3136)
            .field("Nickname", user_data.get('nickname', 'Unknown'), True)
            .field("Connected To", connected_to, True)
            .field("Status", "Active" if user_data.get('is_active', True) else "Inactive", True)
            .footer("Your real identity is always hidden!")
            .build()
        )
        
        await ctx.author.send(embed=embed)
        
        if not isinstance(ctx.channel, discord.DMChannel):
            await ctx.send("Check your DMs!", delete_after=5)
    
    @secret.command(name="delete", description="Delete your secret account")
    async def delete_account(self, ctx: commands.Context):
        user_data = await self.bot.db.get_secret_user(ctx.author.id)
        
        if not user_data:
            return await ctx.send(
                embed=EmbedBuilder.info("No Account", "You don't have a secret account to delete.")
            )
        
        if ctx.author.id in self._active_conversations:
            del self._active_conversations[ctx.author.id]
        
        await self.bot.db.delete_secret_user(ctx.author.id)
        
        await ctx.author.send(
            embed=EmbedBuilder.success(
                "Account Deleted",
                "Your secret account has been permanently deleted.\n"
                "You can register again anytime with `!secret register <nickname>`"
            )
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(SecretChatCog(bot))
