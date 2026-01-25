"""
Ticket System Cog
Complete ticket management system with categories, claiming, and transcripts
"""

import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional
import asyncio
from datetime import datetime
import io

from src.utils.embed_builder import EmbedBuilder, EmbedColor
from src.utils.helpers import generate_id
from src.models.ticket import Ticket, TicketStatus, TicketPriority, TicketCategory


class TicketView(discord.ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    
    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="ticket:close", emoji="🔒")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        ticket = await self.bot.db.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            return await interaction.followup.send("This is not a ticket channel.", ephemeral=True)
        
        if ticket.status == TicketStatus.CLOSED:
            return await interaction.followup.send("This ticket is already closed.", ephemeral=True)
        
        ticket.close(interaction.user.id, "Closed via button")
        await self.bot.db.save_ticket(ticket)
        
        embed = EmbedBuilder.ticket_close(interaction.user, ticket.ticket_id)
        await interaction.channel.send(embed=embed)
        
        await asyncio.sleep(5)
        
        try:
            await interaction.channel.delete(reason=f"Ticket closed by {interaction.user}")
        except discord.Forbidden:
            await interaction.channel.send("I don't have permission to delete this channel.")
    
    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, custom_id="ticket:claim", emoji="📝")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        ticket = await self.bot.db.get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            return await interaction.followup.send("This is not a ticket channel.", ephemeral=True)
        
        guild_config = await self.bot.db.get_guild_config(interaction.guild.id)
        if not guild_config:
            return await interaction.followup.send("Configuration not found.", ephemeral=True)
        
        support_roles = guild_config.settings.tickets.support_role_ids
        user_roles = [r.id for r in interaction.user.roles]
        
        if not any(role_id in user_roles for role_id in support_roles):
            if not interaction.user.guild_permissions.manage_channels:
                return await interaction.followup.send("You don't have permission to claim tickets.", ephemeral=True)
        
        if ticket.claimed_by:
            if ticket.claimed_by == interaction.user.id:
                ticket.unclaim()
                await self.bot.db.save_ticket(ticket)
                await interaction.followup.send(f"You unclaimed this ticket.", ephemeral=True)
                await interaction.channel.send(f"📝 {interaction.user.mention} unclaimed this ticket.")
            else:
                await interaction.followup.send(f"This ticket is already claimed by <@{ticket.claimed_by}>.", ephemeral=True)
        else:
            ticket.claim(interaction.user.id)
            await self.bot.db.save_ticket(ticket)
            await interaction.followup.send(f"You claimed this ticket.", ephemeral=True)
            await interaction.channel.send(f"📝 {interaction.user.mention} claimed this ticket.")


class CreateTicketView(discord.ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot
    
    @discord.ui.button(label="Create Ticket", style=discord.ButtonStyle.success, custom_id="ticket:create", emoji="🎫")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        guild_config = await self.bot.db.get_guild_config(interaction.guild.id)
        if not guild_config:
            return await interaction.followup.send("Ticket system is not configured.", ephemeral=True)
        
        settings = guild_config.settings.tickets
        if not settings.enabled:
            return await interaction.followup.send("Ticket system is disabled.", ephemeral=True)
        
        user_tickets = await self.bot.db.get_user_open_tickets(interaction.guild.id, interaction.user.id)
        if len(user_tickets) >= settings.max_open_tickets:
            return await interaction.followup.send(
                f"You already have {len(user_tickets)} open ticket(s). Maximum is {settings.max_open_tickets}.",
                ephemeral=True
            )
        
        ticket_id = generate_id("TICKET")
        ticket_number = guild_config.get_next_ticket_id()
        
        category = None
        if settings.category_id:
            category = interaction.guild.get_channel(settings.category_id)
        
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True),
            interaction.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True)
        }
        
        for role_id in settings.support_role_ids:
            role = interaction.guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        try:
            channel = await interaction.guild.create_text_channel(
                name=f"ticket-{ticket_number}",
                category=category,
                overwrites=overwrites,
                reason=f"Ticket created by {interaction.user}"
            )
        except discord.Forbidden:
            return await interaction.followup.send("I don't have permission to create channels.", ephemeral=True)
        
        ticket = Ticket(
            ticket_id=ticket_id,
            guild_id=interaction.guild.id,
            channel_id=channel.id,
            creator_id=interaction.user.id
        )
        
        await self.bot.db.save_ticket(ticket)
        await self.bot.db.save_guild_config(guild_config)
        
        embed = EmbedBuilder.ticket_create(interaction.user, "General", ticket_id)
        view = TicketView(self.bot)
        
        await channel.send(content=interaction.user.mention, embed=embed, view=view)
        await interaction.followup.send(f"Ticket created! {channel.mention}", ephemeral=True)


class TicketsCog(commands.Cog, name="Tickets"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.bot.add_view(TicketView(bot))
        self.bot.add_view(CreateTicketView(bot))
    
    @commands.hybrid_group(name="ticket", description="Ticket system commands")
    async def ticket(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            embed = (
                EmbedBuilder(
                    title="🎫 Ticket System",
                    description="Use the subcommands to manage tickets."
                )
                .color(EmbedColor.TICKET)
                .field("Create", "`/ticket create` - Create a new ticket", False)
                .field("Close", "`/ticket close [reason]` - Close the current ticket", False)
                .field("Add", "`/ticket add <user>` - Add a user to the ticket", False)
                .field("Remove", "`/ticket remove <user>` - Remove a user from the ticket", False)
                .build()
            )
            await ctx.send(embed=embed)
    
    @ticket.command(name="create", description="Create a new ticket")
    async def ticket_create(self, ctx: commands.Context):
        await ctx.defer()
        guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
        if not guild_config:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Ticket system is not configured."))
        
        settings = guild_config.settings.tickets
        if not settings.enabled:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Ticket system is disabled."))
        
        user_tickets = await self.bot.db.get_user_open_tickets(ctx.guild.id, ctx.author.id)
        if len(user_tickets) >= settings.max_open_tickets:
            return await ctx.send(embed=EmbedBuilder.error(
                "Limit Reached",
                f"You already have {len(user_tickets)} open ticket(s). Maximum is {settings.max_open_tickets}."
            ))
        
        ticket_id = generate_id("TICKET")
        ticket_number = guild_config.get_next_ticket_id()
        
        category = None
        if settings.category_id:
            category = ctx.guild.get_channel(settings.category_id)
        
        overwrites = {
            ctx.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            ctx.author: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True),
            ctx.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True)
        }
        
        for role_id in settings.support_role_ids:
            role = ctx.guild.get_role(role_id)
            if role:
                overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        
        try:
            channel = await ctx.guild.create_text_channel(
                name=f"ticket-{ticket_number}",
                category=category,
                overwrites=overwrites,
                reason=f"Ticket created by {ctx.author}"
            )
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to create channels."))
        
        ticket = Ticket(
            ticket_id=ticket_id,
            guild_id=ctx.guild.id,
            channel_id=channel.id,
            creator_id=ctx.author.id
        )
        
        await self.bot.db.save_ticket(ticket)
        await self.bot.db.save_guild_config(guild_config)
        
        embed = EmbedBuilder.ticket_create(ctx.author, "General", ticket_id)
        view = TicketView(self.bot)
        
        await channel.send(content=ctx.author.mention, embed=embed, view=view)
        await ctx.send(embed=EmbedBuilder.success("Ticket Created", f"Your ticket has been created: {channel.mention}"))
    
    @ticket.command(name="close", description="Close the current ticket")
    async def ticket_close(self, ctx: commands.Context, *, reason: str = "No reason provided"):
        await ctx.defer()
        ticket = await self.bot.db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This is not a ticket channel."))
        
        if ticket.status == TicketStatus.CLOSED:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This ticket is already closed."))
        
        if ticket.creator_id != ctx.author.id:
            if not ctx.author.guild_permissions.manage_channels:
                guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
                support_roles = guild_config.settings.tickets.support_role_ids if guild_config else []
                user_roles = [r.id for r in ctx.author.roles]
                if not any(role_id in user_roles for role_id in support_roles):
                    return await ctx.send(embed=EmbedBuilder.error("Error", "You don't have permission to close this ticket."))
        
        ticket.close(ctx.author.id, reason)
        await self.bot.db.save_ticket(ticket)
        
        embed = EmbedBuilder.ticket_close(ctx.author, ticket.ticket_id, reason)
        await ctx.send(embed=embed)
        
        await asyncio.sleep(5)
        
        try:
            await ctx.channel.delete(reason=f"Ticket closed by {ctx.author}")
        except discord.Forbidden:
            await ctx.send("I don't have permission to delete this channel.")
    
    @ticket.command(name="add", description="Add a user to the ticket")
    async def ticket_add(self, ctx: commands.Context, user: discord.Member):
        await ctx.defer()
        ticket = await self.bot.db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This is not a ticket channel."))
        
        try:
            await ctx.channel.set_permissions(user, view_channel=True, send_messages=True)
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to modify channel permissions."))
        
        if user.id not in ticket.added_users:
            ticket.added_users.append(user.id)
            await self.bot.db.save_ticket(ticket)
        
        await ctx.send(embed=EmbedBuilder.success("User Added", f"{user.mention} has been added to this ticket."))
    
    @ticket.command(name="remove", description="Remove a user from the ticket")
    async def ticket_remove(self, ctx: commands.Context, user: discord.Member):
        await ctx.defer()
        ticket = await self.bot.db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This is not a ticket channel."))
        
        if user.id == ticket.creator_id:
            return await ctx.send(embed=EmbedBuilder.error("Error", "Cannot remove the ticket creator."))
        
        try:
            await ctx.channel.set_permissions(user, overwrite=None)
        except discord.Forbidden:
            return await ctx.send(embed=EmbedBuilder.error("Error", "I don't have permission to modify channel permissions."))
        
        if user.id in ticket.added_users:
            ticket.added_users.remove(user.id)
            await self.bot.db.save_ticket(ticket)
        
        await ctx.send(embed=EmbedBuilder.success("User Removed", f"{user.mention} has been removed from this ticket."))
    
    @ticket.command(name="claim", description="Claim the current ticket")
    async def ticket_claim(self, ctx: commands.Context):
        await ctx.defer()
        ticket = await self.bot.db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This is not a ticket channel."))
        
        if ticket.claimed_by:
            if ticket.claimed_by == ctx.author.id:
                return await ctx.send(embed=EmbedBuilder.info("Already Claimed", "You have already claimed this ticket."))
            return await ctx.send(embed=EmbedBuilder.error("Error", f"This ticket is already claimed by <@{ticket.claimed_by}>."))
        
        ticket.claim(ctx.author.id)
        await self.bot.db.save_ticket(ticket)
        
        await ctx.send(embed=EmbedBuilder.success("Ticket Claimed", f"{ctx.author.mention} has claimed this ticket."))
    
    @ticket.command(name="unclaim", description="Unclaim the current ticket")
    async def ticket_unclaim(self, ctx: commands.Context):
        await ctx.defer()
        ticket = await self.bot.db.get_ticket_by_channel(ctx.channel.id)
        if not ticket:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This is not a ticket channel."))
        
        if not ticket.claimed_by:
            return await ctx.send(embed=EmbedBuilder.error("Error", "This ticket is not claimed."))
        
        if ticket.claimed_by != ctx.author.id and not ctx.author.guild_permissions.manage_channels:
            return await ctx.send(embed=EmbedBuilder.error("Error", "You can only unclaim tickets you have claimed."))
        
        ticket.unclaim()
        await self.bot.db.save_ticket(ticket)
        
        await ctx.send(embed=EmbedBuilder.success("Ticket Unclaimed", "This ticket has been unclaimed."))
    
    @commands.hybrid_group(name="ticketconfig", description="Configure the ticket system")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await ctx.defer()
            guild_config = await self.bot.db.get_guild_config(ctx.guild.id)
            if not guild_config:
                return await ctx.send("No configuration found.")
            
            settings = guild_config.settings.tickets
            
            embed = (
                EmbedBuilder(
                    title="🎫 Ticket Configuration",
                    description="Current ticket system settings"
                )
                .color(EmbedColor.TICKET)
                .field("Status", "✅ Enabled" if settings.enabled else "❌ Disabled", True)
                .field("Category", f"<#{settings.category_id}>" if settings.category_id else "Not set", True)
                .field("Max Open Tickets", str(settings.max_open_tickets), True)
                .field("Support Roles", str(len(settings.support_role_ids)) + " role(s)", True)
                .build()
            )
            
            await ctx.send(embed=embed)
    
    @ticketconfig.command(name="enable", description="Enable the ticket system")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig_enable(self, ctx: commands.Context):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.tickets.enabled = True
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Ticket System", "Ticket system has been enabled."))
    
    @ticketconfig.command(name="disable", description="Disable the ticket system")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig_disable(self, ctx: commands.Context):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.tickets.enabled = False
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Ticket System", "Ticket system has been disabled."))
    
    @ticketconfig.command(name="category", description="Set the ticket category")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig_category(self, ctx: commands.Context, category: discord.CategoryChannel):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        guild_config.settings.tickets.category_id = category.id
        await self.bot.db.save_guild_config(guild_config)
        
        await ctx.send(embed=EmbedBuilder.success("Ticket Category", f"Tickets will be created in {category.mention}"))
    
    @ticketconfig.command(name="supportrole", description="Add or remove a support role")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig_supportrole(self, ctx: commands.Context, action: str, role: discord.Role):
        await ctx.defer()
        guild_config = await self.bot.db.get_or_create_guild_config(ctx.guild.id)
        
        if action.lower() == "add":
            if role.id not in guild_config.settings.tickets.support_role_ids:
                guild_config.settings.tickets.support_role_ids.append(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Support Role", f"{role.mention} added as a support role."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Already Added", "This role is already a support role."))
        
        elif action.lower() == "remove":
            if role.id in guild_config.settings.tickets.support_role_ids:
                guild_config.settings.tickets.support_role_ids.remove(role.id)
                await self.bot.db.save_guild_config(guild_config)
                await ctx.send(embed=EmbedBuilder.success("Support Role", f"{role.mention} removed from support roles."))
            else:
                await ctx.send(embed=EmbedBuilder.warning("Not Found", "This role is not a support role."))
        
        else:
            await ctx.send(embed=EmbedBuilder.error("Invalid Action", "Use `add` or `remove`."))
    
    @ticketconfig.command(name="panel", description="Send a ticket creation panel")
    @commands.has_permissions(manage_guild=True)
    async def ticketconfig_panel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        await ctx.defer(ephemeral=True)
        channel = channel or ctx.channel
        
        embed = (
            EmbedBuilder(
                title="🎫 Support Tickets",
                description="Need help? Click the button below to create a support ticket.\n\nA staff member will assist you as soon as possible."
            )
            .color(EmbedColor.TICKET)
            .footer("Please be patient after creating a ticket.")
            .build()
        )
        
        view = CreateTicketView(self.bot)
        await channel.send(embed=embed, view=view)
        
        await ctx.send(embed=EmbedBuilder.success("Panel Sent", f"Ticket panel sent to {channel.mention}"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TicketsCog(bot))
