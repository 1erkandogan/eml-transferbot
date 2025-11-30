import discord
from discord.ext import commands
from dotenv import load_dotenv
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import time
import os
from script import transfer_table_creator
from transfer_message import transfer_message

load_dotenv()
token = os.getenv('DISCORD_TOKEN')
channel_id = int(os.getenv('DISCORD_CHANNEL_ID'))
database = os.getenv('DATABASE_URL_LOCAL')
handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)

scheduler = AsyncIOScheduler()


async def send_daily_data():
    channel = bot.get_channel(channel_id)
    if not channel:
        print("Invalid channel ID.")
        return

    df = transfer_table_creator(debug = False, table_name = "transfertable", insert_type="append", ingestion=True, database = database)  # your pandas DataFrame
    text = transfer_message(df)
    if not text:
        return

    if len(text) > 2000:
        lines = text.split("\n")
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 2000:
                await channel.send(chunk.strip(), suppress_embeds=True)
                chunk = ""
            chunk += line + "\n"
        if chunk.strip():
            await channel.send(chunk.strip(), suppress_embeds=True)
    else:
        await channel.send(text, suppress_embeds=True)

# Run send_daily data on user command
@bot.command()
async def show_transfers(ctx):
    await send_daily_data()

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    # Run once immediately
    # await send_daily_data()

    scheduler.add_job(send_daily_data, trigger="cron", minute=0)  # every hour at minute 0, xx:00:00
    #scheduler.add_job(send_daily_data, trigger="interval", hours=0, minutes=0, seconds=30)  # adjust time as needed
    scheduler.start()

bot.run(token, log_handler=handler, log_level=logging.DEBUG)