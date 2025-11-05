import discord
from discord.ext import commands
from dotenv import load_dotenv
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import time
import os
from script import transfer_table_creator  # adjust to your filename

load_dotenv()
token = os.getenv('DISCORD_TOKEN')
channel_id = int(os.getenv('DISCORD_CHANNEL_ID'))

handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()

bot = commands.Bot(command_prefix='!', intents=intents)

scheduler = AsyncIOScheduler()


async def send_daily_data():
    print(channel_id, type(channel_id))
    channel = bot.get_channel(channel_id)
    if not channel:
        print("Invalid channel ID.")
        return

    df = transfer_table_creator(debug = False, table_name = "transfertable", insert_type="append", ingestion=True, env="Local")  # your pandas DataFrame
    text = df.to_markdown(index=False)
    if len(text) > 1900:  # Discord message limit safeguard
        text = text[:1900] + "\n...(truncated)..."
    await channel.send(f"**Daily Data Update**\n```{text}```")

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    # Run once immediately
    await send_daily_data()

    scheduler.add_job(send_daily_data, trigger="cron", hour=9, minute=0, second=0)  # adjust time as needed
    #scheduler.add_job(send_daily_data, trigger="interval", hours=0, minutes=0, seconds=30)  # adjust time as needed
    scheduler.start()


bot.run(token, log_handler=handler, log_level=logging.DEBUG)