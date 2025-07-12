import os
import asyncio
import psycopg2
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv


# Load environment variables
load_dotenv(dotenv_path="../.env")

# Async function to scrape messages
async def scrape_channel(channel_username, limit=30):
    client = TelegramClient("async_session", os.getenv("tg_api_id"), os.getenv("tg_api_hash"))

    try:
        await client.start()
        print(f"Connected. Scraping messages from {channel_username}...")

        async for message in client.iter_messages(channel_username,limit=limit):
            if message.message:  # only if it's a text message
                print(f"[{message.date.strftime('%Y-%m-%d %H:%M:%S')}]{message.message[:80]}")

    except SessionPasswordNeededError:
        print("Two-step verification is enabled. Please configure it.")
    except Exception as e:
        print(f"Error scraping channel: {e}")
    finally:
        await client.disconnect()
        print("Disconnected from Telegram.")

# PostgreSQL connection
def get_pgsql_connection():
    try:
        conn = psycopg2.connect(
          host=os.getenv("pgsql_host=localhost"),
          port=os.getenv("pgsql_port"),
          database= os.getenv("pgsql_db"),
          user=os.getenv("pgsql_user"),
          password=os.getenv("pgsql_pass")
        )
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None