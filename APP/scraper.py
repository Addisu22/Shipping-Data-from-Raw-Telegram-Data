# app/scraper.py

import os
import json
import logging
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(
    filename="logs/scraper.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

client = TelegramClient("telegram_scraper_session", API_ID, API_HASH)


async def scrape_channel(channel_username, limit=50):
    await client.start(PHONE)
    logging.info(f"Connected to Telegram. Scraping: {channel_username}")

    try:
        messages = []
        async for message in client.iter_messages(channel_username, limit=limit):
            msg_data = {
                "id": message.id,
                "date": message.date.isoformat(),
                "text": message.text,
                "has_media": bool(message.media),
                "sender_id": message.sender_id,
                "channel": channel_username
            }

            # Download media (image) if exists
            if message.media:
                date_folder = datetime.now().strftime("%Y-%m-%d")
                save_dir = f"data/raw/images/{channel_username.strip('@')}/{date_folder}"
                os.makedirs(save_dir, exist_ok=True)
                file_path = await client.download_media(message, file=save_dir)
                msg_data["media_path"] = file_path

            messages.append(msg_data)

        # Save to data lake as JSON
        date_folder = datetime.now().strftime("%Y-%m-%d")
        out_dir = f"data/raw/telegram_messages/{date_folder}"
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, f"{channel_username.strip('@')}.json")

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)

        logging.info(f"Saved {len(messages)} messages to {out_file}")

    except Exception as e:
        logging.error(f"Error scraping {channel_username}: {e}")
    finally:
        await client.disconnect()
        logging.info(f"Disconnected from Telegram: {channel_username}")
