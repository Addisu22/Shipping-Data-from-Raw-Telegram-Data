import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_pgsql_connection():
    return psycopg2.connect(
        host=os.getenv("pgsql_host"),
        port=os.getenv("pgsql_port"),
        database=os.getenv("pgsql_db"),
        user=os.getenv("pgsql_user"),
        password=os.getenv("pgsql_pass")
    )

def load_json_to_raw_table(json_path):
    conn = get_pgsql_connection()
    cur = conn.cursor()

    # Create schema and table if not exists
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            message_id INTEGER,
            sender TEXT,
            date TIMESTAMP,
            content TEXT,
            media_url TEXT,
            channel_name TEXT
        );
    """)

    with open(json_path, 'r', encoding='utf-8') as f:
        messages = json.load(f)

    for msg in messages:
        cur.execute("""
            INSERT INTO raw.telegram_messages (message_id, sender, date, content, media_url, channel_name)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            msg.get("message_id"),
            msg.get("sender"),
            msg.get("date"),
            msg.get("content"),
            msg.get("media_url"),
            msg.get("channel_name")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Loaded raw messages into raw.telegram_messages")

# Example usage
load_json_to_raw_table("data/raw/telegram_messages/2025-07-10/lobelia4cosmetics.json")
