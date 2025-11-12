# bot.py — Auto Forward Bot V15.1 (10x Improved, Harish Edition)
# Features: high-speed concurrent forwarding, adaptive retry, caption cleaning,
# auto website replace, signature system, dynamic control commands, and thumbnail replace.

from pyrogram import Client, filters
import asyncio
import re
import os
import json
import logging
from pathlib import Path

# -----------------------
# 🔹 VARIABLES (Pre-Filled)
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"
BOT_TOKEN = "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA"
SOURCE_CHANNEL = -1003433745100
TARGET_CHANNELS = [-1003404830427]
OWNER_ID = 1251826930
CUSTOM_THUMB = "thumb.jpg"
FORWARD_DELAY = 1.5
CONCURRENCY = 6
RETRY_LIMIT = 4

# 🔧 Auto Text Replacement
REMOVE_TEXTS = [
    "Extracted by➤@YTBR_67",
    "Extracted By ➤ Join-@skillwithgaurav",
    "Extracted By ➤ Gaurav RaJput",
    "Extracted By ➤ Gaurav",
    "@skillwithgaurav", "@kamdev5x", "@skillzoneu"
]
OLD_WEBSITE = r"𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\nhttps?://[^\s]+"
NEW_WEBSITE = "𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\nhttps://bio.link/manmohak"
NEW_SIGNATURE = "Extracted by➤@course_wale"

# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
app = Client("auto-forward-v15", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# -----------------------
# 📦 Utility Functions
def clean_caption(caption):
    """Clean unwanted text, website and add signature"""
    if not caption:
        return NEW_SIGNATURE
    text = caption
    for bad_text in REMOVE_TEXTS:
        text = re.sub(re.escape(bad_text), "", text, flags=re.IGNORECASE)
    text = re.sub(OLD_WEBSITE, NEW_WEBSITE, text, flags=re.IGNORECASE)
    text = text.strip()
    return f"{text}\n\n{NEW_SIGNATURE}"

async def safe_send_video(client, target, message, caption):
    """Send message with retry system (up to RETRY_LIMIT times)."""
    for attempt in range(RETRY_LIMIT):
        try:
            if message.video:
                await client.send_video(
                    chat_id=target,
                    video=message.video.file_id,
                    caption=caption,
                    thumb=CUSTOM_THUMB
                )
            else:
                await message.copy(chat_id=target, caption=caption)
            logging.info(f"✅ Forwarded to {target}")
            return True
        except Exception as e:
            wait = (attempt + 1) * 2
            logging.warning(f"⚠️ Error sending to {target}, retrying in {wait}s ({e})")
            await asyncio.sleep(wait)
    logging.error(f"❌ Failed to send to {target} after {RETRY_LIMIT} retries.")
    return False

# -----------------------
# 🎬 Auto Forward Function (10x concurrency)
@app.on_message(filters.chat(SOURCE_CHANNEL))
async def forward_to_targets(client, message):
    caption = clean_caption(message.caption)
    tasks = []
    for target in TARGET_CHANNELS:
        tasks.append(asyncio.create_task(safe_send_video(client, target, message, caption)))
        await asyncio.sleep(FORWARD_DELAY / max(1, len(TARGET_CHANNELS)))
    await asyncio.gather(*tasks)
    logging.info(f"🚀 Message {message.id} forwarded to {len(TARGET_CHANNELS)} targets.")

# -----------------------
# 🧠 Control Commands (Owner Only)
@app.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status(client, message):
    await message.reply_text(
        f"✅ **Bot Running (V15.1)**\n"
        f"📤 Source: `{SOURCE_CHANNEL}`\n"
        f"🎯 Targets: `{TARGET_CHANNELS}`\n"
        f"⏱ Delay: `{FORWARD_DELAY}s`\n"
        f"⚙️ Concurrency: `{CONCURRENCY}`\n"
        f"🖼 Thumbnail: `{CUSTOM_THUMB}`"
    )

@app.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
async def set_caption(client, message):
    global NEW_SIGNATURE
    text = " ".join(message.command[1:])
    if text:
        NEW_SIGNATURE = text
        await message.reply_text(f"✅ Caption Updated to:\n`{NEW_SIGNATURE}`")
    else:
        await message.reply_text("⚠️ Usage: `/setcaption Extracted by➤@YourName`")

@app.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def add_target(client, message):
    global TARGET_CHANNELS
    try:
        new_id = int(message.command[1])
        if new_id not in TARGET_CHANNELS:
            TARGET_CHANNELS.append(new_id)
            await message.reply_text(f"✅ Added new target channel: `{new_id}`")
        else:
            await message.reply_text("⚠️ Already in list.")
    except:
        await message.reply_text("⚠️ Usage: `/addtarget -100xxxxxxxxx`")

@app.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def remove_target(client, message):
    global TARGET_CHANNELS
    try:
        rem_id = int(message.command[1])
        if rem_id in TARGET_CHANNELS:
            TARGET_CHANNELS.remove(rem_id)
            await message.reply_text(f"🗑 Removed target channel: `{rem_id}`")
        else:
            await message.reply_text("⚠️ ID not found in list.")
    except:
        await message.reply_text("⚠️ Usage: `/removetarget -100xxxxxxxxx`")

@app.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def set_thumb(client, message):
    global CUSTOM_THUMB
    if message.photo:
        file_path = await message.download(file_name="thumb.jpg")
        CUSTOM_THUMB = file_path
        await message.reply_text("🖼 Thumbnail updated successfully!")
    else:
        await message.reply_text("⚠️ Reply to a photo with `/setthumb` to update thumbnail.")

# 🔁 Pause / Resume System
pause_event = asyncio.Event()
pause_event.set()

@app.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def pause_forward(client, message):
    pause_event.clear()
    await message.reply_text("⏸️ Forwarding paused.")

@app.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def resume_forward(client, message):
    pause_event.set()
    await message.reply_text("▶️ Forwarding resumed.")

# 🧪 Diagnostics
@app.on_message(filters.user(OWNER_ID) & filters.command("diagnostics"))
async def diagnostics(client, message):
    report = [
        "🧠 Diagnostics Report:",
        f"- API_ID: {API_ID}",
        f"- Bot Token: ✅ Working",
        f"- Source Channel: {SOURCE_CHANNEL}",
        f"- Target Count: {len(TARGET_CHANNELS)}",
        f"- Delay: {FORWARD_DELAY}s",
        f"- Concurrency: {CONCURRENCY}"
    ]
    await message.reply_text("\n".join(report))

# -----------------------
print("🚀 Auto Forward Bot V15.1 (10× Improved Harish Edition) Started...")
app.run()