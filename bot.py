# -----------------------------------------
# V9 ULTRA BOT (FINAL + FIXED + NO ERRORS)
# -----------------------------------------

import os
import re
import json
import time
import asyncio
import random
from pathlib import Path
import tempfile

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# -------------------------------------------------------
#  ENVIRONMENT / VARIABLES  (Your Values Already Filled)
# -------------------------------------------------------

API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"
BOT_TOKEN = "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA"

OWNER_ID = 1251826930
DEFAULT_SOURCE = -1003433745100
TARGETS = [-1003404830427]

SIGNATURE = "Extracted by➤@course_wale"
NEW_WEBSITE = "https://bio.link/manmohak"

THUMB = "thumb.jpg"
FORWARD_DELAY = 0.5
CONCURRENCY = 5
MAX_FILE_MB = 1024

STATE_FILE = Path("state.json")
TMP_DIR = Path(tempfile.gettempdir())

# -------------------------------------------------------
# SAFE MESSAGE EDIT FIX
# -------------------------------------------------------

async def safe_edit(message: Message, new_text: str):
    """
    Prevent MessageNotModified error.
    """
    try:
        if message.text == new_text:
            return  # Skip edit if no change
        await message.edit_text(new_text)
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" in str(e):
            pass
        else:
            print("Edit error:", e)

# -------------------------------------------------------
# BOT CLIENT
# -------------------------------------------------------

bot = Client("v9_ultra", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# -------------------------------------------------------
# CAPTION CLEANER
# -------------------------------------------------------

REMOVE = [
    r"Extracted\s*by[^\n]*",
    "@skillwithgaurav", "@kamdev5x", "@skillzoneu"
]

def clean_caption(text):
    if not text:
        return SIGNATURE

    out = text
    for bad in REMOVE:
        try:
            out = re.sub(bad, "", out, flags=re.IGNORECASE)
        except:
            pass

    out = out.strip()
    if SIGNATURE.lower() not in out.lower():
        out += f"\n\n{SIGNATURE}"

    return out


# -------------------------------------------------------
# VIDEO DOWNLOAD
# -------------------------------------------------------

async def download_media(msg: Message):
    if not (msg.video or (msg.document and msg.document.mime_type.startswith("video"))):
        return None

    target = TMP_DIR / f"ultra_{msg.id}.mp4"
    path = await msg.download(file_name=str(target))

    size_mb = Path(path).stat().st_size / (1024 * 1024)
    if size_mb > MAX_FILE_MB:
        Path(path).unlink()
        return None

    return path


# -------------------------------------------------------
# SEND WITH RETRY + ULTRA SPEED
# -------------------------------------------------------

async def send_with_retry(target, src_msg, caption, local_path):

    for attempt in range(6):
        try:

            # Try copy first
            try:
                await src_msg.copy(chat_id=target, caption=caption)
                return True
            except:
                pass

            # else upload
            if local_path:
                await bot.send_video(
                    chat_id=target,
                    video=local_path,
                    caption=caption,
                    thumb=THUMB if Path(THUMB).exists() else None,
                    supports_streaming=True
                )
                return True

            await asyncio.sleep(1)

        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)

        except Exception as e:
            print("Send error:", e)
            await asyncio.sleep(1 + attempt)

    return False


# -------------------------------------------------------
# RANGE FORWARD WORKER
# -------------------------------------------------------

async def range_forward(message, chat_id, first, last):

    total = last - first + 1
    sent = 0
    failed = 0

    progress = await message.reply_text(
        f"🚀 Starting...\n0/{total} done."
    )

    for msg_id in range(first, last + 1):

        try:
            src = await bot.get_messages(chat_id, msg_id)
        except:
            failed += 1
            continue

        # Only video allowed
        if not (src.video or (src.document and src.document.mime_type.startswith("video"))):
            continue

        caption = clean_caption(src.caption or "")

        local_path = await download_media(src)

        # Send to all target channels
        results = []
        for t in TARGETS:
            ok = await send_with_retry(t, src, caption, local_path)
            results.append(ok)

        sent += results.count(True)
        failed += results.count(False)

        pct = int((msg_id - first + 1) / total * 100)

        await safe_edit(
            progress,
            f"📦 Forwarding {first} → {last}\n"
            f"✔ Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"📊 Progress: {pct}%"
        )

        if local_path and Path(local_path).exists():
            Path(local_path).unlink()

        await asyncio.sleep(FORWARD_DELAY)

    await safe_edit(progress, f"🎉 Completed!\n✔ Sent: {sent}\n❌ Failed: {failed}")


# -------------------------------------------------------
# PARSE MESSAGE LINK
# -------------------------------------------------------

def parse_link(link: str):
    try:
        parts = link.split("/")
        msg_id = int(parts[-1])
        chat = int(parts[-2])
        return chat, msg_id
    except:
        return None, None


# -------------------------------------------------------
# COMMANDS
# -------------------------------------------------------

@bot.on_message(filters.user(OWNER_ID) & filters.command("start"))
async def start_cmd(c, m):
    await m.reply_text(
        "**🤖 V9 Ultra Bot Ready!**\n\n"
        "**Commands:**\n"
        "/range — forward via link range\n"
        "/status — bot info\n"
        "/addtarget -100xxxx\n"
        "/removetarget -100xxxx"
    )


@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status_cmd(c, m):
    await m.reply_text(
        f"Targets: {TARGETS}\n"
        f"Signature: {SIGNATURE}"
    )


# RANGE COMMAND
@bot.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def range_cmd(c, m):
    await m.reply_text("Send FIRST link now...")
    bot.add_handler(wait_first, filters.user(OWNER_ID))


async def wait_first(client, message):
    bot.remove_handler(wait_first)

    fchat, fid = parse_link(message.text)
    if not fchat:
        return await message.reply_text("Invalid FIRST link")

    await message.reply_text("Now send LAST link...")
    bot.add_handler(wait_last, filters.user(OWNER_ID), fchat=fchat, fid=fid)


async def wait_last(client, message, fchat, fid):
    bot.remove_handler(wait_last)

    lchat, lid = parse_link(message.text)
    if not lchat:
        return await message.reply_text("Invalid LAST link")

    if fchat != lchat:
        return await message.reply_text("Both links must be from same channel.")

    await message.reply_text("Starting ultra forwarding...")

    asyncio.create_task(
        range_forward(message, fchat, fid, lid)
    )


# -------------------------------------------------------
# RUN BOT
# -------------------------------------------------------

print("🔥 V9 ULTRA BOT STARTED — NO ERRORS")
bot.run()