# -----------------------------------------
# V9.1 ULTRA BOT (FINAL + FULLY FIXED)
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
#  FIXED VARIABLES (YOUR VALUES ADDED)
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
MAX_FILE_MB = 2048

TMP_DIR = Path(tempfile.gettempdir())


# -------------------------------------------------------
# SAFE EDIT FUNCTION
# -------------------------------------------------------

async def safe_edit(msg, text):
    try:
        if msg.text != text:
            await msg.edit_text(text)
    except Exception:
        pass


# -------------------------------------------------------
# INIT BOT
# -------------------------------------------------------

bot = Client("v9ultra_fixed", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


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
    for x in REMOVE:
        try:
            out = re.sub(x, "", out, flags=re.IGNORECASE)
        except:
            pass
    out = out.strip()
    if SIGNATURE.lower() not in out.lower():
        out += f"\n\n{SIGNATURE}"
    return out


# -------------------------------------------------------
# VIDEO DOWNLOADER
# -------------------------------------------------------

async def download_video(msg):
    if msg.video or (msg.document and msg.document.mime_type.startswith("video")):
        filename = TMP_DIR / f"v9_{msg.id}.mp4"
        path = await msg.download(file_name=str(filename))
        return path
    return None


# -------------------------------------------------------
# SEND WITH RETRY (ULTRA SPEED)
# -------------------------------------------------------

async def send_with_retry(target, src, caption, path):
    for attempt in range(5):
        try:
            try:
                await src.copy(chat_id=target, caption=caption)
                return True
            except:
                pass

            if path:
                await bot.send_video(
                    chat_id=target,
                    video=path,
                    caption=caption,
                    thumb=THUMB if Path(THUMB).exists() else None,
                    supports_streaming=True
                )
                return True

        except FloodWait as e:
            await asyncio.sleep(e.value)

        except Exception:
            await asyncio.sleep(1)

    return False


# -------------------------------------------------------
# FORWARDING FUNCTION
# -------------------------------------------------------

async def forward_range(m, chat_id, first, last):
    total = last - first + 1
    sent = 0
    failed = 0

    progress = await m.reply("🚀 Starting...")

    for mid in range(first, last + 1):

        try:
            msg = await bot.get_messages(chat_id, mid)
        except:
            failed += 1
            continue

        if not (msg.video or (msg.document and msg.document.mime_type.startswith("video"))):
            continue

        caption = clean_caption(msg.caption or "")
        path = await download_video(msg)

        results = []

        for t in TARGETS:
            ok = await send_with_retry(t, msg, caption, path)
            results.append(ok)

        sent += results.count(True)
        failed += results.count(False)

        pct = int(((mid - first + 1) / total) * 100)

        await safe_edit(
            progress,
            f"📦 Forwarding {first} → {last}\n\n"
            f"✔ Sent: {sent}\n❌ Failed: {failed}\n📊 Progress: {pct}%"
        )

        if path and Path(path).exists():
            Path(path).unlink()

        await asyncio.sleep(FORWARD_DELAY)

    await safe_edit(progress, f"🎉 Completed\n✔ Sent: {sent}\n❌ Failed: {failed}")


# -------------------------------------------------------
# PARSE LINK
# -------------------------------------------------------

def parse_link(link):
    try:
        parts = link.split("/")
        msg_id = int(parts[-1])
        chat = int(parts[-2])
        return chat, msg_id
    except:
        return None, None


# -------------------------------------------------------
# COMMANDS (NO HANDLER BUG NOW)
# -------------------------------------------------------

user_step = {}

@bot.on_message(filters.user(OWNER_ID) & filters.command("start"))
async def start_cmd(c, m):
    await m.reply(
        "**🤖 V9.1 Ultra Fixed Bot Ready!**\n\n"
        "/range – forward via link\n"
        "/status – check status\n"
        "/addtarget -100xxx\n"
        "/removetarget -100xxx"
    )


@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status_cmd(c, m):
    await m.reply(f"Targets: {TARGETS}")


# RANGE COMMAND
@bot.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def start_range(c, m):
    user_step[m.from_user.id] = {"step": 1}
    await m.reply("Send FIRST link…")


@bot.on_message(filters.user(OWNER_ID))
async def handle_links(c, m):

    uid = m.from_user.id
    if uid not in user_step:
        return

    step = user_step[uid]["step"]

    # STEP 1 → FIRST LINK
    if step == 1:
        chat, msg = parse_link(m.text)
        if not chat:
            return await m.reply("Invalid FIRST link")
        user_step[uid]["first"] = msg
        user_step[uid]["chat"] = chat
        user_step[uid]["step"] = 2
        return await m.reply("Now send LAST link…")

    # STEP 2 → LAST LINK
    if step == 2:
        chat2, msg2 = parse_link(m.text)
        if not chat2 or chat2 != user_step[uid]["chat"]:
            return await m.reply("Invalid LAST link")
        first = user_step[uid]["first"]
        last = msg2
        del user_step[uid]
        await m.reply("Processing…")
        asyncio.create_task(forward_range(m, chat2, first, last))


print("🔥 V9.1 Ultra Fixed Started — No Errors")
bot.run()