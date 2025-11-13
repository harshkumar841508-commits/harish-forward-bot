# bot.py — V6.5 Ultra Stable Final
# Features:
# - Range Forward (interactive + direct)
# - Video Extract + Upload
# - Resume / Pause / Stop
# - Success/fail counters + progress
# - Caption Cleaner + Signature Replace
# - Website Auto-Replace
# - Thumbnail Setter
# - Add/Remove/List target channels
# - State Save + Diagnostics
# - Optional USER_SESSION for private locked channels
# - Adaptive speed + Multi-target concurrency

import os
import re
import json
import time
import asyncio
import logging
import tempfile
import random
from pathlib import Path
from typing import Optional, List, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError
import aiofiles

# ---------------- CONFIG (fixed + env mix) ----------------
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"

# ⚠️ YOUR BOT TOKEN (hard-coded as requested)
BOT_TOKEN = "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA"

OWNER_ID = 1251826930
DEFAULT_SOURCE = -1003433745100
DEFAULT_TARGETS = [-1003404830427]

FORWARD_DELAY = 0.5
CONCURRENCY = 6
RETRY_LIMIT = 4
MAX_FILE_MB = 600

USER_SESSION = ""   # optional stringsession

REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav",
    r"@kamdev5x", r"@skillzoneu"
]

OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"

THUMB_FILE = "thumb.jpg"

STATE_FILE = Path("state.json")
CONFIG_FILE = Path("v6_config.json")
TMP_DIR = Path(tempfile.gettempdir())

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("v6.5")

def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text())
    except:
        pass
    return default

def save_json(path: Path, data):
    try:
        tmp = str(path) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.error(f"save_json failed: {e}")

# Persistent config
config = load_json(CONFIG_FILE, {
    "targets": DEFAULT_TARGETS.copy(),
    "thumb": THUMB_FILE,
    "signature": DEFAULT_SIGNATURE,
    "forward_delay": FORWARD_DELAY,
    "concurrency": CONCURRENCY
})

state = load_json(STATE_FILE, {})

TARGETS = config["targets"]
THUMB = config["thumb"]
SIGNATURE = config["signature"]
FORWARD_DELAY = config["forward_delay"]
CONCURRENCY = config["concurrency"]

# pyrogram clients
bot = Client("v6_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user = None
if USER_SESSION:
    try:
        user = Client(USER_SESSION, api_id=API_ID, api_hash=API_HASH)
    except:
        user = None

controller = {
    "range_task": None,
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "interactive": {}
}
controller["pause_event"].set()

metrics = {
    "forwards": 0,
    "failures": 0,
    "retries": 0,
    "active_tasks": 0
}

def clean_caption(text: Optional[str]) -> str:
    sig = config["signature"]
    if not text:
        return sig
    out = text
    for pat in REMOVE_PATTERNS:
        try:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        except:
            out = out.replace(pat, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if sig.lower() not in out.lower():
        out += f"\n\n{sig}"
    return out

def parse_link(link: str):
    try:
        link = link.strip()
        parts = link.split("/")
        msg_id = int(parts[-1])
        if "/c/" in link:
            chat_id = int(parts[-2])
            return {"chat_id": chat_id, "msg_id": msg_id}
        else:
            username = parts[-2]
            return {"chat_username": username, "msg_id": msg_id}
    except:
        return None

async def download_media(msg: Message):
    if not (msg.video or (msg.document and msg.document.mime_type.startswith("video"))):
        return None
    path = TMP_DIR / f"v6_{msg.chat.id}_{msg.id}_{int(time.time())}"
    try:
        out = await msg.download(file_name=str(path))
        size = os.path.getsize(out) / (1024*1024)
        if size > MAX_FILE_MB:
            os.remove(out)
            return None
        return out
    except:
        return None

_last_send = {}
async def adaptive_wait(target: int):
    last = _last_send.get(target, 0)
    wait = FORWARD_DELAY
    diff = time.time() - last
    if diff < wait:
        await asyncio.sleep(wait - diff)
    _last_send[target] = time.time()

async def send_retry(client: Client, target: int, src: Message, local: str, caption: str):
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            if not local:
                await src.copy(target, caption=caption)
                metrics["forwards"] += 1
                return True
            else:
                await client.send_video(
                    target,
                    video=local,
                    caption=caption,
                    thumb=THUMB if os.path.exists(THUMB) else None,
                    supports_streaming=True
                )
                metrics["forwards"] += 1
                return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception:
            attempt += 1
            metrics["retries"] += 1
            await asyncio.sleep(2 ** attempt)
    metrics["failures"] += 1
    return False

async def forward_to_all(read_client, src, caption, targets):
    local = None
    if src.video or (src.document and src.document.mime_type.startswith("video")):
        local = await download_media(src)

    sem = asyncio.Semaphore(CONCURRENCY)
    results = {}

    async def _send(t):
        async with sem:
            await adaptive_wait(t)
            ok = await send_retry(bot, t, src, local, caption)
            results[t] = ok

    tasks = [asyncio.create_task(_send(t)) for t in targets]
    await asyncio.gather(*tasks)

    if local and os.path.exists(local):
        os.remove(local)

    return results

async def range_worker(read_client, origin, src_ident, first, last, targets, key):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent = 0
    progress = await origin.reply_text(f"Starting… {first}→{last}")

    try:
        for mid in range(first, last + 1):
            if controller["stop_flag"]:
                controller["stop_flag"] = False
                await progress.edit_text(f"Stopped. Sent {sent}/{total}")
                break

            await controller["pause_event"].wait()

            try:
                if "chat_id" in src_ident:
                    src = await read_client.get_messages(src_ident["chat_id"], mid)
                else:
                    src = await read_client.get_messages(src_ident["chat_username"], mid)
            except:
                continue

            if not src or not (src.video or (src.document and src.document.mime_type.startswith("video"))):
                continue

            caption = clean_caption(src.caption or "")
            await forward_to_all(read_client, src, caption, targets)
            sent += 1

            pct = int((mid-first+1)/total * 100)
            try:
                await progress.edit_text(f"{mid} — {pct}% — Sent {sent}/{total}")
            except:
                pass

        await progress.edit_text(f"Completed. Sent {sent}/{total}")

    except asyncio.CancelledError:
        await progress.edit_text(f"Cancelled. Sent {sent}")
    finally:
        metrics["active_tasks"] -= 1


# ---------------- COMMANDS ----------------

@bot.on_message(filters.user(OWNER_ID) & filters.command("start"))
async def start_cmd(c, m):
    txt = (
        "**V6.5 Extract & Upload Bot**\n\n"
        "**Commands:**\n"
        "/linkforward link1 link2\n"
        "/range (interactive)\n"
        "/pause /resume /stop\n"
        "/setcaption text\n"
        "/setthumb (reply to image)\n"
        "/addtarget /removetarget\n"
        "/listtargets\n"
        "/status\n"
    )
    await m.reply_text(txt)

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status_cmd(c, m):
    await m.reply_text(
        f"Targets: {config['targets']}\n"
        f"Signature: {config['signature']}\n"
        f"Forwards: {metrics['forwards']} Fails: {metrics['failures']}\n"
        f"Active tasks: {metrics['active_tasks']}"
    )

@bot.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
async def setcap(c, m):
    text = " ".join(m.command[1:])
    if not text:
        return await m.reply("Usage: /setcaption text")
    config["signature"] = text
    save_json(CONFIG_FILE, config)
    await m.reply(f"Caption updated:\n{text}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def setthumb(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo:
        return await m.reply("Reply to an image.")
    f = await m.reply_to_message.download("thumb.jpg")
    config["thumb"] = f
    save_json(CONFIG_FILE, config)
    await m.reply("Thumbnail updated.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def addtarget(c, m):
    try:
        tid = int(m.command[1])
    except:
        return await m.reply("Usage: /addtarget -100xxx")

    if tid in config["targets"]:
        return await m.reply("Already added.")

    config["targets"].append(tid)
    save_json(CONFIG_FILE, config)
    await m.reply(f"Added {tid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def removetarget(c, m):
    try:
        tid = int(m.command[1])
    except:
        return await m.reply("Usage: /removetarget -100xxx")

    if tid not in config["targets"]:
        return await m.reply("Not in list.")

    config["targets"].remove(tid)
    save_json(CONFIG_FILE, config)
    await m.reply(f"Removed {tid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("listtargets"))
async def listtargets(c, m):
    await m.reply(str(config["targets"]))

# ---------------- RANGE / INTERACTIVE ----------------

@bot.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def range_cmd(c, m):
    controller["interactive"][m.from_user.id] = {"step": 1}
    await m.reply("Send FIRST link…")

@bot.on_message(filters.user(OWNER_ID) & filters.text)
async def interactive(c, m):
    uid = m.from_user.id
    if uid not in controller["interactive"]:
        return
    
    data = controller["interactive"][uid]
    text = m.text.strip()

    # STEP 1 → GET FIRST LINK
    if data["step"] == 1:
        parsed = parse_link(text)
        if not parsed:
            return await m.reply("Invalid FIRST link.")
        data["first"] = parsed
        data["step"] = 2
        return await m.reply("Good. Now send LAST link…")

    # STEP 2 → GET LAST LINK
    if data["step"] == 2:
        parsed2 = parse_link(text)
        if not parsed2:
            return await m.reply("Invalid LAST link.")

        f = data["first"]["msg_id"]
        l = parsed2["msg_id"]
        if f > l: f, l = l, f

        data["first_id"] = f
        data["last_id"] = l
        data["source"] = data["first"]
        data["step"] = 3
        return await m.reply(f"Confirm forwarding {f}→{l} ? Type YES")

    # STEP 3 → CONFIRM
    if data["step"] == 3:
        if text.lower() != "yes":
            controller["interactive"].pop(uid, None)
            return await m.reply("Cancelled.")
        
        f = data["first_id"]
        l = data["last_id"]
        src = data["source"]

        controller["interactive"].pop(uid, None)

        reader = user if user else bot
        key = f"{src}_{f}_{l}"

        await m.reply(f"Starting… {f}→{l}")
        controller["range_task"] = asyncio.create_task(
            range_worker(reader, m, src, f, l, config["targets"], key)
        )
        return

# ---------------- DIRECT LINK FORWARD ----------------

@bot.on_message(filters.user(OWNER_ID) & filters.command("linkforward"))
async def linkforward(c, m):
    if len(m.command) < 3:
        return await m.reply("Usage: /linkforward link1 link2")

    p1 = parse_link(m.command[1])
    p2 = parse_link(m.command[2])
    if not p1 or not p2:
        return await m.reply("Invalid links.")

    f = p1["msg_id"]
    l = p2["msg_id"]
    if f > l: f, l = l, f

    reader = user if user else bot
    src = p1
    key = f"{src}_{f}_{l}"
    await m.reply(f"Starting direct… {f}→{l}")
    controller["range_task"] = asyncio.create_task(
        range_worker(reader, m, src, f, l, config["targets"], key)
    )

# ---------------- CONTROL: PAUSE / RESUME / STOP ----------------

@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def pause_cmd(c, m):
    controller["pause_event"].clear()
    await m.reply("Paused.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def resume_cmd(c, m):
    controller["pause_event"].set()
    await m.reply("Resumed.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def stop_cmd(c, m):
    controller["stop_flag"] = True
    await m.reply("Stopping task…")

# ---------------- RUN BOT ----------------

print("🚀 V6.5 Bot started successfully...")
bot.run()