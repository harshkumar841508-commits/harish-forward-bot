# ============================================================
# V12 ULTRA MULTI-USER BOT — FINAL FIXED VERSION
# ============================================================

import os
import re
import json
import time
import asyncio
import random
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# -----------------------------
# FIXED GLOBAL VARIABLES (YOURS)
# -----------------------------
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"
BOT_TOKEN = "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA"

OWNER_ID = 1251826930

DEFAULT_SIGNATURE = "Extracted by➤@course_wale"
DEFAULT_TARGETS = [-1003404830427]
DEFAULT_DELAY = 1.5
DEFAULT_CONCURRENCY = 4
RETRY_LIMIT = 4
MAX_FILE_MB = 1500

TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_config.json")
STATE_FILE = Path("v12_state.json")

# -----------------------------
# JSON persistence
# -----------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except:
        pass
    return default

def save_json(path: Path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        print("save_json error:", e)

config = load_json(CONFIG_FILE, {
    "global": {
        "signature": DEFAULT_SIGNATURE,
        "targets": DEFAULT_TARGETS.copy(),
        "delay": DEFAULT_DELAY,
        "concurrency": DEFAULT_CONCURRENCY
    },
    "users": {}
})

state = load_json(STATE_FILE, {})

# Ensure owner exists
config["users"].setdefault(str(OWNER_ID), {
    "role": "owner",
    "quota": 999999999,
    "used": 0,
    "expires": None,
    "targets": DEFAULT_TARGETS.copy(),
    "signature": DEFAULT_SIGNATURE,
    "thumb": None,
    "delay": DEFAULT_DELAY,
    "concurrency": DEFAULT_CONCURRENCY
})
save_json(CONFIG_FILE, config)

# -----------------------------
# Metrics + Controller
# -----------------------------
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}
}
controller["pause_event"].set()

# -----------------------------
# Pyrogram Client
# -----------------------------
bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# -----------------------------
# Caption cleaner
# -----------------------------
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]

OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"

def get_cfg(uid: int):
    return config["users"].setdefault(str(uid), {
        "role": "user",
        "quota": 0,
        "used": 0,
        "expires": None,
        "targets": DEFAULT_TARGETS.copy(),
        "signature": DEFAULT_SIGNATURE,
        "thumb": None,
        "delay": DEFAULT_DELAY,
        "concurrency": DEFAULT_CONCURRENCY
    })

def clean_caption(text: Optional[str], sig: str):
    if not text:
        return sig
    out = text
    for p in REMOVE_PATTERNS:
        out = re.sub(p, "", out, flags=re.IGNORECASE)
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if sig.lower() not in out.lower():
        out = f"{out}\n\n{sig}"
    return out

# -----------------------------
# Utils
# -----------------------------
def parse_link(link: str):
    try:
        if "t.me/c/" in link:
            p = link.split("/")
            return {"chat_id": int(p[-2]), "msg_id": int(p[-1])}
        if "t.me/" in link:
            p = link.split("/")
            return {"username": p[-2], "msg_id": int(p[-1])}
    except:
        return None

async def safe_edit(msg: Message, text: str):
    try:
        if msg and msg.text != text:
            await msg.edit_text(text)
    except:
        pass

# -----------------------------
# Downloader
# -----------------------------
async def download_media(msg: Message):
    try:
        if msg.video or (msg.document and msg.document.mime_type.startswith("video")):
            out = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{time.time()}"
            path = await msg.download(str(out))
            if Path(path).stat().st_size / (1024*1024) > MAX_FILE_MB:
                Path(path).unlink(missing_ok=True)
                return None
            return path
    except Exception as e:
        print("download error:", e)
    return None

_last_send = {}

async def wait_target(tid, delay):
    last = _last_send.get(tid, 0)
    elapsed = time.time() - last
    if elapsed < delay:
        await asyncio.sleep(delay - elapsed)
    _last_send[tid] = time.time()

# -----------------------------
# Sending with retry
# -----------------------------
async def send_retry(client, target, src, local, caption):
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            if not local:
                try:
                    await src.copy(target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except:
                    pass
            if local and Path(local).exists():
                await client.send_document(target, local, caption=caption)
                metrics["forwards"] += 1
                return True
            await src.copy(target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except RPCError:
            metrics["fails"] += 1
            return False
        except Exception:
            attempt += 1
            metrics["retries"] += 1
            await asyncio.sleep((2 ** attempt) + random.random())
    metrics["fails"] += 1
    return False

# -----------------------------
# Forward to targets
# -----------------------------
async def forward_targets(src, caption, targets, delay, concurrency):
    local = await download_media(src)
    sem = asyncio.Semaphore(concurrency)
    results = {}

    async def send_one(t):
        async with sem:
            await wait_target(t, delay)
            ok = await send_retry(bot, t, src, local, caption)
            results[t] = ok

    await asyncio.gather(*(send_one(t) for t in targets))

    if local:
        Path(local).unlink(missing_ok=True)
    return results

# -----------------------------
# Range worker
# -----------------------------
async def range_worker(client_read, msg, source, first, last, targets, key, uid):
    metrics["active"] += 1
    total = last - first + 1

    last_done = state.get(key, {}).get("last", first - 1)
    start = max(first, last_done + 1)

    prog = await msg.reply_text(f"Starting {start} → {last}...")

    for mid in range(start, last + 1):
        if controller["stop_flag"]:
            await safe_edit(prog, "⛔ Stopped by user.")
            break

        await controller["pause_event"].wait()

        try:
            if "chat_id" in source:
                src = await client_read.get_messages(source["chat_id"], mid)
            else:
                src = await client_read.get_messages(source["username"], mid)
        except:
            src = None

        state[key] = {"last": mid}
        save_json(STATE_FILE, state)

        if not src:
            continue
        if not (src.video or (src.document and src.document.mime_type.startswith("video"))):
            continue

        ucfg = get_cfg(uid)
        caption = clean_caption(src.caption or "", ucfg["signature"])
        results = await forward_targets(src, caption, ucfg["targets"], ucfg["delay"], ucfg["concurrency"])

        done = mid - first + 1
        pct = int(done / total * 100)
        succ = sum(1 for v in results.values() if v)
        fail = sum(1 for v in results.values() if not v)

        await safe_edit(
            prog,
            f"Progress: {done}/{total} ({pct}%)\nSuccess: {succ}  Fail: {fail}"
        )

    await safe_edit(prog, "✅ Completed.")
    state.pop(key, None)
    save_json(STATE_FILE, state)
    metrics["active"] -= 1
    controller["range_task"] = None

# -----------------------------
# Commands
# -----------------------------
@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def cmd_pause(c, m):
    controller["pause_event"].clear()
    await m.reply_text("⏸ Paused.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def cmd_resume(c, m):
    controller["pause_event"].set()
    await m.reply_text("▶ Resumed.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(c, m):
    controller["stop_flag"] = True
    await m.reply_text("⛔ Stop signal activated.")

@bot.on_message(filters.command("status"))
async def cmd_status(c, m):
    await m.reply_text(
        f"Active tasks: {metrics['active']}\n"
        f"Forwards: {metrics['forwards']}\nFails: {metrics['fails']}"
    )

# -----------------------------
# linkforward
# -----------------------------
@bot.on_message(filters.command("linkforward"))
async def cmd_linkforward(c, m):
    uid = m.from_user.id
    if uid not in config["users"]:
        return await m.reply_text("Not authorized.")

    if len(m.command) < 3:
        return await m.reply_text("Usage: /linkforward <first_link> <last_link>")

    f = parse_link(m.command[1])
    l = parse_link(m.command[2])
    if not f or not l:
        return await m.reply_text("Invalid links.")

    first, last = f["msg_id"], l["msg_id"]
    if first > last:
        first, last = last, first

    await m.reply_text("Starting...")
    key = f"{uid}_{first}_{last}"

    controller["range_task"] = asyncio.create_task(
        range_worker(bot, m, f, first, last, get_cfg(uid)["targets"], key, uid)
    )

# -----------------------------
# start bot
# -----------------------------
print("V12 ULTRA BOT STARTED.")
bot.run()