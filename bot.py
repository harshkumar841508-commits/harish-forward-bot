# bot.py — V5.5 Single-file Smart Auto-Forward Bot (Final)
# Features: multi-source, multi-target, filters, scheduler, linkforward, concurrency, stats, control panel
# Warning: Keep BOT_TOKEN secret. Use only for legal content.

import os
import re
import json
import time
import asyncio
import logging
from typing import List
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError

# -------------------------
# ========== CONFIG ==========
# Fill values (you already gave these)
API_ID = 28420641
API_HASH = "d1302d5039ae3275c4195b4fcc5ff1f9"
BOT_TOKEN = "8592967336:AAGoj5zAzkPO9nHSFjHYHp7JclEq4Z7KKGg"  # replace if needed
OWNER_ID = 8117462619

# Default lists (editable via commands)
SOURCE_CHANNELS = [-1003240589036]   # list of source channel IDs
TARGET_CHANNELS = [-1003216068164]   # list of target channel IDs

CUSTOM_THUMB = "thumb.jpg"
FORWARD_DELAY = 1.5  # per-target small delay (seconds)

# Caption cleaning
REMOVE_TEXTS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"@YTBR_67",
    r"@skillwithgaurav",
    r"@kamdev5x",
    r"@skillzoneu",
    r"Gaurav\s*RaJput",
    r"Gaurav",
    r"Join-@skillwithgaurav"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"

# Concurrency / speed tuning (start conservative)
CONCURRENT_UPLOADS = int(os.environ.get("CONCURRENT_UPLOADS", "4"))
CONCURRENT_MESSAGE_WORKERS = int(os.environ.get("CONCURRENT_MESSAGE_WORKERS", "2"))
MAX_RETRIES = 3
BACKOFF_BASE = 2

# Files
CONFIG_FILE = "config.json"
LOG_FILE = "bot.log"

# -------------------------
# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)

# -------------------------
# Persistent config (saved runtime)
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to load config.json: %s", e)
    # default config structure
    cfg = {
        "sources": SOURCE_CHANNELS.copy(),
        "targets": TARGET_CHANNELS.copy(),
        "signature": DEFAULT_SIGNATURE,
        "thumb_path": CUSTOM_THUMB,
        "forward_delay": FORWARD_DELAY,
        "remove_patterns": REMOVE_TEXTS.copy(),
        "website_map": [{"pattern": OLD_WEBSITE_RE, "replace_with": NEW_WEBSITE}],
        "filters": [],  # keywords
        "scheduler": {"enabled": False, "interval_seconds": 0, "last_run": 0},
        "stats": {"forwarded": 0, "failed": 0, "skipped": 0}
    }
    save_config(cfg)
    return cfg

def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error("Failed to save config.json: %s", e)

config = load_config()

# -------------------------
# Pyrogram Client
app = Client("v5_5_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Semaphores
upload_semaphore = asyncio.Semaphore(CONCURRENT_UPLOADS)
message_worker_semaphore = asyncio.Semaphore(CONCURRENT_MESSAGE_WORKERS)

# -------------------------
# Utilities: caption cleaning, download/reupload helpers

def clean_caption_text(caption: str) -> str:
    sig = config.get("signature", DEFAULT_SIGNATURE)
    if not caption or caption.strip() == "":
        text = sig
    else:
        text = caption
        # remove patterns
        for pat in config.get("remove_patterns", []):
            try:
                text = re.sub(pat, "", text, flags=re.IGNORECASE)
            except re.error:
                text = text.replace(pat, "")
        # replace website patterns
        for wr in config.get("website_map", []):
            try:
                text = re.sub(wr["pattern"], wr["replace_with"], text, flags=re.IGNORECASE)
            except re.error:
                text = text.replace(wr.get("pattern",""), wr.get("replace_with",""))
        text = text.strip()
        if sig.lower() not in text.lower():
            text = f"{text}\n\n{sig}"
        # ensure site present
        site = config["website_map"][0]["replace_with"]
        if site not in text:
            text += f"\n\n𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\n{site}"
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

async def send_video_with_retry(client: Client, target: int, video_path: str, caption: str, thumb_path: str):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            async with upload_semaphore:
                await client.send_video(
                    chat_id=int(target),
                    video=video_path,
                    caption=caption,
                    thumb=thumb_path if os.path.exists(thumb_path) else None,
                    supports_streaming=True
                )
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "x", getattr(fw, "value", 5))) + 1
            logger.warning("FloodWait %s for target %s - sleeping %s s", wait, target, wait)
            await asyncio.sleep(wait)
            attempt += 1
        except RPCError as rpc:
            logger.error("RPCError sending to %s: %s", target, rpc)
            return False
        except Exception as e:
            attempt += 1
            backoff = BACKOFF_BASE ** attempt
            logger.warning("Send attempt %s to %s failed: %s. Backing off %s s", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    return False

async def process_single_message(client: Client, msg, targets: List[int]):
    """Download once, then upload concurrently to all targets"""
    if not msg:
        return {"sent": 0, "failed": len(targets), "skipped": 0}
    caption = clean_caption_text(msg.caption)
    # apply keyword filters if any
    filters_list = config.get("filters", [])
    if filters_list:
        found = False
        txt = (msg.caption or "") + " " + (getattr(msg, "text", "") or "")
        for kw in filters_list:
            if kw.strip().lower() in txt.lower():
                found = True
                break
        if not found:
            config["stats"]["skipped"] = config["stats"].get("skipped", 0) + 1
            save_config(config)
            return {"sent": 0, "failed": 0, "skipped": 1}

    is_video = bool(msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video/")))
    sent = 0
    failed = 0

    if is_video:
        try:
            video_path = await msg.download(file_name=f"tmp_{int(time.time()*1000)}.mp4")
        except Exception as e:
            logger.error("Download failed for message %s: %s", getattr(msg, "message_id", "?"), e)
            return {"sent": 0, "failed": len(targets), "skipped": 0}

        # schedule concurrent sends for each target
        send_tasks = [asyncio.create_task(send_video_with_retry(client, t, video_path, caption, config.get("thumb_path", CUSTOM_THUMB))) for t in targets]
        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        for res in results:
            if res is True:
                sent += 1
            else:
                failed += 1

        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except:
            pass
        await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
    else:
        # non-video copy sequentially but still bounded by semaphore to avoid bursts
        for t in targets:
            try:
                async with upload_semaphore:
                    await msg.copy(chat_id=int(t), caption=caption)
                sent += 1
            except FloodWait as fw:
                wait = int(getattr(fw, "x", getattr(fw, "value", 5))) + 1
                logger.warning("FloodWait on copy to %s, waiting %s", t, wait)
                await asyncio.sleep(wait)
                try:
                    await msg.copy(chat_id=int(t), caption=caption)
                    sent += 1
                except Exception as e:
                    logger.error("Copy failed after wait to %s: %s", t, e)
                    failed += 1
            except Exception as e:
                logger.error("Copy error to %s: %s", t, e)
                failed += 1
            await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))

    config["stats"]["forwarded"] = config["stats"].get("forwarded", 0) + sent
    config["stats"]["failed"] = config["stats"].get("failed", 0) + failed
    save_config(config)
    return {"sent": sent, "failed": failed, "skipped": 0}

# -------------------------
# Concurrent range forward (used by /linkforward and scheduler)
async def forward_range_concurrent(client: Client, source_channel: int, start_id: int, end_id: int, reply_message):
    # fetch messages list (pyrogram can accept list)
    ids = list(range(start_id, end_id + 1))
    msgs = await client.get_messages(source_channel, ids)
    msgs = [m for m in msgs if m]  # filter None
    total = len(msgs)
    if total == 0:
        await reply_message.edit_text("⚠️ No messages found in that range.")
        return {"sent": 0, "failed": 0, "skipped": 0}

    await reply_message.edit_text(f"🚀 Starting forwarding {total} messages with concurrency...")

    # worker sem to limit parallel message downloads/processing
    sem = message_worker_semaphore

    async def worker(msg):
        async with sem:
            return await process_single_message(client, msg, config.get("targets", []))

    tasks = [asyncio.create_task(worker(m)) for m in msgs]
    sent_total = failed_total = skipped_total = 0
    done = 0
    progress_msg = reply_message
    for fut in asyncio.as_completed(tasks):
        res = await fut
        sent_total += res.get("sent", 0)
        failed_total += res.get("failed", 0)
        skipped_total += res.get("skipped", 0)
        done += 1
        await progress_msg.edit_text(f"📤 Done: {done}/{total} — Sent: {sent_total} | Failed: {failed_total} | Skipped: {skipped_total}")
    await progress_msg.edit_text(f"✅ Completed. Sent: {sent_total} | Failed: {failed_total} | Skipped: {skipped_total}")
    return {"sent": sent_total, "failed": failed_total, "skipped": skipped_total}

# -------------------------
# Owner-only decorator
def owner_only(func):
    async def wrapper(client, message):
        if not message.from_user or message.from_user.id != OWNER_ID:
            await message.reply_text("❌ You are not authorized.")
            return
        return await func(client, message)
    return wrapper

# -------------------------
# Commands: control panel

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
@owner_only
async def cmd_start(client, message):
    txt = ("🤖 **Auto-Forward Bot — Control Panel**\n\n"
           "Use /panel to open quick controls or these commands:\n"
           "/status /verifytargets /addsource /removesource /addtarget /removetarget\n"
           "/setcaption /setfilter /removefilter /setthumb /linkforward /schedule /stop /stats\n")
    await message.reply_text(txt)

@app.on_message(filters.command("status") & filters.user(OWNER_ID))
@owner_only
async def cmd_status(client, message):
    s = config
    txt = (f"✅ Bot Status\nSources: {s.get('sources')}\nTargets: {s.get('targets')}\n"
           f"Signature: {s.get('signature')}\nThumb: {s.get('thumb_path')}\n"
           f"Filters: {s.get('filters')}\nForward delay: {s.get('forward_delay')}\n"
           f"Stats: forwarded={s['stats'].get('forwarded',0)}, failed={s['stats'].get('failed',0)}, skipped={s['stats'].get('skipped',0)}")
    await message.reply_text(txt)

@app.on_message(filters.command("verifytargets") & filters.user(OWNER_ID))
@owner_only
async def cmd_verifytargets(client, message):
    await message.reply_text("🔍 Verifying targets... please wait")
    out = []
    for t in config.get("targets", []):
        try:
            chat = await client.get_chat(int(t))
            title = getattr(chat, "title", str(t))
            out.append(f"✅ {title} (`{t}`)")
        except Exception as e:
            out.append(f"❌ `{t}` | {e}")
    await message.reply_text("\n".join(out))

@app.on_message(filters.command("addsource") & filters.user(OWNER_ID))
@owner_only
async def cmd_addsource(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /addsource -1001234567890")
    if cid not in config["sources"]:
        config["sources"].append(cid)
        save_config(config)
        await message.reply_text(f"✅ Added source {cid}")
    else:
        await message.reply_text("⚠️ Already exists")

@app.on_message(filters.command("removesource") & filters.user(OWNER_ID))
@owner_only
async def cmd_removesource(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /removesource -1001234567890")
    if cid in config["sources"]:
        config["sources"].remove(cid)
        save_config(config)
        await message.reply_text(f"🗑 Removed source {cid}")
    else:
        await message.reply_text("⚠️ Not found")

@app.on_message(filters.command("addtarget") & filters.user(OWNER_ID))
@owner_only
async def cmd_addtarget(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /addtarget -1001234567890")
    if cid not in config["targets"]:
        config["targets"].append(cid)
        save_config(config)
        await message.reply_text(f"✅ Added target {cid}")
    else:
        await message.reply_text("⚠️ Already exists")

@app.on_message(filters.command("removetarget") & filters.user(OWNER_ID))
@owner_only
async def cmd_removetarget(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /removetarget -1001234567890")
    if cid in config["targets"]:
        config["targets"].remove(cid)
        save_config(config)
        await message.reply_text(f"🗑 Removed target {cid}")
    else:
        await message.reply_text("⚠️ Not found")

@app.on_message(filters.command("setcaption") & filters.user(OWNER_ID))
@owner_only
async def cmd_setcaption(client, message):
    new = message.text.replace("/setcaption","").strip()
    if not new:
        return await message.reply_text("Usage: /setcaption <text>")
    config["signature"] = new
    save_config(config)
    await message.reply_text(f"✅ Signature set to:\n{new}")

@app.on_message(filters.command("setfilter") & filters.user(OWNER_ID))
@owner_only
async def cmd_setfilter(client, message):
    kw = message.text.replace("/setfilter","").strip()
    if not kw:
        return await message.reply_text("Usage: /setfilter <keyword>")
    config.setdefault("filters", [])
    config["filters"].append(kw)
    save_config(config)
    await message.reply_text(f"✅ Filter added: {kw}")

@app.on_message(filters.command("removefilter") & filters.user(OWNER_ID))
@owner_only
async def cmd_removefilter(client, message):
    try:
        term = message.text.split(maxsplit=1)[1].strip()
    except:
        return await message.reply_text("Usage: /removefilter <keyword>")
    if term in config.get("filters", []):
        config["filters"].remove(term)
        save_config(config)
        await message.reply_text(f"✅ Removed filter: {term}")
    else:
        await message.reply_text("⚠️ Not found")

@app.on_message(filters.command("setthumb") & filters.user(OWNER_ID))
@owner_only
async def cmd_setthumb(client, message):
    # reply to a photo or send photo with caption /setthumb
    target_msg = message.reply_to_message
    if not target_msg:
        return await message.reply_text("Reply to an image with /setthumb")
    if target_msg.photo or (target_msg.document and getattr(target_msg.document, "mime_type", "").startswith("image/")):
        path = await target_msg.download(file_name="thumb.jpg")
        config["thumb_path"] = path
        save_config(config)
        await message.reply_text("✅ Thumbnail updated")
    else:
        await message.reply_text("Reply must contain an image")

# -------------------------
# Linkforward command
@app.on_message(filters.command("linkforward") & filters.user(OWNER_ID))
@owner_only
async def cmd_linkforward(client, message):
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply_text("Usage: /linkforward <first_link> <last_link>")
    def extract_id(link):
        m = re.search(r"/(\d+)$", link)
        return int(m.group(1)) if m else None
    s = extract_id(parts[1]); e = extract_id(parts[2])
    if not s or not e:
        return await message.reply_text("❌ Invalid links. Provide full message links.")
    if s > e:
        s, e = e, s
    progress = await message.reply_text(f"🔁 Preparing to forward messages {s} → {e} ...")
    await forward_range_concurrent(client, message.chat.id if message.chat else config["sources"][0], s, e, progress)

# -------------------------
# Scheduler: periodic forwarding of latest N messages per source
scheduler_task = None
scheduler_lock = asyncio.Lock()

async def scheduler_worker(client: Client):
    """Periodic job to fetch new messages from sources and forward latest ones automatically."""
    try:
        while config.get("scheduler", {}).get("enabled", False):
            interval = config["scheduler"].get("interval_seconds", 0)
            if interval <= 0:
                break
            logger.info("Scheduler tick - fetching latest from sources")
            for src in config.get("sources", []):
                # fetch last N messages (e.g., 10) and forward only new ones based on last_run
                last_run = config.get("scheduler", {}).get("last_run", 0)
                # get last 20 messages
                msgs = await client.get_history(src, limit=20)
                # filter only messages newer than last_run (use date)
                to_forward = []
                for m in reversed(msgs):  # oldest first
                    if hasattr(m, "date") and (int(m.date.timestamp()) > last_run):
                        to_forward.append(m)
                if to_forward:
                    logger.info("Scheduler: forwarding %s messages from %s", len(to_forward), src)
                    # create a fake progress message by sending to owner
                    progress_msg = await client.send_message(OWNER_ID, f"Scheduler: forwarding {len(to_forward)} messages from {src}")
                    # process sequentially but uses concurrent workers internally
                    ids = [m.message_id for m in to_forward]
                    msgs2 = await client.get_messages(src, ids)
                    await forward_range_concurrent(client, src, min(ids), max(ids), progress_msg)
                    config["scheduler"]["last_run"] = int(time.time())
                    save_config(config)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("Scheduler cancelled")
    except Exception as e:
        logger.exception("Scheduler error: %s", e)

@app.on_message(filters.command("schedule") & filters.user(OWNER_ID))
@owner_only
async def cmd_schedule(client, message):
    global scheduler_task
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply_text("Usage: /schedule <seconds>  (e.g., /schedule 3600)")
    try:
        interval = int(parts[1])
    except:
        return await message.reply_text("Interval must be integer seconds")
    config["scheduler"]["enabled"] = True
    config["scheduler"]["interval_seconds"] = interval
    config["scheduler"]["last_run"] = int(time.time()