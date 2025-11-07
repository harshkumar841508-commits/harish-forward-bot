
# bot_improved.py — Improved V5.5 (Adaptive concurrency, SQLite persistence, robust retries)
# Replace your existing bot.py with this file.
# Keep BOT_TOKEN secret.

import os
import re
import json
import time
import sqlite3
import asyncio
import logging
import tempfile
from datetime import datetime
from typing import List, Optional
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---------------- CONFIG (fill as before) ----------------
API_ID = 28420641
API_HASH = "d1302d5039ae3275c4195b4fcc5ff1f9"
BOT_TOKEN = "8592967336:AAGoj5zAzkPO9nHSFjHYHp7JclEq4Z7KKGg"
OWNER_ID = 8117462619

# default lists (editable via commands)
DEFAULT_SOURCES = [-1003240589036]
DEFAULT_TARGETS = [-1003216068164]

DEFAULT_THUMB = "thumb.jpg"
DEFAULT_DELAY = 0.5  # base delay between per-target sends (may be adapted)
MAX_FILE_SIZE_MB = 200  # don't attempt to forward files larger than this (safety)
# caption replacement rules
DEFAULT_REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*", r"Extracted\s*By[^\n]*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu",
    r"Gaurav\s*RaJput", r"Gaurav", r"Join-@skillwithgaurav"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"

# concurrency tuning (start conservative)
INITIAL_CONCURRENT_UPLOADS = int(os.environ.get("CONCURRENT_UPLOADS", "4"))
INITIAL_CONCURRENT_MESSAGE_WORKERS = int(os.environ.get("CONCURRENT_MESSAGE_WORKERS", "2"))
MAX_RETRIES = 4
BACKOFF_BASE = 2

# files
DB_FILE = "bot.db"
LOG_FILE = "bot_improved.log"

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---------------- Database (SQLite) ----------------
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS forwarded (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_chat INTEGER,
        source_msg_id INTEGER,
        target_chat INTEGER,
        UNIQUE(source_chat, source_msg_id, target_chat)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stats (
        key TEXT PRIMARY KEY,
        value INTEGER
    )""")
    conn.commit()
    return conn

db = init_db()
db_lock = asyncio.Lock()

def db_set_config(key: str, value: str):
    cur = db.cursor()
    cur.execute("REPLACE INTO config(key,value) VALUES (?,?)", (key, value))
    db.commit()

def db_get_config(key: str) -> Optional[str]:
    cur = db.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def db_increment_stat(key: str, n: int = 1):
    cur = db.cursor()
    cur.execute("INSERT INTO stats(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value = value + ?", (key, n, n))
    db.commit()

def has_been_forwarded(source_chat: int, source_msg_id: int, target_chat: int) -> bool:
    cur = db.cursor()
    cur.execute("SELECT 1 FROM forwarded WHERE source_chat=? AND source_msg_id=? AND target_chat=?", (source_chat, source_msg_id, target_chat))
    return cur.fetchone() is not None

def mark_forwarded(source_chat: int, source_msg_id: int, target_chat: int):
    cur = db.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO forwarded(source_chat, source_msg_id, target_chat) VALUES (?,?,?)",
                    (source_chat, source_msg_id, target_chat))
        db.commit()
    except Exception as e:
        logger.exception("DB insert forward failed: %s", e)

# ---------------- Persistent in-memory config loaded from DB/config.json fallback ----------------
def load_runtime_config():
    # try from DB, else fallback to defaults
    cfg_json = db_get_config("runtime")
    if cfg_json:
        try:
            return json.loads(cfg_json)
        except:
            pass
    cfg = {
        "sources": DEFAULT_SOURCES.copy(),
        "targets": DEFAULT_TARGETS.copy(),
        "signature": DEFAULT_SIGNATURE,
        "thumb_path": DEFAULT_THUMB,
        "forward_delay": DEFAULT_DELAY,
        "remove_patterns": DEFAULT_REMOVE_PATTERNS.copy(),
        "website_map": [{"pattern": OLD_WEBSITE_RE, "replace_with": NEW_WEBSITE}],
        "filters": [],  # keywords
        "scheduler": {"enabled": False, "interval_seconds": 0, "last_run": 0}
    }
    db_set_config("runtime", json.dumps(cfg))
    return cfg

def save_runtime_config(cfg):
    db_set_config("runtime", json.dumps(cfg))

config = load_runtime_config()

# ---------------- Pyrogram client ----------------
app = Client("v5_5_bot_improved", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# dynamic semaphores (will be adjusted on FloodWait)
current_concurrent_uploads = INITIAL_CONCURRENT_UPLOADS
current_concurrent_workers = INITIAL_CONCURRENT_MESSAGE_WORKERS
upload_semaphore = asyncio.Semaphore(current_concurrent_uploads)
worker_semaphore = asyncio.Semaphore(current_concurrent_workers)

# per-target cooldowns (target -> next_allowed_timestamp)
target_cooldowns = {}  # map target_id -> timestamp

# message queue and control
message_queue = asyncio.Queue()

# adaptive counters to detect throttling
floodwait_events = 0
last_flood_time = 0

# ---------------- Utility functions ----------------
def clean_caption_text(caption: str) -> str:
    sig = config.get("signature", DEFAULT_SIGNATURE)
    if not caption or caption.strip() == "":
        text = sig
    else:
        text = caption
        for pat in config.get("remove_patterns", []):
            try:
                text = re.sub(pat, "", text, flags=re.IGNORECASE)
            except re.error:
                text = text.replace(pat, "")
        for wr in config.get("website_map", []):
            try:
                text = re.sub(wr["pattern"], wr["replace_with"], text, flags=re.IGNORECASE)
            except re.error:
                text = text.replace(wr.get("pattern",""), wr.get("replace_with",""))
        text = text.strip()
        if sig.lower() not in text.lower():
            text = f"{text}\n\n{sig}"
        site = config["website_map"][0]["replace_with"]
        if site not in text:
            text += f"\n\n𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\n{site}"
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

async def adaptive_reduce_concurrency():
    global current_concurrent_uploads, current_concurrent_workers, upload_semaphore, worker_semaphore
    # reduce concurrency on repeated floodwaits
    old_u = current_concurrent_uploads
    old_w = current_concurrent_workers
    current_concurrent_uploads = max(1, current_concurrent_uploads // 2)
    current_concurrent_workers = max(1, current_concurrent_workers // 2)
    upload_semaphore = asyncio.Semaphore(current_concurrent_uploads)
    worker_semaphore = asyncio.Semaphore(current_concurrent_workers)
    logger.warning("Adaptive throttle: uploads %s->%s, workers %s->%s", old_u, current_concurrent_uploads, old_w, current_concurrent_workers)

async def adaptive_increase_concurrency():
    global current_concurrent_uploads, current_concurrent_workers, upload_semaphore, worker_semaphore
    # slowly ramp up if no flood in last minute
    now = time.time()
    if now - last_flood_time > 60:
        current_concurrent_uploads = min(12, current_concurrent_uploads + 1)
        current_concurrent_workers = min(6, current_concurrent_workers + 1)
        upload_semaphore = asyncio.Semaphore(current_concurrent_uploads)
        worker_semaphore = asyncio.Semaphore(current_concurrent_workers)
        logger.info("Adaptive increase: uploads -> %s, workers -> %s", current_concurrent_uploads, current_concurrent_workers)

def ensure_temp_dir():
    tmp = tempfile.gettempdir()
    return tmp

def is_file_too_big(path: str):
    try:
        size_mb = os.path.getsize(path) / (1024*1024)
        return size_mb > MAX_FILE_SIZE_MB
    except:
        return True

# ---------------- Robust sender with retries and adaptive handling ----------------
async def send_video_with_retry(client: Client, target: int, video_path: str, caption: str, thumb_path: str):
    global floodwait_events, last_flood_time
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            # per-target cooldown check
            now = time.time()
            next_allowed = target_cooldowns.get(target, 0)
            if now < next_allowed:
                wait = next_allowed - now
                logger.info("Cooldown for %s active, sleeping %s", target, wait)
                await asyncio.sleep(wait)

            async with upload_semaphore:
                await client.send_video(
                    chat_id=int(target),
                    video=video_path,
                    caption=caption,
                    thumb=thumb_path if os.path.exists(thumb_path) else None,
                    supports_streaming=True
                )
            # success: set small cooldown for this target
            target_cooldowns[target] = time.time() + 0.5  # half second cooldown
            return True
        except FloodWait as fw:
            # FloodWait object may expose .value or .x
            wait = int(getattr(fw, "value", getattr(fw, "x", 10))) + 1
            floodwait_events += 1
            last_flood_time = time.time()
            logger.warning("FloodWait %s seconds for target %s (attempt %s)", wait, target, attempt)
            # cool down and adapt: reduce concurrency
            await adaptive_reduce_concurrency()
            await asyncio.sleep(wait)
            attempt += 1
        except RPCError as rpc:
            logger.error("RPCError sending to %s: %s", target, rpc)
            return False
        except Exception as e:
            attempt += 1
            backoff = BACKOFF_BASE ** attempt
            logger.exception("Send attempt %s to %s failed: %s — backing off %ss", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    logger.error("Failed to send video to %s after %s attempts", target, MAX_RETRIES)
    return False

# ---------------- Process a single message (download once, upload to all targets) ----------------
async def process_single_message(client: Client, msg, targets: List[int]):
    if not msg:
        return {"sent": 0, "failed": 0, "skipped": 0}
    caption = clean_caption_text(msg.caption)
    # apply filters
    filters_list = config.get("filters", [])
    if filters_list:
        match = False
        txt = (msg.caption or "") + " " + (getattr(msg, "text", "") or "")
        for kw in filters_list:
            if kw.strip().lower() in txt.lower():
                match = True
                break
        if not match:
            db_increment_stat("skipped")
            return {"sent": 0, "failed": 0, "skipped": 1}

    is_video = bool(msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video/")))
    sent = failed = skipped = 0

    if is_video:
        # download to temp
        tmpdir = ensure_temp_dir()
        fname = os.path.join(tmpdir, f"tmp_{msg.chat.id}_{msg.message_id}_{int(time.time()*1000)}.mp4")
        try:
            path = await msg.download(file_name=fname)
            if is_file_too_big(path):
                logger.warning("File too big, skipping: %s", path)
                db_increment_stat("skipped")
                try: os.remove(path)
                except: pass
                return {"sent":0,"failed":0,"skipped":1}
        except Exception as e:
            logger.exception("Download error for %s:%s -> %s", msg.chat.id, msg.message_id, e)
            return {"sent":0,"failed":len(targets),"skipped":0}

        # create tasks to send to targets but respect idempotency
        send_tasks = []
        for t in targets:
            if has_been_forwarded(msg.chat.id, msg.message_id, t):
                logger.info("Already forwarded %s:%s to %s — skipping", msg.chat.id, msg.message_id, t)
                skipped += 1
                continue
            task = asyncio.create_task(send_video_with_retry(client, t, path, caption, config.get("thumb_path", DEFAULT_THUMB)))
            send_tasks.append((t, task))

        if send_tasks:
            results = await asyncio.gather(*(t for _, t in send_tasks), return_exceptions=True)
            for (target, _), res in zip(send_tasks, results):
                ok = res is True
                if ok:
                    sent += 1
                    mark_forwarded(msg.chat.id, msg.message_id, target)
                    db_increment_stat("forwarded")
                else:
                    failed += 1
                    db_increment_stat("failed")
        # cleanup
        try:
            if os.path.exists(path):
                os.remove(path)
        except:
            pass
        await adaptive_increase_concurrency()
    else:
        # non-video: copy per target with cooldown & idempotency
        for t in targets:
            if has_been_forwarded(msg.chat.id, msg.message_id, t):
                skipped += 1
                continue
            try:
                async with upload_semaphore:
                    await msg.copy(chat_id=int(t), caption=caption)
                sent += 1
                mark_forwarded(msg.chat.id, msg.message_id, t)
                db_increment_stat("forwarded")
            except FloodWait as fw:
                wait = int(getattr(fw, "value", getattr(fw, "x", 10))) + 1
                logger.warning("FloodWait on copy to %s sleep %s", t, wait)
                await asyncio.sleep(wait)
                try:
                    await msg.copy(chat_id=int(t), caption=caption)
                    sent += 1
                    mark_forwarded(msg.chat.id, msg.message_id, t)
                    db_increment_stat("forwarded")
                except Exception as e:
                    logger.exception("Copy retry failed to %s: %s", t, e)
                    failed += 1
                    db_increment_stat("failed")
            except Exception as e:
                logger.exception("Copy failed to %s: %s", t, e)
                failed += 1
                db_increment_stat("failed")
            await asyncio.sleep(config.get("forward_delay", DEFAULT_DELAY))
    return {"sent":sent,"failed":failed,"skipped":skipped}

# ---------------- Forward range concurrently (main heavy-lifter) ----------------
async def forward_range_concurrent(client: Client, source_chat: int, start_id: int, end_id: int, progress_message):
    ids = list(range(start_id, end_id+1))
    msgs = await client.get_messages(source_chat, ids)
    msgs = [m for m in msgs if m]
    total = len(msgs)
    if total == 0:
        await progress_message.edit_text("⚠️ No messages found.")
        return {"sent":0,"failed":0,"skipped":0}

    await progress_message.edit_text(f"🚀 Forwarding {total} messages with concurrency (workers={current_concurrent_workers})...")
    tasks = []
    for m in msgs:
        # submit worker tasks but worker_semaphore bound reduces parallelism
        async def schedule_worker(mg):
            async with worker_semaphore:
                return await process_single_message(client, mg, config.get("targets", []))
        tasks.append(asyncio.create_task(schedule_worker(m)))

    sent_total = failed_total = skipped_total = 0
    done = 0
    for fut in asyncio.as_completed(tasks):
        res = await fut
        sent_total += res.get("sent",0)
        failed_total += res.get("failed",0)
        skipped_total += res.get("skipped",0)
        done += 1
        await progress_message.edit_text(f"📤 Done: {done}/{total} — Sent: {sent_total} | Failed: {failed_total} | Skipped: {skipped_total}")
    await progress_message.edit_text(f"✅ Completed. Sent: {sent_total} | Failed: {failed_total} | Skipped: {skipped_total}")
    return {"sent":sent_total,"failed":failed_total,"skipped":skipped_total}

# ---------------- Owner-only decorator ----------------
def owner_only(func):
    async def wrapper(client, message):
        if not message.from_user or message.from_user.id != OWNER_ID:
            await message.reply_text("❌ Not authorized.")
            return
        return await func(client, message)
    return wrapper

# ---------------- Commands ----------------

@app.on_message(filters.command("start") & filters.user(OWNER_ID))
@owner_only
async def cmd_start(client, message):
    await message.reply_text("Bot running. Use /panel or /help for commands.")

@app.on_message(filters.command("status") & filters.user(OWNER_ID))
@owner_only
async def cmd_status(client, message):
    s = config
    text = (f"✅ Status\nSources: {s.get('sources')}\nTargets: {s.get('targets')}\n"
            f"Signature: {s.get('signature')}\nThumb: {s.get('thumb_path')}\n"
            f"Filters: {s.get('filters')}\nWorkers: {current_concurrent_workers} Uploads: {current_concurrent_uploads}\n"
            f"Stats forwarded={db_get_stat('forwarded')}, failed={db_get_stat('failed')}, skipped={db_get_stat('skipped')}")
    await message.reply_text(text)

def db_get_stat(k):
    cur = db.cursor()
    cur.execute("SELECT value FROM stats WHERE key=?", (k,))
    r = cur.fetchone()
    return r[0] if r else 0

@app.on_message(filters.command("verifytargets") & filters.user(OWNER_ID))
@owner_only
async def cmd_verifytargets(client, message):
    await message.reply_text("🔍 Verifying targets...")
    out = []
    for t in config.get("targets", []):
        try:
            chat = await client.get_chat(int(t))
            out.append(f"✅ {getattr(chat,'title', str(t))} (`{t}`)")
        except Exception as e:
            out.append(f"❌ `{t}` | {e}")
    await message.reply_text("\n".join(out))

@app.on_message(filters.command("addsource") & filters.user(OWNER_ID))
@owner_only
async def cmd_addsource(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /addsource -100123...")
    if cid not in config["sources"]:
        config["sources"].append(cid); save_runtime_config(config)
        await message.reply_text(f"✅ Added source {cid}")
    else:
        await message.reply_text("⚠️ already exists")

@app.on_message(filters.command("removesource") & filters.user(OWNER_ID))
@owner_only
async def cmd_removesource(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /removesource -100123...")
    if cid in config["sources"]:
        config["sources"].remove(cid); save_runtime_config(config)
        await message.reply_text(f"🗑 Removed {cid}")
    else:
        await message.reply_text("⚠️ Not found")

@app.on_message(filters.command("addtarget") & filters.user(OWNER_ID))
@owner_only
async def cmd_addtarget(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /addtarget -100123...")
    if cid not in config["targets"]:
        config["targets"].append(cid); save_runtime_config(config)
        await message.reply_text(f"✅ Added target {cid}")
    else:
        await message.reply_text("⚠️ already exists")

@app.on_message(filters.command("removetarget") & filters.user(OWNER_ID))
@owner_only
async def cmd_removetarget(client, message):
    try:
        cid = int(message.text.split()[1])
    except:
        return await message.reply_text("Usage: /removetarget -100123...")
    if cid in config["targets"]:
        config["targets"].remove(cid); save_runtime_config(config)
        await message.reply_text(f"🗑 Removed {cid}")
    else:
        await message.reply_text("⚠️ Not found")

@app.on_message(filters.command("setcaption") & filters.user(OWNER_ID))
@owner_only
async def cmd_setcaption(client, message):
    text = message.text.replace("/setcaption","").strip()
    if not text:
        return await message.reply_text("Usage: /setcaption <text>")
    config["signature"] = text; save_runtime_config(config)
    await message.reply_text(f"✅ Signature set: {text}")

@app.on_message(filters.command("setfilter") & filters.user(OWNER_ID))
@owner_only
async def cmd_setfilter(client, message):
    kw = message.text.replace("/setfilter","").strip()
    if not kw:
        return await message.reply_text("Usage: /setfilter <keyword>")
    config.setdefault("filters", []).append(kw)
    save_runtime_config(config)
    await message.reply_text(f"✅ Filter added: {kw}")

@app.on_message(filters.command("removefilter") & filters.user(OWNER_ID))
@owner_only
async def cmd_removefilter(client, message):
    try:
        term = message.text.split(maxsplit=1)[1].strip()
    except:
        return await message.reply_text("Usage: /removefilter <keyword>")
    if term in config.get("filters", []):
        config["filters"].remove(term); save_runtime_config(config)
        await message.reply_text("✅ removed")
    else:
        await message.reply_text("⚠️ not found")

@app.on_message(filters.command("setthumb") & filters.user(OWNER_ID))
@owner_only
async def cmd_setthumb(client, message):
    if not message.reply_to_message:
        return await message.reply_text("Reply to an image with /setthumb")
    tgt = message.reply_to_message
    if not (tgt.photo or (tgt.document and getattr(tgt.document,"mime_type","", "").startswith("image/"))):
        return await message.reply_text("Reply must contain image")
    path = await tgt.download(file_name="thumb.jpg")
    config["thumb_path"] = path; save_runtime_config(config)
    await message.reply_text("✅ Thumb updated")

@app.on_message(filters.command("linkforward") & filters.user(OWNER_ID))
@owner_only
async def cmd_linkforward(client, message):
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply_text("Usage: /linkforward <link1> <link2>")
    def extract_id(link):
        m = re.search(r"/(\d+)$", link)
        return int(m.group(1)) if m else None
    s = extract_id(parts[1]); e = extract_id(parts[2])
    if not s or not e:
        return await message.reply_text("Invalid links")
    if s > e: s,e = e,s
    progress = await message.reply_text(f"🔁 Forwarding {s} → {e} ...")
    await forward_range_concurrent(client, config.get("sources", [])[0], s, e, progress)

@app.on_message(filters.command("schedule") & filters.user(OWNER_ID))
@owner_only
async def cmd_schedule(client, message):
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply_text("Usage: /schedule <seconds>")
    try:
        interval = int(parts[1])
    except:
        return await message.reply_text("Interval must be int seconds")
    config["scheduler"]["enabled"] = True
    config["scheduler"]["interval_seconds"] = interval
    config["scheduler"]["last_run"] = int(time.time())
    save_runtime_config(config)
    # start worker task if not running
    global scheduler_task
    if 'scheduler_task' in globals() and scheduler_task and not scheduler_task.done():
        scheduler_task.cancel()
    scheduler_task = asyncio.create_task(scheduler_worker(client))
    await message.reply_text(f"✅ Scheduler set: every {interval}s")

@app.on_message(filters.command("stop") & filters.user(OWNER_ID))
@owner_only
async def cmd_stop(client, message):
    config["scheduler"]["enabled"] = False
    save_runtime_config(config)
    global scheduler_task
    if 'scheduler_task' in globals() and scheduler_task and not scheduler_task.done():
        scheduler_task.cancel()
    await message.reply_text("⏹ Scheduler stopped")

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
@owner_only
async def cmd_stats(client, message):
    forwarded = db_get_stat("forwarded")
    failed = db_get_stat("failed")
    skipped = db_get_stat("skipped")
    await message.reply_text(f"Stats: forwarded={forwarded}, failed={failed}, skipped={skipped}")

@app.on_message(filters.command("viewlog") & filters.user(OWNER_ID))
@owner_only
async def cmd_viewlog(client, message):
    if not os.path.exists(LOG_FILE):
        return await message.reply_text("No log file")
    with open(LOG_FILE,"r",encoding="utf-8") as f:
        data = f.read()[-3500:]
    await message.reply_text(f"`{data}`", disable_web_page_preview=True)

@app.on_message(filters.command("clearlog") & filters.user(OWNER_ID))
@owner_only
async def cmd_clearlog(client, message):
    open(LOG_FILE,"w").close()
    await message.reply_text("✅ Log cleared")

# ---------------- Scheduler worker ----------------
async def scheduler_worker(client: Client):
    try:
        while config.get("scheduler", {}).get("enabled", False):
            interval = config["scheduler"].get("interval_seconds", 0)
            if interval <= 0:
                break
            for src in config.get("sources", []):
                # get last 20 messages and forward new ones
                msgs = await client.get_history(src, limit=20)
                to_forward = []
                last_run = config["scheduler"].get("last_run", 0)
                for m in reversed(msgs):
                    if hasattr(m, "date") and (int(m.date.timestamp()) > last_run):
                        to_forward.append(m)
                if to_forward:
                    progress_msg = await client.send_message(OWNER_ID, f"Scheduler forwarding {len(to_forward)} from {src}")
                    ids = [m.message_id for m in to_forward]
                    await forward_range_concurrent(client, src, min(ids), max(ids), progress_msg)
                    config["scheduler"]["last_run"] = int(time.time())
                    save_runtime_config(config)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("Scheduler cancelled")
    except Exception as e:
        logger.exception("Scheduler error: %s", e)

# ---------------- Auto-forward on new messages (real-time) ----------------
@app.on_message(filters.chat(lambda c: c in config.get("sources", [])))
async def on_source_message(client, message):
    # submit to background processing
    logger.info("New source message %s:%s queued", message.chat.id, message.message_id)
    asyncio.create_task(process_single_message(client, message, config.get("targets", [])))

# ---------------- Startup ----------------
if __name__ == "__main__":
    logger.info("Starting improved bot V5.5")
    # ensure db and config ok
    save_runtime_config(config)
    scheduler_task = None
    if config.get("scheduler", {}).get("enabled", False):
        scheduler_task = asyncio.get_event_loop().create_task(scheduler_worker(app))
    app.run()