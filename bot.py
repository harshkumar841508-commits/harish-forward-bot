# bot.py — V9 ULTRA (single-file)
# Requirements: pyrogram, tgcrypto, aiofiles, python-dotenv, aiohttp
# Deploy: Procfile -> worker: python bot.py

import os
import re
import json
import time
import math
import random
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError
import aiofiles

# -------------------------
# ========== CONFIG ==========
# -------------------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA")
OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))
# SOURCE default (can be changed by commands)
DEFAULT_SOURCE = int(os.getenv("SOURCE_CHANNEL", "-1003433745100"))
# Comma separated targets env var e.g. -100111,-100222
env_targets = os.getenv("TARGET_CHANNELS", "-1003404830427")
DEFAULT_TARGETS = [int(x.strip()) for x in env_targets.split(",") if x.strip()]

# Performance tuning
DEFAULT_CONCURRENCY = int(os.getenv("CONCURRENCY", "12"))   # ultra speed tuned
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.25"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "5"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "4096"))  # 4GB limit guard
USER_SESSION = os.getenv("USER_SESSION", "").strip()  # optional StringSession for private channel read
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TMP_DIR = Path(tempfile.gettempdir())

# Caption cleaning
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu",
    r"Join-@skillwithgaurav", r"Gaurav RaJput", r"Gaurav"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = os.getenv("NEW_WEBSITE", "https://bio.link/manmohak")
DEFAULT_SIGNATURE = os.getenv("DEFAULT_SIGNATURE", "Extracted by➤@course_wale")
DEFAULT_THUMB = os.getenv("THUMB_FILE", "thumb.jpg")

# Files
STATE_FILE = Path("state.json")
CONFIG_FILE = Path("config.json")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("v9-ultra")

# -------------------------
# ========= PERSIST =========
# -------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("load_json failed %s: %s", path, e)
    return default

def safe_write_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.error("safe_write_json failed: %s", e)

# load config+state
config = load_json(CONFIG_FILE, {
    "targets": DEFAULT_TARGETS.copy(),
    "thumb": DEFAULT_THUMB,
    "signature": DEFAULT_SIGNATURE,
    "concurrency": DEFAULT_CONCURRENCY,
    "forward_delay": FORWARD_DELAY,
})
state = load_json(STATE_FILE, {})

# runtime variables
TARGETS = config.get("targets", DEFAULT_TARGETS.copy())
THUMB = config.get("thumb", DEFAULT_THUMB)
SIGNATURE = config.get("signature", DEFAULT_SIGNATURE)
CONCURRENCY = config.get("concurrency", DEFAULT_CONCURRENCY)
FORWARD_DELAY = config.get("forward_delay", FORWARD_DELAY)

# metrics
metrics = {"forwards": 0, "failures": 0, "retries": 0, "active_tasks": 0}

# -------------------------
# ======= CLIENTS ==========
# -------------------------
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing. Set BOT_TOKEN environment.")
    raise SystemExit("Missing BOT_TOKEN")

bot = Client("v9_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client = None
if USER_SESSION:
    try:
        user_client = Client(session_name=USER_SESSION, api_id=API_ID, api_hash=API_HASH)
        logger.info("USER_SESSION configured.")
    except Exception as e:
        logger.warning("USER_SESSION init failed: %s", e)
        user_client = None

# controller
controller = {
    "range_task": None,
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "interactive": {}
}
controller["pause_event"].set()

# per-target last-send to adaptive rate
_last_send_time: Dict[int, float] = {}

# -------------------------
# ======= UTILITIES ========
# -------------------------
def clean_caption(text: Optional[str]) -> str:
    sig = config.get("signature", SIGNATURE)
    if not text:
        return sig
    out = text
    for pat in REMOVE_PATTERNS:
        try:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        except re.error:
            out = out.replace(pat, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if sig.lower() not in out.lower():
        out = f"{out}\n\n{sig}"
    return re.sub(r"\n{3,}", "\n\n", out)

def parse_msgid_and_chat(link: str) -> Optional[Dict[str,Any]]:
    try:
        link = link.strip()
        if "t.me/" not in link:
            return None
        parts = link.split("/")
        msg_id = int(parts[-1])
        if "/c/" in link:
            chat_id = int(parts[-2])
            return {"chat_id": chat_id, "msg_id": msg_id}
        else:
            chat_name = parts[-2]
            return {"chat_username": chat_name, "msg_id": msg_id}
    except:
        return None

async def download_media_to_tmp(msg: Message) -> Optional[str]:
    if not (msg.video or (msg.document and getattr(msg.document, "mime_type","").startswith("video"))):
        return None
    try:
        base = TMP_DIR / f"v9_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        path = await msg.download(file_name=str(base))
        size_mb = Path(path).stat().st_size / (1024*1024)
        if size_mb > MAX_FILE_MB:
            logger.warning("File too large %.1fMB skipping", size_mb)
            try: Path(path).unlink()
            except: pass
            return None
        return path
    except Exception as e:
        logger.warning("download_media_to_tmp error: %s", e)
        return None

async def adaptive_wait_for_target(target: int):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    min_interval = config.get("forward_delay", FORWARD_DELAY) / max(1, len(config.get("targets", [1])))
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]=None) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            if local_path is None:
                await src_msg.copy(chat_id=target, caption=caption)
                metrics["forwards"] += 1
                return True
            # send from local file (no re-encode)
            if local_path and Path(local_path).exists():
                suffix = Path(local_path).suffix.lower()
                if suffix in [".mp4", ".mkv", ".webm", ".mov", ".avi"]:
                    await client_for_send.send_video(chat_id=target, video=local_path, caption=caption, thumb=thumb if thumb and Path(thumb).exists() else None, supports_streaming=True)
                else:
                    await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback copy
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            logger.warning("FloodWait %s -> sleeping", wait)
            await asyncio.sleep(wait)
        except RPCError as rpc:
            logger.error("RPCError sending to %s: %s", target, rpc)
            metrics["failures"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = min(60, (2 ** attempt) + random.random()*2)
            logger.warning("send attempt %d to %s failed: %s; backoff %.1f", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    metrics["failures"] += 1
    return False

async def forward_msg_to_targets(client_for_send: Client, src_msg: Message, caption: str, targets: List[int], client_for_read: Client):
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type","").startswith("video")):
        # attempt to copy without download for speed; if fails download once and reuse
        try:
            # quick copy trial to first target — but we will use per-target copy inside send_with_retry
            pass
        except Exception:
            pass
    # download once if copy fails later
    sem = asyncio.Semaphore(config.get("concurrency", DEFAULT_CONCURRENCY))
    results = {}
    # We'll download lazily on first failure
    downloaded = False
    local_path = None
    async def _send(tid):
        nonlocal downloaded, local_path
        async with sem:
            await adaptive_wait_for_target(tid)
            # try copy first
            ok = False
            try:
                await src_msg.copy(chat_id=tid, caption=caption)
                ok = True
                metrics["forwards"] += 1
            except Exception as e:
                # fallback to download+upload
                if not downloaded:
                    local_path = await download_media_to_tmp(src_msg)
                    downloaded = True
                ok = await send_with_retry(client_for_send, tid, src_msg, local_path, caption, thumb=config.get("thumb"))
            results[tid] = bool(ok)
            await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
    tasks = [asyncio.create_task(_send(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)
    if local_path:
        try: Path(local_path).unlink()
        except: pass
    return results

# -------------------------
# ===== RANGE WORKER =======
# -------------------------
async def range_worker(client_for_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str):
    metrics["active_tasks"] += 1
    total = last - first + 1
    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start = max(first, last_sent + 1)
    sent_count = 0 if last_sent < first else (last_sent - first + 1)
    progress_msg = await origin_msg.reply_text(f"Starting forward {start} → {last} (total {total}) to {len(targets)} targets.")
    start_time = time.time()
    try:
        for mid in range(start, last + 1):
            if controller["stop_flag"]:
                controller["stop_flag"] = False
                await progress_msg.edit_text(f"Stopped by owner. Sent: {sent_count}/{total}")
                break
            await controller["pause_event"].wait()
            # fetch message
            try:
                if "chat_id" in source_identifier:
                    src = await client_for_read.get_messages(source_identifier["chat_id"], mid)
                else:
                    src = await client_for_read.get_messages(source_identifier["chat_username"], mid)
            except Exception as e:
                logger.warning("get_messages failed for %s:%s -> %s", source_identifier, mid, e)
                src = None
            if not src:
                state.setdefault(task_key, {"first": first, "last": last})
                state[task_key]["last_sent"] = mid
                safe_write_json(STATE_FILE, state)
                continue
            # skip non-video
            if not (src.video or (src.document and getattr(src.document,"mime_type","").startswith("video"))):
                state.setdefault(task_key, {"first": first, "last": last})
                state[task_key]["last_sent"] = mid
                safe_write_json(STATE_FILE, state)
                continue
            caption = clean_caption(src.caption or src.text or "")
            t0 = time.time()
            results = await forward_msg_to_targets(bot, src, caption, targets, client_for_read)
            t1 = time.time()
            sent_count += 1
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            safe_write_json(STATE_FILE, state)
            succ = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            elapsed = t1 - start_time
            avg_per_msg = elapsed / max(1, (mid - first + 1))
            progress = (mid - first + 1) / total * 100
            eta = max(0, (total - (mid - first + 1)) * avg_per_msg)
            speed_msg = f"{progress:.1f}% • Sent {mid-first+1}/{total} • Succ:{succ} Fail:{fail} • ETA:{int(eta)}s"
            try:
                await progress_msg.edit_text(speed_msg)
            except: pass
        await progress_msg.edit_text(f"✅ Completed. Sent approx {sent_count}/{total}")
    except asyncio.CancelledError:
        await progress_msg.edit_text(f"Cancelled. Sent {sent_count}/{total}")
    except Exception as e:
        logger.exception("range_worker error: %s", e)
        await progress_msg.edit_text(f"❌ Error: {e}\nSent: {sent_count}/{total}")
    finally:
        metrics["active_tasks"] -= 1

# -------------------------
# ===== MAIN UI & CMDs ====
# -------------------------
def main_keyboard():
    kb = [
        [InlineKeyboardButton("▶ Interactive", callback_data="interactive"), InlineKeyboardButton("🔁 LinkForward", callback_data="linkforward")],
        [InlineKeyboardButton("⏸ Pause", callback_data="pause"), InlineKeyboardButton("▶ Resume", callback_data="resume"), InlineKeyboardButton("⏹ Stop", callback_data="stop")],
        [InlineKeyboardButton("📊 Stats", callback_data="stats"), InlineKeyboardButton("⚙️ Settings", callback_data="settings")]
    ]
    return InlineKeyboardMarkup(kb)

@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start(c: Client, m: Message):
    text = (
        "**V9 ULTRA — Video Extract & Forward**\n\n"
        "Owner Commands:\n"
        "/linkforward <link1> <link2>  - direct forward between two links\n"
        "/range  - interactive first->last\n"
        "/pause /resume /stop\n"
        "/status\n"
        "/setcaption <text>\n"
        "/setthumb (reply to image)\n"
        "/addtarget <id> /removetarget <id> /listtargets\n"
        "/exportstate /importstate\n"
        "/diagnostics\n\n"
        "Use inline panel below for quick actions."
    )
    await m.reply_text(text, reply_markup=main_keyboard())

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(c: Client, m: Message):
    cfg = config
    await m.reply_text(
        f"Targets: `{cfg.get('targets')}`\nSignature: {cfg.get('signature')}\n"
        f"Forwards: {metrics['forwards']}  Fails: {metrics['failures']}  Retries: {metrics['retries']}\nActive tasks: {metrics['active_tasks']}"
    )

@bot.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
async def cmd_setcaption(c: Client, m: Message):
    txt = " ".join(m.command[1:])
    if not txt:
        await m.reply_text("Usage: /setcaption <text>")
        return
    config["signature"] = txt
    safe_write_json(CONFIG_FILE, config)
    await m.reply_text(f"Signature set to:\n`{txt}`")

@bot.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def cmd_setthumb(c: Client, m: Message):
    if not m.reply_to_message:
        await m.reply_text("Reply to an image with /setthumb")
        return
    p = await m.reply_to_message.download(file_name="thumb.jpg")
    config["thumb"] = p
    safe_write_json(CONFIG_FILE, config)
    await m.reply_text("Thumbnail updated.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def cmd_addtarget(c: Client, m: Message):
    try:
        tid = int(m.command[1])
    except:
        await m.reply_text("Usage: /addtarget -100123...")
        return
    pool = config.get("targets", [])
    if tid in pool:
        await m.reply_text("Already present.")
        return
    pool.append(tid)
    config["targets"] = pool
    safe_write_json(CONFIG_FILE, config)
    await m.reply_text(f"Added {tid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def cmd_removetarget(c: Client, m: Message):
    try:
        tid = int(m.command[1])
    except:
        await m.reply_text("Usage: /removetarget -100123...")
        return
    pool = config.get("targets", [])
    if tid in pool:
        pool.remove(tid)
        config["targets"] = pool
        safe_write_json(CONFIG_FILE, config)
        await m.reply_text(f"Removed {tid}")
    else:
        await m.reply_text("Not found.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("listtargets"))
async def cmd_listtargets(c: Client, m: Message):
    await m.reply_text(f"Targets: `{config.get('targets', [])}`")

@bot.on_message(filters.user(OWNER_ID) & filters.command("diagnostics"))
async def cmd_diag(c: Client, m: Message):
    out = []
    try:
        me = await c.get_me()
        out.append(f"Bot @{getattr(me, 'username', 'unknown')}")
    except Exception as e:
        out.append(f"Bot auth failed: {e}")
    try:
        await c.get_chat(DEFAULT_SOURCE)
        out.append("Default source accessible")
    except Exception as e:
        out.append(f"Default source issue: {e}")
    await m.reply_text("\n".join(out))

@bot.on_message(filters.user(OWNER_ID) & filters.command("exportstate"))
async def cmd_exportstate(c: Client, m: Message):
    safe_write_json(CONFIG_FILE, config)
    safe_write_json(STATE_FILE, state)
    await m.reply_text("Config and state exported.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("importstate"))
async def cmd_importstate(c: Client, m: Message):
    global config, state
    config = load_json(CONFIG_FILE, config)
    state = load_json(STATE_FILE, state)
    await m.reply_text("Imported config and state.")

# -------------------------
# ===== Interactive range ==
# -------------------------
@bot.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def cmd_range_start(c: Client, m: Message):
    uid = m.from_user.id
    controller["interactive"][uid] = {"step": 1}
    await m.reply_text("Send FIRST message link (e.g. https://t.me/c/3433745100/164)")

@bot.on_message(filters.user(OWNER_ID) & filters.text)
async def interactive_listener(c: Client, m: Message):
    uid = m.from_user.id
    if uid not in controller["interactive"]:
        return
    data = controller["interactive"][uid]
    txt = m.text.strip()
    if data.get("step") == 1:
        parsed = parse_msgid_and_chat(txt)
        if not parsed:
            await m.reply_text("Could not parse first link. Send again.")
            return
        data["first_parsed"] = parsed
        data["step"] = 2
        await m.reply_text("First saved. Now send LAST message link.")
        return
    if data.get("step") == 2:
        parsed2 = parse_msgid_and_chat(txt)
        if not parsed2:
            await m.reply_text("Could not parse last link. Send again.")
            return
        first = data["first_parsed"]
        last = parsed2
        fmid = first["msg_id"]; lmid = last["msg_id"]
        if fmid > lmid:
            fmid, lmid = lmid, fmid
        controller["interactive"].pop(uid, None)
        await m.reply_text(f"Confirm forwarding {fmid} → {lmid} ? Type YES to start.")
        controller["interactive"][uid] = {"confirm": True, "first": fmid, "last": lmid, "source": first}
        return
    if data.get("confirm"):
        if txt.lower()