# bot.py  — V6 Final (Range Forward + Video Extract & Upload + Control Panel)
# Features:
# - /range (interactive) and /linkforward (direct) for first/last links
# - Only videos (video messages or video-like documents)
# - Concurrency & retry with adaptive backoff
# - Pause / Resume / Stop
# - Progress reporting with success/fail counters
# - /setcaption, /setthumb, /addtarget, /removetarget, /listtargets
# - Diagnostics, export/import state.json
# - Optional USER_SESSION for private-channel reading (StringSession)
# - Health HTTP endpoint (optional)
# Deploy: Heroku Procfile -> worker: python bot.py

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
import aiohttp
import aiofiles

# ---------------- CONFIG (use env vars; safer)
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
# BOT_TOKEN should normally be set in environment (Heroku Config Vars).
# If you really want to hardcode (not recommended), replace "" with your token string.
BOT_TOKEN = os.getenv("8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA", "")  # <-- put token in Heroku Config Vars
OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))

# Defaults (can be overridden by commands)
DEFAULT_SOURCE = int(os.getenv("SOURCE_CHANNEL", "-1003433745100"))
env_targets = os.getenv("TARGET_CHANNELS", "-1003404830427")
DEFAULT_TARGETS = [int(x.strip()) for x in env_targets.split(",") if x.strip()]

# Tuning
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.5"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "6"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "600"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))

# Optional: user session string for reading private channels (StringSession)
USER_SESSION = os.getenv("USER_SESSION", "").strip()

# Caption replacements & cleaning
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = os.getenv("NEW_WEBSITE", "https://bio.link/manmohak")
DEFAULT_SIGNATURE = os.getenv("DEFAULT_SIGNATURE", "Extracted by➤@course_wale")
THUMB_FILE = os.getenv("THUMB_FILE", "thumb.jpg")

# Persistence
STATE_FILE = Path("state.json")
CONFIG_FILE = Path("v6_config.json")
TMP_DIR = Path(tempfile.gettempdir())

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("v6")

# ---------------- persistence helpers ----------------
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

# load config/state
config = load_json(CONFIG_FILE, {"targets": DEFAULT_TARGETS.copy(), "thumb": THUMB_FILE, "signature": DEFAULT_SIGNATURE, "forward_delay": FORWARD_DELAY, "concurrency": CONCURRENCY})
state = load_json(STATE_FILE, {})

# update runtime from config
TARGETS = config.get("targets", DEFAULT_TARGETS.copy())
THUMB = config.get("thumb", THUMB_FILE)
SIGNATURE = config.get("signature", DEFAULT_SIGNATURE)
FORWARD_DELAY = config.get("forward_delay", FORWARD_DELAY)
CONCURRENCY = config.get("concurrency", CONCURRENCY)

# ---------------- pyrogram clients ----------------
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing. Set BOT_TOKEN in environment (Heroku Config Vars).")
    raise SystemExit("Missing BOT_TOKEN")

bot = Client("v6_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user = None
if USER_SESSION:
    try:
        user = Client(session_name=USER_SESSION, api_id=API_ID, api_hash=API_HASH)
        logger.info("USER_SESSION configured — private channel extraction enabled.")
    except Exception as e:
        logger.warning("USER_SESSION init failed: %s", e)
        user = None

# controller & metrics
controller = {"range_task": None, "pause_event": asyncio.Event(), "stop_flag": False, "interactive": {}}
controller["pause_event"].set()
metrics = {"forwards": 0, "failures": 0, "retries": 0, "active_tasks": 0}

# ---------------- utilities ----------------
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
    """Parse t.me/c/<chatid>/<msgid> or t.me/<username>/<msgid>"""
    try:
        link = link.strip()
        if "t.me/" not in link:
            return None
        parts = link.split("/")
        msg_id = int(parts[-1])
        if "/c/" in link:
            # pattern: https://t.me/c/<chatid>/<msgid>
            chat_id = int(parts[-2])
            return {"chat_id": chat_id, "msg_id": msg_id}
        else:
            chat_name = parts[-2]
            return {"chat_username": chat_name, "msg_id": msg_id}
    except:
        return None

async def download_media(msg: Message) -> Optional[str]:
    """Download video or video-like document to temp and return path"""
    if not (msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video"))):
        return None
    try:
        target = TMP_DIR / f"v6_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        path = await msg.download(file_name=str(target))
        # size guard
        try:
            size_mb = Path(path).stat().st_size / (1024*1024)
            if size_mb > MAX_FILE_MB:
                logger.warning("File too large %.1fMB skipping", size_mb)
                try: Path(path).unlink(); return None
                except: return None
        except Exception:
            pass
        return path
    except Exception as e:
        logger.warning("download_media error: %s", e)
        return None

# adaptive wait per target (simple)
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    min_interval = FORWARD_DELAY / max(1, len(config.get("targets", [1])))
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# send with retries and backoff
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]=None) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # prefer copy if possible
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # else send from local file
            if local_path and Path(local_path).exists():
                suffix = Path(local_path).suffix.lower()
                if suffix in [".mp4", ".mkv", ".webm"]:
                    await client_for_send.send_video(chat_id=target, video=local_path, caption=caption, thumb=thumb if thumb and Path(thumb).exists() else None, supports_streaming=True)
                else:
                    await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # if still here, try copy once more
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
            backoff = (2 ** attempt) + random.random()
            logger.warning("send attempt %d to %s failed: %s; backoff %.1f", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    metrics["failures"] += 1
    return False

async def forward_msg_to_targets(client_for_send: Client, src_msg: Message, caption: str, targets: List[int], client_for_read: Client):
    """Download once if needed, then forward concurrently to targets"""
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type","").startswith("video")):
        local_path = await download_media(src_msg)
    sem = asyncio.Semaphore(config.get("concurrency", CONCURRENCY))
    results = {}
    async def _send(tid):
        async with sem:
            await adaptive_wait_for_target(tid)
            ok = await send_with_retry(client_for_send, tid, src_msg, local_path, caption, thumb=config.get("thumb"))
            results[tid] = bool(ok)
            # small spacing to avoid bursts
            await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
    tasks = [asyncio.create_task(_send(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)
    # cleanup local file
    if local_path:
        try: Path(local_path).unlink()
        except: pass
    return results

# ---------------- Range worker (persistent resume) ----------------
async def range_worker(client_for_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str):
    metrics["active_tasks"] += 1
    total = last - first + 1
    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start = max(first, last_sent + 1)
    sent = 0 if last_sent < first else (last_sent - first + 1)
    progress_msg = await origin_msg.reply_text(f"Starting forward {start} → {last} (total {total}) to {len(targets)} targets.")
    try:
        for mid in range(start, last + 1):
            if controller["stop_flag"]:
                controller["stop_flag"] = False
                await progress_msg.edit_text(f"Stopped by owner. Sent: {sent}/{total}")
                break
            await controller["pause_event"].wait()
            # fetch message
            try:
                if "chat_id" in source_identifier:
                    src = await client_for_read.get_messages(source_identifier["chat_id"], mid)
                else:
                    username = source_identifier.get("chat_username")
                    src = await client_for_read.get_messages(username, mid)
            except Exception as e:
                logger.warning("get_messages failed for %s:%s -> %s", source_identifier, mid, e)
                src = None
            if not src:
                # persist progress and continue
                state.setdefault(task_key, {"first": first, "last": last})
                state[task_key]["last_sent"] = mid
                safe_write_json(STATE_FILE, state)
                continue
            # ensure it's video
            if not (src.video or (src.document and getattr(src.document,"mime_type","").startswith("video"))):
                # skip non-video
                state.setdefault(task_key, {"first": first, "last": last})
                state[task_key]["last_sent"] = mid
                safe_write_json(STATE_FILE, state)
                continue
            caption = clean_caption(src.caption or src.text or "")
            results = await forward_msg_to_targets(bot, src, caption, targets, client_for_read)
            sent += 1
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            safe_write_json(STATE_FILE, state)
            succ = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            pct = int((mid - first + 1) / total * 100)
            try:
                await progress_msg.edit_text(f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}% — Success:{succ} Fail:{fail}")
            except: pass
        await progress_msg.edit_text(f"✅ Completed. Sent approx {sent}/{total}")
    except asyncio.CancelledError:
        await progress_msg.edit_text(f"Cancelled. Sent {sent}/{total}")
    except Exception as e:
        logger.exception("range_worker error: %s", e)
        await progress_msg.edit_text(f"❌ Error: {e}\nSent: {sent}/{total}")
    finally:
        metrics["active_tasks"] -= 1

# ---------------- Commands & flows ----------------
def main_kb():
    kb = [
        [InlineKeyboardButton("▶ Interactive", "interactive"), InlineKeyboardButton("🔁 LinkForward", "linkforward")],
        [InlineKeyboardButton("⏸ Pause", "pause"), InlineKeyboardButton("▶ Resume", "resume"), InlineKeyboardButton("⏹ Stop", "stop")],
        [InlineKeyboardButton("🧪 Diagnostics", "diagnostics"), InlineKeyboardButton("⚙️ Status", "status")]
    ]
    return InlineKeyboardMarkup(kb)

@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start(client, message: Message):
    txt = (
        "**Video Extract & Upload Bot V6 (Final)**\n\n"
        "Owner Commands:\n"
        "/linkforward <link1> <link2> - direct start\n"
        "/range - interactive start (first then last)\n"
        "/pause /resume /stop\n"
        "/status\n"
        "/setcaption <text>\n"
        "/setthumb (reply to photo)\n"
        "/addtarget /removetarget /listtargets\n"
        "/exportstate /importstate\n"
        "/diagnostics\n"
    )
    await message.reply_text(txt, reply_markup=main_kb())

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(client, message: Message):
    await message.reply_text(
        f"✅ Running\nSource default: `{DEFAULT_SOURCE}`\nTargets: `{config.get('targets')}`\n"
        f"Forwards: {metrics['forwards']}  Failures: {metrics['failures']}  Retries: {metrics['retries']}\n"
        f"Active tasks: {metrics['active_tasks']}"
    )

@bot.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
async def cmd_setcaption(client, message: Message):
    txt = " ".join(message.command[1:])
    if not txt:
        await message.reply_text("Usage: /setcaption <text>")
        return
    config["signature"] = txt
    safe_write_json(CONFIG_FILE, config)
    await message.reply_text(f"Signature set: {txt}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def cmd_setthumb(client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Reply to an image with /setthumb")
        return
    p = await message.reply_to_message.download(file_name="thumb.jpg")
    config["thumb"] = p
    safe_write_json(CONFIG_FILE, config)
    await message.reply_text("Thumbnail updated.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def cmd_addtarget(client, message: Message):
    try:
        tid = int(message.command[1])
    except:
        await message.reply_text("Usage: /addtarget -100123...")
        return
    pool = config.get("targets", [])
    if tid in pool:
        await message.reply_text("Already present.")
        return
    pool.append(tid)
    config["targets"] = pool
    safe_write_json(CONFIG_FILE, config)
    await message.reply_text(f"Added {tid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def cmd_removetarget(client, message: Message):
    try:
        tid = int(message.command[1])
    except:
        await message.reply_text("Usage: /removetarget -100123...")
        return
    pool = config.get("targets", [])
    if tid in pool:
        pool.remove(tid)
        config["targets"] = pool
        safe_write_json(CONFIG_FILE, config)
        await message.reply_text(f"Removed {tid}")
    else:
        await message.reply_text("Not in pool.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("listtargets"))
async def cmd_listtargets(client, message: Message):
    await message.reply_text(f"Targets: `{config.get('targets', [])}`")

@bot.on_message(filters.user(OWNER_ID) & filters.command("diagnostics"))
async def cmd_diag(client, message: Message):
    parts = []
    try:
        me = await client.get_me()
        parts.append(f"Bot @{me.username}")
    except Exception as e:
        parts.append(f"Bot auth failed: {e}")
    try:
        await client.get_chat(DEFAULT_SOURCE)
        parts.append("Default source accessible")
    except Exception as e:
        parts.append(f"Default source issue: {e}")
    await message.reply_text("\n".join(parts))

# Interactive /range
@bot.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def cmd_range_start(client, message: Message):
    uid = message.from_user.id
    controller["interactive"][uid] = {"step":1}
    await message.reply_text("Send FIRST message link (e.g. https://t.me/c/3240589036/340)")

@bot.on_message(filters.user(OWNER_ID) & filters.text)
async def interactive_listener(client, message: Message):
    uid = message.from_user.id
    if uid not in controller["interactive"]:
        return
    data = controller["interactive"][uid]
    txt = message.text.strip()
    if data["step"] == 1:
        parsed = parse_msgid_and_chat(txt)
        if not parsed:
            await message.reply_text("Could not parse first link. Send again.")
            return
        data["first_parsed"] = parsed
        data["step"] = 2
        await message.reply_text("First saved. Now send LAST message link.")
        return
    if data["step"] == 2:
        parsed2 = parse_msgid_and_chat(txt)
        if not parsed2:
            await message.reply_text("Could not parse last link. Send again.")
            return
        first = data["first_parsed"]
        last = parsed2
        fmid = first["msg_id"]; lmid = last["msg_id"]
        if fmid > lmid:
            fmid, lmid = lmid, fmid
        # confirmed
        controller["interactive"].pop(uid, None)
        await message.reply_text(f"Confirm to forward {fmid} → {lmid}? Reply 'yes' to start.")
        # store in temp dict until confirm
        controller["interactive"][uid] = {"confirm": True, "first": fmid, "last": lmid, "source": first}
        return
    if data.get("confirm"):
        if txt.lower() in ("yes","y"):
            first = data["first"]; last = data["last"]; source = data["source"]
            controller["interactive"].pop(uid, None)
            await message.reply_text(f"Starting forward {first} → {last} ...")
            client_for_read = user if user else bot
            key = f"{source.get('chat_id', source.get('chat_username'))}_{first}_{last}"
            task = asyncio.create_task(range_worker(client_for_read, message, source, first, last, config.get("targets", []), key))
            controller["range_task"] = task
            return
        else:
            controller["interactive"].pop(uid, None)
            await message.reply_text("Cancelled.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("linkforward"))
async def cmd_linkforward(client, message: Message):
    parts = message.text.split()
    if len(parts) != 3:
        await message.reply_text("Usage: /linkforward <link1> <link2>")
        return
    a = parse_msgid_and_chat(parts[1]); b = parse_msgid_and_chat(parts[2])
    if not a or not b:
        await message.reply_text("Couldn't parse links.")
        return
    fmid = a["msg_id"]; lmid = b["msg_id"]
    if fmid > lmid:
        fmid, lmid = lmid, fmid
    source = a
    client_for_read = user if user else bot
    key = f"{source.get('chat_id', source.get('chat_username'))}_{fmid}_{lmid}"
    await message.reply_text(f"Starting range {fmid} → {lmid} ...")
    task = asyncio.create_task(range_worker(client_for_read, message, source, fmid, lmid, config.get("targets", []), key))
    controller["range_task"] = task

# Pause / Resume / Stop
@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def cmd_pause(client, message: Message):
    controller["pause_event"].clear(); await message.reply_text("Paused.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def cmd_resume(client, message: Message):
    controller["pause_event"].set(); await message.reply_text("Resumed.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(client, message: Message):
    controller["stop_flag"] = True
    t = controller.get("range_task")
    if t and not t.done():
        t.cancel()
    await message.reply_text("Stop signaled.")

# export/import state
@bot.on_message(filters.user(OWNER_ID) & filters.command("exportstate"))
async def cmd_exportstate(client, message: Message):
    if STATE_FILE.exists():
        await message.reply_document(str(STATE_FILE))
    else:
        await message.reply_text("No state file present.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("importstate"))
async def cmd_importstate(client, message: Message):
    if message.reply_to_message and message.reply_to_message.document:
        p = await message.reply_to_message.download()
        try:
            data = json.loads(Path(p).read_text(encoding="utf-8"))
            global state
            state = data
            safe_write_json(STATE_FILE, state)
            await message.reply_text("Imported state.")
        except Exception as e:
            await message.reply_text(f"Import failed: {e}")
    else:
        await message.reply_text("Reply to a JSON state file to import.")

# ---------------- Health HTTP server (aiohttp) ----------------
async def start_health_server():
    async def handler(request):
        return aiohttp.web.json_response({
            "status": "ok",
            "metrics": metrics,
            "targets": len(config.get("targets", []))
        })
    app_ws = aiohttp.web.Application()
    app_ws.router.add_get("/health", handler)
    runner = aiohttp.web.AppRunner(app_ws)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    logger.info("Health endpoint running on port %s", HEALTH_PORT)

# ---------------- startup ----------------
async def start_user_if_needed():
    global user
    if USER_SESSION and user:
        try:
            await user.start()
            logger.info("User session started.")
        except Exception as e:
            logger.warning("User session start failed: %s", e)

async def on_startup():
    try:
        await start_health_server()
    except Exception as e:
        logger.warning("Health server failed: %s", e)
    await start_user_if_needed()
    logger.info("Bot ready. Metrics: %s", metrics)

def run():
    logger.info("Starting Video Extract & Upload Bot V6")
    safe_write_json(CONFIG_FILE, config)
    safe_write_json(STATE_FILE, state)
    loop = asyncio.get_event_loop()
    loop.create_task(on_startup())
    bot.run()

if __name__ == "__main__":
    run()