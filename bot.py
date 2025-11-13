"""
V8 Ultra-Speed Premium Telegram Forward Bot (bot.py)

Setup:
 - Add to repository root:
   - bot.py (this file)
   - Procfile -> `worker: python bot.py`
   - requirements.txt -> pyrogram, tgcrypto, aiofiles, python-dotenv, aiohttp (if health server used)

Configuration (recommended: use Heroku Config Vars or system env):
 - API_ID, API_HASH, BOT_TOKEN, OWNER_ID
 - SOURCE_CHANNEL (optional default), TARGET_CHANNELS (comma-separated)
 - USER_SESSION (optional StringSession to read private channels without adding bot)
 - FORWARD_DELAY (float), CONCURRENCY (int), RETRY_LIMIT (int), MAX_FILE_MB (int)

Security note:
 - Hardcoding BOT_TOKEN is insecure; recommended: set BOT_TOKEN in environment.
 - If you insist, replace BOT_TOKEN = os.getenv(...) with your token string below.

"""

import os
import re
import json
import time
import math
import asyncio
import logging
import tempfile
import random
from pathlib import Path
from typing import Optional, List, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, BadRequest

import aiofiles

# ---------- CONFIG (use env vars) ----------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")

# Recommended: set BOT_TOKEN in env. If you really want to hardcode, you can replace the getenv below.
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # <-- set in Heroku Config Vars
# Example hardcode (NOT RECOMMENDED):
# BOT_TOKEN = "8359601755:AAEZTVLTD9YlXb..." 

OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))

# Defaults (can be overridden by commands or env)
DEFAULT_SOURCE = int(os.getenv("SOURCE_CHANNEL", "-1003433745100"))
env_targets = os.getenv("TARGET_CHANNELS", "-1003404830427")
DEFAULT_TARGETS = [int(x.strip()) for x in env_targets.split(",") if x.strip()]

FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.2"))   # seconds between sends per target
CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))          # parallel uploads
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "2000"))        # max file to download (MB)
HEALTH_ENABLED = os.getenv("HEALTH_ENABLED", "0") == "1"
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))

USER_SESSION = os.getenv("USER_SESSION", "")  # optional StringSession for private channel reading

# Caption cleaning & replace rules
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

# ---------- FILES & LOG ----------
STATE_FILE = Path("state.json")
CONFIG_FILE = Path("v8_config.json")
TMP_DIR = Path(tempfile.gettempdir())
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("v8")

# ---------- helpers for persistence ----------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("load_json %s failed: %s", path, e)
    return default

def safe_write_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.error("safe_write_json failed: %s", e)

# ---------- load config/state ----------
config = load_json(CONFIG_FILE, {
    "targets": DEFAULT_TARGETS.copy(),
    "thumb": THUMB_FILE,
    "signature": DEFAULT_SIGNATURE,
    "forward_delay": FORWARD_DELAY,
    "concurrency": CONCURRENCY
})
state = load_json(STATE_FILE, {})

# update runtime from config
TARGETS = config.get("targets", DEFAULT_TARGETS.copy())
THUMB = config.get("thumb", THUMB_FILE)
SIGNATURE = config.get("signature", DEFAULT_SIGNATURE)
FORWARD_DELAY = config.get("forward_delay", FORWARD_DELAY)
CONCURRENCY = config.get("concurrency", CONCURRENCY)

# ---------- sanity check ----------
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing. Set BOT_TOKEN in environment or hardcode (not recommended).")
    raise SystemExit("Missing BOT_TOKEN")

# ---------- pyrogram clients ----------
bot = Client("v8_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user = None
if USER_SESSION:
    try:
        user = Client(session_name=USER_SESSION, api_id=API_ID, api_hash=API_HASH)
        logger.info("USER_SESSION provided — user-client enabled for private channels.")
    except Exception as e:
        logger.warning("USER_SESSION init failed: %s", e)
        user = None

# ---------- controllers & metrics ----------
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
    "active_tasks": 0,
    "bytes_forwarded": 0,
    "start_time": None
}

# ---------- utility functions ----------
def clean_caption(text: Optional[str]) -> str:
    sig = config.get("signature", SIGNATURE) if "SIGNATURE" in globals() else config.get("signature", SIGNATURE if 'SIGNATURE' in globals() else DEFAULT_SIGNATURE)
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
    """Parse t.me links:
       - https://t.me/c/<chatid>/<msgid>  -> internal chat id (private)
       - https://t.me/<username>/<msgid>  -> username
    """
    try:
        link = link.strip()
        if "t.me/" not in link:
            return None
        parts = link.split("/")
        msg_id = int(parts[-1])
        # if contains '/c/' pattern
        if "/c/" in link or (len(parts) >= 3 and parts[-3] == "c"):
            chat_id = int(parts[-2])
            return {"chat_id": int(str(chat_id)), "msg_id": msg_id}
        else:
            chat_name = parts[-2]
            return {"chat_username": chat_name, "msg_id": msg_id}
    except Exception:
        return None

async def download_media(msg: Message) -> Optional[str]:
    """Download video-like media to temp, return local path or None.
       Will enforce MAX_FILE_MB.
    """
    if not (msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video"))):
        return None
    try:
        fname = TMP_DIR / f"v8_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        path = await msg.download(file_name=str(fname))
        size_mb = Path(path).stat().st_size / (1024*1024)
        if size_mb > MAX_FILE_MB:
            logger.warning("File too large (%.1f MB) — skipping", size_mb)
            try: Path(path).unlink()
            except: pass
            return None
        return path
    except Exception as e:
        logger.warning("download_media error: %s", e)
        return None

# adaptive per-target wait (simple)
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    min_interval = config.get("forward_delay", FORWARD_DELAY) / max(1, len(config.get("targets", [1])))
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# send with retry, returns tuple(success:boolean, bytes_sent:int)
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]=None) -> (bool, int):
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # Prefer copy (fast) if local_path is None
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    # we can't measure bytes for copy; estimate 0
                    return True, 0
                except Exception:
                    pass
            # If we have local file, send from file (upload) — will bypass restrictions
            if local_path and Path(local_path).exists():
                # send_video preserves original size
                await client_for_send.send_video(
                    chat_id=target,
                    video=str(local_path),
                    caption=caption,
                    thumb=thumb if thumb and Path(thumb).exists() else None,
                    supports_streaming=True
                )
                metrics["forwards"] += 1
                sent_bytes = Path(local_path).stat().st_size
                metrics["bytes_forwarded"] += sent_bytes
                return True, sent_bytes
            # fallback: try copy again
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True, 0
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            logger.warning("FloodWait %s -> sleeping", wait)
            await asyncio.sleep(wait)
        except RPCError as rpc:
            logger.error("RPCError sending to %s: %s", target, rpc)
            metrics["failures"] += 1
            return False, 0
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = (2 ** attempt) + random.random()
            logger.warning("send attempt %d to %s failed: %s; backoff=%.1f", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    metrics["failures"] += 1
    return False, 0

# forward one source message to multiple targets concurrently, returns result map and bytes forwarded
async def forward_msg_to_targets(client_for_send: Client, src_msg: Message, caption: str, targets: List[int], thumb: Optional[str]=None):
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type", "").startswith("video")):
        # download once if necessary (some channels require download to bypass restrictions)
        # We'll try copy first and only download if copy fails per-target.
        # But for reliability and measured speed, we download once and upload to all (this is faster for multiple targets).
        local_path = await download_media(src_msg)

    sem = asyncio.Semaphore(config.get("concurrency", CONCURRENCY))
    results = {}
    bytes_sent_map = {}

    async def _send_one(tid):
        async with sem:
            await adaptive_wait_for_target(tid)
            # try fast copy first (works for public and when allowed)
            success = False
            bytes_sent = 0
            try:
                # Try copy; if it fails (restriction), fallback to upload from local_path
                try:
                    await src_msg.copy(chat_id=tid, caption=caption)
                    success = True
                except Exception as e_copy:
                    # fallback to upload
                    if not local_path:
                        # download now
                        nonlocal_local = await download_media(src_msg)
                        local = nonlocal_local
                    else:
                        local = local_path
                    if local:
                        ok, b = await send_with_retry(client_for_send, tid, src_msg, local, caption, thumb=thumb)
                        success = ok
                        bytes_sent = b
            except FloodWait as fw:
                await asyncio.sleep(getattr(fw, "value", 5) + 1)
            except Exception as e:
                logger.warning("Forward to %s failed: %s", tid, e)
                # final retry via upload
                if local_path:
                    ok, b = await send_with_retry(client_for_send, tid, src_msg, local_path, caption, thumb=thumb)
                    success = ok
                    bytes_sent = b
            results[tid] = bool(success)
            bytes_sent_map[tid] = bytes_sent
            await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)
    # cleanup local file if any
    if local_path:
        try: Path(local_path).unlink()
        except: pass
    return results, bytes_sent_map

# ---------- Range worker with live progress ----------
async def range_worker(client_for_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str):
    metrics["active_tasks"] += 1
    if metrics["start_time"] is None:
        metrics["start_time"] = time.time()
    total_msgs = max(0, last - first + 1)
    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start_idx = max(first, last_sent + 1)
    processed = 0 if last_sent < first else (last_sent - first + 1)
    speed_calc_bytes = 0
    speed_calc_ts = time.time()

    progress_text = (f"Starting forward `{start_idx}` → `{last}` (total {total_msgs})\n"
                     f"Targets: {len(targets)}\n")
    progress_msg = await origin_msg.reply_text(progress_text)

    try:
        for mid in range(start_idx, last + 1):
            # stop check
            if controller["stop_flag"]:
                controller["stop_flag"] = False
                await progress_msg.edit_text(f"Stopped by owner. Processed {processed}/{total_msgs}")
                break

            await controller["pause_event"].wait()

            # fetch message
            try:
                if "chat_id" in source_identifier:
                    src = await client_for_read.get_messages(source_identifier["chat_id"], mid)
                else:
                    username = source_identifier.get("chat_username")
                    src = await client_for_read.get_messages(username, mid)
            except BadRequest as br:
                logger.warning("BadRequest fetching %s:%s => %s", source_identifier, mid, br)
                src = None
            except Exception as e:
                logger.warning("get_messages failed for %s:%s -> %s", source_identifier, mid, e)
                src = None

            # persist progress and skip if no src
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            safe_write_json(STATE_FILE, state)

            if not src:
                # update progress (skipped)
                try:
                    pct = int((mid - first + 1) / total_msgs * 100) if total_msgs else 0
                    await progress_msg.edit_text(f"Skipping {mid} — {pct}% — Processed {processed}/{total_msgs}")
                except: pass
                continue

            # ensure only video-like
            if not (src.video or (src.document and getattr(src.document, "mime_type", "").startswith("video"))):
                try:
                    pct = int((mid - first + 1) / total_msgs * 100) if total_msgs else 0
                    await progress_msg.edit_text(f"Skipping non-video {mid} — {pct}% — Processed {processed}/{total_msgs}")
                except: pass
                continue

            caption = clean_caption(src.caption or src.text or "")

            # forward to targets
            results_map, bytes_map = await forward_msg_to_targets(bot, src, caption, targets, thumb=config.get("thumb"))

            # update counters
            succ = sum(1 for v in results_map.values() if v)
            fail = sum(1 for v in results_map.values() if not v)
            processed += 1

            # update speed calculation
            bytes_now = sum(bytes_map.values()) or 0
            speed_calc_bytes += bytes_now
            now_ts = time.time()
            elapsed = now_ts - speed_calc_ts
            speed_bps = speed_calc_bytes / elapsed if elapsed > 0 else 0
            # reset window every ~5s
            if elapsed > 5:
                speed_calc_bytes = 0
                speed_calc_ts = now_ts

            # update progress message
            try:
                pct = int((mid - first + 1) / total_msgs * 100) if total_msgs else 0
                avg_speed_kb = speed_bps / 1024
                text = (f"Forwarded {mid} ({mid-first+1}/{total_msgs}) — {pct}%\n"
                        f"Success: {succ}  Fail: {fail}\n"
                        f"Processed files: {processed}/{total_msgs}\n"
                        f"Speed: {avg_speed_kb:.1f} KB/s\n"
                        f"Total forwards: {metrics['forwards']}  Failures: {metrics['failures']}")
                await progress_msg.edit_text(text)
            except Exception:
                pass

            # persist
            state[task_key]["last_sent"] = mid
            safe_write_json(STATE_FILE, state)

        # Completed
        await progress_msg.edit_text(f"✅ Completed range {first} → {last}. Processed {processed}/{total_msgs}")
    except asyncio.CancelledError:
        await progress_msg.edit_text(f"❗ Cancelled. Processed {processed}/{total_msgs}")
    except Exception as e:
        logger.exception("range_worker error: %s", e)
        await progress_msg.edit_text(f"❌ Error: {e}\nProcessed {processed}/{total_msgs}")
    finally:
        metrics["active_tasks"] -= 1

# ---------- Inline Keyboard (icons + commands) ----------
def main_keyboard():
    kb = [
        [InlineKeyboardButton("▶ Start (linkforward)", "linkforward"),
         InlineKeyboardButton("🧭 Range (interactive)", "range")],
        [InlineKeyboardButton("⏸ Pause", "pause"),
         InlineKeyboardButton("▶ Resume", "resume"),
         InlineKeyboardButton("⏹ Stop", "stop")],
        [InlineKeyboardButton("⚙️ Status", "status"),
         InlineKeyboardButton("🧪 Diagnostics", "diagnostics")]
    ]
    return InlineKeyboardMarkup(kb)

# ---------- Bot Commands ----------
@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start(client, message: Message):
    text = (
        "**V8 Ultra-Speed Forwarder — Commands**\n\n"
        "🔹 `/linkforward <link1> <link2>` — start direct (first/last)\n"
        "🔹 `/range` — interactive first then last\n"
        "🔹 `/pause` `/resume` `/stop`\n"
        "🔹 `/status` — live stats\n"
        "🔹 `/setcaption <text>` — change signature\n"
        "🔹 Reply to image + `/setthumb` — set thumbnail\n"
        "🔹 `/addtarget -100...` `/removetarget -100...` `/listtargets`\n"
        "🔹 `/diagnostics` — connectivity checks\n\n"
        "🛡️ Private-channel support: set USER_SESSION env var (StringSession) to read private channels without adding bot."
    )
    await message.reply_text(text, reply_markup=main_keyboard())

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(client, message: Message):
    up_time = (time.time() - metrics["start_time"]) if metrics["start_time"] else 0
    speed_kb = (metrics["bytes_forwarded"] / up_time / 1024) if up_time > 0 else 0
    await message.reply_text(
        f"✅ Running\nTargets: `{config.get('targets')}`\nForwards: {metrics['forwards']}  Failures: {metrics['failures']}\n"
        f"Active tasks: {metrics['active_tasks']}\nBytes forwarded: {metrics['bytes_forwarded']} ({speed_kb:.1f} KB/s)\nUptime: {int(up_time)}s"
    )

@bot.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
asyn