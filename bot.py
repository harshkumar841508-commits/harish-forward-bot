#!/usr/bin/env python3
# bot.py - V12 Ultra Max (complete single-file)
# Requirements: pyrogram, tgcrypto, aiohttp
# Put secrets in env: API_ID, API_HASH, BOT_TOKEN, OWNER_ID, USER_SESSION (optional)

import os
import re
import json
import time
import asyncio
import random
import tempfile
import signal
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from aiohttp import web

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError, SessionPasswordNeeded, PhoneCodeInvalid, ApiIdInvalid

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# -----------------------------
# Config / Env (user-provided defaults)
# -----------------------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
BOT_TOKEN = os.getenv("8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA", "")
OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))

DEFAULT_SIGNATURE = os.getenv("DEFAULT_SIGNATURE", "Extracted by➤@course_wale")
DEFAULT_TARGETS_ENV = os.getenv("TARGET_CHANNELS", "-1003404830427")
DEFAULT_TARGETS = [int(x.strip()) for x in DEFAULT_TARGETS_ENV.split(",") if x.strip()]
DEFAULT_DELAY = float(os.getenv("DEFAULT_DELAY", "1.5"))
DEFAULT_CONCURRENCY = int(os.getenv("DEFAULT_CONCURRENCY", "4"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "1500"))

TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_config.json")
STATE_FILE = Path("v12_state.json")

# -----------------------------
# Helpers for JSON persistence
# -----------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("load_json error %s: %s", path, e)
    return default

def save_json(path: Path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        log.warning("save_json error %s: %s", path, e)

# -----------------------------
# Initial config/state
# -----------------------------
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

config.setdefault("users", {})
config["users"].setdefault(str(OWNER_ID), {
    "role": "owner",
    "quota": 99999999,
    "used": 0,
    "expires": None,
    "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
    "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
    "thumb": None,
    "delay": config["global"].get("delay", DEFAULT_DELAY),
    "concurrency": config["global"].get("concurrency", DEFAULT_CONCURRENCY)
})
save_json(CONFIG_FILE, config)

# -----------------------------
# Runtime metrics + controller
# -----------------------------
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}
}
controller["pause_event"].set()

if not BOT_TOKEN:
    log.error("BOT_TOKEN not set in env. Exiting.")
    raise SystemExit("Set BOT_TOKEN environment variable before running")

# -----------------------------
# Initialize Pyrogram clients
# -----------------------------
bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

USER_SESSION = os.getenv("USER_SESSION", "")
user_client: Optional[Client] = None
if USER_SESSION:
    try:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        log.info("User session started — private channel reading enabled.")
    except Exception as e:
        log.warning("User session failed to start: %s", e)
        user_client = None
else:
    log.info("No USER_SESSION — private channels readable only if bot is member/admin.")

# -----------------------------
# Caption cleaning and helpers
# -----------------------------
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = os.getenv("NEW_WEBSITE", "https://bio.link/manmohak")

def get_user_cfg(user_id: int) -> Dict[str, Any]:
    u = config["users"].get(str(user_id))
    if not u:
        config["users"][str(user_id)] = {
            "role": "user",
            "quota": 0,
            "used": 0,
            "expires": None,
            "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
            "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
            "thumb": None,
            "delay": config["global"].get("delay", DEFAULT_DELAY),
            "concurrency": config["global"].get("concurrency", DEFAULT_CONCURRENCY)
        }
        save_json(CONFIG_FILE, config)
        return config["users"][str(user_id)]
    return u

def clean_caption(text: Optional[str], signature: str) -> str:
    if not text:
        return signature
    out = text
    for pat in REMOVE_PATTERNS:
        try:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        except re.error:
            out = out.replace(pat, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if signature.lower() not in out.lower():
        out = f"{out}\n\n{signature}"
    return re.sub(r"\n{3,}", "\n\n", out)

# -----------------------------
# parse_msg_link (robust)
# -----------------------------
def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        link = link.strip()
        link = link.split("?")[0].split("#")[0]
        parts = [p for p in link.split("/") if p]
        # handle t.me and telegram.me urls
        if any(d in link for d in ("t.me", "telegram.me")):
            # pattern t.me/c/<chatid>/<msgid>
            if "c" in parts:
                try:
                    idx = parts.index("c")
                    chatnum = parts[idx+1]
                    msgid = parts[idx+2]
                    chat_id = int(f"-100{chatnum}")
                    return {"chat_id": chat_id, "msg_id": int(msgid)}
                except Exception:
                    return None
            # pattern t.me/username/msgid
            if len(parts) >= 2:
                username = parts[-2]
                msgid = parts[-1]
                return {"chat_username": username, "msg_id": int(msgid)}
        # fallback: last two segments
        if len(parts) >= 2 and parts[-1].isdigit():
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
        return None
    return None

# -----------------------------
# safe_edit
# -----------------------------
async def safe_edit(msg: Optional[Message], new_text: str):
    if not msg:
        return
    try:
        old = getattr(msg, "text", "") or ""
        if old.strip() == new_text.strip():
            return
        await msg.edit_text(new_text)
    except Exception:
        return

# -----------------------------
# download_media
# -----------------------------
async def download_media(msg: Message) -> Optional[str]:
    try:
        if msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video")):
            out = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
            path = await msg.download(file_name=str(out))
            try:
                size_mb = Path(path).stat().st_size / (1024*1024)
                if size_mb > MAX_FILE_MB:
                    try: Path(path).unlink()
                    except: pass
                    return None
            except Exception:
                pass
            return path
    except Exception as e:
        log.warning("download_media error: %s", e)
    return None

# -----------------------------
# Adaptive wait for per-target pacing
# -----------------------------
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# -----------------------------
# send_with_retry
# -----------------------------
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # If we can copy (no downloaded file), try copying first
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # if there is a local file, send it via provided client
            if local_path and Path(local_path).exists():
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback: copy
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            log.info("FloodWait %s seconds while sending to %s", wait, target)
            await asyncio.sleep(wait)
        except RPCError as rpc:
            log.warning("RPCError sending to %s: %s", target, rpc)
            metrics["fails"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = (2 ** attempt) + random.random()
            log.warning("send attempt %s to %s failed: %s; backoff %s", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

# -----------------------------
# forward_to_targets
# -----------------------------
async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float, client_for_send: Client = None) -> Dict[int, bool]:
    if client_for_send is None:
        client_for_send = bot
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type", "").startswith("video")):
        local_path = await download_media(src_msg)

    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[int, bool] = {}

    async def _send_one(tid: int):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(client_for_send, int(tid), src_msg, local_path, caption)
            results[int(tid)] = bool(ok)
            await asyncio.sleep(0.15)

    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    if local_path:
        try: Path(local_path).unlink()
        except: pass

    return results

# -----------------------------
# fetch_source_message
# -----------------------------
async def fetch_source_message(source: Dict[str, Any], mid: int) -> Optional[Message]:
    try:
        reader = user_client if user_client else bot
        if "chat_id" in source:
            return await reader.get_messages(source["chat_id"], mid)
        else:
            return await reader.get_messages(source["chat_username"], mid)
    except Exception as e:
        log.warning("fetch_source_message error: %s", e)
        return None

# -----------------------------
# range_worker (task)
# -----------------------------
async def range_worker(client_read: Client, origin_msg: Message, source_identifier: Dict[str, Any], first: int, last: int, targets: List[int], task_key: str, starter_uid: int):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_total = 0
    fail_total = 0

    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start_mid = max(first, last_sent + 1)

    user_cfg = get_user_cfg(starter_uid)
    signature = user_cfg.get("signature", config["global"].get("signature", DEFAULT_SIGNATURE))
    delay = float(user_cfg.get("delay", config["global"].get("delay", DEFAULT_DELAY)))
    concurrency = int(user_cfg.get("concurrency", config["global"].get("concurrency", DEFAULT_CONCURRENCY)))
    targets_use = user_cfg.get("targets", targets)

    try:
        progress_msg = await origin_msg.reply_text(f"Starting forward {start_mid} → {last} (total {total}) to {len(targets_use)} targets.")
    except Exception:
        progress_msg = None

    try:
        for mid in range(start_mid, last + 1):
            if controller.get("stop_flag"):
                controller["stop_flag"] = False
                await safe_edit(progress_msg, f"⛔ Stopped by owner. Sent: {sent_total}/{total}")
                break

            await controller["pause_event"].wait()

            src = await fetch_source_message(source_identifier, mid)
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            save_json(STATE_FILE, state)

            if not src:
                continue
            if not (src.video or (src.document and getattr(src.document, "mime_type", "").startswith("video"))):
                # skip non-video
                continue

            caption = clean_caption(src.caption or src.text or "", signature)

            sending_client = bot
            results = await forward_to_targets(src, caption, targets_use, concurrency, delay, client_for_send=sending_client)

            ok = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            sent_total += ok
            fail_total += fail

            pct = int((mid - first + 1) / total * 100)
            try:
                await safe_edit(progress_msg, f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}% — Success:{ok} Fail:{fail}\nTotalSent:{sent_total} TotalFail:{fail_total}")
            except:
                pass

            await asyncio.sleep(delay)
        await safe_edit(progress_msg, f"✅ Completed. Sent approx {sent_total}/{total} Fail:{fail_total}")
    except Exception as e:
        log.exception("range_worker exception")
        try:
            await safe_edit(progress_msg, f"❌ Error: {e}\nSent: {sent_total}/{total}")
        except:
            pass
    finally:
        metrics["active_tasks"] -= 1
        controller["range_task"] = None
        try:
            state.pop(task_key, None)
            save_json(STATE_FILE, state)
        except:
            pass

# -----------------------------
# Utility to build source dict from link or chat id
# -----------------------------
def build_source_from_arg(arg: str) -> Optional[Dict[str, Any]]:
    # Accept link or numeric chat id or username
    if arg.startswith("http"):
        return parse_msg_link(arg)
    if arg.isdigit() or (arg.startswith("-") and arg[1:].isdigit()):
        return {"chat_id": int(arg)}
    if arg.startswith("@"):
        return {"chat_username": arg[1:]}
    # fallback: username without @
    return {"chat_username": arg}

# -----------------------------
# Bot command handlers: admin only wrappers
# -----------------------------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID or config["users"].get(str(user_id), {}).get("role") == "owner"

@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    txt = ("V12 Ultra Max — Forward Bot\n\n"
           "Commands (admin): /addtarget, /rmtarget, /setdelay, /setconcurrency, /setsignature, /targets, /status\n"
           "Usage: /go <link> <first>-<last>\nExamples:\n /go https://t.me/chan/123 10-50\n /go @username 100-120\n\nOwner-only commands: /stop /pause /resume /shutdown")
    await msg.reply_text(txt)

@bot.on_message(filters.command("status") & filters.user(OWNER_ID))
async def cmd_status(_, msg: Message):
    uptime = "uptime info not tracked"
    txt = (f"V12 Ultra Max Status\n\n"
           f"Metrics: forwards={metrics['forwards']} fails={metrics['fails']} retries={metrics['retries']} active_tasks={metrics['active_tasks']}\n"
           f"Config global: targets={config['global'].get('targets')} delay={config['global'].get('delay')} concurrency={config['global'].get('concurrency')}\n"
           f"Active range task: {bool(controller.get('range_task'))}\n")
    await msg.reply_text(txt)

@bot.on_message(filters.command("targets") & filters.user(OWNER_ID))
async def cmd_targets(_, msg: Message):
    await msg.reply_text("Global targets:\n" + "\n".join(map(str, config["global"].get("targets", []))))

@bot.on_message(filters.command("addtarget") & filters.user(OWNER_ID))
async def cmd_addtarget(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply_text("Usage: /addtarget <chat_id or @username>")
    arg = msg.command[1]
    try:
        if arg.startswith("@"):
            # store username
            config["global"].setdefault("targets", []).append(arg)
        else:
            config["global"].setdefault("targets", []).append(int(arg))
        save_json(CONFIG_FILE, config)
        await msg.reply_text("Added target.")
    except Exception as e:
        await msg.reply_text(f"Error: {e}")

@bot.on_message(filters.command("rmtarget") & filters.user(OWNER_ID))
async def cmd_rmtarget(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply_text("Usage: /rmtarget <chat_id or @username>")
    arg = msg.command[1]
    try:
        if arg.startswith("@"):
            config["global"]["targets"] = [t for t in config["global"]["targets"] if t != arg]
        else:
            config["global"]["targets"] = [t for t in config["global"]["targets"] if int(t) != int(arg)]
        save_json(CONFIG_FILE, config)
        await msg.reply_text("Removed (if existed).")
    except Exception as e:
        await msg.reply_text(f"Error: {e}")

@bot.on_message(filters.command("setdelay") & filters.user(OWNER_ID))
async def cmd_setdelay(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply_text("Usage: /setdelay <seconds>")
    try:
        config["global"]["delay"] = float(msg.command[1])
        save_json(CONFIG_FILE, config)
        await msg.reply_text(f"Set global delay to {config['global']['delay']}")
    except Exception as e:
        await msg.reply_text(f"Error: {e}")

@bot.on_message(filters.command("setconcurrency") & filters.user(OWNER_ID))
async def cmd_setconcurrency(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply_text("Usage: /setconcurrency <n>")
    try:
        config["global"]["concurrency"] = int(msg.command[1])
        save_json(CONFIG_FILE, config)
        await msg.reply_text(f"Set global concurrency to {config['global']['concurrency']}")
    except Exception as e:
        await msg.reply_text(f"Error: {e}")

@bot.on_message(filters.command("setsignature") & filters.user(OWNER_ID))
async def cmd_setsignature(_, msg: Message):
    if len(msg.command) < 2:
        return await msg.reply_text("Usage: /setsignature <text>")
    sig = " ".join(msg.command[1:])
    config["global"]["signature"] = sig
    save_json(CONFIG_FILE, config)
    await msg.reply_text(f"Set signature to:\n{sig}")

# -----------------------------
# Control commands: stop/pause/resume
# -----------------------------
@bot.on_message(filters.command("stop") & filters.user(OWNER_ID))
async def cmd_stop(_, msg: Message):
    controller["stop_flag"] = True
    controller["pause_event"].set()
    await msg.reply_text("Stop signal sent. Current active range will stop soon.")

@bot.on_message(filters.command("pause") & filters.user(OWNER_ID))
async def cmd_pause(_, msg: Message):
    controller["pause_event"].clear()
    await msg.reply_text("Paused. Use /resume to continue.")

@bot.on_message(filters.command("resume") & filters.user(OWNER_ID))
async def cmd_resume(_, msg: Message):
    controller["pause_event"].set()
    await msg.reply_text("Resumed.")

@bot.on_message(filters.command("shutdown") & filters.user(OWNER_ID))
async def cmd_shutdown(_, msg: Message):
    await msg.reply_text("Shutting down...")
    await bot.stop()
    if user_client:
        try:
            await user_client.stop()
        except:
            pass
    os._exit(0)

# -----------------------------
# Main command: /go or reply-based
# Usage:
# /go <source> <first>-<last>
# If replied to a message in source channel, you can use /gorange <first>-<last>
# -----------------------------
@bot.on_message(filters.command(["go", "gorange"]) & filters.user(OWNER_ID))
async def cmd_go(_, msg: Message):
    """
    /go <link_or_chat> <first>-<last>
    or
    reply to a message from source (use gorange <first>-<last>)
    """
    try:
        if msg.command and msg.command[0].lower() == "gorange" and msg.reply_to_message:
            # use reply_to_message chat as source
            src_chat = msg.reply_to_message.chat
            if src_chat.username:
                source = {"chat_username": src_chat.username}
            else:
                source = {"chat_id": src_chat.id}
            if len(msg.command) < 2:
                return await msg.reply_text("Usage: gorange <first>-<last> (reply to a message in source channel)")
            rng = msg.command[1]
        else:
            if len(msg.command) < 3:
                return await msg.reply_text("Usage: /go <source_link_or_chat> <first>-<last>")
            source = build_source_from_arg(msg.command[1])
            rng = msg.command[2]

        if not source:
            return await msg.reply_text("Invalid source.")
        if "-" not in rng:
            return await msg.reply_text("Range must be like 10-50")
        first_s, last_s = rng.split("-", 1)
        first = int(first_s)
        last = int(last_s)
        if first > last:
            return await msg.reply_text("Invalid range: first > last")

        # choose targets from global config (owner can edit)
        targets = config["global"].get("targets", DEFAULT_TARGETS.copy())
        task_key = f"{source.get('chat_id') or source.get('chat_username')}_{first}_{last}_{int(time.time())}"
        starter_uid = msg.from_user.id if msg.from_user else OWNER_ID

        # start range_worker in background
        loop = asyncio.get_event_loop()
        task = loop.create_task(range_worker(bot, msg, source, first, last, targets, task_key, starter_uid))
        controller["range_task"] = task
        await msg.reply_text(f"Started forwarding {first}-{last} from {source} to {len(targets)} targets.")
    except Exception as e:
        log.exception("cmd_go exception")
        await msg.reply_text(f"Error: {e}")

# -----------------------------
# small convenience: reply forward single message to configured targets
# usage: reply to a message with /forward
# -----------------------------
@bot.on_message(filters.command("forward") & filters.user(OWNER_ID))
async def cmd_forward_single(_, msg: Message):
    if not msg.reply_to_message:
        return await msg.reply_text("Reply to a message containing the video/document to forward.")
    src = msg.reply_to_message
    targets = config["global"].get("targets", DEFAULT_TARGETS.copy())
    signature = config["global"].get("signature", DEFAULT_SIGNATURE)
    caption = clean_caption(src.caption or src.text or "", signature)
    await msg.reply_text(f"Forwarding to {len(targets)} targets...")
    res = await forward_to_targets(src, caption, targets, config["global"].get("concurrency", 4), config["global"].get("delay", 1.5))
    ok = sum(1 for v in res.values() if v)
    failed = sum(1 for v in res.values() if not v)
    await msg.reply_text(f"Done. Success: {ok} Fail: {failed}")

# -----------------------------
# Small admin utilities: /whoami, /help
# -----------------------------
@bot.on_message(filters.command("whoami"))
async def cmd_whoami(_, msg: Message):
    await msg.reply_text(f"Your id: {msg.from_user.id}\nOwner id: {OWNER_ID}")

@bot.on_message(filters.command("help"))
async def cmd_help(_, msg: Message):
    help_text = (
        "/go <source> <first>-<last> - start range forward\n"
        "reply + gorange <first>-<last> - reply in source channel and run\n"
        "/forward - reply to a message and forward to configured targets\n"
        "/targets /addtarget /rmtarget - manage global targets (owner only)\n"
        "/setdelay /setconcurrency /setsignature - config\n"
        "/pause /resume /stop /shutdown - control\n"
    )
    await msg.reply_text(help_text)

# -----------------------------
# Health server (optional), helpful for Heroku
# -----------------------------
async def handle_health(request):
    return web.Response(text="ok")

async def start_health_server(loop):
    app = web.Application()
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8080")))
    await site.start()
    log.info("Health server started on port %s", os.getenv("PORT", "8080"))

# -----------------------------
# Graceful shutdown handling
# -----------------------------
def _signal_handler(sig, frame):
    log.info("Signal received, exiting.")
    try:
        asyncio.get_event_loop().create_task(bot.stop())
    except Exception:
        pass
    os._exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# -----------------------------
# Main entrypoint
# -----------------------------
async def main():
    # Start health server (optional)
    try:
        await start_health_server(asyncio.get_event_loop())
    except Exception as e:
        log.warning("Health server failed to start: %s", e)

    try:
        await bot.start()
        log.info("Bot started.")
    except ApiIdInvalid as e:
        log.error("ApiIdInvalid or ApiHash wrong: %s", e)
        raise
    except Exception as e:
        log.exception("Bot failed to start: %s", e)
        raise

    # start user client if provided (already started above synchronously)
    if user_client:
        try:
            if not user_client.is_connected:
                await user_client.start()
            log.info("User client active.")
        except Exception as e:
            log.warning("User client start error: %s", e)

    # keep running until stopped
    while True:
        await asyncio.sleep(10)

# run
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Exiting")
    except Exception as e:
        log.exception("Fatal error in main: %s", e)