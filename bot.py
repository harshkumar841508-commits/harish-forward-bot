# bot.py  -- V12 Multi-User Panel (single-file)
# Features: multi-user, per-user config, range/link forward, persistence, optional user session for private channels.

import os
import re
import json
import time
import asyncio
import random
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# ---------------------------
# CONFIG (defaults included; override with ENV)
# ---------------------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA")  # recommended: set in Heroku config vars

OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))  # your owner id

# Default global options
DEFAULT_SOURCE = int(os.getenv("DEFAULT_SOURCE", "-1003433745100"))
DEFAULT_TARGETS = [int(x) for x in os.getenv("DEFAULT_TARGETS", "-1003404830427").split(",") if x.strip()]
DEFAULT_SIGNATURE = os.getenv("DEFAULT_SIGNATURE", "Extracted by➤@course_wale")
DEFAULT_THUMB = os.getenv("DEFAULT_THUMB", "thumb.jpg")
DEFAULT_FORWARD_DELAY = float(os.getenv("DEFAULT_FORWARD_DELAY", "0.8"))
DEFAULT_CONCURRENCY = int(os.getenv("DEFAULT_CONCURRENCY", "4"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "1500"))

# Optional user session for reading private channels (StringSession)
USER_SESSION = os.getenv("USER_SESSION", "").strip()

# Files
USERS_FILE = Path("users.json")
STATE_FILE = Path("state.json")
TMP_DIR = Path(tempfile.gettempdir())

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("v12_multiuser")

# ---------------------------
# Persistence helpers
# ---------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("load_json %s failed: %s", path, e)
    return default

def save_json(path: Path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.error("save_json failed %s: %s", path, e)

# Load persisted data
_users = load_json(USERS_FILE, {})  # structure: {user_id_str: { "targets":[], "signature":"", "thumb":"", "delay":x, "concurrency":x }}
_state = load_json(STATE_FILE, {})  # structure for resume tasks

# Ensure owner's config exists
if str(OWNER_ID) not in _users:
    _users[str(OWNER_ID)] = {
        "targets": DEFAULT_TARGETS.copy(),
        "signature": DEFAULT_SIGNATURE,
        "thumb": DEFAULT_THUMB,
        "delay": DEFAULT_FORWARD_DELAY,
        "concurrency": DEFAULT_CONCURRENCY,
        "allowed": True,  # owner allowed
    }
    save_json(USERS_FILE, _users)

# ---------------------------
# Pyrogram clients
# ---------------------------
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN missing. Set BOT_TOKEN env var.")
    raise SystemExit("Missing BOT_TOKEN")

bot = Client("v12_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client = None
if USER_SESSION:
    try:
        user_client = Client(session_name="user_session", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        logger.info("USER_SESSION enabled")
    except Exception as e:
        logger.warning("USER_SESSION init failed: %s", e)
        user_client = None

# Controller per-user tasks
controllers: Dict[str, Dict[str, Any]] = {}  # key = user_id_str -> { "task": asyncio.Task, "pause": asyncio.Event(), "stop": False }

# Metrics
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}

# ---------------------------
# Utilities
# ---------------------------
def get_user_cfg(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    cfg = _users.get(key)
    if not cfg:
        cfg = {
            "targets": DEFAULT_TARGETS.copy(),
            "signature": DEFAULT_SIGNATURE,
            "thumb": DEFAULT_THUMB,
            "delay": DEFAULT_FORWARD_DELAY,
            "concurrency": DEFAULT_CONCURRENCY,
            "allowed": False
        }
        _users[key] = cfg
        save_json(USERS_FILE, _users)
    return cfg

def set_user_cfg(user_id: int, cfg: Dict[str, Any]):
    _users[str(user_id)] = cfg
    save_json(USERS_FILE, _users)

def clean_caption(text: Optional[str], user_id: int) -> str:
    cfg = get_user_cfg(user_id)
    sig = cfg.get("signature", DEFAULT_SIGNATURE)
    if not text:
        return sig
    out = text
    # remove common extracted lines and replace site
    patterns = [
        r"Extracted\s*by[^\n]*",
        r"Extracted\s*By[^\n]*",
        r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
    ]
    for p in patterns:
        try:
            out = re.sub(p, "", out, flags=re.IGNORECASE)
        except:
            out = out.replace(p, "")
    # replace specific website patterns
    out = re.sub(r"https?://[^\s]*riyasmm\.shop[^\s]*", "https://bio.link/manmohak", out, flags=re.IGNORECASE)
    out = out.strip()
    if sig.lower() not in out.lower():
        out = f"{out}\n\n{sig}"
    return re.sub(r"\n{3,}", "\n\n", out)

def parse_link(link: str):
    """
    Accepts forms:
    https://t.me/c/<chatid>/<msgid>
    https://t.me/<username>/<msgid>
    returns dict with chat_id or username and msg_id
    """
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
            username = parts[-2]
            return {"chat_username": username, "msg_id": msg_id}
    except Exception:
        return None

async def download_media(msg: Message) -> Optional[str]:
    if not (msg.video or (msg.document and getattr(msg.document, "mime_type","").startswith("video"))):
        return None
    try:
        fname = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        path = await msg.download(file_name=str(fname))
        try:
            size_mb = Path(path).stat().st_size / (1024*1024)
            if size_mb > MAX_FILE_MB:
                logger.warning("File too large %.1fMB -> skipping", size_mb)
                Path(path).unlink(missing_ok=True)
                return None
        except Exception:
            pass
        return path
    except Exception as e:
        logger.warning("download_media failed: %s", e)
        return None

# send with retry and backoff
async def send_with_retry(client_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str], user_cfg: Dict[str, Any]) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # try copy first (fast)
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # else send from file
            if local_path and Path(local_path).exists():
                suffix = Path(local_path).suffix.lower()
                if suffix in [".mp4", ".mkv", ".webm", ".mov"]:
                    await client_send.send_video(chat_id=target, video=local_path, caption=caption, thumb=thumb if thumb and Path(thumb).exists() else None, supports_streaming=True)
                else:
                    await client_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback to copy (try once)
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            logger.warning("FloodWait %s", wait)
            await asyncio.sleep(wait)
        except RPCError as rpc:
            logger.error("RPCError sending to %s: %s", target, rpc)
            metrics["fails"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = (2 ** attempt) + random.random()
            logger.warning("send attempt %d to %s failed: %s; backoff %.1f", attempt, target, e, backoff)
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

# Adaptive wait per-target to avoid bursts
_last_send_time: Dict[int, float] = {}
async def adaptive_wait(target: int, delay_per_forward: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    min_interval = delay_per_forward
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# ---------------------------
# Range worker (per-user)
# ---------------------------
async def range_worker_for_user(user_id: int, origin_msg: Message, source_ident: Dict[str, Any], first: int, last: int):
    user_key = str(user_id)
    cfg = get_user_cfg(user_id)
    targets = cfg.get("targets", [])
    thumb = cfg.get("thumb", DEFAULT_THUMB)
    delay = cfg.get("delay", DEFAULT_FORWARD_DELAY)
    concurrency = cfg.get("concurrency", DEFAULT_CONCURRENCY)
    client_read = user_client if user_client else bot

    # controller init
    ctrl = controllers.setdefault(user_key, {})
    pause_ev = ctrl.setdefault("pause", asyncio.Event())
    pause_ev.set()
    ctrl.setdefault("stop", False)

    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_count = 0
    fail_count = 0

    # progress message
    try:
        progress_msg = await origin_msg.reply_text(f"Starting forward {first} → {last} to {len(targets)} targets.")
    except Exception:
        progress_msg = None

    # resume info from state
    task_key = f"{user_key}_{source_ident.get('chat_id', source_ident.get('chat_username'))}_{first}_{last}"
    last_sent = _state.get(task_key, {}).get("last_sent", first - 1)
    start_mid = max(first, last_sent + 1)

    sem = asyncio.Semaphore(concurrency)

    try:
        for mid in range(start_mid, last + 1):
            if controllers.get(user_key, {}).get("stop", False):
                await origin_msg.reply_text("Stopped by user.")
                break
            await pause_ev.wait()
            # fetch message
            try:
                if "chat_id" in source_ident:
                    src = await client_read.get_messages(source_ident["chat_id"], mid)
                else:
                    src = await client_read.get_messages(source_ident["chat_username"], mid)
            except Exception as e:
                logger.warning("get_messages failed %s:%s -> %s", source_ident, mid, e)
                src = None

            # update state even if missing
            _state.setdefault(task_key, {"first": first, "last": last})
            _state[task_key]["last_sent"] = mid
            save_json(STATE_FILE, _state)

            if not src:
                continue
            # skip non-video
            if not (src.video or (src.document and getattr(src.document, "mime_type","").startswith("video"))):
                continue

            caption = clean_caption(src.caption or src.text or "", user_id)
            local_path = await download_media(src)

            # send concurrently to targets (limited by sem)
            async def _send_to_target(tid):
                async with sem:
                    await adaptive_wait(tid, delay)
                    ok = await send_with_retry(bot, tid, src, local_path, caption, thumb, cfg)
                    return ok

            tasks = [asyncio.create_task(_send_to_target(int(t))) for t in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # count
            ok = sum(1 for r in results if r is True)
            fails = sum(1 for r in results if r is False or isinstance(r, Exception))
            sent_count += ok
            fail_count += fails

            # progress update
            pct = int((mid - first + 1) / total * 100)
            text = f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}% — Success:{ok} Fail:{fails}\nTotal Sent:{sent_count} Total Fail:{fail_count}"
            try:
                if progress_msg:
                    await progress_msg.edit_text(text)
            except Exception:
                pass

            # cleanup
            if local_path:
                try:
                    Path(local_path).unlink()
                except:
                    pass

            # small sleep to avoid bursts
            await asyncio.sleep(delay)

        try:
            if progress_msg:
                await progress_msg.edit_text(f"✅ Completed. Sent approx {sent_count} Fail {fail_count}")
        except Exception:
            pass

    except asyncio.CancelledError:
        try:
            if progress_msg:
                await progress_msg.edit_text(f"Cancelled. Sent {sent_count}/{total}")
        except:
            pass
    except Exception as e:
        logger.exception("range_worker error: %s", e)
        try:
            if progress_msg:
                await progress_msg.edit_text(f"❌ Error: {e}")
        except:
            pass
    finally:
        metrics["active_tasks"] -= 1
        # clear controller
        controllers.pop(user_key, None)

# ---------------------------
# Commands
# ---------------------------

def is_owner(user_id: int):
    return int(user_id) == int(OWNER_ID)

@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start(c, m):
    text = (
        "V12 Multi-User Forward Bot\n\n"
        "Owner commands:\n"
        "/adduser <user_id> - allow new user\n"
        "/removeuser <user_id>\n"
        "/listusers\n"
        "/setglobal <option> <value>\n\n"
        "User commands (after owner adds you):\n"
        "/myconfig - show your config\n"
        "/addtarget -100xxx\n"
        "/removetarget -100xxx\n"
        "/listtargets\n"
        "/setsignature <text>\n"
        "/setthumb (reply to photo)\n"
        "/range (interactive first+last links)\n"
        "/linkforward <link1> <link2>\n"
        "/pause /resume /stop\n"
        "/status\n"
    )
    await m.reply(text)

# Owner: add user
@bot.on_message(filters.user(OWNER_ID) & filters.command("adduser"))
async def cmd_adduser(c, m):
    try:
        uid = int(m.command[1])
    except:
        return await m.reply("Usage: /adduser <user_id>")
    cfg = {
        "targets": DEFAULT_TARGETS.copy(),
        "signature": DEFAULT_SIGNATURE,
        "thumb": DEFAULT_THUMB,
        "delay": DEFAULT_FORWARD_DELAY,
        "concurrency": DEFAULT_CONCURRENCY,
        "allowed": True
    }
    _users[str(uid)] = cfg
    save_json(USERS_FILE, _users)
    await m.reply(f"Added user {uid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removeuser"))
async def cmd_removeuser(c, m):
    try:
        uid = int(m.command[1])
    except:
        return await m.reply("Usage: /removeuser <user_id>")
    _users.pop(str(uid), None)
    save_json(USERS_FILE, _users)
    await m.reply(f"Removed user {uid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("listusers"))
async def cmd_listusers(c, m):
    users_list = [k for k,v in _users.items() if v.get("allowed")]
    await m.reply("Allowed users:\n" + "\n".join(users_list))

# User: get my config
@bot.on_message(filters.me & filters.command("myconfig") | filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("myconfig"))
async def cmd_myconfig(c, m):
    uid = m.from_user.id
    cfg = get_user_cfg(uid)
    await m.reply(f"Your config:\nTargets: {cfg.get('targets')}\nSignature: {cfg.get('signature')}\nDelay: {cfg.get('delay')}\nConcurrency: {cfg.get('concurrency')}")

# addtarget
@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("addtarget"))
async def cmd_addtarget(c, m):
    uid = m.from_user.id
    cfg = get_user_cfg(uid)
    try:
        tid = int(m.command[1])
    except:
        return await m.reply("Usage: /addtarget -100xxxxxxxx")
    pool = cfg.get("targets", [])
    if tid in pool:
        return await m.reply("Already present")
    pool.append(tid)
    cfg["targets"] = pool
    set_user_cfg(uid, cfg)
    await m.reply(f"Added {tid}")

@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("removetarget"))
async def cmd_removetarget(c, m):
    uid = m.from_user.id
    cfg = get_user_cfg(uid)
    try:
        tid = int(m.command[1])
    except:
        return await m.reply("Usage: /removetarget -100xxxxxxxx")
    pool = cfg.get("targets", [])
    if tid in pool:
        pool.remove(tid)
        cfg["targets"] = pool
        set_user_cfg(uid, cfg)
        await m.reply(f"Removed {tid}")
    else:
        await m.reply("Not in list")

@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("listtargets"))
async def cmd_listtargets(c, m):
    uid = m.from_user.id
    cfg = get_user_cfg(uid)
    await m.reply(f"Targets: {cfg.get('targets')}")

# setsignature
@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("setsignature"))
async def cmd_setsignature(c, m):
    uid = m.from_user.id
    text = " ".join(m.command[1:])
    if not text:
        return await m.reply("Usage: /setsignature <text>")
    cfg = get_user_cfg(uid)
    cfg["signature"] = text
    set_user_cfg(uid, cfg)
    await m.reply(f"Signature set to:\n{text}")

# setthumb (reply to photo)
@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("setthumb"))
async def cmd_setthumb(c, m):
    uid = m.from_user.id
    if not m.reply_to_message:
        return await m.reply("Reply to an image with /setthumb")
    p = await m.reply_to_message.download(file_name=f"thumb_{uid}.jpg")
    cfg = get_user_cfg(uid)
    cfg["thumb"] = p
    set_user_cfg(uid, cfg)
    await m.reply("Thumbnail updated")

# pause/resume/stop (per-user)
@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("pause"))
async def cmd_pause(c, m):
    uid = str(m.from_user.id)
    ctrl = controllers.setdefault(uid, {})
    ev = ctrl.setdefault("pause", asyncio.Event())
    ev.clear()
    await m.reply("Paused")

@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("resume"))
async def cmd_resume(c, m):
    uid = str(m.from_user.id)
    ctrl = controllers.setdefault(uid, {})
    ev = ctrl.setdefault("pause", asyncio.Event())
    ev.set()
    await m.reply("Resumed")

@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("stop"))
async def cmd_stop(c, m):
    uid = str(m.from_user.id)
    ctrl = controllers.setdefault(uid, {})
    ctrl["stop"] = True
    await m.reply("Stop signal sent")

# status
@bot.on_message(filters.user(lambda uid: str(uid) in _users and _users[str(uid)].get("allowed")) & filters.command("status"))
async def cmd_status(c, m):
    uid = m.from_user.id
    cfg = get_user_cfg(uid)
    await m.reply(f"Targets: {cfg.get('targets')}\nSignature: {cfg.get('signature')}\nDelay:{cfg.get('delay')}\nConcurrency:{cfg.get('concurrency')}\nForwards:{metrics['forwards']} Fails:{metrics['fails']} Active:{metrics['active_ta