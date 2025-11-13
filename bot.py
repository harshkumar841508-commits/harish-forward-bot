# ============================================================
# V12 ULTRA MULTI-USER BOT — FINAL MERGED bot.py
# Features:
# - Multi-user panel (owner can add users, set quotas)
# - Interactive /range (first->last) and /linkforward
# - Per-user configs (targets, signature, thumb, delay, concurrency)
# - Pause / Resume / Stop
# - Progress messages with safe_edit (no MESSAGE_NOT_MODIFIED)
# - Exact-size forwarding (download & send as document/video)
# - Retry, FloodWait handling, adaptive wait per-target
# - Persistence: config (v12_config.json) and state (v12_state.json)
# - Health server (optional), export/import state
# ============================================================

import os
import re
import json
import time
import asyncio
import random
import tempfile
import signal
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
# Put your bot token here (or use Heroku Config Var BOT_TOKEN)
BOT_TOKEN = "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA"

OWNER_ID = 1251826930

# Default global options (owner can change)
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
# Simple JSON persistence
# -----------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def save_json(path: Path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        print("save_json error:", e)

# initial config & state
config = load_json(CONFIG_FILE, {
    "global": {
        "signature": DEFAULT_SIGNATURE,
        "targets": DEFAULT_TARGETS.copy(),
        "delay": DEFAULT_DELAY,
        "concurrency": DEFAULT_CONCURRENCY
    },
    "users": {}  # user_id_str -> {role,quota,used,expires,targets,signature,thumb,delay,concurrency}
})
state = load_json(STATE_FILE, {})

# ensure owner exists
config.setdefault("users", {})
config["users"].setdefault(str(OWNER_ID), {
    "role": "owner",
    "quota": 99999999,
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
# Metrics & Controller
# -----------------------------
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}  # per-user interactive state
}
controller["pause_event"].set()

# -----------------------------
# Pyrogram client
# -----------------------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not set. Put token in BOT_TOKEN variable or use env var.")

bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Optional: If you use a USER_SESSION (StringSession) to read private channels,
# add code here to initialize a user client. For now we will use bot for reading.
user_client = None
USER_SESSION = os.getenv("USER_SESSION", "")
if USER_SESSION:
    try:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        print("User session started for private channel reading.")
    except Exception as e:
        print("User session start failed:", e)
        user_client = None

# -----------------------------
# Caption Cleaner
# -----------------------------
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"

def get_user_cfg(user_id: int) -> Dict[str,Any]:
    u = config["users"].get(str(user_id))
    if not u:
        # create default
        config["users"][str(user_id)] = {
            "role": "user",
            "quota": 0,
            "used": 0,
            "expires": None,
            "targets": DEFAULT_TARGETS.copy(),
            "signature": DEFAULT_SIGNATURE,
            "thumb": None,
            "delay": DEFAULT_DELAY,
            "concurrency": DEFAULT_CONCURRENCY
        }
        save_json(CONFIG_FILE, config)
        return config["users"][str(user_id)]
    return u

def clean_caption(text: Optional[str], signature: str) -> str:
    if not text:
        return signature
    out = text
    for p in REMOVE_PATTERNS:
        try:
            out = re.sub(p, "", out, flags=re.IGNORECASE)
        except:
            out = out.replace(p, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if signature.lower() not in out.lower():
        out = f"{out}\n\n{signature}"
    return out

# -----------------------------
# Utilities: parse link, safe_edit
# -----------------------------
def parse_link(link: str) -> Optional[Dict[str,Any]]:
    try:
        link = link.strip()
        if "t.me/c/" in link:
            parts = link.split("/")
            return {"chat_id": int(parts[-2]), "msg_id": int(parts[-1])}
        if "t.me/" in link:
            parts = link.split("/")
            return {"username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
        return None
    return None

async def safe_edit(msg: Optional[Message], new_text: str):
    if msg is None:
        return
    try:
        old = getattr(msg, "text", "") or ""
        if old.strip() == new_text.strip():
            return
        await msg.edit_text(new_text)
    except Exception:
        # ignore edit errors
        return

# -----------------------------
# Download media (exact size)
# -----------------------------
async def download_media(msg: Message) -> Optional[str]:
    try:
        if msg.video or (msg.document and getattr(msg.document, "mime_type","").startswith("video")):
            out = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
            path = await msg.download(file_name=str(out))
            try:
                size_mb = Path(path).stat().st_size / (1024*1024)
                if size_mb > MAX_FILE_MB:
                    # too large — remove and skip
                    try: Path(path).unlink()
                    except: pass
                    return None
            except:
                pass
            return path
    except Exception as e:
        print("download_media error:", e)
    return None

# -----------------------------
# Adaptive wait per target
# -----------------------------
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# -----------------------------
# Send with retries + backoff
# -----------------------------
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # try copy (fast)
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # send local file
            if local_path and Path(local_path).exists():
                # send as document to preserve exact file
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback to copy
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            print(f"FloodWait {wait}s while sending to {target}")
            await asyncio.sleep(wait)
        except RPCError as rpc:
            print("RPCError:", rpc)
            metrics["fails"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = (2 ** attempt) + random.random()
            print(f"send attempt {attempt} failed to {target}: {e} backoff {backoff:.1f}s")
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

# -----------------------------
# Forward to multiple targets (concurrent)
# -----------------------------
async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], thumb: Optional[str], concurrency: int, delay: float):
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document,"mime_type","").startswith("video")):
        local_path = await download_media(src_msg)

    sem = asyncio.Semaphore(concurrency)
    results: Dict[int,bool] = {}

    async def _send_one(tid: int):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(bot, tid, src_msg, local_path, caption, thumb)
            results[tid] = bool(ok)
            await asyncio.sleep(0.2)  # small gap between sends

    tasks = [asyncio.create_task(_send_one(int(t))) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    # cleanup downloaded file
    if local_path:
        try: Path(local_path).unlink()
        except: pass

    return results

# -----------------------------
# Range worker (main loop)
# -----------------------------
async def range_worker(client_read: Client, origin_msg: Message, source_ident: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str, starter_uid: int):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_count = 0
    fail_count = 0

    # resume from state if exists
    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start_mid = max(first, last_sent + 1)

    try:
        progress_msg = await origin_msg.reply_text(f"Starting forward {start_mid} → {last} (total {total}) to {len(targets)} targets.")
    except Exception:
        progress_msg = None

    for mid in range(start_mid, last + 1):
        if controller.get("stop_flag"):
            try:
                await safe_edit(progress_msg, f"⛔ Stopped. Sent approx {sent_count}/{total}")
            except:
                pass
            break

        await controller["pause_event"].wait()

        # fetch message
        try:
            if "chat_id" in source_ident:
                src = await client_read.get_messages(source_ident["chat_id"], mid)
            else:
                src = await client_read.get_messages(source_ident["username"], mid)
        except Exception as e:
            print("get_messages error:", e)
            src = None

        # update persisted progress
        state.setdefault(task_key, {"first": first, "last": last})
        state[task_key]["last_sent"] = mid
        save_json(STATE_FILE, state)

        if not src:
            continue
        if not (src.video or (src.document and getattr(src.document,"mime_type","").startswith("video"))):
            continue

        # determine signature / user cfg
        starter_cfg = get_user_cfg(starter_uid) if str(starter_uid) in config["users"] else None
        signature = starter_cfg.get("signature") if starter_cfg and starter_cfg.get("signature") else config["global"].get("signature", DEFAULT_SIGNATURE)
        thumb = starter_cfg.get("thumb") if starter_cfg and starter_cfg.get("thumb") else None
        concurrency = starter_cfg.get("concurrency") if starter_cfg and starter_cfg.get("concurrency") else config["global"].get("concurrency", DEFAULT_CONCURRENCY)
        delay = starter_cfg.get("delay") if starter_cfg and starter_cfg.get("delay") else config["global"].get("delay", DEFAULT_DELAY)
        targets_use = starter_cfg.get("targets") if starter_cfg and starter_cfg.get("targets") else targets

        caption = clean_caption(src.caption or src.text or "", signature)

        results = await forward_to_targets(src, caption, targets_use, thumb, concurrency, delay)

        ok = sum(1 for v in results.values() if v)
        fails = sum(1 for v in results.values() if not v)
        sent_count += ok
        fail_count += fails

        pct = int((mid - first + 1) / total * 100)
        status_text = f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}%\nSuccess:{ok} Fail:{fails}\nTotal Sent:{sent_count} Total Fail:{fail_count}"
        try:
            await safe_edit(progress_msg, status_text)
        except:
            pass

        # sleep small delay
        await asyncio.sleep(delay)

    try:
        await safe_edit(progress_msg, f"✅ Completed. Sent approx {sent_count}/{total} Fail:{fail_count}")
    except:
        pass

    # cleanup
    metrics["active_tasks"] -= 1
    controller["range_task"] = None
    # remove state for this task
    try:
        state.pop(task_key, None)
        save_json(STATE_FILE, state)
    except:
        pass

# -----------------------------
# Commands & Handlers
# -----------------------------
# Decorator to check owner or allowed user
def is_owner(uid: int) -> bool:
    return str(uid) == str(OWNER_ID) or config["users"].get(str(uid), {}).get("role") == "owner"

def has_quota(uid: int, cost: int = 1) -> bool:
    u = config["users"].get(str(uid))
    if not u:
        return False
    if u.get("expires"):
        try:
            if datetime.fromisoformat(u["expires"]) < datetime.utcnow():
                return False
        except:
            pass
    if u.get("quota", 0) - u.get("used", 0) >= cost:
        return True
    return False

def consume_quota(uid: int, cost: int = 1):
    u = config["users"].setdefault(str(uid), {"role":"user","quota":0,"used":0,"expires":None})
    u["used"] = u.get("used", 0) + cost
    save_json(CONFIG_FILE, config)

def require_allowed_filter(uid: int) -> bool:
    return is_owner(uid) or has_quota(uid)

# /start and /help
@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start_owner(c, m):
    text = (
        "**V12 Multi-User Forward Bot**\n\n"
        "Owner Commands:\n"
        "/adduser <user_id> <quota> [days] — add user\n"
        "/removeuser <user_id>\n"
        "/listusers\n"
        "/setglobal <key> <value>\n"
        "/linkforward <link1> <link2> [targets]\n"
        "/range — interactive\n"
        "/pause /resume /stop\n"
        "/exportstate /importstate\n"
        "/init — start background services\n"
    )
    await m.reply_text(text)

@bot.on_message(filters.command("start") & ~filters.user(OWNER_ID))
async def cmd_start_user(c, m):
    await m.reply_text("Hi — If you have been added by owner you can use /range, /linkforward, /status etc. Contact owner to be added.")

# Owner: add user
@bot.on_message(filters.user(OWNER_ID) & filters.command("adduser"))
async def cmd_adduser(c, m):
    try:
        uid = str(int(m.command[1]))
        quota = int(m.command[2]) if len(m.command) > 2 else 100
        days = int(m.command[3]) if len(m.command) > 3 else None
    except:
        return await m.reply_text("Usage: /adduser <user_id> <quota> [days_valid]")
    expires = None
    if days:
        expires = (datetime.utcnow() + timedelta(days=days)).isoformat()
    config["users"][uid] = {
        "role": "user", "quota": quota, "used": 0, "expires": expires,
        "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
        "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
        "thumb": None, "delay": config["global"].get("delay", DEFAULT_DELAY),
        "concurrency": config["global"].get("concurrency", DEFAULT_CONCURRENCY)
    }
    save_json(CONFIG_FILE, config)
    await m.reply_text(f"Added user {uid} quota={quota} expires={expires}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removeuser"))
async def cmd_removeuser(c, m):
    try:
        uid = str(int(m.command[1]))
    except:
        return await m.reply_text("Usage: /removeuser <user_id>")
    config["users"].pop(uid, None)
    save_json(CONFIG_FILE, config)
    await m.reply_text(f"Removed {uid}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("listusers"))
async def cmd_listusers(c, m):
    lines = []
    for k,v in config.get("users", {}).items():
        lines.append(f"{k} role={v.get('role')} quota={v.get('quota')} used={v.get('used')} expires={v.get('expires')}")
    await m.reply_text("\n".join(lines) if lines else "No users")

# Per-user config commands
@bot.on_message(filters.command("myconfig") & filters.private)
async def cmd_myconfig(c, m):
    uid = m.from_user.id
    ucfg = config["users"].get(str(uid))
    if not ucfg:
        return await m.reply_text("You are not registered. Contact owner.")
    await m.reply_text(
        f"Your config:\nTargets: {ucfg.get('targets')}\nSignature: {ucfg.get('signature')}\nDelay: {ucfg.get('delay')}\nConcurrency: {ucfg.get('concurrency')}\nQuota: {ucfg.get('quota')} Used: {ucfg.get('used')}"
    )

@bot.on_message(filters.command("addtarget") & filters.private)
async def cmd_addtarget(c, m):
    uid = m.from_user.id
    ucfg = get_user_cfg(uid)
    try:
        tid = int(m.command[1])
    except:
        return await m.reply_text("Usage: /addtarget -100xxxxx")
    pool = ucfg.get("targets", [])
    if tid in pool:
        return await m.reply_text("Already present")
    pool.append(tid)
    ucfg["targets"] = pool
    save_json(CONFIG_FILE, config)
    await m.reply_text(f"Added target {tid}")

@bot.on_message(filters.command("removetarget") & filters.private)
async def cmd_removetarget(c, m):
    uid = m.from_user.id
    ucfg = get_user_cfg(uid)
    try:
        tid = int(m.command[1])
    except:
        return await m.reply_text("Usage: /removetarget -100xxxxx")
    pool = ucfg.get("targets", [])
    if tid in pool:
        pool.remove(tid)
        ucfg["targets"] = pool
        save_json(CONFIG_FILE, config)
        return await m.reply_text(f"Removed {tid}")
    return await m.reply_text("Not in list")

@bot.on_message(filters.command("setsignature") & filters.private)
async def cmd_setsignature(c, m):
    uid = m.from_user.id
    text = " ".join(m.command[1:])
    if not text:
        return await m.reply_text("Usage: /setsignature <text>")
    ucfg = get_user_cfg(uid)
    ucfg["signature"] = text
    save_json(CONFIG_FILE, config)
    await m.reply_text(f"Signature updated to: {text}")

@bot.on_message(filters.command("setthumb") & filters.private)
async def cmd_setthumb(c, m):
    uid = m.from_user.id
    if not m.reply_to_message or not (m.reply_to_message.photo or m.reply_to_message.document):
        return await m.reply_text("Reply to a photo with /setthumb")
    path = await m.reply_to_message.download(file_name=f"thumb_{uid}.jpg")
    ucfg = get_user_cfg(uid)
    ucfg["thumb"] = path
    save_json(CONFIG_FILE, config)
    await m.reply_text("Thumbnail updated.")

# Owner control: set global option
@bot.on_message(filters.user(OWNER_ID) & filters.command("setglobal"))
as