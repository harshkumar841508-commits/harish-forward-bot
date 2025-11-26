# bot.py — V12 ULTRA MAX (HARD-CODED VARS) — Ready for Heroku
# NOTE: This file contains your secrets (BOT_TOKEN, USER_SESSION, API_HASH etc).
# Keep it private. Prefer using Heroku Config Vars in production.

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
# HARD-CODED CONFIG (as requested)
# -----------------------------
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"

# Your Bot token (hard-coded)
BOT_TOKEN = "8226478770:AAH1Zz63qXkdD_jD2n-5xeO4ZfoRKPL6uKk"

# Owner (your Telegram user id)
OWNER_ID = 1251826930

# Source and default target channels
SOURCE_CHANNEL = -1003175017722                 # your provided source channel id
DEFAULT_TARGETS = [-1003428767711]              # your provided default target id(s)

# Optional user session string for reading private channels (hard-coded)
USER_SESSION = "BQF8MNAAhbMoovGajQnCBIFyLI32AMSA8MFuEgHTNUiobuJb9jP5_GZnuqc75Bws4GMpFzoGDGH8ykeXRL-ieoxskpmslTT0fGu82K1Fc0pl9HpPgTplcZAN5Vz1KprigbcT6uEobAtfF3QWBdmbhaFtPyZUGripqHzH6WHQKvfjEc0B2P3xqfZoFipqBA6jpdcWnvMeAWkN7RIWWP3lflhTK7lGa3ROdf0nJ7ZQG-rlPosG4CZbL72xteLBvECKR2p-O6fEdQ7iCHz0omte-PWdnWbW8HQAv-vWVqq5A_LDIs8RhPyfc4iSvRjNejpwaKaD_Gq1pVQe3lSZuFirhTpZylBK4gAAAABKnVzyAA"

# Tuning
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"
DEFAULT_DELAY = 1.0
DEFAULT_CONCURRENCY = 4
RETRY_LIMIT = 4
MAX_FILE_MB = 1500

# Files + persistence
TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_config.json")
STATE_FILE = Path("v12_state.json")

# -----------------------------
# Helpers: JSON load / save
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

# -----------------------------
# Initial config + state
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

# ensure owner exists
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
# Metrics & controller
# -----------------------------
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}
}
controller["pause_event"].set()

# -----------------------------
# Pyrogram clients: bot + optional user
# -----------------------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not set in file.")

bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client: Optional[Client] = None
if USER_SESSION:
    try:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        print("User session started — private channel reading enabled.")
    except Exception as e:
        print("User session failed:", e)
        user_client = None
else:
    print("No USER_SESSION — private channels readable only if bot is member/admin.")

# -----------------------------
# Caption cleaning / replacements
# -----------------------------
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"

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
# Utils: parse links, safe_edit
# -----------------------------
def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    """
    Parse links like:
      - https://t.me/c/123456/12  -> chat_id = -100123456, msg_id = 12
      - https://t.me/username/12   -> chat_username, msg_id
    """
    try:
        link = link.strip().split("?")[0].split("#")[0]
        if "t.me/c/" in link or "telegram.me/c/" in link:
            parts = [p for p in link.split("/") if p]
            # find 'c' index
            for i,p in enumerate(parts):
                if p == "c" and i+2 < len(parts):
                    chatnum = parts[i+1]
                    msgid = parts[i+2]
                    if chatnum.isdigit() and msgid.isdigit():
                        return {"chat_id": int(f"-100{chatnum}"), "msg_id": int(msgid)}
            return None
        if "t.me/" in link or "telegram.me/" in link:
            parts = [p for p in link.split("/") if p]
            if len(parts) >= 2:
                username = parts[-2]
                msgid = parts[-1]
                if msgid.isdigit():
                    return {"chat_username": username, "msg_id": int(msgid)}
        # fallback: last two segments
        parts = [p for p in link.split("/") if p]
        if len(parts) >= 2 and parts[-1].isdigit():
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
        return None
    return None

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
# Download & send (exact size)
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
        print("download_media error:", e)
    return None

# adaptive wait per target
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# send with retries/backoff
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # Try copy first
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # send local file (document to keep exact size)
            if local_path and Path(local_path).exists():
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback copy
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
            print(f"send attempt {attempt} to {target} failed: {e}; backoff {backoff:.1f}")
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

# forward concurrently to multiple targets
async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float, client_for_send: Client = None) -> Dict[int,bool]:
    if client_for_send is None:
        client_for_send = bot
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type", "").startswith("video")):
        local_path = await download_media(src_msg)

    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[int,bool] = {}

    async def _send_one(tid: int):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(client_for_send, int(tid), src_msg, local_path, caption)
            results[int(tid)] = bool(ok)
            await asyncio.sleep(0.2)

    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    if local_path:
        try: Path(local_path).unlink()
        except: pass

    return results

# fetch source message using user_client (if present) or bot
async def fetch_source_message(source: Dict[str, Any], mid: int) -> Optional[Message]:
    try:
        reader = user_client if user_client else bot
        if "chat_id" in source:
            return await reader.get_messages(source["chat_id"], mid)
        else:
            return await reader.get_messages(source["chat_username"], mid)
    except Exception as e:
        print("fetch_source_message error:", e)
        return None

# range worker (resume/pause/stop + progress)
async def range_worker(client_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str, starter_uid: int):
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
            # persist progress
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            save_json(STATE_FILE, state)

            if not src:
                continue
            if not (src.video or (src.document and getattr(src.document, "mime_type", "").startswith("video"))):
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
        print("range_worker exception:", e)
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
# Owner / user helpers
# -----------------------------
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

# -----------------------------
# Commands: owner + users
# -----------------------------
@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start_owner(c, m):
    text = (
        "**V12 Ultra Multi-User Forward Bot (Hardcoded)**\n\n"
        "Owner Commands:\n"
        "/adduser <user_id> <quota> [days]\n"
        "/removeuser <user_id>\n"
        "/listusers\n"
        "/setglobal <key> <value>\n"
        "/linkforward <first_link> <last_link> [targets]\n"
        "/range — interactive\n"
        "/pause /resume /stop\n"
        "/exportstate /importstate\n"
        "/init — start background services\n"
        "\nUser Commands (private):\n"
        "/range (interactive)\n"
        "/linkforward <link1> <link2>\n"
        "/myconfig\n"
        "/addtarget /removetarget /listtargets\n"
        "/setsignature /setthumb\n"
    )
    await m.reply_text(text)

@bot.on_message(filters.command("start") & ~filters.user(OWNER_ID))
async def cmd_start_user(c, m):
    await m.reply_text("Hello — contact owner to be added. If added, use /range or /linkforward.")

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
        "role": "user",
        "quota": quota,
        "used": 0,
        "expires": expires,
        "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
        "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
        "thumb": None,
        "delay": config["global"].get("delay", DEFAULT_DELAY),
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
    for k, v in config.get("users", {}).items():
        lines.append(f"{k} role={v.get('role')} quota={v.get('quota')} used={v.get('used')} expires={v.get('expires')}")
    await m.reply_text("\n".join(lines) if lines else "No users")

@bot.on_message(filters.user(OWNER_ID) & filters.command("setglobal"))
async def cmd_setglobal(c, m):
    if len(m.command) < 3:
        return await m.reply_text("Usage: /setglobal <key> <value>")
    key = m.command[1].lower()
    val = " ".join(m.command[2:])
    if key in ("signature", "delay", "concurrency", "targets"):
        if key == "delay":
            try:
                config["global"]["delay"] = float(val)
            except:
                return await m.reply_text("delay must be numeric")
        elif key == "concurrency":
            try:
                config["global"]["concurrency"] = int(val)
            except:
                return await m.reply_text("concurrency must be int")
        elif key == "targets":
            try:
                lst = [int(x.strip()) for x in val.split(",") if x.strip()]
                config["global"]["targets"] = lst
            except:
                return await m.reply_text("targets must be comma separated ids")
        else:
            config["global"][key] = val
        save_json(CONFIG_FILE, config)
        return await m.reply_text(f"Global {key} set to {val}")
    return await m.reply_text("Allowed keys: signature, delay, concurrency, targets")

@bot.on_message(filters.command("myconfig") & filters.private)
async def cmd_myconfig(c, m):
    uid = m.from_user.id
    ucfg = config["users"].get(str(uid))
    if not ucfg:
        return await m.reply_text("You