# ============================================================
# V12 ULTRA MULTI-USER BOT — FULL (USER SESSION SUPPORTED)
# Single-file ready to deploy (bot.py)
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
# CONFIG (prefilled values; change if needed or use ENV)
# -----------------------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
# Put your bot token here or set as HEROKU Config Var BOT_TOKEN
BOT_TOKEN = os.getenv("BOT_TOKEN", "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA")

# Owner (use your Telegram user id)
OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))

# Defaults
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
# SIMPLE JSON PERSISTENCE
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
# INITIAL CONFIG & STATE
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

# Ensure owner present
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
# METRICS & CONTROLLER
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
# SESSIONS: BOT + OPTIONAL USER
# -----------------------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not set. Put token in BOT_TOKEN or env var.")

bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

USER_SESSION = os.getenv("USER_SESSION", "")
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
# CAPTION CLEANER & REPLACEMENTS
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
# UTILITIES: parse link, safe_edit
# -----------------------------
def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        link = link.strip()
        if "t.me/c/" in link:
            parts = link.split("/")
            return {"chat_id": int(parts[-2]), "msg_id": int(parts[-1])}
        if "t.me/" in link:
            parts = link.split("/")
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
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
# DOWNLOAD MEDIA (exact size)
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

# -----------------------------
# ADAPTIVE WAIT (per target) + send with retries
# -----------------------------
_last_send_time: Dict[int, float] = {}

async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # Try copy first (fast)
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # If we have downloaded file, send document to preserve file
            if local_path and Path(local_path).exists():
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # Fallback copy
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

# -----------------------------
# Forward to multiple targets concurrently
# -----------------------------
async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float) -> Dict[int, bool]:
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type", "").startswith("video")):
        local_path = await download_media(src_msg)

    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[int, bool] = {}

    async def _send_one(tid: int):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(bot, int(tid), src_msg, local_path, caption)
            results[int(tid)] = bool(ok)
            await asyncio.sleep(0.2)

    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    if local_path:
        try: Path(local_path).unlink()
        except: pass

    return results

# -----------------------------
# FETCH SOURCE MESSAGE (bot or user session)
# -----------------------------
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

# -----------------------------
# Range worker (resume/pause/stop + progress)
# -----------------------------
async def range_worker(client_read: Client, origin_msg: Message, source_identifier: Dict[str, Any], first: int, last: int, targets: List[int], task_key: str, starter_uid: int):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_total = 0
    fail_total = 0

    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start_mid = max(first, last_sent + 1)

    # user config
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

            results = await forward_to_targets(src, caption, targets_use, concurrency, delay)

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
# OWNER & USER MANAGEMENT HELPERS
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
# COMMANDS (owner + user)
# -----------------------------
@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start_owner(c, m):
    text = (
        "**V12 Ultra Multi-User Forward Bot**\n\n"
        "Owner Commands:\n"
        "/adduser <user_id> <quota> [days]\n"
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
    await m.reply_text("Hello — ask owner to add you. If added, use /range or /linkforward.")

# owner: add/remove/list users
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

# owner setglobal
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

# per-user commands
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

# pause/resume/stop/status (owner only)
@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def cmd_pause(c, m):
    controller["pause_event"].clear()
    await m.reply_text("⏸ Paused")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def cmd_resume(c, m):
    controller["pause_event"].set()
    await m.reply_text("▶ Resumed")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(c, m):
    controller["stop_flag"] = True
    await m.reply_text("⛔ Stop signal set")

@bot.on_message(filters.command("status"))
async def cmd_status(c, m):
    await m.reply_text(
        f"Targets: {config['global'].get('targets')}\nSignature: {config['global'].get('signature')}\n"
        f"Forwards: {metrics.get('forwards')} Fails: {metrics.get('fails')} Retries: {metrics.get('retries')} Active: {metrics.get('active_tasks')}"
    )

# export/import state (owner)
@bot.on_message(filters.user(OWNER_ID) & filters.command("exportstate"))
async def cmd_exportstate(c, m):
    try:
        p = STATE_FILE
        await m.reply_document(str(p))
    except Exception as e:
        await m.reply_text(f"Export failed: {e}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("importstate"))
async def cmd_importstate(c, m):
    if not m.reply_to_message or not m.reply_to_message.document:
        return await m.reply_text("Reply to a JSON file with /importstate")
    fpath = await m.reply_to_message.download(file_name=str(TMP_DIR / "v12_state_import.json"))
    try:
        new = json.loads(Path(fpath).read_text(encoding="utf-8"))
        save_json(STATE_FILE, new)
        await m.reply_text("Imported state")
    except Exception as e:
        await m.reply_text(f"Import failed: {e}")

# linkforward (owner or allowed users)
@bot.on_message(filters.command("linkforward"))
async def cmd_linkforward(c, m):
    args = m.command[1:]
    if len(args) < 2:
        return await m.reply_text("Usage: /linkforward <first_link> <last_link> [targets(optional comma list)]")
    p1 = parse_msg_link(args[0])
    p2 = parse_msg_link(args[1])
    if not p1 or not p2:
        return await m.reply_text("Invalid links.")
    first = min(p1["msg_id"], p2["msg_id"])
    last = max(p1["msg_id"], p2["msg_id"])
    targets_use = config["global"].get("targets", DEFAULT_TARGETS.copy())
    if len(args) > 2:
        try:
            tlist = [int(x.strip()) for x in args[2].split(",") if x.strip()]
            if tlist: targets_use = tlist
        except:
            pass
    client_for_read = user_client if user_client else bot
    key = f"{p1.get('chat_id', p1.get('chat_username'))}_{first}_{last}"
    await m.reply_text(f"Starting forward {first} → {last} to {len(targets_use)} targets.")
    controller["range_task"] = asyncio.create_task(range_worker(client_for_read, m, p1, first, last, targets_use, key, m.from_user.id))

# interactive /range (owner or allowed)
@bot.on_message(filters.command("range") & filters.private)
async def cmd_range_start(c, m):
    uid = m.from_user.id
    if not (is_owner(uid) or has_quota(uid)):
        return await m.reply_text("You are not allowed or quota exhausted.")
    controller["interactive"][str(uid)] = {"step": 1}
    await m.reply_text("Send FIRST message link (t.me/c/<chatid>/<msgid> or t.me/<username>/<msgid>)")

@bot.on_message(filters.text & filters.private)
async def interactive_listener(c, m):
    uid = str(m.from_user.id)
    if uid not in controller["interactive"]:
        return
    data = controller["interactive"][uid]
    txt = m.text.strip()
    if data.get("step") == 1:
        parsed = parse_msg_link(txt)
        if not parsed:
            return await m.reply_text("Could not parse first link. Send again.")
        data["first_parsed"] = parsed
        data["step"] = 2
        return await m.reply_text("First saved. Now send LAST message link.")
    if data.get("step") == 2:
        parsed2 = parse_msg_link(txt)
        if not parsed2:
            return await m.reply_text("Could not parse last link. Send again.")
        first = data["first_parsed"]["msg_id"]
        last = parsed2["msg_id"]
        if first > last:
            first, last = last, first
        controller["interactive"].pop(uid, None)
        await m.reply_text(f"Confirm to forward {first} → {last}? Reply 'yes' to start.")
        controller["interactive"][uid] = {"confirm": True, "first": first, "last": last, "source": data["first_parsed"]}
        return
    if data.get("confirm"):
        if txt.lower() in ("yes", "y"):
            info = controller["interactive"].pop(uid, None)
            if not info:
                return await m.reply_text("No active operation.")
            first = info["first"]; last = info["last"]; source = info["source"]
            await m.reply_text(f"Starting forward {first} → {last} ...")
            client_for_read = user_client if user_client else bot
            key = f"{source.get('chat_id', source.get('chat_username'))}_{first}_{last}"
            controller["range_task"] = asyncio.create_task(range_worker(client_for_read, m, source, first, last, config["global"].get("targets", DEFAULT_TARGETS.copy()), key, m.from_user.id))
            return await m.reply_text("Started.")
        else:
            controller["interactive"].pop(uid, None)
            return await m.reply_text("Cancelled.")

# diagnostics / init
@bot.on_message(filters.user(OWNER_ID) & filters.command("init"))
async def cmd_init(c, m):
    # start periodic save + optional health server
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_state_save())
    try:
        loop.create_task(start_health_server(int(os.getenv("HEALTH_PORT", "8080"))))
        await m.reply_text("Background services started.")
    except Exception as e:
        await m.reply_text(f"Background start error: {e}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("diagnostics"))
async def cmd_diagnostics(c, m):
    parts = []
    try:
        me = await c.get_me()
        parts.append(f"Bot @{me.username}")
    except Exception as e:
        parts.append(f"Bot auth failed: {e}")
    try:
        await c.get_chat(config["global"].get("targets", DEFAULT_TARGETS)[0])
        parts.append("Default target accessible")
    except Exception as e:
        parts.append(f"Default target issue: {e}")
    await m.reply_text("\n".join(parts))

# -----------------------------
# Periodic save & health server
# -----------------------------
from aiohttp import web

async def periodic_state_save(interval: int = 15):
    while True:
        try:
            save_json(STATE_FILE, state)
            save_json(CONFIG_FILE, config)
        except Exception as e:
            print("periodic_state_save error:", e)
        await asyncio.sleep(interval)

async def start_health_server(port: int = 8080):
    app = web.Application()
    async def health(request):
        return web.json_response({"ok": True, "forwards": metrics.get("forwards", 0), "fails": metrics.get("fails", 0)})
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print("Health server started on port", port)

# -----------------------------
# Graceful shutdown
# -----------------------------
def shutdown_save():
    try:
        save_json(STATE_FILE, state)
        save_json(CONFIG_FILE, config)
    except Exception as e:
        print("shutdown save error:", e)

def _on_signal(sig, frame):
    print("Signal received:", sig)
    shutdown_save()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(bot.stop())
        if user_client:
            loop.create_task(user_client.stop())
    except Exception:
        pass

signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)

# -----------------------------
# START BOT
# -----------------------------
if __name__ == "__main__":
    print("🚀 V12 ULTRA MULTI-USER BOT (with USER_SESSION support) starting...")
    # ensure configs/state written
    save_json(CONFIG_FILE, config)
    save_json(STATE_FILE, state)
    # start background periodic save automatically
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_state_save())
    try:
        loop.create_task(start_health_server(int(os.getenv("HEALTH_PORT", "8080"))))
    except Exception as e:
        print("Health server start failed:", e)
    bot.run()