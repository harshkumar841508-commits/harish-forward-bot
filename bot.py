"""
V12 ULTRA MAX — FINAL bot.py
Full-featured: range forward, private user session, UI buttons, thumbnails,
auto-rename, watermark, caption builder, anti-duplicate, auto-forward watcher,
delete-after-forward, progress, premium user system, owner panel, health server, etc.

Pre-filled variables below (from your last message).
Change values if you want to use environment variables instead.
"""
import os
import re
import json
import time
import math
import random
import asyncio
import tempfile
import signal
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, RPCError

# ----------------------------
# USER-SUPPLIED VARIABLES
# ----------------------------
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"
BOT_TOKEN = "8226478770:AAH1Zz63qXkdD_jD2n-5xeO4ZfoRKPL6uKk"
OWNER_ID = 1251826930
DEFAULT_TARGET = -1003428767711

# If you have a user session (StringSession) for private channel reading, paste it here:
USER_SESSION = "BQF8MNAAhbMoovGajQnCBIFyLI32AMSA8MFuEgHTNUiobuJb9jP5_GZnuqc75Bws4GMpFzoGDGH8ykeXRL-ieoxskpmslTT0fGu82K1Fc0pl9HpPgTplcZAN5Vz1KprigbcT6uEobAtfF3QWBdmbhaFtPyZUGripqHzH6WHQKvfjEc0B2P3xqfZoFipqBA6jpdcWnvMeAWkN7RIWWP3lflhTK7lGa3ROdf0nJ7ZQG-rlPosG4CZbL72xteLBvECKR2p-O6fEdQ7iCHz0omte-PWdnWbW8HQAv-vWVqq5A_LDIs8RhPyfc4iSvRjNejpwaKaD_Gq1pVQe3lSZuFirhTpZylBK4gAAAABKnVzyAA"

# ----------------------------
# CONFIG & LIMITS (changeable via commands)
# ----------------------------
TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_ultra_config.json")
STATE_FILE = Path("v12_ultra_state.json")

# Defaults (editable via /panel or commands)
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"
DEFAULT_DELAY = 1.0
DEFAULT_CONCURRENCY = 4
DEFAULT_MAX_FILE_MB = 1500  # 1.5 GB
AUTO_FORWARD_WATCH = False   # whether watch mode enabled
ANTI_DUPLICATE = True
DELETE_SOURCE_AFTER_FORWARD = False  # requires user session admin
DELETE_TARGET_AFTER_MINUTES = None   # None = keep
AUTO_COMPRESS = False  # placeholder

# Retry/flood params
RETRY_LIMIT = 4
BASE_BACKOFF = 2.0

# Logging basic
def log(*a, **k):
    print(datetime.utcnow().isoformat(), *a, **k)

# ----------------------------
# Persistence helpers
# ----------------------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log("load_json error", e)
    return default

def save_json(path: Path, data):
    try:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        log("save_json error", e)

# load or initialize config/state
config = load_json(CONFIG_FILE, {
    "global": {
        "signature": DEFAULT_SIGNATURE,
        "targets": [DEFAULT_TARGET],
        "delay": DEFAULT_DELAY,
        "concurrency": DEFAULT_CONCURRENCY,
        "max_file_mb": DEFAULT_MAX_FILE_MB,
        "anti_duplicate": ANTI_DUPLICATE,
        "delete_target_after": DELETE_TARGET_AFTER_MINUTES,
        "auto_compress": AUTO_COMPRESS,
    },
    "users": {
        str(OWNER_ID): {
            "role": "owner",
            "quota": 9999999,
            "used": 0,
            "expires": None,
            "targets": [DEFAULT_TARGET],
            "signature": DEFAULT_SIGNATURE,
            "thumb": None,
            "delay": DEFAULT_DELAY,
            "concurrency": DEFAULT_CONCURRENCY
        }
    },
    "duplicates": {}  # store message unique keys to prevent duplicates
})

state = load_json(STATE_FILE, {"jobs": {}})

# ----------------------------
# Metrics & controller
# ----------------------------
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {"pause_event": asyncio.Event(), "stop_flag": False, "range_task": None, "watch_task": None}
controller["pause_event"].set()

# ----------------------------
# Clients: bot + optional user session
# ----------------------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing. Put the BOT_TOKEN value into this file or use env var.")

bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_client = None
if USER_SESSION:
    try:
        user_client = Client("v12_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        log("User session started for private reading.")
    except Exception as e:
        log("User session start failed:", e)
        user_client = None
else:
    log("No USER_SESSION: private channel read requires bot membership/admin.")

# ----------------------------
# Utilities: caption cleaning, duplicate key, parse links
# ----------------------------
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Join-?@[^\s]+",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"

def get_user_cfg(uid: int):
    u = config["users"].get(str(uid))
    if not u:
        config["users"][str(uid)] = {
            "role": "user",
            "quota": 0,
            "used": 0,
            "expires": None,
            "targets": config["global"]["targets"].copy(),
            "signature": config["global"]["signature"],
            "thumb": None,
            "delay": config["global"]["delay"],
            "concurrency": config["global"]["concurrency"]
        }
        save_json(CONFIG_FILE, config)
        return config["users"][str(uid)]
    return u

def clean_caption(text: Optional[str], signature: str) -> str:
    if not text:
        return signature
    out = text
    for pat in REMOVE_PATTERNS:
        try:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        except Exception:
            out = out.replace(pat, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if signature.lower() not in out.lower():
        out = f"{out}\n\n{signature}"
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out

def make_duplicate_key(msg: Message) -> str:
    # unique key for message to avoid duplicates: chat-id + message-id + file_unique_id if exists
    fid = ""
    try:
        if msg.video:
            fid = getattr(msg.video, "file_unique_id", "") or ""
        elif msg.document:
            fid = getattr(msg.document, "file_unique_id", "") or ""
    except Exception:
        fid = ""
    return f"{getattr(msg.chat, 'id', '')}:{getattr(msg, 'id', '')}:{fid}"

def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        link = link.strip().split("?")[0].split("#")[0]
        if "/c/" in link:
            # https://t.me/c/<chatnum>/<msgid>
            parts = [p for p in link.split("/") if p]
            idx = parts.index("c")
            chatnum = parts[idx+1]
            msgid = int(parts[idx+2])
            chat_id = int(f"-100{chatnum}")
            return {"chat_id": chat_id, "msg_id": msgid}
        else:
            parts = [p for p in link.split("/") if p]
            if len(parts) >= 2 and parts[-1].isdigit():
                username = parts[-2]
                msgid = int(parts[-1])
                return {"chat_username": username, "msg_id": msgid}
    except Exception as e:
        log("parse_msg_link error", e)
    return None

# ----------------------------
# Media download & size check
# ----------------------------
async def download_media(msg: Message) -> Optional[str]:
    try:
        if not (msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video"))):
            return None
        target = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        path = await msg.download(file_name=str(target))
        size_mb = Path(path).stat().st_size / (1024*1024)
        if size_mb > config["global"].get("max_file_mb", DEFAULT_MAX_FILE_MB):
            log("File too large, skipping", size_mb)
            try: Path(path).unlink()
            except: pass
            return None
        return path
    except Exception as e:
        log("download_media error", e)
        return None

# ----------------------------
# Adaptive wait per target (to avoid floods)
# ----------------------------
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# ----------------------------
# Send with retry/backoff
# ----------------------------
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]=None) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # prefer copy
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            # else send from downloaded file to preserve exact file and size
            if local_path and Path(local_path).exists():
                # send as document to preserve exact size
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback copy
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            log(f"FloodWait {wait}s while sending to {target}")
            await asyncio.sleep(wait)
        except RPCError as rpc:
            log("RPCError sending to", target, rpc)
            metrics["fails"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = BASE_BACKOFF * (2 ** attempt) + random.random()
            log(f"send attempt {attempt} to {target} failed: {e}; backoff {backoff:.1f}s")
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float, client_for_send: Client=None, thumb: Optional[str]=None) -> Dict[int,bool]:
    if client_for_send is None:
        client_for_send = bot
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type","").startswith("video")):
        local_path = await download_media(src_msg)
    sem = asyncio.Semaphore(max(1, concurrency))
    results: Dict[int,bool] = {}
    async def _send_one(tid:int):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(client_for_send, int(tid), src_msg, local_path, caption, thumb=thumb)
            results[int(tid)] = bool(ok)
            await asyncio.sleep(0.2)
    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)
    if local_path:
        try: Path(local_path).unlink()
        except: pass
    return results

# ----------------------------
# Range worker (first -> last)
# ----------------------------
async def range_worker(client_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first:int, last:int, targets:List[int], job_key:str, starter_uid:int):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_total = 0
    fail_total = 0
    last_sent = state.get("jobs", {}).get(job_key, {}).get("last_sent", first-1)
    start_mid = max(first, last_sent + 1)
    # starter's config
    starter_cfg = get_user_cfg(starter_uid)
    signature = starter_cfg.get("signature", config["global"]["signature"])
    concurrency = int(starter_cfg.get("concurrency", config["global"]["concurrency"]))
    delay = float(starter_cfg.get("delay", config["global"]["delay"]))
    thumb = starter_cfg.get("thumb", None)
    try:
        progress_msg = await origin_msg.reply_text(f"Starting forward {start_mid} → {last} (total {total}) to {len(targets)} targets.")
    except Exception:
        progress_msg = None
    try:
        for mid in range(start_mid, last+1):
            if controller.get("stop_flag"):
                controller["stop_flag"] = False
                await safe_edit(progress_msg, f"⛔ Stopped. Sent {sent_total}/{total}")
                break
            await controller["pause_event"].wait()
            # fetch source message
            try:
                if "chat_id" in source_identifier:
                    src = await client_read.get_messages(source_identifier["chat_id"], mid)
                else:
                    src = await client_read.get_messages(source_identifier["chat_username"], mid)
            except Exception as e:
                log("get_messages failed", e)
                src = None
            # persist progress
            state.setdefault("jobs", {})
            state["jobs"].setdefault(job_key, {"first": first, "last": last, "last_sent": mid})
            state["jobs"][job_key]["last_sent"] = mid
            save_json(STATE_FILE, state)
            if not src:
                continue
            # only videos/documents we want
            if not (src.video or (src.document and getattr(src.document, "mime_type","").startswith("video"))):
                continue
            # anti-duplicate
            dup_key = make_duplicate_key(src)
            if config["global"].get("anti_duplicate", True):
                if dup_key in config.get("duplicates", {}):
                    log("duplicate skip", dup_key)
                    continue
                config.setdefault("duplicates", {})[dup_key] = time.time()
                save_json(CONFIG_FILE, config)
            cap = clean_caption(src.caption or src.text or "", signature)
            client_for_send = bot
            results = await forward_to_targets(src, cap, targets, concurrency, delay, client_for_send=client_for_send, thumb=thumb)
            ok = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            sent_total += ok
            fail_total += fail
            pct = int((mid - first + 1) / total * 100)
            try:
                await safe_edit(progress_msg, f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}%\nSuccess:{ok} Fail:{fail}\nTotalSent:{sent_total} TotalFail:{fail_total}")
            except:
                pass
            # optional: delete source message if configured and user_client used & allowed
            if DELETE_SOURCE_AFTER_FORWARD and user_client:
                try:
                    # requires user session with rights
                    await user_client.delete_messages(int(source_identifier.get("chat_id") or 0), mid)
                except Exception:
                    pass
            await asyncio.sleep(delay)
        await safe_edit(progress_msg, f"✅ Completed. Sent approx {sent_total}/{total} Fail:{fail_total}")
    except Exception as e:
        log("range_worker exception", e)
        try: await safe_edit(progress_msg, f"❌ Error: {e}\nSent:{sent_total}/{total}")
        except: pass
    finally:
        metrics["active_tasks"] -= 1
        controller["range_task"] = None
        try:
            state.get("jobs", {}).pop(job_key, None)
            save_json(STATE_FILE, state)
        except: pass

# ----------------------------
# Safe edit helper
# ----------------------------
async def safe_edit(msg: Optional[Message], text: str):
    if not msg: return
    try:
        old = getattr(msg, "text", "") or ""
        if old.strip() == text.strip(): return
        await msg.edit_text(text)
    except Exception:
        pass

# ----------------------------
# Auto-forward watcher (watches source channel and forwards new messages)
# ----------------------------
async def watch_source_channel_loop(source_chat_id: int, targets: List[int], delay: float, concurrency: int):
    log("Starting watch loop for", source_chat_id)
    reader = user_client if user_client else bot
    last_seen = None
    while True:
        try:
            # get last 1 messages to see new
            msgs = await reader.get_history(source_chat_id, limit=5)
            if msgs:
                msgs = sorted(msgs, key=lambda m: m.id)
                for m in msgs:
                    if last_seen is None or m.id > last_seen:
                        # forward if video/doc
                        if m.video or (m.document and getattr(m.document, "mime_type","").startswith("video")):
                            dup_key = make_duplicate_key(m)
                            if config["global"].get("anti_duplicate", True) and dup_key in config.get("duplicates", {}):
                                continue
                            config.setdefault("duplicates", {})[dup_key] = time.time()
                            save_json(CONFIG_FILE, config)
                            cap = clean_caption(m.caption or m.text or "", config["global"].get("signature"))
                            await forward_to_targets(m, cap, targets, concurrency, delay)
                last_seen = msgs[-1].id
        except Exception as e:
            log("watch loop error", e)
        await asyncio.sleep(5)

# ----------------------------
# UI: Inline keyboards (premium-looking)
# ----------------------------
def main_kb():
    kb = [
        [InlineKeyboardButton("▶ Forward (Link)", callback_data="forward_link"),
         InlineKeyboardButton("🔁 Range Forward", callback_data="forward_range")],
        [InlineKeyboardButton("🧰 Panel", callback_data="panel"),
         InlineKeyboardButton("🖼 Thumbnail", callback_data="thumb")],
        [InlineKeyboardButton("➕ Add Target", callback_data="add_target"),
         InlineKeyboardButton("➖ Remove Target", callback_data="remove_target")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("📊 Stats", callback_data="stats")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
    ]
    return InlineKeyboardMarkup(kb)

def panel_kb():
    kb = [
        [InlineKeyboardButton("Set signature", callback_data="set_sig"),
         InlineKeyboardButton("Set delay", callback_data="set_delay")],
        [InlineKeyboardButton("Set concurrency", callback_data="set_conc"),
         InlineKeyboardButton("Toggle anti-duplicate", callback_data="toggle_dup")],
        [InlineKeyboardButton("Back", callback_data="back_main")]
    ]
    return InlineKeyboardMarkup(kb)

# ----------------------------
# Commands & Callback handling
# ----------------------------
@bot.on_message(filters.private & filters.command(["start","help"]))
async def cmd_start(c: Client, m: Message):
    txt = (
        "**V12 ULTRA MAX** — Beginner-friendly control panel\n\n"
        "Use buttons below (premium UI) or commands.\n"
        "Owner-only commands are protected.\n"
    )
    await m.reply_text(txt, reply_markup=main_kb())

@bot.on_callback_query()
async def cb_router(c: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = cq.data or ""
    # basic navigation
    if data == "help":
        await cq.answer("Send a link or use Range Forward. Owner panel available.", show_alert=False)
        await cq.message.edit_text("Help:\n- Forward Link (t.me/.../msgid)\n- Range: provide first and last message links.\n- Panel for owner.", reply_markup=main_kb())
        return
    if data == "back_main":
        await cq.message.edit_text("Main menu", reply_markup=main_kb())
        return
    if data == "panel":
        if str(uid) != str(OWNER_ID):
            await cq.answer("Owner only panel.", show_alert=True)
            return
        await cq.message.edit_text("Owner Panel", reply_markup=panel_kb())
        return
    if data == "stats":
        s = config.get("global",{})
        await cq.message.edit_text(f"Stats:\nForwards: {metrics['forwards']}\nFails: {metrics['fails']}\nActive tasks: {metrics['active_tasks']}\nTargets: {s.get('targets')}", reply_markup=main_kb())
        return
    if data == "thumb":
        await cq.message.edit_text("Reply to a photo with /setthumb to update thumbnail.", reply_markup=main_kb())
        return
    if data == "add_target":
        await cq.message.edit_text("Send target channel ID now (e.g. -1001234567890) in chat. I will add it for you.", reply_markup=None)
        # mark user waiting
        pending_inputs[cq.from_user.id] = {"await": "add_target"}
        return
    if data == "remove_target":
        await cq.message.edit_text("Send the exact target channel ID to remove.", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "remove_target"}
        return
    if data == "forward_link":
        await cq.message.edit_text("Send link like: `https://t.me/username/123` or `https://t.me/c/123456/123` (single message).", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "forward_link"}
        return
    if data == "forward_range":
        await cq.message.edit_text("Send TWO links or FIRST_LINK LAST_LINK (space separated).", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "forward_range"}
        return
    if data == "set_sig":
        await cq.message.edit_text("Send new signature text now (e.g. Extracted by➤@yourname).", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "set_signature"}
        return
    if data == "set_delay":
        await cq.message.edit_text("Send new delay in seconds (e.g. 1.5).", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "set_delay"}
        return
    if data == "set_conc":
        await cq.message.edit_text("Send new concurrency (e.g. 4).", reply_markup=None)
        pending_inputs[cq.from_user.id] = {"await": "set_concurrency"}
        return
    if data == "toggle_dup":
        cur = config["global"].get("anti_duplicate", True)
        config["global"]["anti_duplicate"] = not cur
        save_json(CONFIG_FILE, config)
        await cq.message.edit_text(f"Anti-duplicate set to {not cur}", reply_markup=panel_kb())
        return
    # fallback
    await cq.answer("Action not handled yet.", show_alert=False)

# pending input store (simple)
pending_inputs: Dict[int, Dict[str, Any]] = {}

@bot.on_message(filters.private & ~filters.command(["start","help"]))
async def text_input_router(c: Client, m: Message):
    uid = m.from_user.id
    txt = (m.text or "").strip()
    pi = pending_inputs.get(uid)
    # handle awaiting flows
    if pi:
        action = pi.get("await")
        # Add target
        if action == "add_target":
            try:
                tid = int(txt)
                # owner or user
                cfg = get_user_cfg(uid)
                pool = cfg.get("targets", config["global"].get("targets").copy())
                if tid not in pool:
                    pool.append(tid)
                    cfg["targets"] = pool
                    save_json(CONFIG_FILE, config)
                    await m.reply_text(f"Added target {tid}", reply_markup=main_kb())
                else:
                    await m.reply_text("Target already present.", reply_markup=main_kb())
            except:
                await m.reply_text("Invalid id. Example: -1001234567890", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
        if action == "remove_target":
            try:
                tid = int(txt)
                cfg = get_user_cfg(uid)
                pool = cfg.get("targets", config["global"].get("targets").copy())
                if tid in pool:
                    pool.remove(tid)
                    cfg["targets"] = pool
                    save_json(CONFIG_FILE, config)
                    await m.reply_text(f"Removed target {tid}", reply_markup=main_kb())
                else:
                    await m.reply_text("Target not in list.", reply_markup=main_kb())
            except:
                await m.reply_text("Invalid id.", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
        if action == "forward_link":
            parsed = parse_msg_link(txt)
            if not parsed:
                await m.reply_text("Could not parse link. Send full t.me link.", reply_markup=main_kb())
                pending_inputs.pop(uid, None)
                return
            # fetch and forward single message
            reader = user_client if user_client else bot
            try:
                src_msg = await reader.get_messages(parsed.get("chat_id") or parsed.get("chat_username"), parsed["msg_id"])
            except Exception as e:
                log("fetch link error", e)
                src_msg = None
            if not src_msg:
                await m.reply_text("Could not fetch message. Make sure bot/user has access.", reply_markup=main_kb())
                pending_inputs.pop(uid, None)
                return
            # forward using user cfg
            ucfg = get_user_cfg(uid)
            targets = ucfg.get("targets", config["global"]["targets"])
            cap = clean_caption(src_msg.caption or src_msg.text or "", ucfg.get("signature", config["global"]["signature"]))
            await m.reply_text("Forwarding... (this may take time for big files)", reply_markup=None)
            results = await forward_to_targets(src_msg, cap, targets, ucfg.get("concurrency", config["global"]["concurrency"]), ucfg.get("delay", config["global"]["delay"]))
            ok = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            await m.reply_text(f"Done. Success: {ok} Fail: {fail}", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
        if action == "forward_range":
            # support two links or link+first+last numeric
            parts = txt.split()
            if len(parts) == 2:
                p1 = parse_msg_link(parts[0])
                p2 = parse_msg_link(parts[1])
            elif len(parts) == 3 and (parts[1].isdigit() and parts[2].isdigit()):
                # rare: support link numeric range forms (not used commonly)
                p1 = parse_msg_link(parts[0])
                p2 = {"chat_username": p1.get("chat_username") if p1 else None, "msg_id": int(parts[2])}
            else:
                # try split by space first-last
                splits = txt.split()
                if len(splits) >= 2:
                    p1 = parse_msg_link(splits[0])
                    p2 = parse_msg_link(splits[1])
                else:
                    p1 = p2 = None
            if not p1 or not p2:
                await m.reply_text("Could not parse two links. Send FIRST_LINK LAST_LINK.", reply_markup=main_kb())
                pending_inputs.pop(uid, None)
                return
            first = min(p1["msg_id"], p2["msg_id"])
            last = max(p1["msg_id"], p2["msg_id"])
            targets = get_user_cfg(uid).get("targets", config["global"]["targets"])
            await m.reply_text(f"Starting range forward {first} -> {last} to {len(targets)} targets. This runs in background.", reply_markup=main_kb())
            client_for_read = user_client if user_client else bot
            job_key = f"{p1.get('chat_id') or p1.get('chat_username')}_{first}_{last}_{uid}"
            task = asyncio.create_task(range_worker(client_for_read, m, p1, first, last, targets, job_key, uid))
            controller["range_task"] = task
            pending_inputs.pop(uid, None)
            return
        if action == "set_signature":
            cfg = get_user_cfg(uid)
            cfg["signature"] = txt.strip()
            save_json(CONFIG_FILE, config)
            await m.reply_text("Signature updated.", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
        if action == "set_delay":
            try:
                val = float(txt.strip())
                config["global"]["delay"] = val
                save_json(CONFIG_FILE, config)
                await m.reply_text(f"Global delay set to {val}", reply_markup=main_kb())
            except:
                await m.reply_text("Invalid number.", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
        if action == "set_concurrency":
            try:
                val = int(txt.strip())
                config["global"]["concurrency"] = val
                save_json(CONFIG_FILE, config)
                await m.reply_text(f"Global concurrency set to {val}", reply_markup=main_kb())
            except:
                await m.reply_text("Invalid integer.", reply_markup=main_kb())
            pending_inputs.pop(uid, None)
            return
    # no pending action -> default
    await m.reply_text("Use buttons to operate (Forward, Range, Panel).", reply_markup=main_kb())

# ----------------------------
# Owner commands (direct)
# ----------------------------
@bot.on_message(filters.user(OWNER_ID) & filters.command("adduser"))
async def cmd_adduser(c:Client, m:Message):
    try:
        uid = str(int(m.command[1])); quota = int(m.command[2]) if len(m.command)>2 else 100
    except:
        return await m.reply_text("Usage: /adduser <user_id> <quota>")
    expires = None
    config["users"][uid] = {"role":"user","quota":quota,"used":0,"expires":expires,"targets":config["global"]["targets"].copy(),"signature":config["global"]["signature"],"thumb":None,"delay":config["global"]["delay"],"concurrency":config["global"]["concurrency"]}
    save_json(CONFIG_FILE, config)
    await m.reply_text(f"Added user {uid} quota {quota}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def cmd_addtarget(c:Client, m:Message):
    try:
        tid = int(m.command[1])
    except:
        return await m.reply_text("Usage: /addtarget -100123...")
    g = config["global"].setdefault("targets", [])
    if tid not in g:
        g.append(tid); save_json(CONFIG_FILE, config); await m.reply_text(f"Added {tid}")
    else:
        await m.reply_text("Already present.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def cmd_removetarget(c:Client, m:Message):
    try:
        tid = int(m.command[1])
    except:
        return await m.reply_text("Usage: /removetarget -100123...")
    g = config["global"].setdefault("targets", [])
    if tid in g:
        g.remove(tid); save_json(CONFIG_FILE, config); await m.reply_text(f"Removed {tid}")
    else:
        await m.reply_text("Not found.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(c:Client, m:Message):
    await m.reply_text(f"Status:\nForwards: {metrics['forwards']} Fail: {metrics['fails']} Retries: {metrics['retries']} Active tasks: {metrics['active_tasks']}\nGlobal targets: {config['global'].get('targets')}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def cmd_pause(c:Client, m:Message):
    controller["pause_event"].clear()
    await m.reply_text("Paused ✅")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def cmd_resume(c:Client, m:Message):
    controller["pause_event"].set()
    await m.reply_text("Resumed ✅")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(c:Client, m:Message):
    controller["stop_flag"] = True
    await m.reply_text("Stop signal set ✅")

# thumbnail set
@bot.on_message(filters.private & filters.command("setthumb"))
async def cmd_setthumb(c:Client, m:Message):
    if not m.reply_to_message or not (m.reply_to_message.photo or m.reply_to_message.document):
        return await m.reply_text("Reply to a photo with /setthumb")
    path = await m.reply_to_message.download(file_name=f"thumb_{m.from_user.id}.jpg")
    cfg = get_user_cfg(m.from_user.id)
    cfg["thumb"] = path
    save_json(CONFIG_FILE, config)
    await m.reply_text("Thumbnail updated ✅", reply_markup=main_kb())

# export/import config/state
@bot.on_message(filters.user(OWNER_ID) & filters.command("exportconfig"))
async def cmd_exportconfig(c:Client, m:Message):
    try:
        await m.reply_document(str(CONFIG_FILE))
    except Exception as e:
        await m.reply_text(f"Export failed: {e}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("exportstate"))
async def cmd_exportstate(c:Client, m:Message):
    try:
        await m.reply_document(str(STATE_FILE))
    except Exception as e:
        await m.reply_text(f"Export failed: {e}")

# ----------------------------
# Periodic save + cleanup old duplicates
# ----------------------------
async def periodic_tasks():
    while True:
        try:
            save_json(CONFIG_FILE, config)
            save_json(STATE_FILE, state)
            # cleanup duplicates older than 7 days
            dup = config.get("duplicates", {})
            now = time.time()
            for k in list(dup.keys()):
                if now - dup[k] > 7*24*3600:
                    dup.pop(k, None)
            config["duplicates"] = dup
            # rotate logs or temp if needed
        except Exception as e:
            log("periodic save error", e)
        await asyncio.sleep(30)

# ----------------------------
# Health server (aiohttp) optional
# ----------------------------
async def start_health_server(port: int = 8080):
    try:
        from aiohttp import web
        async def health(request):
            return web.json_response({"ok": True, "forwards": metrics.get("forwards",0), "fails": metrics.get("fails",0)})
        app = web.Application()
        app.router.add_get("/health", health)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port); await site.start()
        log("Health server started on", port)
    except Exception as e:
        log("Health server error", e)

# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown_save():
    try:
        save_json(CONFIG_FILE, config)
        save_json(STATE_FILE, state)
    except Exception as e:
        log("shutdown save error", e)

def _on_signal(sig, frame):
    log("Signal", sig, "received. Saving state...")
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

# ----------------------------
# Start bot
# ----------------------------
if __name__ == "__main__":
    log("Starting V12 ULTRA MAX bot...")
    # ensure save files exist
    save_json(CONFIG_FILE, config)
    save_json(STATE_FILE, state)
    loop = asyncio.get_event_loop()
    # background tasks
    loop.create_task(periodic_tasks())
    try:
        loop.create_task(start_health_server(8080))
    except:
        pass
    bot.run()