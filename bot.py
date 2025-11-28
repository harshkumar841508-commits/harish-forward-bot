# bot.py — V12 ULTRA MAX (Heroku-ready, prefilled with user's variables)
# Features: range forward, link forward, user_session support, retries, persistence, admin controls
# Dependencies: pyrogram, tgcrypto, aiohttp (optional), python-dotenv (optional)
# Use: python bot.py

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

# ----------------- USER PROVIDED VARIABLES (prefilled) -----------------
API_ID = 24916176
API_HASH = "15e8847a5d612831b6a42c5f8d846a8a"
BOT_TOKEN = "8226478770:AAH1Zz63qXkdD_jD2n-5xeO4ZfoRKPL6uKk"
OWNER_ID = 1251826930

# channels & session (your values)
SOURCE_CHANNEL_ID = -1003175017722
DEFAULT_TARGETS = [-1003428767711]  # list; you can add more
USER_SESSION = "BQF8MNAAhbMoovGajQnCBIFyLI32AMSA8MFuEgHTNUiobuJb9jP5_GZnuqc75Bws4GMpFzoGDGH8ykeXRL-ieoxskpmslTT0fGu82K1Fc0pl9HpPgTplcZAN5Vz1KprigbcT6uEobAtfF3QWBdmbhaFtPyZUGripqHzH6WHQKvfjEc0B2P3xqfZoFipqBA6jpdcWnvMeAWkN7RIWWP3lflhTK7lGa3ROdf0nJ7ZQG-rlPosG4CZbL72xteLBvECKR2p-O6fEdQ7iCHz0omte-PWdnWbW8HQAv-vWVqq5A_LDIs8RhPyfc4iSvRjNejpwaKaD_Gq1pVQe3lSZuFirhTpZylBK4gAAAABKnVzyAA"

# behaviour tuning
FORWARD_DELAY = 0.6       # base delay between sends per target (seconds)
CONCURRENCY = 4           # how many parallel sends to targets
RETRY_LIMIT = 4
MAX_FILE_MB = 1500
TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_config.json")
STATE_FILE = Path("v12_state.json")

# cleaning rules
REMOVE_PATTERNS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"Extracted By ➤.*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"
DEFAULT_SIGNATURE = "Extracted by➤@course_wale"

# ----------------- helpers: json persistence -----------------
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

# load or init config & state
config = load_json(CONFIG_FILE, {
    "global": {
        "signature": DEFAULT_SIGNATURE,
        "targets": DEFAULT_TARGETS.copy(),
        "delay": FORWARD_DELAY,
        "concurrency": CONCURRENCY,
        "thumb": None
    },
    "users": {}
})
state = load_json(STATE_FILE, {})

# ensure owner record
config.setdefault("users", {})
config["users"].setdefault(str(OWNER_ID), {
    "role": "owner",
    "quota": 99999999,
    "used": 0,
    "expires": None,
    "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
    "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
    "thumb": None,
    "delay": config["global"].get("delay", FORWARD_DELAY),
    "concurrency": config["global"].get("concurrency", CONCURRENCY)
})
save_json(CONFIG_FILE, config)

# metrics & controller
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}
}
controller["pause_event"].set()

# ----------------- Pyrogram clients -----------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing. Set BOT_TOKEN variable.")

bot = Client("v12_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client: Optional[Client] = None
if USER_SESSION:
    try:
        user_client = Client("v12_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        print("User session started — private channel reading enabled.")
    except Exception as e:
        print("User session start failed:", e)
        user_client = None
else:
    print("No USER_SESSION — private channels readable only if bot is member/admin.")

# ----------------- utilities -----------------
def get_user_cfg(user_id: int) -> Dict[str, Any]:
    u = config["users"].get(str(user_id))
    if not u:
        config["users"][str(user_id)] = {
            "role": "user", "quota": 0, "used": 0, "expires": None,
            "targets": config["global"].get("targets", DEFAULT_TARGETS.copy()),
            "signature": config["global"].get("signature", DEFAULT_SIGNATURE),
            "thumb": None, "delay": config["global"].get("delay", FORWARD_DELAY),
            "concurrency": config["global"].get("concurrency", CONCURRENCY)
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
    return re.sub(r"\n{3,}", "\n\n", out)

def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    """
    Accepts:
    - https://t.me/c/123456/12  -> returns chat_id=-100123456, msg_id
    - https://t.me/username/12   -> returns chat_username, msg_id
    - t.me/... without scheme
    """
    try:
        l = link.strip().split("?")[0].split("#")[0]
        if "/c/" in l:
            parts = l.split("/")
            # last parts: ... /c/<chatnum>/<msgid>
            msgid = int(parts[-1])
            chatnum = parts[-2]
            # convert chatnum to -100<chatnum>
            chat_id = int(f"-100{chatnum}")
            return {"chat_id": chat_id, "msg_id": msgid}
        else:
            parts = [p for p in l.split("/") if p]
            if len(parts) >= 2:
                msgid = int(parts[-1])
                username = parts[-2]
                # strip domain if present
                if username.lower().endswith("t.me") or username.lower().endswith("telegram.me"):
                    return None
                return {"chat_username": username, "msg_id": msgid}
    except Exception:
        return None

async def safe_edit(msg: Optional[Message], text: str):
    if not msg:
        return
    try:
        old = getattr(msg, "text", "") or ""
        if old.strip() == text.strip():
            return
        await msg.edit_text(text)
    except Exception:
        return

# ----------------- download & send helpers -----------------
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
                    print("Skipped large file:", size_mb)
                    return None
            except Exception:
                pass
            return path
    except Exception as e:
        print("download_media error:", e)
    return None

_last_send_time: Dict[int, float] = {}

async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str, thumb: Optional[str]=None) -> bool:
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
            # send downloaded file
            if local_path and Path(local_path).exists():
                # send as document to preserve exact file
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
            # fallback copy
            await src_msg.copy(chat_id=target, caption=caption)
            metrics["forwards"] += 1
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            print("FloodWait", wait)
            await asyncio.sleep(wait)
        except RPCError as rpc:
            print("RPCError:", rpc)
            metrics["fails"] += 1
            return False
        except Exception as e:
            attempt += 1
            metrics["retries"] += 1
            backoff = (2 ** attempt) + random.random()
            print(f"send attempt {attempt} failed to {target}: {e}; backoff {backoff:.1f}")
            await asyncio.sleep(backoff)
    metrics["fails"] += 1
    return False

async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float, client_for_send: Client):
    local_path = None
    if src_msg.video or (src_msg.document and getattr(src_msg.document, "mime_type","").startswith("video")):
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

# ----------------- range worker -----------------
async def range_worker(client_read: Client, origin_msg: Message, source_identifier: Dict[str,Any], first: int, last: int, targets: List[int], task_key: str, starter_uid: int):
    metrics["active_tasks"] += 1
    total = last - first + 1
    sent_total = 0
    fail_total = 0
    last_sent = state.get(task_key, {}).get("last_sent", first - 1)
    start_mid = max(first, last_sent + 1)

    user_cfg = get_user_cfg(starter_uid)
    signature = user_cfg.get("signature", config["global"].get("signature", DEFAULT_SIGNATURE))
    delay = float(user_cfg.get("delay", config["global"].get("delay", FORWARD_DELAY)))
    concurrency = int(user_cfg.get("concurrency", config["global"].get("concurrency", CONCURRENCY)))
    targets_use = user_cfg.get("targets", targets)

    try:
        progress_msg = await origin_msg.reply_text(f"Starting forward {start_mid} → {last} (total {total}) to {len(targets_use)} targets.")
    except Exception:
        progress_msg = None

    try:
        for mid in range(start_mid, last + 1):
            if controller.get("stop_flag"):
                controller["stop_flag"] = False
                await safe_edit(progress_msg, f"⛔ Stopped. Sent: {sent_total}/{total}")
                break
            await controller["pause_event"].wait()
            src = await fetch_source_message(source_identifier, mid)
            state.setdefault(task_key, {"first": first, "last": last})
            state[task_key]["last_sent"] = mid
            save_json(STATE_FILE, state)

            if not src:
                continue
            if not (src.video or (src.document and getattr(src.document,"mime_type","").startswith("video"))):
                continue

            caption = clean_caption(src.caption or src.text or "", signature)
            sending_client = bot  # we send using bot (bot must be admin in target channels)
            results = await forward_to_targets(src, caption, targets_use, concurrency, delay, sending_client)

            ok = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            sent_total += ok
            fail_total += fail

            pct = int((mid - first + 1) / total * 100)
            try:
                await safe_edit(progress_msg, f"Forwarded {mid} ({mid-first+1}/{total}) — {pct}% — Success:{ok} Fail:{fail}\nTotalSent:{sent_total} TotalFail:{fail_total}")
            except: pass

            await asyncio.sleep(delay)
        await safe_edit(progress_msg, f"✅ Completed. Sent approx {sent_total}/{total} Fail:{fail_total}")
    except Exception as e:
        print("range_worker exception:", e)
        try:
            await safe_edit(progress_msg, f"❌ Error: {e}\nSent: {sent_total}/{total}")
        except: pass
    finally:
        metrics["active_tasks"] -= 1
        controller["range_task"] = None
        try:
            state.pop(task_key, None)
            save_json(STATE_FILE, state)
        except: pass

# ----------------- Commands -----------------
def is_owner(uid: int) -> bool:
    return str(uid) == str(OWNER_ID) or config["users"].get(str(uid), {}).get("role") == "owner"

@bot.on_message(filters.user(OWNER_ID) & filters.command(["start","help"]))
async def cmd_start_owner(client, message):
    text = (
        "**V12 Ultra Max Forward Bot**\n\n"
        "Owner commands:\n"
        "/linkforward <first_link> <last_link> [targets]\n"
        "/range — interactive\n"
        "/pause /resume /stop\n"
        "/setsignature <text>\n"
        "/setthumb (reply to photo)\n"
        "/addtarget <id>\n"
        "/removetarget <id>\n"
        "/listtargets\n"
        "/status\n"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("start") & ~filters.user(OWNER_ID))
async def cmd_start_user(client, message):
    await message.reply_text("Hello — you are not the owner. Contact owner for access.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(client, message):
    await message.reply_text(
        f"Source: `{SOURCE_CHANNEL_ID}`\nTargets: `{config['global'].get('targets')}`\nForwards: {metrics.get('forwards')} Fails: {metrics.get('fails')}\nActive tasks: {metrics.get('active_tasks')}"
    )

@bot.on_message(filters.user(OWNER_ID) & filters.command("setsignature"))
async def cmd_setsignature(client, message):
    txt = " ".join(message.command[1:])
    if not txt:
        return await message.reply_text("Usage: /setsignature <text>")
    config["global"]["signature"] = txt
    save_json(CONFIG_FILE, config)
    await message.reply_text(f"Signature set to: {txt}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def cmd_setthumb(client, message):
    if not message.reply_to_message:
        return await message.reply_text("Reply to an image with /setthumb")
    path = await message.reply_to_message.download(file_name="thumb.jpg")
    config["global"]["thumb"] = path
    save_json(CONFIG_FILE, config)
    await message.reply_text("Thumbnail updated.")

@bot.on_message(filters.command("listtargets") & filters.user(OWNER_ID))
async def cmd_listtargets(client, message):
    await message.reply_text(f"Targets: `{config['global'].get('targets')}`")

@bot.on_message(filters.command("addtarget") & filters.user(OWNER_ID))
async def cmd_addtarget(client, message):
    try:
        tid = int(message.command[1])
    except:
        return await message.reply_text("Usage: /addtarget -100xxxxxxx")
    pool = config["global"].get("targets", [])
    if tid in pool:
        return await message.reply_text("Already present.")
    pool.append(tid)
    config["global"]["targets"] = pool
    save_json(CONFIG_FILE, config)
    await message.reply_text(f"Added {tid}")

@bot.on_message(filters.command("removetarget") & filters.user(OWNER_ID))
async def cmd_removetarget(client, message):
    try:
        tid = int(message.command[1])
    except:
        return await message.reply_text("Usage: /removetarget -100xxxxxxx")
    pool = config["global"].get("targets", [])
    if tid in pool:
        pool.remove(tid)
        config["global"]["targets"] = pool
        save_json(CONFIG_FILE, config)
        return await message.reply_text(f"Removed {tid}")
    return await message.reply_text("Not found.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("pause"))
async def cmd_pause(client, message):
    controller["pause_event"].clear()
    await message.reply_text("⏸ Paused")

@bot.on_message(filters.user(OWNER_ID) & filters.command("resume"))
async def cmd_resume(client, message):
    controller["pause_event"].set()
    await message.reply_text("▶ Resumed")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(client, message):
    controller["stop_flag"] = True
    await message.reply_text("⛔ Stop signal set")

@bot.on_message(filters.command("exportstate") & filters.user(OWNER_ID))
async def cmd_exportstate(client, message):
    try:
        await message.reply_document(str(STATE_FILE))
    except Exception as e:
        await message.reply_text(f"Export failed: {e}")

@bot.on_message(filters.command("importstate") & filters.user(OWNER_ID))
async def cmd_importstate(client, message):
    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text("Reply to state JSON with /importstate")
    p = await message.reply_to_message.download(file_name=str(TMP_DIR/"import_state.json"))
    try:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        save_json(STATE_FILE, data)
        await message.reply_text("Imported state.")
    except Exception as e:
        await message.reply_text(f"Import failed: {e}")

# /linkforward <link1> <link2> [targets]
@bot.on_message(filters.command("linkforward") & filters.user(OWNER_ID))
async def cmd_linkforward(client, message):
    args = message.command[1:]
    if len(args) < 2:
        return await message.reply_text("Usage: /linkforward <first_link> <last_link> [targets comma separated]")
    p1 = parse_msg_link(args[0])
    p2 = parse_msg_link(args[1])
    if not p1 or not p2:
        return await message.reply_text("Invalid links.")
    first = min(p1["msg_id"], p2["msg_id"])
    last = max(p1["msg_id"], p2["msg_id"])
    targets_use = config["global"].get("targets", DEFAULT_TARGETS.copy())
    if len(args) > 2:
        try:
            tlist = [int(x.strip()) for x in args[2].split(",") if x.strip()]
            if tlist:
                targets_use = tlist
        except:
            pass
    client_for_read = user_client if user_client else bot
    key = f"{p1.get('chat_id', p1.get('chat_username'))}_{first}_{last}"
    await message.reply_text(f"Starting forward {first} → {last} to {len(targets_use)} targets.")
    controller["range_task"] = asyncio.create_task(range_worker(client_for_read, message, p1, first, last, targets_use, key, message.from_user.id))

# interactive /range
@bot.on_message(filters.command("range") & filters.private)
async def cmd_range_start(client, message):
    u