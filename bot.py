# bot.py  -- V12 Ultra Max (complete)
# Requires: pyrogram, tgcrypto, pytz (optional)
# Run: python bot.py
# NOTE: It's safer to set secrets as env vars on Heroku (BOT_TOKEN, API_ID, API_HASH, USER_SESSION etc.)

import os
import re
import json
import time
import asyncio
import random
import tempfile
import signal
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# --------------------------
# CONFIG (defaults from user's provided values)
# --------------------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8359601755:AAEZTVLTD9YlXbcnoUAt1lfskOJnVmbX2BA")  # recommended: put in Heroku Config Vars, not here
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

# --------------------------
# JSON helpers
# --------------------------
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

# --------------------------
# initial config/state
# --------------------------
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

metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
controller = {
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "range_task": None,
    "interactive": {}
}
controller["pause_event"].set()

if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
    raise SystemExit("BOT_TOKEN not set. Put token in BOT_TOKEN env var or inside the file (not recommended).")

# --------------------------
# Clients
# --------------------------
bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

USER_SESSION = os.getenv("USER_SESSION", "")  # optional string session
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

# --------------------------
# Caption cleanup
# --------------------------
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
# parse_msg_link
# -----------------------------
def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        link = link.strip()
        link = link.split("?")[0].split("#")[0]
        parts = [p for p in link.split("/") if p]
        # t.me/c/<chatnum>/<msgid>
        if "t.me" in link or "telegram.me" in link or "t.me" in parts:
            # search for 'c'
            if "c" in parts:
                try:
                    idx = parts.index("c")
                    chatnum = parts[idx + 1]
                    msgid = parts[idx + 2]
                    if chatnum.isdigit() and msgid.isdigit():
                        chat_id = int(f"-100{chatnum}")
                        return {"chat_id": chat_id, "msg_id": int(msgid)}
                except Exception:
                    return None
            else:
                # assume last two are username/msgid
                if len(parts) >= 2 and parts[-1].isdigit():
                    username = parts[-2]
                    msgid = parts[-1]
                    return {"chat_username": username, "msg_id": int(msgid)}
        # fallback
        if len(parts) >= 2 and parts[-1].isdigit():
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
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
        print("download_media error:", e)
    return None

# -----------------------------
# adaptive wait & send_with_retry
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
            if local_path is None:
                try:
                    await src_msg.copy(chat_id=target, caption=caption)
                    metrics["forwards"] += 1
                    return True
                except Exception:
                    pass
            if local_path and Path(local_path).exists():
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                metrics["forwards"] += 1
                return True
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
            await asyncio.sleep(0.25)

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
        print("fetch_source_message error:", e)
        return None

# -----------------------------
# range_worker
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
# Basic command handlers
# -----------------------------
@bot.on_message(filters.private & filters.command("start"))
async def cmd_start(client, message: Message):
    await message.reply_text("V12 Ultra Max bot ✅\nUse /help for commands.")

@bot.on_message(filters.private & filters.command("help"))
async def cmd_help(client, message: Message):
    text = (
        "Commands:\n"
        "/start - start bot\n"
        "/help - this message\n"
        "/forward <t.me link> <first> <last> - forward a range of messages from link (channel link)\n\n"
        "Example:\n"
        "/forward https://t.me/username/123 100 110"
    )
    await message.reply_text(text)

@bot.on_message(filters.user(OWNER_ID) & filters.command("forward"))
async def cmd_forward(client, message: Message):
    # Owner-only simple forward command: /forward <link> <first> <last>
    args = message.text.split()
    if len(args) < 4:
        await message.reply_text("Usage: /forward <t.me link> <first> <last>")
        return
    link = args[1]
    try:
        first = int(args[2])
        last = int(args[3])
    except ValueError:
        await message.reply_text("First and last must be integers.")
        return
    parsed = parse_msg_link(link)
    if not parsed:
        await message.reply_text("Could not parse link.")
        return

    task_key = f"{message.from_user.id}:{link}:{first}-{last}:{int(time.time())}"
    if controller.get("range_task"):
        await message.reply_text("Another range task is running. Stop it first or wait.")
        return

    # start range worker in background
    controller["range_task"] = asyncio.create_task(range_worker(bot, message, parsed, first, last, config["global"].get("targets", DEFAULT_TARGETS.copy()), task_key, message.from_user.id))
    await message.reply_text(f"Started forwarding {first} → {last} in background. Task key: {task_key}")

@bot.on_message(filters.user(OWNER_ID) & filters.command("stop"))
async def cmd_stop(client, message: Message):
    controller["stop_flag"] = True
    await message.reply_text("Stop signal set. Range worker will stop soon.")

@bot.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def cmd_status(client, message: Message):
    await message.reply_text(json.dumps({"metrics": metrics, "controller": {"active_task": bool(controller.get("range_task"))}}, indent=2))

# -----------------------------
# Graceful shutdown
# -----------------------------
def _shutdown(signum, frame):
    print("Shutdown signal received:", signum)
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(bot.stop())
    except Exception:
        pass

signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print("Starting V12 Ultra Max bot...")
    # ensure config saved
    save_json(CONFIG_FILE, config)
    try:
        bot.run()
    except Exception as e:
        print("Bot run error:", e)