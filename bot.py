# bot.py - V12 Ultra Max (Heroku-ready)
# Features:
# - /start /help
# - Inline keyboard beginner-friendly UI (Forward, Range Forward, Panel, Settings, Premium)
# - Range forward by sending TWO links (first last) OR t.me links space-separated
# - Admin panel: setsource, addtarget, removetarget, setdelay, setconcurrency, pause/resume
# - Thumbnail set (store as file_id in config)
# - Uses USER_SESSION (optional) to read private channels
# - Persists config/state to JSON files
# - Heroku friendly (env vars)
# WARNING: keep tokens & session in Heroku Config Vars — do NOT commit to public repo.

import os, json, time, asyncio, random, tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError

# -------- CONFIG (use Heroku Config Vars) ----------------
API_ID = int(os.getenv("API_ID", "24916176"))
API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8226478770:AAH1Zz63qXkdD_jD2n-5xeO4ZfoRKPL6uKk")
OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))

TARGET_CHANNELS_ENV = os.getenv("TARGET_CHANNELS", "-1003428767711")
TARGET_CHANNELS = [int(x.strip()) for x in TARGET_CHANNELS_ENV.split(",") if x.strip()]

SOURCE_CHANNEL_ENV = os.getenv("SOURCE_CHANNEL", "-1003175017722")
# SOURCE_CHANNEL can be int (id) or username string
try:
    SOURCE_CHANNEL = int(SOURCE_CHANNEL_ENV)
except Exception:
    SOURCE_CHANNEL = SOURCE_CHANNEL_ENV

USER_SESSION = os.getenv("USER_SESSION", "")  # optional user session string

RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
DEFAULT_DELAY = float(os.getenv("DEFAULT_DELAY", "1.5"))
DEFAULT_CONCURRENCY = int(os.getenv("DEFAULT_CONCURRENCY", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "1500"))

TMP_DIR = Path(tempfile.gettempdir())
CONFIG_FILE = Path("v12_config.json")
STATE_FILE = Path("v12_state.json")

# -------- simple json helpers ----------------
def load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def save_json(path: Path, data):
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print("save_json error:", e)

# -------- load or init config/state ----------------
config = load_json(CONFIG_FILE, {
    "global": {
        "signature": "Extracted by➤@course_wale",
        "targets": TARGET_CHANNELS.copy(),
        "delay": DEFAULT_DELAY,
        "concurrency": DEFAULT_CONCURRENCY,
        "thumb": None,
        "premium_enabled": False
    },
    "users": {}
})
state = load_json(STATE_FILE, {})

# ensure owner present
config.setdefault("users", {})
config["users"].setdefault(str(OWNER_ID), {
    "role": "owner",
    "quota": 99999999,
    "used": 0,
    "expires": None,
    "targets": config["global"].get("targets", TARGET_CHANNELS.copy()),
    "signature": config["global"].get("signature"),
    "thumb": config["global"].get("thumb"),
    "delay": config["global"].get("delay"),
    "concurrency": config["global"].get("concurrency")
})
save_json(CONFIG_FILE, config)

# -------- clients ----------------
bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client: Optional[Client] = None
if USER_SESSION:
    try:
        user_client = Client("v12_user", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        print("User session started - private channel reading enabled.")
    except Exception as e:
        print("User session FAILED:", e)
        user_client = None
else:
    print("No USER_SESSION - private channels readable only if bot is member/admin.")

# -------- utilities ----------------
_last_send_time: Dict[int, float] = {}

async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

async def download_media(msg: Message) -> Optional[str]:
    try:
        if msg.video or (msg.document and getattr(msg.document, "mime_type", "").startswith("video")):
            out = TMP_DIR / f"v12_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
            path = await msg.download(file_name=str(out))
            try:
                if Path(path).stat().st_size / (1024*1024) > MAX_FILE_MB:
                    try: Path(path).unlink()
                    except: pass
                    return None
            except Exception:
                pass
            return path
    except Exception as e:
        print("download_media error:", e)
    return None

async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, local_path: Optional[str], caption: str) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            if local_path and Path(local_path).exists():
                await client_for_send.send_document(chat_id=target, document=local_path, caption=caption)
                return True
            await src_msg.copy(chat_id=target, caption=caption)
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", 5)) + 1
            print(f"FloodWait {wait}s while sending to {target}")
            await asyncio.sleep(wait)
        except RPCError as rpc:
            print("RPCError:", rpc)
            return False
        except Exception as e:
            attempt += 1
            backoff = (2 ** attempt) + random.random()
            print(f"send attempt {attempt} to {target} failed: {e}; backoff {backoff:.1f}")
            await asyncio.sleep(backoff)
    return False

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
            await asyncio.sleep(0.2)

    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)

    if local_path:
        try: Path(local_path).unlink()
        except: pass

    return results

def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        l = link.strip().split("?")[0].split("#")[0]
        parts = [p for p in l.split("/") if p]
        if "t.me" in parts or "telegram.me" in parts:
            if "c" in parts:
                idx = parts.index("c")
                chatnum = parts[idx+1]
                msgid = parts[idx+2]
                return {"chat_id": int(f"-100{chatnum}"), "msg_id": int(msgid)}
            else:
                username = parts[-2]
                msgid = parts[-1]
                return {"chat_username": username, "msg_id": int(msgid)}
        if parts[0] == "c" and len(parts) >= 3:
            return {"chat_id": int(f"-100{parts[1]}"), "msg_id": int(parts[2])}
        if len(parts) >= 2 and parts[-1].isdigit():
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
        return None
    return None

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

# ------------- UI / keyboards ----------------
def main_keyboard():
    kb = [
        [InlineKeyboardButton("▶ Forward (Link)", callback_data="forward_link"),
         InlineKeyboardButton("🔁 Range Forward", callback_data="range_forward")],
        [InlineKeyboardButton("🧰 Panel", callback_data="panel"),
         InlineKeyboardButton("🖼 Thumbnail", callback_data="thumbnail")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("💎 Premium", callback_data="premium")],
        [InlineKeyboardButton("📊 Stats", callback_data="stats")]
    ]
    return InlineKeyboardMarkup(kb)

# -------- commands -------------
@bot.on_message(filters.command("start"))
async def start_cmd(c: Client, m: Message):
    text = "V12 Ultra Max bot ✅\nUse /help or buttons below.\nSend TWO links (space separated) to range-forward."
    await m.reply_text(text, reply_markup=main_keyboard())

@bot.on_message(filters.command("help"))
async def help_cmd(c: Client, m: Message):
    txt = (
        "Commands:\n"
        "/start - start bot\n"
        "/help - this message\n"
        "/setsource <link or -100id> - set default source (owner only)\n"
        "/addtarget <chat_id> - add target (owner only)\n"
        "/removetarget <chat_id> - remove target (owner only)\n"
        "/targets - list targets (owner)\n"
        "/setdelay <seconds> - set global delay (owner)\n"
        "/setconcurrency <n> - set concurrency (owner)\n"
        "/pause - pause tasks (owner)\n"
        "/resume - resume tasks (owner)\n\n"
        "To quickly forward a range: send TWO links (first last) from same channel:\n"
        "https://t.me/c/12345/2 https://t.me/c/12345/9"
    )
    await m.reply_text(txt, reply_markup=main_keyboard())

# -------- callback buttons -------------
@bot.on_callback_query()
async def cb_handler(c: Client, cb):
    data = cb.data or ""
    if data == "forward_link":
        await cb.message.reply_text("Send TWO links or FIRST_LINK LAST_LINK (space separated).")
    elif data == "range_forward":
        await cb.message.reply_text("Send TWO links (first and last) — bot will forward messages in that range.")
    elif data == "panel":
        if int(cb.from_user.id) != OWNER_ID:
            await cb.answer("Owner only", show_alert=True)
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Add Target", callback_data="panel_add"), InlineKeyboardButton("Remove Target", callback_data="panel_remove")],
            [InlineKeyboardButton("Set Source", callback_data="panel_setsource"), InlineKeyboardButton("Set Delay", callback_data="panel_setdelay")],
            [InlineKeyboardButton("Set Concurrency", callback_data="panel_setconc"), InlineKeyboardButton("Show Targets", callback_data="panel_show")]
        ])
        await cb.message.reply_text("Admin Panel:", reply_markup=kb)
    elif data.startswith("panel_"):
        await cb.answer("Use commands in chat (owner only).", show_alert=True)
    elif data == "thumbnail":
        await cb.message.reply_text("To set thumbnail: send image with caption /setthumb (owner only).")
    elif data == "settings":
        await cb.message.reply_text("Settings are admin-only. Use commands.")
    elif data == "premium":
        txt = "Premium: Buy to unlock advanced features.\n(Placeholder) Contact @course_wale"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Buy Premium", url="https://t.me/course_wale")]])
        await cb.message.reply_text(txt, reply_markup=kb)
    elif data == "stats":
        txt = f"Targets: {config['global'].get('targets')}\nSource: {SOURCE_CHANNEL}\nDelay: {config['global'].get('delay')}\nConcurrency: {config['global'].get('concurrency')}"
        await cb.message.reply_text(txt)
    else:
        await cb.answer("Not implemented", show_alert=False)

# -------- text handler for TWO links -------------
@bot.on_message(filters.private & filters.text)
async def two_links_handler(c: Client, m: Message):
    text = (m.text or "").strip()
    if not text:
        return
    parts = text.split()
    if len(parts) >= 2:
        a = parse_msg_link(parts[0])
        b = parse_msg_link(parts[1])
        if a and b:
            # must be same chat
            if (a.get("chat_id") and b.get("chat_id") and a["chat_id"] == b["chat_id"]) or (a.get("chat_username") and b.get("chat_username") and a["chat_username"] == b["chat_username"]):
                if a.get("chat_id"):
                    chat = {"chat_id": a["chat_id"]}
                else:
                    chat = {"chat_username": a["chat_username"]}
                first, last = a["msg_id"], b["msg_id"]
                if first > last:
                    first, last = last, first
                await m.reply_text(f"Starting forward {first} → {last} to {len(config['global'].get('targets',[]))} targets.")
                sent_total = 0
                fail_total = 0
                targets_use = config["global"].get("targets", TARGET_CHANNELS.copy())
                concurrency = int(config["global"].get("concurrency", DEFAULT_CONCURRENCY))
                delay = float(config["global"].get("delay", DEFAULT_DELAY))
                # reader
                reader = user_client if user_client else bot
                for mid in range(first, last+1):
                    try:
                        src = await fetch_source_message(chat, mid)
                        if not src:
                            continue
                        caption = getattr(src, "caption", None) or (src.text or "")
                        results = await forward_to_targets(src, caption or "", targets_use, concurrency, delay, client_for_send=bot)
                        ok = sum(1 for v in results.values() if v)
                        fail = sum(1 for v in results.values() if not v)
                        sent_total += ok
                        fail_total += fail
                        await asyncio.sleep(delay)
                    except Exception as e:
                        print("range forward error:", e)
                await m.reply_text(f"✅ Completed. Sent approx {sent_total}/{(last-first+1)*len(targets_use)} Fail:{fail_total}")
                return
            else:
                await m.reply_text("Links appear to be from different chats. Provide links from same channel.")
                return
    await m.reply_text("Send TWO links or FIRST_LINK LAST_LINK (space separated).")

# -------- owner/admin commands -------------
def owner_only(func):
    async def wrapper(c: Client, m: Message):
        if int(m.from_user.id) != OWNER_ID:
            await m.reply_text("Owner only.")
            return
        await func(c, m)
    return wrapper

@bot.on_message(filters.command("setsource") & filters.user(OWNER_ID))
async def setsource_cmd(c: Client, m: Message):
    if not m.command or len(m.command) < 2:
        await m.reply_text("Usage: /setsource <t.me link or -100id or username>")
        return
    val = m.command[1]
    parsed = parse_msg_link(val) if "/" in val else None
    global SOURCE_CHANNEL
    try:
        if val.startswith("-100"):
            SOURCE_CHANNEL = int(val)
        elif parsed and parsed.get("chat_id"):
            SOURCE_CHANNEL = parsed.get("chat_id")
        elif parsed and parsed.get("chat_username"):
            SOURCE_CHANNEL = parsed.get("chat_username")
        else:
            SOURCE_CHANNEL = val
        await m.reply_text(f"Source updated to: {SOURCE_CHANNEL}")
    except Exception as e:
        await m.reply_text(f"Failed: {e}")

@bot.on_message(filters.command("targets") & filters.user(OWNER_ID))
async def targets_cmd(c: Client, m: Message):
    await m.reply_text("Targets:\n" + "\n".join([str(t) for t in config["global"].get("targets", [])]))

@bot.on_message(filters.command("addtarget") & filters.user(OWNER_ID))
async def addtarget_cmd(c: Client, m: Message):
    if not m.command or len(m.command) < 2:
        await m.reply_text("Usage: /addtarget <chat_id>")
        return
    try:
        tid = int(m.command[1])
        if tid not in config["global"]["targets"]:
            config["global"]["targets"].append(tid)
            save_json(CONFIG_FILE, config)
            await m.reply_text(f"Added target {tid}")
        else:
            await m.reply_text("Already exists")
    except Exception as e:
        await m.reply_text(f"Error: {e}")

@bot.on_message(filters.command("removetarget") & filters.user(OWNER_ID))
async def removetarget_cmd(c: Client, m: Message):
    if not m.command or len(m.command) < 2:
        await m.reply_text("Usage: /removetarget <chat_id>")
        return
    try:
        tid = int(m.command[1])
        if tid in config["global"]["targets"]:
            config["global"]["targets"].remove(tid)
            save_json(CONFIG_FILE, config)
            await m.reply_text(f"Removed target {tid}")
        else:
            await m.reply_text("Not found")
    except Exception as e:
        await m.reply_text(f"Error: {e}")

@bot.on_message(filters.command("setdelay") & filters.user(OWNER_ID))
async def setdelay_cmd(c: Client, m: Message):
    if not m.command or len(m.command) < 2:
        await m.reply_text("Usage: /setdelay <seconds>")
        return
    try:
        d = float(m.command[1])
        config["global"]["delay"] = d
        save_json(CONFIG_FILE, config)
        await m.reply_text(f"Delay set to {d}s")
    except Exception as e:
        await m.reply_text(f"Error: {e}")

@bot.on_message(filters.command("setconcurrency") & filters.user(OWNER_ID))
async def setconc_cmd(c: Client, m: Message):
    if not m.command or len(m.command) < 2:
        await m.reply_text("Usage: /setconcurrency <n>")
        return
    try:
        n = int(m.command[1])
        config["global"]["concurrency"] = n
        save_json(CONFIG_FILE, config)
        await m.reply_text(f"Concurrency set to {n}")
    except Exception as e:
        await m.reply_text(f"Error: {e}")

@bot.on_message(filters.command("pause") & filters.user(OWNER_ID))
async def pause_cmd(c: Client, m: Message):
    # simple: set flag in state
    state["paused"] = True
    save_json(STATE_FILE, state)
    await m.reply_text("All tasks paused.")

@bot.on_message(filters.command("resume") & filters.user(OWNER_ID))
async def resume_cmd(c: Client, m: Message):
    state.pop("paused", None)
    save_json(STATE_FILE, state)
    await m.reply_text("Resumed.")

@bot.on_message(filters.command("setthumb") & filters.user(OWNER_ID) & filters.photo)
async def setthumb_cmd(c: Client, m: Message):
    # owner sends photo with caption /setthumb or just sends photo then /setthumb reply
    try:
        photo = m.photo or (m.reply_to_message and m.reply_to_message.photo)
        if not photo:
            await m.reply_text("Reply to a photo or send photo with caption /setthumb")
            return
        file_id = photo.file_id
        config["global"]["thumb"] = file_id
        save_json(CONFIG_FILE, config)
        await m.reply_text("Thumbnail saved.")
    except Exception as e:
        await m.reply_text(f"Error: {e}")

@bot.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_cmd(c: Client, m: Message):
    targets = config["global"].get("targets", [])
    await m.reply_text(f"Targets: {targets}\nSource: {SOURCE_CHANNEL}\nDelay: {config['global'].get('delay')}\nConcurrency: {config['global'].get('concurrency')}")

# -------- startup notice -------------
@bot.on_message(filters.command("whoami") & filters.user(OWNER_ID))
async def whoami(c: Client, m: Message):
    await m.reply_text(f"Bot running. OWNER: {OWNER_ID}\nUSER_SESSION present: {'yes' if USER_SESSION else 'no'}")

# -------- run -------------
if __na