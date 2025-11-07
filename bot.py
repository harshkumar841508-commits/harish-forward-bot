"""
bot.py  — Auto Forward Bot V8.5 (Hybrid Extractor + Interactive Forward)
Features:
 - Interactive /forward (asks first link then last link)
 - Works with Bot token and optionally a User StringSession (to extract from channels where bot is not admin)
 - Extract (download) -> Re-upload (no forward tag) with thumbnail and cleaned caption
 - Progress bar, ETA, Pause/Resume/Stop, persistence (state.json)
 - Concurrency, retries, exponential backoff
 - Inline control panel and beginner-friendly commands

SETUP (quick):
 1) Install requirements:
    pip install pyrogram tgcrypto aiofiles python-dotenv

 2) Environment variables (Heroku Config Vars recommended):
    API_ID, API_HASH, BOT_TOKEN, OWNER_ID
    Optional: USER_SESSION (a Pyrogram StringSession for your user account)
    Optional: SOURCE_CHANNEL (default fallback), TARGET_CHANNELS (csv), FORWARD_DELAY, CONCURRENCY

 3) To create USER_SESSION (run locally once):
    from pyrogram import Client
    from pyrogram.session import StringSession
    app = Client("me", api_id=API_ID, api_hash=API_HASH)
    with app:
        print(StringSession.save(app.session))

    Copy the printed string and set as USER_SESSION env var (keep secret).

USAGE (owner-only):
 - /start or /help : shows commands
 - /panel : open inline buttons
 - /forward : start interactive forward (bot asks first then last link)
 - /linkforward <link1> <link2> : one-line forward
 - /pause, /resume, /stop : control range tasks
 - /setthumb (reply to image) : update thumbnail
 - /setcaption <text> : set footer signature
 - /addtarget /removetarget /listtargets etc.

NOTE:
 - Respect Telegram rules. Don't repost illegal/copyrighted/NSFW material.
"""
import os
import re
import json
import time
import asyncio
import tempfile
from pathlib import Path
from typing import Optional, List
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, RPCError

# ----------------------------
# CONFIG (environment or defaults)
API_ID = int(os.getenv("API_ID", "28420641"))
API_HASH = os.getenv("API_HASH", "d1302d5039ae3275c4195b4fcc5ff1f9")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8592967336:AAGoj5zAzkPO9nHSFjHYHp7JclEq4Z7KKGg")
USER_SESSION = os.getenv("USER_SESSION", "")   # optional: your pyrogram StringSession
OWNER_ID = int(os.getenv("OWNER_ID", "8117462619"))

# Source & targets (edit env or defaults)
SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", "-1003240589036"))  # default primary source if needed
env_targets = os.getenv("TARGET_CHANNELS", "-1003216068164")
TARGET_CHANNELS = [int(x.strip()) for x in env_targets.split(",") if x.strip()]

# Thumbnail and behavior
THUMB_FILE = os.getenv("THUMB_FILE", "thumb.jpg")   # default thumbnail filename in repo root
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.5"))  # seconds between actions
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))   # how many concurrent target uploads per message
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "3"))

# Caption cleaning / replacements
REMOVE_TEXTS = [
    r"Extracted\s*by[^\n]*",
    r"Extracted\s*By[^\n]*",
    r"@YTBR_67", r"@skillwithgaurav", r"@kamdev5x", r"@skillzoneu",
    r"Gaurav\s*RaJput", r"Gaurav", r"Join-@skillwithgaurav"
]
OLD_WEBSITE_RE = r"https?://[^\s]*riyasmm\.shop[^\s]*"
NEW_WEBSITE = "https://bio.link/manmohak"
DEFAULT_SIGNATURE = os.getenv("DEFAULT_SIGNATURE", "Extracted by➤@course_wale")

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "250"))  # skip > this

# Persistence files
STATE_FILE = Path("state.json")
CONFIG_FILE = Path("v85_config.json")

# ----------------------------
# Load/save runtime config (persist small settings)
def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except:
            pass
    cfg = {
        "targets": TARGET_CHANNELS.copy(),
        "thumb": THUMB_FILE,
        "signature": DEFAULT_SIGNATURE,
        "forward_delay": FORWARD_DELAY,
        "concurrency": CONCURRENCY
    }
    save_config(cfg)
    return cfg

def save_config(cfg):
    try:
        CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    except Exception as e:
        print("save_config error:", e)

config = load_config()
# keep quick handles
TARGETS_POOL = config.get("targets", [])
THUMB_FILE = config.get("thumb", THUMB_FILE)
SIGNATURE = config.get("signature", DEFAULT_SIGNATURE)
FORWARD_DELAY = config.get("forward_delay", FORWARD_DELAY)
CONCURRENCY = config.get("concurrency", CONCURRENCY)

# ----------------------------
# Pyrogram clients: bot and optional user
bot = Client("v85_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user = None
if USER_SESSION:
    # user is optional; must be a valid StringSession
    try:
        user = Client(session_name=USER_SESSION, api_id=API_ID, api_hash=API_HASH)
    except Exception as e:
        print("USER_SESSION invalid or creation failed:", e)
        user = None

# controller & persistence
controller = {
    "range_task": None,   # currently running range task
    "pause_event": asyncio.Event(),
    "stop_flag": False,
    "interactive_wait": {},  # temp store for interactive forward flow {user_id: {"step":1,"first":None}}
}
controller["pause_event"].set()

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {}

def save_state(state):
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        print("save_state error:", e)

state = load_state()

# ----------------------------
# Utilities
def clean_caption(text: Optional[str]) -> str:
    sig = config.get("signature", SIGNATURE)
    if not text:
        return sig
    out = text
    for pat in REMOVE_TEXTS:
        try:
            out = re.sub(pat, "", out, flags=re.IGNORECASE)
        except re.error:
            out = out.replace(pat, "")
    out = re.sub(OLD_WEBSITE_RE, NEW_WEBSITE, out, flags=re.IGNORECASE)
    out = out.strip()
    if sig.lower() not in out.lower():
        out = f"{out}\n\n{sig}"
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out

def parse_msgid_from_link(link: str) -> Optional[int]:
    """Accepts t.me/c/<chan>/<id> or t.me/<username>/<id>"""
    try:
        parts = link.strip().split("/")
        return int(parts[-1])
    except:
        return None

async def download_message_media(client: Client, msg: Message) -> Optional[str]:
    """Download message media to temp file; returns filepath or None"""
    try:
        # choose filename in temp dir
        tmpdir = tempfile.gettempdir()
        base = f"v85_{msg.chat.id}_{msg.id}_{int(time.time()*1000)}"
        # if video or document (video)
        if msg.video:
            fpath = await msg.download(file_name=f"{base}.mp4")
        elif msg.document and getattr(msg.document, "mime_type", "").startswith("video"):
            fpath = await msg.download(file_name=f"{base}.mp4")
        elif msg.photo:
            fpath = await msg.download(file_name=f"{base}.jpg")
        else:
            # for text or unsupported types, we won't download
            return None
        # size check
        try:
            size_mb = (Path(fpath).stat().st_size) / (1024*1024)
            if size_mb > MAX_FILE_SIZE_MB:
                print(f"File too big {size_mb:.1f}MB; skipping")
                try: Path(fpath).unlink()
                except: pass
                return None
        except Exception:
            pass
        return fpath
    except Exception as e:
        print("download error:", e)
        return None

async def send_media_with_retries(client: Client, target: int, local_path: str, caption: str, thumb: Optional[str]=None) -> bool:
    attempt = 0
    backoff = 1.0
    while attempt < RETRY_LIMIT:
        try:
            # Use send_video on mp4s, send_photo on jpg, else send_document
            suffix = Path(local_path).suffix.lower()
            if suffix in [".mp4", ".mkv", ".webm"]:
                await client.send_video(chat_id=target, video=local_path, caption=caption, thumb=thumb if thumb and Path(thumb).exists() else None, supports_streaming=True)
            elif suffix in [".jpg", ".jpeg", ".png", ".webp"]:
                await client.send_photo(chat_id=target, photo=local_path, caption=caption)
            else:
                await client.send_document(chat_id=target, document=local_path, caption=caption)
            return True
        except FloodWait as fw:
            wait = int(getattr(fw, "value", getattr(fw, "x", 10))) + 1
            print(f"FloodWait {wait}s, sleeping...")
            await asyncio.sleep(wait)
        except RPCError as r:
            print("RPCError sending:", r)
            return False
        except Exception as e:
            attempt += 1
            print(f"send attempt {attempt} failed: {e}, backoff {backoff}s")
            await asyncio.sleep(backoff)
            backoff *= 2
    return False

async def forward_message_to_targets(client_for_download: Client, src_msg: Message, targets: List[int], use_reupload=True):
    """
    For a single source message: download (if needed) and reupload to targets concurrently.
    If use_reupload False -> will attempt message.copy (fast) (requires bot admin)
    """
    caption = clean_caption(src_msg.caption or src_msg.text or "")
    # if message is pure text and small -> use copy
    is_media = bool(src_msg.video or src_msg.document or src_msg.photo)
    if is_media:
        # download with client_for_download (user if available else bot)
        local_path = await download_message_media(client_for_download, src_msg)
        if not local_path:
            # fallback to copy (maybe allowed)
            print("No local file, trying message.copy fallback")
            results = []
            for t in targets:
                try:
                    await src_msg.copy(chat_id=t, caption=caption)
                    results.append(True)
                except Exception as e:
                    print("copy fallback error:", e)
                    results.append(False)
                await asyncio.sleep(FORWARD_DELAY)
            return results
        # send concurrently with concurrency semaphore
        sem = asyncio.Semaphore(config.get("concurrency", CONCURRENCY))
        async def _send_to(tid):
            async with sem:
                ok = await send_media_with_retries(bot if bot else client_for_download, tid, local_path, caption, thumb=config.get("thumb", THUMB_FILE))
                await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
                return ok
        tasks = [asyncio.create_task(_send_to(t)) for t in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # cleanup file
        try:
            Path(local_path).unlink()
        except:
            pass
        return results
    else:
        # text only: copy (fast)
        results = []
        for t in targets:
            try:
                await src_msg.copy(chat_id=t, caption=caption)
                results.append(True)
            except Exception as e:
                print("copy text error:", e)
                results.append(False)
            await asyncio.sleep(config.get("forward_delay", FORWARD_DELAY))
        return results

# ----------------------------
# Inline keyboards & helpers
def main_panel_kb():
    kb = [
        [InlineKeyboardButton("▶ Forward (interactive)", "btn_forward"),
         InlineKeyboardButton("🔁 LinkForward", "btn_linkforward")],
        [InlineKeyboardButton("⏸ Pause", "btn_pause"),
         InlineKeyboardButton("▶ Resume", "btn_resume"),
         InlineKeyboardButton("⏹ Stop", "btn_stop")],
        [InlineKeyboardButton("🎯 Targets", "btn_targets"),
         InlineKeyboardButton("🖼 SetThumb (reply)", "btn_thumb")],
        [InlineKeyboardButton("❓ Help", "btn_help"), InlineKeyboardButton("⚙ Config", "btn_config")]
    ]
    return InlineKeyboardMarkup(kb)

def targets_kb():
    kb = []
    for t in TARGETS_POOL:
        label = f"{'✅' if t in config.get('targets', []) else '⬜'} {str(t)[-6:]}"
        kb.append([InlineKeyboardButton(label, f"toggle_{t}")])
    kb.append([InlineKeyboardButton("Done", "targets_done")])
    return InlineKeyboardMarkup(kb)

# ----------------------------
# Owner-only check decorator
def owner_only(func):
    async def wrapper(client, message):
        if not message.from_user or message.from_user.id != OWNER_ID:
            await message.reply_text("❌ You are not authorized to use this command.")
            return
        return await func(client, message)
    return wrapper

# ----------------------------
# Commands & callbacks
@bot.on_message(filters.command(["start","help"]))
@owner_only
async def cmd_help(client, message: Message):
    txt = (
        "**Auto Forward Bot V8.5 — Commands (Owner only)**\n\n"
        "`/forward` - Interactive: bot asks first link then last link\n"
        "`/linkforward <link1> <link2>` - Direct range forward\n"
        "`/pause` `/resume` `/stop` - Control running range task\n"
        "`/setthumb` - Reply to an image to set thumbnail\n"
        "`/setcaption <text>` - Change signature appended to captions\n"
        "`/addtarget -100id` / `/removetarget -100id` / `/listtargets`\n"
        "`/panel` - Open button panel\n\n"
        "To extract from channels where bot is not admin, set USER_SESSION env (StringSession) and restart bot.\n"
        "Make sure you have rights to repost content before using this bot."
    )
    await message.reply_text(txt, reply_markup=main_panel_kb())

@bot.on_message(filters.command("panel"))
@owner_only
async def cmd_panel(client, message: Message):
    await message.reply_text("Control Panel:", reply_markup=main_panel_kb())

@bot.on_callback_query()
async def on_cb(client, cq):
    if cq.from_user.id != OWNER_ID:
        await cq.answer("Not allowed", show_alert=True)
        return
    data = cq.data or ""
    if data == "btn_forward":
        await cq.answer()
        await cq.message.reply_text("Send /forward to start interactive forward (I will then ask for links).")
    elif data == "btn_linkforward":
        await cq.answer()
        await cq.message.reply_text("Usage: /linkforward <first_link> <last_link>")
    elif data == "btn_pause":
        controller["pause_event"].clear()
        await cq.answer("Paused")
    elif data == "btn_resume":
        controller["pause_event"].set()
        await cq.answer("Resumed")
    elif data == "btn_stop":
        controller["stop_flag"] = True
        task = controller.get("range_task")
        if task and not task.done():
            task.cancel()
        await cq.answer("Stop signaled")
    elif data == "btn_targets":
        await cq.answer()
        await cq.message.edit_text("Toggle targets:", reply_markup=targets_kb())
    elif data.startswith("toggle_"):
        tid = int(data.split("_",1)[1])
        cfg_targets = config.get("targets", [])
        if tid in cfg_targets:
            cfg_targets.remove(tid)
        else:
            cfg_targets.append(tid)
        config["targets"] = cfg_targets
        save_config(config)
        await cq.answer("Toggled")
        await cq.message.edit_text("Toggle targets:", reply_markup=targets_kb())
    elif data == "targets_done":
        await cq.answer("Saved targets")
        await cq.message.edit_text(f"Targets saved: `{config.get('targets',[])}`", reply_markup=main_panel_kb())
    elif data == "btn_thumb":
        await cq.answer()
        await cq.message.reply_text("Reply to an image with /setthumb to set new thumbnail.")
    elif data == "btn_help":
        await cq.answer()
        await cmd_help(client, cq.message)
    elif data == "btn_config":
        await cq.answer()
        await cq.message.reply_text(f"Config: delay={config.get('forward_delay')}, concurrency={config.get('concurrency')}", reply_markup=main_panel_kb())
    else:
        await cq.answer()

# set thumbnail
@bot.on_message(filters.command("setthumb") & filters.reply)
@owner_only
async def cmd_setthumb(client, message: Message):
    if not message.reply_to_message:
        await message.reply_text("Reply to an image message with /setthumb")
        return
    try:
        path = await message.reply_to_message.download(file_name="thumb.jpg")
        config["thumb"] = path
        save_config(config)
        await message.reply_text("Thumbnail updated.")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

# set caption signature
@bot.on_message(filters.command("setcaption"))
@owner_only
async def cmd_setcaption(client, message: Message):
    txt = " ".join(message.command[1:])
    if not txt:
        await message.reply_text("Usage: /setcaption <text>")
        return
    config["signature"] = txt
    save_config(config)
    await message.reply_text(f"Signature set to: {txt}")

# targets management
@bot.on_message(filters.command("addtarget"))
@owner_only
async def cmd_addtarget(client, message: Message):
    try:
        tid = int(message.command[1])
    except:
        await message.reply_text("Usage: /addtarget -1001234567890")
        return
    if tid in TARGETS_POOL:
        await message.reply_text("Already in pool.")
        return
    TARGETS_POOL.append(tid)
    cfg = config.get("targets", [])
    cfg.append(tid)
    config["targets"] = cfg
    save_config(config)
    await message.reply_text(f"Added {tid}")

@bot.on_message(filters.command("removetarget"))
@owner_only
async def cmd_removetarget(client, message: Message):
    try:
        tid = int(message.command[1])
    except:
        await message.reply_text("Usage: /removetarget -1001234567890")
        return
    if tid in TARGETS_POOL:
        TARGETS_POOL.remove(tid)
    cfg = config.get("targets", [])
    if tid in cfg:
        cfg.remove(tid)
    config["targets"] = cfg
    save_config(config)
    await message.reply_text(f"Removed {tid}")

@bot.on_message(filters.command("listtargets"))
@owner_only
async def cmd_listtargets(client, message: Message):
    await message.reply_text(f"Pool: `{TARGETS_POOL}`\nSelected: `{config.get('targets',[])}`")

# interactive forward: /forward starts the flow: bot asks first link then last link
@bot.on_message(filters.command("forward"))
@owner_only
async def cmd_forward_interactive(client, message: Message):
    uid = message.from_user.id
    controller["interactive_wait"][uid] = {"step": 1, "first": None}
    await message.reply_text("Send FIRST message link now (e.g. https://t.me/c/3240589036/340).")

# capture free-form messages containing links for interactive flow (owner only)
@bot.on_message(filters.text & filters.user(OWNER_ID))
async def interactive_listener(client, message: Message):
    uid = message.from_user.id
    if uid not in controller["interactive_wait"]:
        return  # not in interactive mode
    data = controller["interactive_wait"][uid]
    text = message.text.strip()
    if data["step"] == 1:
        first_id = parse_msgid_from_link(text)
        if not first_id:
            await message.reply_text("Couldn't parse first link. Send a link like https://t.me/c/3240589036/340")
            return
        data["first"] = first_id
        data["step"] = 2
        await message.reply_text(f"First id set to {first_id}. Now send LAST message link.")
        return
    if data["step"] == 2:
        last_id = parse_msgid_from_link(text)
        if not last_id:
            await message.reply_text("Couldn't parse last link. Send a link like https://t.me/c/3240589036/345")
            return
        first_id = data.get("first")
        if not first_id:
            await message.reply_text("First id missing, restart with /forward")
            controller["interactive_wait"].pop(uid, None)
            return
        # done: run range forward (ask for confirmation)
        await message.re