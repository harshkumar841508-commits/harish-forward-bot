# bot.py - V12 Ultra Max (with your variables prefilled)
import os
import json
import time
import asyncio
import random
from pathlib import Path
from typing import Optional, Dict, Any, List

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

# -----------------------
# --- YOUR PROVIDED DEFAULTS (can be overridden by env vars on Heroku)
# -----------------------
# If you prefer env-only, remove the values below and set Config Vars in Heroku.
DEFAULT_API_ID = int(os.getenv("API_ID", "24916176"))
DEFAULT_API_HASH = os.getenv("API_HASH", "15e8847a5d612831b6a42c5f8d846a8a")
DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN", "8226478770:AAH1Zz63qXkdD_jD2n-5xeO4ZfoRKPL6uKk")
DEFAULT_OWNER_ID = int(os.getenv("OWNER_ID", "1251826930"))
DEFAULT_SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", "-1003175017722"))
# You can provide many target channels comma-separated
DEFAULT_TARGETS_ENV = os.getenv("TARGET_CHANNELS", "-1003428767711")
DEFAULT_TARGETS = [int(x.strip()) for x in DEFAULT_TARGETS_ENV.split(",") if x.strip()]

# If you have a user session string and want private channel reading via user session,
# set USER_SESSION env var OR put your session string below (unsafe if posted publicly).
DEFAULT_USER_SESSION = os.getenv("USER_SESSION", "BQF8MNAAhbMoovGajQnCBIFyLI32AMSA8MFuEgHTNUiobuJb9jP5_GZnuqc75Bws4GMpFzoGDGH8ykeXRL-ieoxskpmslTT0fGu82K1Fc0pl9HpPgTplcZAN5Vz1KprigbcT6uEobAtfF3QWBdmbhaFtPyZUGripqHzH6WHQKvfjEc0B2P3xqfZoFipqBA6jpdcWnvMeAWkN7RIWWP3lflhTK7lGa3ROdf0nJ7ZQG-rlPosG4CZbL72xteLBvECKR2p-O6fEdQ7iCHz0omte-PWdnWbW8HQAv-vWVqq5A_LDIs8RhPyfc4iSvRjNejpwaKaD_Gq1pVQe3lSZuFirhTpZylBK4gAAAABKnVzyAA")

# runtime/tuning
DEFAULT_DELAY = float(os.getenv("DEFAULT_DELAY", "1.5"))
DEFAULT_CONCURRENCY = int(os.getenv("DEFAULT_CONCURRENCY", "2"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "4"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "1500"))

# state file
STATE_FILE = Path("v12_state.json")

# -----------------------
# read final runtime config (env overrides defaults)
# -----------------------
API_ID = int(os.getenv("API_ID", DEFAULT_API_ID))
API_HASH = os.getenv("API_HASH", DEFAULT_API_HASH)
BOT_TOKEN = os.getenv("BOT_TOKEN", DEFAULT_BOT_TOKEN)
OWNER_ID = int(os.getenv("OWNER_ID", DEFAULT_OWNER_ID))
SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", DEFAULT_SOURCE_CHANNEL))
TARGETS = [int(x.strip()) for x in os.getenv("TARGET_CHANNELS", ",".join(map(str, DEFAULT_TARGETS))).split(",") if x.strip()]

USER_SESSION = os.getenv("USER_SESSION", DEFAULT_USER_SESSION).strip()

# -----------------------
# state helpers
# -----------------------
def load_state():
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def save_state(s):
    try:
        STATE_FILE.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print("save_state error:", e)

state = load_state()
metrics = {"forwards": 0, "fails": 0, "retries": 0, "active_tasks": 0}
interactive: Dict[int, Dict[str, Any]] = {}

# -----------------------
# Validate token & start clients
# -----------------------
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN not configured. Set BOT_TOKEN env var or put it in file (not recommended).")

bot = Client("v12_ultra_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

user_client: Optional[Client] = None
if USER_SESSION:
    try:
        user_client = Client("user_session", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION)
        user_client.start()
        print("User session started — private channel reading enabled.")
    except Exception as e:
        print("Failed to start user session:", e)
        user_client = None
else:
    print("No USER_SESSION provided — bot-only access (private channels require bot membership).")

# -----------------------
# util: parse t.me links
# -----------------------
def parse_msg_link(link: str) -> Optional[Dict[str, Any]]:
    try:
        l = link.strip()
        l = l.split("?")[0].split("#")[0]
        parts = [p for p in l.split("/") if p]
        if not parts:
            return None
        # If domain mention present
        if "t.me" in parts or "telegram.me" in parts:
            nonempty = parts
            if "c" in nonempty:
                try:
                    idx = nonempty.index("c")
                    chatnum = nonempty[idx + 1]
                    msgid = nonempty[idx + 2]
                    if chatnum.isdigit():
                        return {"chat_id": int(f"-100{chatnum}"), "msg_id": int(msgid)}
                except Exception:
                    return None
            else:
                if len(nonempty) >= 2:
                    username = nonempty[-2]
                    msgid = nonempty[-1]
                    return {"chat_username": username, "msg_id": int(msgid)}
        # fallback: /<something>/<id>
        if len(parts) >= 2 and parts[-1].isdigit():
            return {"chat_username": parts[-2], "msg_id": int(parts[-1])}
    except Exception:
        return None

# -----------------------
# adaptive wait
# -----------------------
_last_send_time: Dict[int, float] = {}
async def adaptive_wait_for_target(target: int, min_interval: float):
    last = _last_send_time.get(target, 0)
    elapsed = time.time() - last
    if elapsed < min_interval:
        await asyncio.sleep(min_interval - elapsed)
    _last_send_time[target] = time.time()

# -----------------------
# send with retry
# -----------------------
async def send_with_retry(client_for_send: Client, target: int, src_msg: Message, caption: str) -> bool:
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            # try fast copy
            try:
                await src_msg.copy(chat_id=target, caption=caption)
                metrics["forwards"] += 1
                return True
            except Exception:
                pass
            # fallback: download then upload
            if src_msg.document or src_msg.video or src_msg.photo:
                path = await src_msg.download()
                try:
                    await client_for_send.send_document(chat_id=target, document=path, caption=caption)
                finally:
                    try: Path(path).unlink()
                    except: pass
                metrics["forwards"] += 1
                return True
            # fallback: send as text
            text = src_msg.text or src_msg.caption or ""
            if text:
                await client_for_send.send_message(chat_id=target, text=text)
                metrics["forwards"] += 1
                return True
            return False
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

# -----------------------
# forward to targets wrapper
# -----------------------
async def forward_to_targets(src_msg: Message, caption: str, targets: List[int], concurrency: int, delay: float, client_for_send: Client):
    sem = asyncio.Semaphore(max(1, concurrency))
    results = {}
    async def _send_one(tid):
        async with sem:
            await adaptive_wait_for_target(tid, delay)
            ok = await send_with_retry(client_for_send, int(tid), src_msg, caption)
            results[int(tid)] = bool(ok)
            await asyncio.sleep(0.2)
    tasks = [asyncio.create_task(_send_one(t)) for t in targets]
    await asyncio.gather(*tasks, return_exceptions=True)
    return results

# -----------------------
# fetch source message (use user_client if available)
# -----------------------
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

# -----------------------
# Commands
# -----------------------
@bot.on_message(filters.private & filters.command("start"))
async def start_cmd(c: Client, m: Message):
    await m.reply_text("V12 Ultra Max bot ✅\nUse /help for commands.", quote=True)

@bot.on_message(filters.private & filters.command("help"))
async def help_cmd(c: Client, m: Message):
    txt = (
        "Commands (interactive):\n"
        "/start - start bot\n"
        "/help - this message\n"
        "/forward - interactive: I'll ask FIRST link then LAST link\n"
        "/setsource <id> - owner only set source channel id\n        /targets - show configured targets\n    "
    )
    await m.reply_text(txt, quote=True)

@bot.on_message(filters.private & filters.command("targets"))
async def targets_cmd(c: Client, m: Message):
    await m.reply_text(f"Targets: {TARGETS}", quote=True)

@bot.on_message(filters.private & filters.command("setsource"))
async def setsource_cmd(c: Client, m: Message):
    if not m.from_user or m.from_user.id != DEFAULT_OWNER_ID:
        await m.reply_text("Only owner can change source.", quote=True)
        return
    parts = m.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await m.reply_text("Usage: /setsource -1001234567890", quote=True)
        return
    try:
        v = int(parts[1].strip())
        global SOURCE_CHANNEL
        SOURCE_CHANNEL = v
        await m.reply_text(f"Source channel set to {v}. Update Heroku Config Var SOURCE_CHANNEL to persist.", quote=True)
    except Exception:
        await m.reply_text("Invalid id.", quote=True)

# start forward interactive
@bot.on_message(filters.private & filters.command("forward"))
async def forward_start(c: Client, m: Message):
    uid = m.from_user.id
    interactive[uid] = {"stage": "ask_first"}
    await m.reply_text("Send FIRST link (from source channel).", quote=True)

# listener to capture links during interactive flow
@bot.on_message(filters.private & ~filters.command(["start","help","forward","setsource","targets"]))
async def interactive_listener(c: Client, m: Message):
    uid = m.from_user.id
    if uid not in interactive:
        return
    data = interactive[uid]
    stage = data.get("stage")
    txt = (m.text or "").strip()
    parsed = parse_msg_link(txt)
    if not parsed:
        await m.reply_text("Couldn't parse link. Send a t.me link like https://t.me/c/3357986784/2", quote=True)
        return
    # ensure source channel match (optional)
    # If parsed uses chat_id, verify it equals SOURCE_CHANNEL or allow it if owner.
    if "chat_id" in parsed and parsed["chat_id"] != SOURCE_CHANNEL and m.from_user.id != DEFAULT_OWNER_ID:
        # let owner bypass; otherwise require same source channel
        await m.reply_text(f"Link chat_id {parsed['chat_id']} doesn't match configured source {SOURCE_CHANNEL}. Use the configured source channel or owner can override.", quote=True)
        return

    if stage == "ask_first":
        data["first"] = parsed
        data["stage"] = "ask_last"
        await m.reply_text("FIRST saved. Now send LAST link.", quote=True)
        return
    if stage == "ask_last":
        data["last"] = parsed
        data["stage"] = "working"
        await m.reply_text("LAST saved. Starting forward task...", quote=True)
        asyncio.create_task(do_forward_range(m, data, uid))
        return

# -----------------------
# worker do_forward_range
# -----------------------
async def do_forward_range(origin_msg: Message, data: Dict[str, Any], starter_uid: int):
    metrics["active_tasks"] += 1
    try:
        first = data.get("first")
        last = data.get("last")
        if not first or not last:
            await origin_msg.reply_text("Missing links. Aborting.", quote=True)
            interactive.pop(starter_uid, None)
            return

        # determine chat id (use chat_id if present)
        chat_identifier = first.get("chat_id") or first.get("chat_username")
        start_mid = int(first["msg_id"])
        end_mid = int(last["msg_id"])
        if start_mid > end_mid:
            start_mid, end_mid = end_mid, start_mid

        targets = TARGETS.copy()
        if not targets:
            await origin_msg.reply_text("No targets configured (TARGET_CHANNELS).", quote=True)
            interactive.pop(starter_uid, None)
            return

        sending_client = bot  # can switch to user_client if you want to send via user account
        total = end_mid - start_mid + 1
        sent_total = 0
        fail_total = 0

        progress = await origin_msg.reply_text(f"Starting forward {start_mid} → {end_mid} (total {total}) to {len(targets)} targets.", quote=True)

        # fetch each message and forward
        for mid in range(start_mid, end_mid + 1):
            src = await fetch_source_message(first, mid)
            if not src:
                # message missing — skip
                await asyncio.sleep(0.1)
                continue

            caption = src.caption or src.text or ""
            results = await forward_to_targets(src, caption, targets, DEFAULT_CONCURRENCY, DEFAULT_DELAY, client_for_send=sending_client)

            ok = sum(1 for v in results.values() if v)
            fail = sum(1 for v in results.values() if not v)
            sent_total += ok
            fail_total += fail

            try:
                pct = int((mid - start_mid + 1) / total * 100)
                await progress.edit_text(f"Forwarded {mid} ({mid-start_mid+1}/{total}) — {pct}% — Success:{ok} Fail:{fail}\nTotalSent:{sent_total} TotalFail:{fail_total}")
            except Exception:
                pass

            await asyncio.sleep(DEFAULT_DELAY)

        try:
            await progress.edit_text(f"✅ Completed. Sent approx {sent_total}/{total} Fail:{fail_total}")
        except Exception:
            pass

    except Exception as e:
        print("do_forward_range error:", e)
        try:
            await origin_msg.reply_text(f"Error: {e}", quote=True)
        except:
            pass
    finally:
        interactive.pop(starter_uid, None)
        metrics["active_tasks"] -= 1

# -----------------------
# start
# -----------------------
if __name__ == "__main__":
    print("Starting V12 Ultra Max bot...")
    bot.run()