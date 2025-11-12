from pyrogram import Client, filters
import asyncio
import re
import os
import aiofiles

# -----------------------
# 🔹 VARIABLES (Pre-Filled)
API_ID = int(os.getenv("API_ID", 28420641))
API_HASH = os.getenv("API_HASH", "d1302d5039ae3275c4195b4fcc5ff1f9")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8592967336:AAGoj5zAzkPO9nHSFjHYHp7JclEq4Z7KKGg")
OWNER_ID = int(os.getenv("OWNER_ID", 8117462619))
SOURCE_CHANNEL = int(os.getenv("SOURCE_CHANNEL", -1003240589036))
TARGET_CHANNELS = list(map(int, os.getenv("TARGET_CHANNELS", "-1003216068164").split()))
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", 1.5))
CUSTOM_THUMB = "thumb.jpg"

# 🔧 Text Replacement Patterns
REMOVE_TEXTS = [
    "Extracted by➤@YTBR_67",
    "Extracted By ➤ Join-@skillwithgaurav",
    "Extracted By ➤ Gaurav RaJput",
    "Extracted By ➤ Gaurav",
    "@skillwithgaurav", "@kamdev5x", "@skillzoneu"
]
OLD_WEBSITE = r"𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\nhttps?://[^\s]+"
NEW_WEBSITE = "𝚆𝚎𝚋𝚜𝚒𝚝𝚎 👇🥵\nhttps://bio.link/manmohak"
NEW_SIGNATURE = "Extracted by➤@course_wale"
# -----------------------

app = Client("auto-forward-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# 🧠 Utility: Clean Caption
def clean_caption(caption):
    if not caption:
        return NEW_SIGNATURE
    text = caption
    for bad_text in REMOVE_TEXTS:
        text = re.sub(re.escape(bad_text), "", text, flags=re.IGNORECASE)
    text = re.sub(OLD_WEBSITE, NEW_WEBSITE, text, flags=re.IGNORECASE)
    text = text.strip()
    return f"{text}\n\n{NEW_SIGNATURE}"


# 🎬 Forward from Source to Targets
@app.on_message(filters.chat(SOURCE_CHANNEL))
async def auto_forward(client, message):
    caption = clean_caption(message.caption)
    for target in TARGET_CHANNELS:
        try:
            if message.video:
                await client.send_video(
                    chat_id=target,
                    video=message.video.file_id,
                    caption=caption,
                    thumb=CUSTOM_THUMB
                )
            elif message.photo:
                await client.send_photo(chat_id=target, photo=message.photo.file_id, caption=caption)
            elif message.document:
                await client.send_document(chat_id=target, document=message.document.file_id, caption=caption)
            else:
                await message.copy(chat_id=target, caption=caption)
            await asyncio.sleep(FORWARD_DELAY)
        except Exception as e:
            print(f"❌ Error forwarding to {target}: {e}")


# 🧠 /status Command
@app.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status(_, msg):
    await msg.reply_text(
        f"✅ **Bot Running Successfully!**\n\n"
        f"📤 Source: `{SOURCE_CHANNEL}`\n"
        f"🎯 Targets: `{TARGET_CHANNELS}`\n"
        f"⚙️ Delay: `{FORWARD_DELAY}s`\n"
        f"🖼 Thumbnail: `{CUSTOM_THUMB}`"
    )


# 🧠 /addtarget Command
@app.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def add_target(_, msg):
    global TARGET_CHANNELS
    try:
        new_id = int(msg.command[1])
        if new_id not in TARGET_CHANNELS:
            TARGET_CHANNELS.append(new_id)
            await msg.reply_text(f"✅ Added new target: `{new_id}`")
        else:
            await msg.reply_text("⚠️ Already exists.")
    except:
        await msg.reply_text("Usage: `/addtarget -100xxxxxxxxx`")


# 🧠 /setthumb Command
@app.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def set_thumb(_, msg):
    global CUSTOM_THUMB
    if msg.photo:
        file_path = await msg.download(file_name="thumb.jpg")
        CUSTOM_THUMB = file_path
        await msg.reply_text("🖼 Thumbnail updated successfully!")
    else:
        await msg.reply_text("⚠️ Reply to a photo with `/setthumb` to update thumbnail.")


# 🧠 /range Command → Forward using first and last link
@app.on_message(filters.user(OWNER_ID) & filters.command("range"))
async def forward_range(client, message):
    if len(message.command) < 3:
        await message.reply_text("⚠️ Usage: `/range first_link last_link`")
        return

    try:
        first_link, last_link = message.command[1], message.command[2]
        start_id = int(first_link.split("/")[-1])
        end_id = int(last_link.split("/")[-1])
        source_chat_id = int(first_link.split("/c/")[1].split("/")[0])
        await message.reply_text(f"🔄 Forwarding from `{start_id}` to `{end_id}`...")

        for msg_id in range(start_id, end_id + 1):
            try:
                msg = await client.get_messages(source_chat_id, msg_id)
                caption = clean_caption(msg.caption)
                for target in TARGET_CHANNELS:
                    try:
                        await msg.copy(chat_id=target, caption=caption)
                        await asyncio.sleep(FORWARD_DELAY / 3)  # ⚡ Speed boost
                    except Exception as e:
                        print(f"Error forwarding to {target}: {e}")
            except Exception as e:
                print(f"Error fetching message {msg_id}: {e}")

        await message.reply_text("✅ Forwarding completed successfully!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


print("🚀 Auto Forward Bot V8.5 (Hybrid) started successfully!")
app.run()