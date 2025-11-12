

from pyrogram import Client, filters
import asyncio
import re
import os

# -----------------------
# 🔹 VARIABLES (Pre-Filled)
API_ID = 28420641
API_HASH = "d1302d5039ae3275c4195b4fcc5ff1f9"
BOT_TOKEN = "8592967336:AAGoj5zAzkPO9nHSFjHYHp7JclEq4Z7KKGg"  # ⚠️ Yahan apna working bot token daalna
SOURCE_CHANNEL = -1003240589036
TARGET_CHANNELS = [-1003216068164]  # Add more IDs if needed
OWNER_ID = 8117462619
CUSTOM_THUMB = "thumb.jpg"
FORWARD_DELAY = 1.5

# 🔧 Auto Text Replacement
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


# 📦 Utility Functions
def clean_caption(caption):
    if not caption:
        return NEW_SIGNATURE
    text = caption
    for bad_text in REMOVE_TEXTS:
        text = re.sub(re.escape(bad_text), "", text, flags=re.IGNORECASE)
    text = re.sub(OLD_WEBSITE, NEW_WEBSITE, text, flags=re.IGNORECASE)
    text = text.strip()
    return f"{text}\n\n{NEW_SIGNATURE}"


# 🎬 Auto Forward Function
@app.on_message(filters.chat(SOURCE_CHANNEL))
async def forward_to_targets(client, message):
    for target in TARGET_CHANNELS:
        try:
            caption = clean_caption(message.caption)
            if message.video:
                await client.send_video(
                    chat_id=target,
                    video=message.video.file_id,
                    caption=caption,
                    thumb=CUSTOM_THUMB
                )
            else:
                await message.copy(chat_id=target, caption=caption)
            await asyncio.sleep(FORWARD_DELAY)
        except Exception as e:
            print(f"❌ Error forwarding to {target}: {e}")


# 🧠 Control Commands (Only for Owner)
@app.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status(client, message):
    await message.reply_text(
        f"✅ **Bot Status:** Running\n"
        f"📤 Source: `{SOURCE_CHANNEL}`\n"
        f"🎯 Targets: `{TARGET_CHANNELS}`\n"
        f"⏱ Delay: `{FORWARD_DELAY}s`\n"
        f"🖼 Thumbnail: `{CUSTOM_THUMB}`"
    )


@app.on_message(filters.user(OWNER_ID) & filters.command("setcaption"))
async def set_caption(client, message):
    global NEW_SIGNATURE
    text = " ".join(message.command[1:])
    if text:
        NEW_SIGNATURE = text
        await message.reply_text(f"✅ Caption Updated to:\n`{NEW_SIGNATURE}`")
    else:
        await message.reply_text("⚠️ Usage: `/setcaption Extracted by➤@YourName`")


@app.on_message(filters.user(OWNER_ID) & filters.command("addtarget"))
async def add_target(client, message):
    global TARGET_CHANNELS
    try:
        new_id = int(message.command[1])
        if new_id not in TARGET_CHANNELS:
            TARGET_CHANNELS.append(new_id)
            await message.reply_text(f"✅ Added new target channel: `{new_id}`")
        else:
            await message.reply_text("⚠️ Already in list.")
    except:
        await message.reply_text("⚠️ Usage: `/addtarget -100xxxxxxxxx`")


@app.on_message(filters.user(OWNER_ID) & filters.command("removetarget"))
async def remove_target(client, message):
    global TARGET_CHANNELS
    try:
        rem_id = int(message.command[1])
        if rem_id in TARGET_CHANNELS:
            TARGET_CHANNELS.remove(rem_id)
            await message.reply_text(f"🗑 Removed target channel: `{rem_id}`")
        else:
            await message.reply_text("⚠️ ID not found in list.")
    except:
        await message.reply_text("⚠️ Usage: `/removetarget -100xxxxxxxxx`")


@app.on_message(filters.user(OWNER_ID) & filters.command("setthumb"))
async def set_thumb(client, message):
    global CUSTOM_THUMB
    if message.photo:
        file_path = await message.download(file_name="thumb.jpg")
        CUSTOM_THUMB = file_path
        await message.reply_text("🖼 Thumbnail updated successfully!")
    else:
        await message.reply_text("⚠️ Reply to a photo with `/setthumb` to update thumbnail.")


# 🚀 Start
print("🚀 Final Auto Forward Bot V4 (Control + Clean Caption + Website Replace + Thumbnail) Started...")
app.run()