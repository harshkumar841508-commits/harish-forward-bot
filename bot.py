from pyrogram import Client, filters
import asyncio
import re

# -----------------------
# 🔹 VARIABLES
API_ID = 28420641
API_HASH = "d1302d5039ae3275c4195b4fcc5ff1f9"
BOT_TOKEN = "8466954877:AAE2sAqA1gWEX9AA4fj6J0W_PeGk-PON7obk"  # ⚠️ Apna token lagao agar naya ho
SOURCE_CHANNEL = -1003209816876
TARGET_CHANNELS = [-1002929317490]
OWNER_ID = 8117462619
CUSTOM_THUMB = "thumb.jpg"
FORWARD_DELAY = 1.5

# 👇 Replace/remove text and add new line
REMOVE_TEXT = "Extracted by➤@YTBR_67"
NEW_SIGNATURE = "Extracted by➤@course_wale"
# -----------------------

app = Client("auto-forward-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_message(filters.chat(SOURCE_CHANNEL))
async def forward_to_targets(client, message):
    for target in TARGET_CHANNELS:
        try:
            # 🧠 Step 1: Get original caption
            caption = message.caption or ""

            # 🚫 Step 2: Remove unwanted text (case-insensitive)
            cleaned_caption = re.sub(re.escape(REMOVE_TEXT), "", caption, flags=re.IGNORECASE).strip()

            # ✨ Step 3: Add your new signature line
            final_caption = (cleaned_caption + "\n\n" + NEW_SIGNATURE).strip()

            # 🎬 Step 4: Forward video with thumbnail
            if message.video:
                await client.send_video(
                    chat_id=target,
                    video=message.video.file_id,
                    caption=final_caption,
                    thumb=CUSTOM_THUMB
                )
            else:
                await message.copy(chat_id=target, caption=final_caption)

            await asyncio.sleep(FORWARD_DELAY)

        except Exception as e:
            print(f"❌ Error forwarding to {target}: {e}")


@app.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status(client, message):
    await message.reply_text(
        f"✅ Bot Running\nSource: {SOURCE_CHANNEL}\nTargets: {TARGET_CHANNELS}\nDelay: {FORWARD_DELAY}s"
    )


print("🚀 Auto Forward Bot (Clean Caption + Custom Signature + Thumbnail) Started...")
app.run()