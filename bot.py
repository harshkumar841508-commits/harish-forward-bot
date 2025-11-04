from pyrogram import Client, filters
import asyncio

# -----------------------
# 🔹 ADD YOUR VARIABLES HERE
API_ID = 28420641
API_HASH = "d1302d5039ae3275c4195b4fcc5ff1f9"
BOT_TOKEN = "7571653761:AAGyrPg20_oztjwX8soLIfn87narBIOvuQI"  # ⚠️ apna naya token yahan daalna
SOURCE_CHANNEL = -1003146528259
TARGET_CHANNELS = [-1002848385970]
OWNER_ID = 8117462619
DEFAULT_CAPTION = "🔥 Shared by @course_wale"
FORWARD_DELAY = 1.5
# -----------------------

app = Client("auto-forward-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_message(filters.chat(SOURCE_CHANNEL))
async def forward_to_targets(client, message):
    for target in TARGET_CHANNELS:
        try:
            await message.copy(chat_id=target, caption=DEFAULT_CAPTION or message.caption)
            await asyncio.sleep(FORWARD_DELAY)
        except Exception as e:
            print(f"❌ Error forwarding to {target}: {e}")

@app.on_message(filters.user(OWNER_ID) & filters.command("status"))
async def status(client, message):
    await message.reply_text(
        f"✅ Bot Running\nSource: {SOURCE_CHANNEL}\nTargets: {TARGET_CHANNELS}\nDelay: {FORWARD_DELAY}s"
    )

print("🚀 Auto Forward Bot Started...")
app.run()
