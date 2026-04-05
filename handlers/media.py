import os
from telegram import Update
from telegram.ext import ContextTypes

VIDEO_EXT = (".mp4", ".mkv", ".mov")

async def stream_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /media filename")
        return

    filename = context.args[0]

    if not filename.endswith(VIDEO_EXT):
        await update.message.reply_text("Not a supported video.")
        return

    path = f"downloads/{filename}"

    if not os.path.exists(path):
        await update.message.reply_text("File not found")
        return

    await update.message.reply_video(video=open(path, "rb"), supports_streaming=True)
