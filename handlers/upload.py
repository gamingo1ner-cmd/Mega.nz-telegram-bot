import os
from telegram import Update
from telegram.ext import ContextTypes

DOWNLOAD_DIR = "downloads"

async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document or update.message.video or update.message.audio

    if not file:
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    tg_file = await file.get_file()
    path = os.path.join(DOWNLOAD_DIR, file.file_name)

    await tg_file.download_to_drive(path)

    await update.message.reply_text(f"Uploaded and stored: {file.file_name}")
