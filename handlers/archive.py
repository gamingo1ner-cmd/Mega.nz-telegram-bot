import zipfile
import os
from telegram import Update
from telegram.ext import ContextTypes

async def unzip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /unzip filename.zip")
        return

    filename = context.args[0]
    path = f"downloads/{filename}"

    if not os.path.exists(path):
        await update.message.reply_text("Zip not found")
        return

    extract_dir = f"downloads/{filename}_extracted"

    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    await update.message.reply_text(f"Extracted to {extract_dir}")
