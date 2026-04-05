import os
from telegram import Update
from telegram.ext import ContextTypes

DOWNLOAD_DIR = "downloads"

async def download_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /download filename")
        return

    filename = context.args[0]
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    if not os.path.exists(filepath):
        await update.message.reply_text("File not found.")
        return

    await update.message.reply_document(document=open(filepath, "rb"))
