from telegram import Update
from telegram.ext import ContextTypes
from models.db import get_stats

ADMIN_ID = 123456789

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.effective_user.id != ADMIN_ID:
        return

    stats = get_stats()

    await update.message.reply_text(str(stats))
