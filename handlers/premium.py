from telegram import Update
from telegram.ext import ContextTypes
from models.db import activate_premium

async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if len(context.args) < 2:
        await update.message.reply_text("/premium days quota_gb")
        return

    days = int(context.args[0])
    quota = int(context.args[1])

    activate_premium(update.effective_user.id, days, quota)

    await update.message.reply_text("Premium activated.")
