import sqlite3
from datetime import datetime, timedelta

DB = "bot.db"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        premium_until TEXT,
        quota INTEGER
    )
    """)

    conn.commit()
    conn.close()


def activate_premium(user_id, days, quota):

    conn = sqlite3.connect(DB)
    c = conn.cursor()

    expiry = datetime.now() + timedelta(days=days)

    c.execute("""
    INSERT OR REPLACE INTO users(user_id,premium_until,quota)
    VALUES(?,?,?)
    """,(user_id,expiry,quota))

    conn.commit()
    conn.close()


def get_stats():

    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM users")

    total_users = c.fetchone()[0]

    return {"users":total_users}
