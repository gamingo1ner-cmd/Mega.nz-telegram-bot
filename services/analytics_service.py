stats = {
    "uploads":0,
    "downloads":0
}

def log_upload():
    stats["uploads"] += 1

def log_download():
    stats["downloads"] += 1

def get_stats():
    return stats
