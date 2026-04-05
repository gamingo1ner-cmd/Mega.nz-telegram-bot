import time
from redis_queue import get_job
from mega_downloader import download_mega
from services.analytics_service import log_download

def worker_loop():
    print("Worker started")

    while True:
        job = get_job()

        if not job:
            time.sleep(2)
            continue

        link = job["link"]
        user = job["user"]

        try:
            path = download_mega(link)
            log_download()
            print(f"Downloaded for user {user}: {path}")

        except Exception as e:
            print("Worker error:", e)


if __name__ == "__main__":
    worker_loop()
