import os
from mega import Mega

DOWNLOAD_DIR = "downloads"

def download_mega(link):

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    mega = Mega()
    m = mega.login()

    file = m.download_url(link, DOWNLOAD_DIR)

    return file
