import subprocess
import os

def generate_thumbnail(video_path):

    thumb = video_path + ".jpg"

    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-ss", "00:00:03",
        "-vframes", "1",
        thumb
    ]

    subprocess.run(cmd)

    return thumb


def create_hls(video):

    out_dir = video + "_hls"

    os.makedirs(out_dir, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-i", video,
        "-codec:", "copy",
        "-start_number", "0",
        "-hls_time", "10",
        "-hls_list_size", "0",
        "-f", "hls",
        f"{out_dir}/playlist.m3u8"
    ]

    subprocess.run(cmd)

    return out_dir
