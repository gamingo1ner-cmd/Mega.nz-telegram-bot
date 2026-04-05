import redis
import json

r = redis.Redis(host="localhost", port=6379, db=0)

QUEUE = "download_queue"

def push_job(link, user):

    job = {
        "link": link,
        "user": user
    }

    r.lpush(QUEUE, json.dumps(job))


def get_job():

    job = r.rpop(QUEUE)

    if not job:
        return None

    return json.loads(job)
