import requests
import pystac
import planetary_computer
import rioxarray
import time
from datetime import datetime
from source.db.postgres import Postgres
import redis
from source.video.video import Video
import time
from datetime import datetime
import pyproj
import numpy as np
from io import BytesIO

db_util = Postgres()
redis_queue = redis.Redis(host='localhost', port=6379, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")

while True:
    try:
        video = Video.from_bytes(redis_queue.lpop('videos-assembly-ready'))
        for frame in video.get_frames(db_util):
            print(frame)


    except Exception as ex:
        time.sleep(1)
        print(ex)