import requests
import pystac
import planetary_computer
import rioxarray
import time
from datetime import datetime
from old.source.db.postgres import Postgres
import redis
from old.source.video.video import Video
import time
from datetime import datetime
import pyproj
import numpy as np
from io import BytesIO

db_util = Postgres()
redis_queue = redis.Redis(host='cache', port=6379, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")

while True:
    try:
        video = Video.from_bytes(redis_queue.lpop('videos-assembly-ready'))
        frames = video.get_frames(db_util)
        frames_list = []
        for frame in frames:
            ds = np.frombuffer(frame.message.image_data, dtype=np.float64)
            ds = ds.reshape(
                frame.message.img_height, frame.message.img_width, 3
            )
            frames_list.append(ds)
        video.assemble_frames(frames_list)
        # time.sleep(1)


    except Exception as ex:
        # time.sleep(1)
        print(ex)
        # time.sleep(1)