from old.source.db.postgres import Postgres
from old.source.redis_queue.redis import Redis
import redis
from old.source.video.video import Video, VideoState
from old.source.frame.frame import Frame
from old.source.frame.frame import get_items
import time
from datetime import datetime
import base64
import psycopg2
import numpy as np

db_util = Postgres()
redis_util = Redis()

while True:
    try:
        video = Video.from_bytes(redis_util.redis.lpop(VideoState.VIDEO_STATE_NEW_REQUEST.name))
        frames = get_items(video.message.start, video.message.end, video.bbox)
        for frame in frames:
            if frame.properties["eo:cloud_cover"] < .5:
                collection_time = frame.properties['datetime']
                frame_item_id = frame.id
                frame_href = ""
                for link in frame.links:
                    if link.rel == 'self':
                        frame_href = link.href
                        break
                unsigned_blue_href = frame.assets['B02'].href
                unsigned_green_href = frame.assets["B03"].href
                unsigned_red_href = frame.assets["B04"].href
                min_lon, min_lat, max_lon, max_lat = frame.bbox
                geometry = frame.geometry
                status = 0
                frame_id=-1
                user_id = 0
                image_data = np.empty(1).tobytes()
                datetime_object = datetime.fromisoformat(collection_time.replace('Z', '+00:00'))
                img_width = 0
                img_height = 0
                frame_image = Frame(frame_id, video.message.video_id, frame_item_id, frame_href, unsigned_blue_href,
                                    unsigned_green_href, unsigned_red_href, min_lon, max_lon, min_lat, max_lat,
                                    datetime_object, memoryview(image_data), img_width, img_height, status)
                frame_image.new_request(db_util)
                # time.sleep(1)

    except Exception as ex:
        # time.sleep(1)
        print(ex)