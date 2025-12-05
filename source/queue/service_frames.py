from source.db.postgres import Postgres
from source.frame.frame import Frame
from redis import Redis
import time

def get_next_frame_request(db_util: Postgres) -> Frame:
    query = "SELECT frame_id, video_id, frame_item_id, frame_item_href, \
            blue_href, green_href, red_href, min_lon, max_lon, \
            min_lat, max_lat, collection_time_utc, image_data, status \
        FROM frame \
        WHERE status = 0 \
        ORDER BY created_at ASC \
        LIMIT 1000000;"
    try:
        db_util.cur.execute(query)
        db_util.conn.commit()
        while (record := db_util.cur.fetchone()) != None:
            yield Frame(*record)
    except Exception as ex:
        raise ex


import redis

db_util = Postgres()

r = redis.Redis(host='localhost', port=6379, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")

while True:
    request_iter = get_next_frame_request(db_util)

    for frame in request_iter:
        if not r.exists("frame-"+str(frame.message.frame_id)):
            r.lpush('frames', frame.message.SerializeToString())
            r.set("frame-"+str(frame.message.frame_id), 0)
    time.sleep(1)