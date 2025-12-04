from db.postgres import Postgres
from python.video.video import Video
from redis import Redis
import time

def get_next_video_request(db_util: Postgres) -> Video:
    query = "SELECT video_id, user_id, start_time, end_time, lon, lat, min_lon, \
        max_lon, min_lat, max_lat, status \
        FROM video \
        WHERE status = 0 \
        ORDER BY created_at ASC \
        LIMIT 1000000;"
    try:
        db_util.cur.execute(query)
        db_util.conn.commit()
        while (record := db_util.cur.fetchone()) != None:
            yield Video(*record)
    except Exception as ex:
        raise ex


import redis

db_util = Postgres()

r = redis.Redis(host='localhost', port=6379, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")

while True:
    request_iter = get_next_video_request(db_util)

    for video in request_iter:
        if not r.exists(str(video.message.video_id)):
            r.lpush('videos', video.message.SerializeToString())
            r.set(str(video.message.video_id), 0)
    time.sleep(1)
        # if r.get(str(video.message.video_id)) == 1:
        #     ##message currently being processed, so will not reload queue
        #     pass
        #
        # if r.get(str(video.message.video_id)) == 2:
        #     ##message did not process successfully, so reload queue
        #     r.lpush('videos', video.message.SerializeToString())
        #     r.set(str(video.message.video_id), 0)







# import redis
#
# # Connect to Redis
# r = redis.Redis(host='localhost', port=6379, decode_responses=True, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")
#
# # Set a key-value pair
# r.set('mykey', 'myvalue')
#
# # Get the value
# value = r.get('mykey')
# print(f"Retrieved value: {value}")
#
# # Delete the key
# r.delete('mykey')


