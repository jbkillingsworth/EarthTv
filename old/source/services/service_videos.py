from old.source.db.postgres import Postgres
from old.source.video.video import Video, VideoState
from old.source.redis_queue.redis import Redis
import time
from enum import Enum

db_util = Postgres()
redis_util = Redis()

class RedisUpdateError(Exception):
    """Raised when Redis fails to update """
    pass

def video_is_state_new_request(video: Video):
    if video.message.status == 0:
        return True
    else:
        return False

def video_state_new_request_exists_in_queue(video: Video):
    if redis_util.redis.exists(VideoState.VIDEO_STATE_NEW_REQUEST.name + "-" + str(video.message.video_id)):
        return True
    else:
        return False

def video_state_new_request_add_to_queue(video: Video, redis_util: Redis):
    try:
        redis_util.redis.lpush(VideoState.VIDEO_STATE_NEW_REQUEST.name, video.message.SerializeToString())
        redis_util.redis.set(VideoState.VIDEO_STATE_NEW_REQUEST.name + "-" + str(video.message.video_id), 0)
    except Exception as e:
        raise RedisUpdateError

def video_state_new_request_remove_queue_flag(video: Video, redis_util: Redis):
    try:
        redis_util.redis.delete(VideoState.VIDEO_STATE_NEW_REQUEST.name + "-" + str(video.message.video_id))
    except Exception as e:
        raise RedisUpdateError

def video_is_state_frame_complete(video: Video):
    if video.message.status == 1:
        return True

def video_state_frame_complete_exists_in_queue(video: Video):
    if redis_util.redis.exists(VideoState.VIDEO_STATE_FRAME_COMPLETE.name, + "-" + str(video.message.video_id)):
        return True
    else:
        return False

def video_state_frame_complete_add_to_queue(video: Video, redis_util: Redis):
    try:
        redis_util.redis.lpush(VideoState.VIDEO_STATE_FRAME_COMPLETE.name, video.message.SerializeToString())
        redis_util.redis.set(VideoState.VIDEO_STATE_FRAME_COMPLETE.name + "-" + str(video.message.video_id), 0)
    except Exception as e:
        raise RedisUpdateError

def get_next_video_request(db_util: Postgres) -> Video:
    query = "SELECT video_id, user_id, start_time, end_time, lon, lat, min_lon, \
        max_lon, min_lat, max_lat, status \
        FROM video \
        ORDER BY created_at ASC \
        LIMIT 1000000;"
    try:
        db_util.cur.execute(query)
        db_util.conn.commit()
        while (record := db_util.cur.fetchone()) != None:
            yield Video(*record)
        yield None
    except Exception as ex:
        raise ex

if __name__ == '__main__':

    while True:
        request_iter = get_next_video_request(db_util)
        for video in request_iter:
            if video_is_state_new_request(video):
                if not video_state_new_request_exists_in_queue(video):
                    video_state_new_request_add_to_queue(video, redis_util)
            elif video_is_state_frame_complete(video):
                if not video_state_frame_complete_exists_in_queue(video):
                    video_state_frame_complete_add_to_queue(video, redis_util)
