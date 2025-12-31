from old.source.video.video import Video
from old.source.db.postgres import Postgres
from datetime import date

def create(lat: float, lon: float, window_size: float, start_year: int, end_year: int,
           start_month: int, end_month: int, start_day: int, end_day: int, db_util: Postgres) -> Video:

    min_lon = lon-window_size
    max_lon = lon+window_size
    min_lat = lat-window_size
    max_lat = lat+window_size

    start = date(start_year, start_month, start_day)
    end = date(end_year, end_month, end_day)

    video_id=-1
    user_id = 0
    status = 0

    video = Video(video_id, user_id, start, end, lon, lat, min_lon, max_lon, min_lat, max_lat, status)
    video.new_request(db_util)

    return video
