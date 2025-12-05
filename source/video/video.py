from source.db.postgres import Postgres
from datetime import datetime, date
import calendar
import pytz
from source.proto import video_pb2

class Video:
    def __init__(self, video_id: int, user_id: int, start: date, end: date, lon: float, lat: float,
                 min_lon: float, max_lon: float, min_lat: float, max_lat: float, status: int):
        self.message = video_pb2.Video()
        self.message.video_id = video_id
        self.message.user_id = user_id
        eastern = pytz.timezone('America/New_York')
        start = eastern.localize(datetime.utcfromtimestamp(calendar.timegm(start.timetuple())))
        end = eastern.localize(datetime.utcfromtimestamp(calendar.timegm(end.timetuple())))
        self.message.start = int(start.timestamp())
        self.message.end = int(end.timestamp())
        self.message.lon = lon
        self.message.lat = lat
        self.message.min_lon = min_lon
        self.message.max_lon = max_lon
        self.message.min_lat = min_lat
        self.message.max_lat = max_lat
        self.message.status = status
        self.bbox = [min_lon, min_lat, max_lon, max_lat]

    @staticmethod
    def from_bytes(bytes):
        message = video_pb2.Video()
        message.ParseFromString(bytes)
        start = datetime.fromtimestamp(message.start).date()
        end = datetime.fromtimestamp(message.end).date()
        return Video(message.video_id, message.user_id, start, end,
                     message.lon, message.lat, message.min_lon, message.max_lon,
                     message.min_lat, message.max_lat, message.status)
    def new_request(self, db_util: Postgres):
        try:
            eastern = pytz.timezone('America/New_York')
            start = eastern.localize(datetime.utcfromtimestamp(self.message.start))
            end = eastern.localize(datetime.utcfromtimestamp(self.message.end))
            query = ("INSERT INTO video (user_id, start_time, end_time, lon, lat, min_lon, max_lon, min_lat, max_lat, status) \
                         VALUES ({}, '{}', '{}', {}, {}, {}, {}, {}, {}, {})".format(self.message.user_id, start, end,
                                                                                     self.message.lon, self.message.lat,
                                                                                     self.message.min_lon, self.message.max_lon,
                                                                                     self.message.min_lat, self.message.max_lat,
                                                                                     self.message.status))
            db_util.cur.execute(query)
            db_util.conn.commit()
        except Exception as ex:
            raise ex
