from old.source.db.postgres import Postgres
from datetime import datetime, date
import calendar
import pytz
from old.source.proto import video_pb2
from old.source.frame.frame import Frame
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.animation as animation
import uuid
from enum import Enum

class VideoState(Enum):
    VIDEO_STATE_NEW_REQUEST = 0
    VIDEO_STATE_FRAME_COMPLETE = 1

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

    def get_frames(self, db_util: Postgres):
        try:
            query = "SELECT frame_id, video_id, frame_item_id, frame_item_href, \
                     blue_href, green_href, red_href, min_lon, max_lon, \
                     min_lat, max_lat, collection_time_utc, image_data, img_width, img_height, status \
                     FROM frame WHERE video_id = {}".format(self.message.video_id)
            db_util.cur.execute(query)
            db_util.conn.commit()
            while (record := db_util.cur.fetchone()) != None:
                yield Frame(*record)
        except Exception as ex:
            raise ex

    @staticmethod
    def count_video_requests(db_util: Postgres):
        try:
            query = "SELECT COUNT(*) \
                 FROM video WHERE status = {}".format(0)
            db_util.cur.execute(query)
            db_util.conn.commit()
            return db_util.cur.fetchone()[0]
        except Exception as ex:
            raise ex

    @staticmethod
    def delete_video_requests(db_util: Postgres):
        try:
            query = "DELETE FROM video WHERE status = {}".format(0)
            db_util.cur.execute(query)
            db_util.conn.commit()
        except Exception as ex:
            raise ex

    def assemble_frames(self, items_list):
        ds = items_list[0]
        ds = ds + ds.min()
        ds = ds/ds.max()

        h = ds.shape[0]
        w = ds.shape[1]
        fig = plt.figure(figsize=(10, 5.625))
        ax = plt.axes(xlim=(0, w), ylim=(0, h))
        ax.axis('off')
        fig.subplots_adjust(left=-0.01, bottom=-0.01, right=1.01, top=1.01, wspace=None, hspace=None)
        im=plt.imshow(ds, interpolation='none')

        def animate(i):
            print(i)
            ds = items_list[i]
            ds = ds + ds.min()
            ds = ds/ds.max()
            # print(t)
            h = ds.shape[1]
            w = ds.shape[2]
            im.set_data(ds)
            return [im]

        fps = 1

        anim = animation.FuncAnimation(
            fig,
            animate,
            frames = len(items_list) - 1,#len(my_heap) - 1,
            interval = 10000 / fps # in ms
        )

        anim.save('/app/output/' + str(uuid.uuid4()) + '.mp4', fps=fps, dpi=200)
