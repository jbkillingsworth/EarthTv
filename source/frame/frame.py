from datetime import datetime
import pystac
import pystac_client
import planetary_computer
from source.db.postgres import Postgres
import calendar
import pytz
from source.proto import frame_pb2
import pickle
import numpy as np
from io import BytesIO
import psycopg2

def get_items(start, end, bbox) -> pystac.item_collection.ItemCollection:

    #TODO: check if PC uses EST for timezone
    eastern = pytz.timezone('America/New_York')
    start = eastern.localize(datetime.utcfromtimestamp(start))
    end = eastern.localize(datetime.utcfromtimestamp(end))

    time_range = start.isoformat() + "/" + end.isoformat()

    collections = ["sentinel-2-l2a"]

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace, # Needed to sign asset URLs for access
    )

    search = catalog.search(
        collections=collections,
        bbox=bbox,     # Use 'bbox' for bounding box
        datetime=time_range, # Use 'datetime' for time filtering
    )

    items = search.item_collection()

    return items

class Frame:
    def __init__(self, frame_id: int, video_id: int, frame_item_id: str,
                 frame_item_href: str, blue_href: str, green_href: str,
                 red_href: str, min_lon: float, max_lon: float,
                 min_lat: float, max_lat: float, collection_time_utc: int,
                 image_data: list[int], img_width:int, img_height: int, status: int = 0):
        self.message = frame_pb2.Frame()
        self.message.frame_id = frame_id
        self.message.video_id = video_id
        self.message.frame_item_id = frame_item_id
        self.message.frame_item_href = frame_item_href
        self.message.blue_href = blue_href
        self.message.green_href = green_href
        self.message.red_href = red_href
        self.message.min_lon = min_lon
        self.message.max_lon = max_lon
        self.message.min_lat = min_lat
        self.message.max_lat = max_lat
        self.message.collection_time_utc = int(collection_time_utc.timestamp())
        self.message.image_data = bytes(image_data)
        self.message.img_width = img_width
        self.message.img_height = img_height
        self.message.status = status

    @staticmethod
    def from_bytes(bytes):
        message = frame_pb2.Frame()
        message.ParseFromString(bytes)
        collection_time_utc = datetime.utcfromtimestamp(message.collection_time_utc)
        return Frame(message.frame_id, message.video_id, message.frame_item_id, message.frame_item_href,
                     message.blue_href, message.green_href, message.red_href,
                     message.min_lon, message.max_lon, message.min_lat,
                     message.max_lat, collection_time_utc, message.image_data,
                     message.img_width, message.img_height, message.status)

    def new_request(self, db_util: Postgres):
        try:
            ds = np.frombuffer(self.message.image_data, dtype=np.float64)
            eastern = pytz.timezone('America/New_York')
            collection_time_utc = eastern.localize(datetime.utcfromtimestamp(self.message.collection_time_utc))
            query = ("INSERT INTO frame (video_id, frame_item_id, frame_item_href, \
            blue_href, green_href, red_href, min_lon, max_lon, \
            min_lat, max_lat, collection_time_utc, image_data, img_width, img_height, status) \
                     VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, {}, {}, {})".format(
                                                                                 self.message.video_id,
                                                                                 self.message.frame_item_id,
                                                                                 self.message.frame_item_href,
                                                                                 self.message.blue_href,
                                                                                 self.message.green_href,
                                                                                 self.message.red_href,
                                                                                 self.message.min_lon,
                                                                                 self.message.max_lon,
                                                                                 self.message.min_lat,
                                                                                 self.message.max_lat,
                                                                                 collection_time_utc, psycopg2.Binary(ds), self.message.img_width, self.message.img_height,
                                                                                 self.message.status))
            db_util.cur.execute(query)
            db_util.conn.commit()
        except Exception as ex:
            raise ex

    def get_video_lat_lon(self, video_id:int, db_util: Postgres):
        try:
            query = ("SELECT lat, lon FROM video WHERE video_id = {}".format(video_id))
            db_util.cur.execute(query)
            db_util.conn.commit()
            lat, lon = db_util.cur.fetchone()
            return lat, lon
        except Exception as ex:
            raise ex

    def set_image_data(self, db_util: Postgres):
        try:
            ds = np.frombuffer(self.message.image_data, dtype=np.float64)
            query = ("UPDATE frame \
                     SET image_data = {}, status = 1, img_width = {}, img_height = {} \
                     WHERE frame_id = {};".format(psycopg2.Binary(ds), self.message.img_width,
                                                  self.message.img_height, self.message.frame_id))
            db_util.cur.execute(query)
            db_util.conn.commit()
        except Exception as ex:
            raise ex
