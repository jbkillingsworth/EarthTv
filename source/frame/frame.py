from datetime import datetime
import pystac
import pystac_client
import planetary_computer
from video.video import Video
from db.postgres import Postgres
import calendar
import pytz
from source.proto import frame_pb2
import pickle
import numpy as np
from io import BytesIO
import psycopg2

def get_items(video: Video) -> pystac.item_collection.ItemCollection:

    #TODO: check if PC uses EST for timezone
    eastern = pytz.timezone('America/New_York')
    start = eastern.localize(datetime.utcfromtimestamp(video.message.start))
    end = eastern.localize(datetime.utcfromtimestamp(video.message.end))

    time_range = start.isoformat() + "/" + end.isoformat()

    collections = ["sentinel-2-l2a"]

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace, # Needed to sign asset URLs for access
    )

    search = catalog.search(
        collections=collections,
        bbox=video.bbox,     # Use 'bbox' for bounding box
        datetime=time_range, # Use 'datetime' for time filtering
    )

    items = search.item_collection()

    return items

class Frame:
    def __init__(self, frame_id: int, video_id: int, frame_item_id: str,
                 frame_item_href: str, blue_href: str, green_href: str,
                 red_href: str, min_lon: float, max_lon: float,
                 min_lat: float, max_lat: float, collection_time_utc: int,
                 image_data: list[int], status: int = 0):
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
        self.message.status = status

    @staticmethod
    def from_bytes(bytes):
        message = frame_pb2.Frame()
        message.ParseFromString(bytes)
        collection_time_utc = datetime.utcfromtimestamp(message.collection_time_utc)
        return Frame(message.frame_id, message.video_id, message.frame_item_id, message.frame_item_href,
                     message.blue_href, message.green_href, message.red_href,
                     message.min_lon, message.max_lon, message.min_lat,
                     message.max_lat, collection_time_utc, message.image_data, message.status)

    def new_request(self, db_util: Postgres):
        try:
            ds = np.frombuffer(self.message.image_data, dtype=np.float64)
            eastern = pytz.timezone('America/New_York')
            collection_time_utc = eastern.localize(datetime.utcfromtimestamp(self.message.collection_time_utc))
            query = ("INSERT INTO frame (video_id, frame_item_id, frame_item_href, \
            blue_href, green_href, red_href, min_lon, max_lon, \
            min_lat, max_lat, collection_time_utc, image_data, status) \
                     VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, {})".format(
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
                                                                                 collection_time_utc, psycopg2.Binary(ds),
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
                     SET image_data = {}, status = 1 \
                     WHERE frame_id = {};".format(psycopg2.Binary(ds), self.message.frame_id))
            db_util.cur.execute(query)
            db_util.conn.commit()
        except Exception as ex:
            raise ex
# items = get_items(lon, lat, start, end)








# job = create_db_video_record(Job(user_id, start, end), db_util)
#

#
#     @staticmethod
#     def on_create(self, image: Image, image_job_id: int, status: int) -> Image:
#         image.job_id = image.job_id
#         image.image_id = image.image_id
#         image.image_job_id = image_job_id
#         image.status = status
#         return image
#
# def create_db_image_record(image: Image):
#     def create_db_image_job(image: Image):
#         pass
#     try:
#         image_job_id, status = create_db_image_job(image)
#         return Image.on_create(image, image_job_id, status)
#     except Exception as ex:
#         raise ex
#
# for item in items:
#     try:
#         image = Image(job.job_id, item.id)
#         create_db_image_record(image)
#     except Exception as ex:
#         raise ex
#
#
# ## at this point, you can poll the database image to see when each image job for a given video job is successfully complete
# ## once it's complete the processing can start
#
# ## create an image table entry for each image
# ## create a request table with a fk to each image
#
# def filter_by_cloud_cover(items: pystac.item_collection.ItemCollection, cloud_cover: float = .5) -> pystac.item_collection.ItemCollection:
#     for item in items:
#         try:
#             if item.properties["eo:cloud_cover"] <= cloud_cover:
#                 yield item
#         except Exception as e:
#             print(e)
#             pass
#
# import rioxarray
# import numpy as np
# import pyproj
#
# def fetch_pixels(item: pystac.item.Item, transform: pyproj.transformer.transform) -> np.ndarray:
#     signed_item = planetary_computer.sign(item)
#
#     blue_href = signed_item.assets["B08"].href
#     green_href = signed_item.assets["B03"].href
#     red_href = signed_item.assets["B04"].href
#
#     dsb = rioxarray.open_rasterio(blue_href)
#     dsg = rioxarray.open_rasterio(green_href)
#     dsr = rioxarray.open_rasterio(red_href)
#
#     meters_width = 1600
#     meters_height = 900
#
#     easting, northing = transform.transform(lon, lat)
#
#     blue = dsb.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
#     green = dsg.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
#     red = dsr.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
#
#     date_string = item.properties['datetime']
#     format_string = "%Y-%m-%dT%H:%M:%S.%fZ"
#
#     ts = datetime.strptime(date_string, format_string).timestamp()
#     return np.dstack([red.to_numpy()[0,:,:]/red.to_numpy().max(), blue.to_numpy()[0,:,:]/blue.to_numpy().max(), green.to_numpy()[0,:,:]/green.to_numpy().max()])
#
# from transforms import get_transform
#
# transform = get_transform()
#
# ## put ite
#
# pixels = fetch_pixels(items[0], transform)