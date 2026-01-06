from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from executors import video_pb2, frame_pb2, image_pb2
from datetime import datetime
import pystac
import pystac_client
import planetary_computer
import calendar
import pytz
import pickle
import numpy as np
from io import BytesIO
import psycopg2
from datetime import datetime, date
import calendar
import pytz
# from source.proto import video_pb2
# from source.frame.frame import Frame
import numpy as np
import uuid
from enum import Enum

# Configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'video--1',
    'auto.offset.reset': 'earliest'
}
schema_registry_conf = {'url': 'http://localhost:8081'}

# Initialize clients
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

video_protobuf_deserializer = ProtobufDeserializer(video_pb2.Video, conf={'specific.protobuf.value.type': video_pb2.Video})
frame_protobuf_deserializer = ProtobufDeserializer(frame_pb2.Frame, conf={'specific.protobuf.value.type': frame_pb2.Frame})
image_protobuf_deserializer = ProtobufDeserializer(image_pb2.Image, conf={'specific.protobuf.value.type': image_pb2.Image})

consumer = Consumer(kafka_config)
consumer.subscribe(['video-0', 'frame-0'])

print("Confluent Consumer started. Waiting for messages...")

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

def get_frames(video):
    results = []
    bbox = [video.min_lon, video.min_lat, video.max_lon,video.max_lat]
    frames = get_items(video.start, video.end, bbox)
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
            frame_image = Frame(frame_id, video.video_id, frame_item_id, frame_href, unsigned_blue_href,
                                unsigned_green_href, unsigned_red_href, min_lon, max_lon, min_lat, max_lat,
                                datetime_object, memoryview(image_data), img_width, img_height, status)
            results.append(frame_image)
            # time.sleep(1)
    return results

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

class Image:
    def __init__(self, image_id: int, frame_id: int, image_data, frame_href, status: int = 0):
        self.message = image_pb2.Image()
        self.message.image_id = image_id
        self.message.frame_id = frame_id
        self.message.image_data = image_data
        self.message.frame_href = frame_href
        self.message.status = status

import producer

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        if msg.topic() == 'video-0':
            video = video_protobuf_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if video is not None:
                frames_gen = get_frames(video)
                for frame in frames_gen:
                    producer.produce_frame('frame', 0, frame.message)

        if msg.topic() == 'frame-0':
            frame = frame_protobuf_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if frame is not None:
                image = Image(-1, frame.frame_id, b'', frame.blue_href, -1)
                producer.produce_image('image', 0, image.message)

                # frames_gen = get_frames(video)
                # for frame in frames_gen:
                #     producer.produce_frame('frame', 0, frame.message)

            # print(f"Received message: ID={user_message.id}, Name={user_message.name}, Email={user_message.email}")

except KeyboardInterrupt:
    pass
except Exception as ex:
    print(ex)
finally:
    consumer.close()


























# import pystac_client
# import planetary_computer
# from datetime import datetime
# from tqdm.notebook import tqdm
# import numpy as np
# import rasterio
# import requests
# import pystac
# import rioxarray
# import pyproj
# import psycopg2
# import numpy as np
# import matplotlib.pyplot as plt
#
# target_crs = "EPSG:32616"
# source_crs = "EPSG:4326"
# transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
#
# r = requests.get(
#     "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2C_MSIL2A_20250327T162931_R083_T16SED_20250327T231859"
# )
#
# item = pystac.Item.from_dict(r.json())
# signed_item = planetary_computer.sign(item)
#
# signed_blue_href = signed_item.assets['B02'].href
# signed_green_href = signed_item.assets["B03"].href
# signed_red_href = signed_item.assets["B04"].href
#
# dsb = rioxarray.open_rasterio(signed_blue_href)
# dsg = rioxarray.open_rasterio(signed_green_href)
# dsr = rioxarray.open_rasterio(signed_red_href)
#
# meters_width = 1600
# meters_height = 900
#
# lat, lon = 34.8, -86
# easting, northing = transformer.transform(lon, lat)
#
# blue = dsb.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
# green = dsg.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
# red = dsr.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
# ds = np.dstack([red.to_numpy()[0,:,:]/red.to_numpy().max(), blue.to_numpy()[0,:,:]/blue.to_numpy().max(), green.to_numpy()[0,:,:]/green.to_numpy().max()])
#
# from old.source.db.postgres import Postgres
# db_util = Postgres()
#
# image_data = psycopg2.Binary(ds)
#
# query = ("UPDATE frame SET image_data={} WHERE frame_id={};".format(image_data, 1))
# # query = "select * from frame;"
# db_util.cur.execute(query)
# db_util.conn.commit()
# # lat, lon = db_util.cur.fetchone()
# query = "select * from frame;"
# db_util.cur.execute(query)
# db_util.conn.commit()
# response = db_util.cur.fetchone()
# img = np.frombuffer(bytes(response[13])).reshape(ds.shape[0], ds.shape[1], ds.shape[2])
# plt.imshow(img)
# plt.show()
# print(response)