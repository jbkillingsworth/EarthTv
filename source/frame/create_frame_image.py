import requests
import pystac
import planetary_computer
import rioxarray
from frame import get_items
import time
from datetime import datetime
from source.db.postgres import Postgres
import redis
from source.video.video import Video
from source.frame.frame import Frame
from frame import get_items
import time
from datetime import datetime
import pyproj
import numpy as np
from io import BytesIO

db_util = Postgres()
redis_queue = redis.Redis(host='cache', port=6379, password="eYVX7EwVmmxKPCDmwMtyKVge8oLd2t81")

target_crs = "EPSG:32616"
source_crs = "EPSG:4326"
transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

while True:
    try:
        frame = Frame.from_bytes(redis_queue.lpop('frames'))
        print('trying to pull frame info')
        r = requests.get(
            frame.message.frame_item_href
        )
        print('pulled frame info')
        item = pystac.Item.from_dict(r.json())
        signed_item = planetary_computer.sign(item)
        print('trying to pull frame data')
        signed_blue_href = signed_item.assets['B02'].href
        signed_green_href = signed_item.assets["B03"].href
        signed_red_href = signed_item.assets["B04"].href
        dsb = rioxarray.open_rasterio(signed_blue_href)
        dsg = rioxarray.open_rasterio(signed_green_href)
        dsr = rioxarray.open_rasterio(signed_red_href)
        print('pulled frame data')

        meters_width = 1600
        meters_height = 900

        lat, lon = frame.get_video_lat_lon(frame.message.video_id, db_util)
        easting, northing = transformer.transform(lon, lat)

        blue = dsb.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
        green = dsg.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
        red = dsr.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
        ds = np.dstack([red.to_numpy()[0,:,:]/red.to_numpy().max(), blue.to_numpy()[0,:,:]/blue.to_numpy().max(), green.to_numpy()[0,:,:]/green.to_numpy().max()])
        # buffer = BytesIO()
        # np.save(buffer, ds)
        # npy_bytes = buffer.getvalue()
        frame.message.image_data = bytes(ds)#ds.tobytes(order='C')
        frame.message.img_width = ds.shape[1]
        frame.message.img_height = ds.shape[0]
        frame.set_image_data(db_util)
        # time.sleep(1)
    except Exception as ex:
        # time.sleep(1)
        print(ex)