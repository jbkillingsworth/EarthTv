import pystac_client
import planetary_computer
from datetime import datetime
from tqdm.notebook import tqdm
import numpy as np
import rasterio
import requests
import pystac
import rioxarray
import pyproj
import psycopg2
import numpy as np
import matplotlib.pyplot as plt

target_crs = "EPSG:32616"
source_crs = "EPSG:4326"
transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

r = requests.get(
    "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2C_MSIL2A_20250327T162931_R083_T16SED_20250327T231859"
)

item = pystac.Item.from_dict(r.json())
signed_item = planetary_computer.sign(item)

signed_blue_href = signed_item.assets['B02'].href
signed_green_href = signed_item.assets["B03"].href
signed_red_href = signed_item.assets["B04"].href

dsb = rioxarray.open_rasterio(signed_blue_href)
dsg = rioxarray.open_rasterio(signed_green_href)
dsr = rioxarray.open_rasterio(signed_red_href)

meters_width = 1600
meters_height = 900

lat, lon = 34.8, -86
easting, northing = transformer.transform(lon, lat)

blue = dsb.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
green = dsg.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
red = dsr.rio.clip_box(minx=easting-meters_width, miny=northing-meters_height, maxx=easting+meters_width, maxy=northing+meters_height)
ds = np.dstack([red.to_numpy()[0,:,:]/red.to_numpy().max(), blue.to_numpy()[0,:,:]/blue.to_numpy().max(), green.to_numpy()[0,:,:]/green.to_numpy().max()])

from source.db.postgres import Postgres
db_util = Postgres()

image_data = psycopg2.Binary(ds)

query = ("UPDATE frame SET image_data={} WHERE frame_id={};".format(image_data, 1))
# query = "select * from frame;"
db_util.cur.execute(query)
db_util.conn.commit()
# lat, lon = db_util.cur.fetchone()
query = "select * from frame;"
db_util.cur.execute(query)
db_util.conn.commit()
response = db_util.cur.fetchone()
img = np.frombuffer(bytes(response[13])).reshape(ds.shape[0], ds.shape[1], ds.shape[2])
plt.imshow(img)
plt.show()
print(response)