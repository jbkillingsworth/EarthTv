from source.db.postgres import Postgres
from source.video.video import Video
from datetime import date
import time

db_util = Postgres()

import csv

# Open the CSV file in read mode with newline='' to handle line endings correctly
with open('/app/cities.csv', 'r', newline='') as csvfile:
    # Create a reader object
    csv_reader = csv.reader(csvfile)

    # Iterate over each row in the CSV file
    for row in csv_reader:
        # Each 'row' is a list of strings representing the columns in that row
        city, state, lat, lon = row

        lat = float(lat)
        lon = float(lon)

        # lon = -86.541016
        # lat = 34.865189
        WINDOW_SIZE = .001
        min_lon = lon-WINDOW_SIZE
        max_lon = lon+WINDOW_SIZE
        min_lat = lat-WINDOW_SIZE
        max_lat = lat+WINDOW_SIZE

        start_year = 2019
        end_year = 2025
        start_month = 1
        end_month = 3
        start_day = 5
        end_day = 28
        start = date(start_year, start_month, start_day)
        end = date(end_year, end_month, end_day)

        video_id=-1
        user_id = 0
        status = 0

        # while True:
        video = Video(video_id, user_id, start, end, lon, lat, min_lon, max_lon, min_lat, max_lat, status)
        video.new_request(db_util)
        # time.sleep(10)

