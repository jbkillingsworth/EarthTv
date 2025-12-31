FROM python:3.11.13-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends build-essential libgdal-dev gdal-bin ffmpeg
RUN pip3 install -r requirements.txt
ENV PYTHONPATH="/app"