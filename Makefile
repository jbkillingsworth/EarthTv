create-protos:
	cd executors && protoc --python_out=. --proto_path=. video.proto && \
protoc --python_out=. --proto_path=. frame.proto && \
protoc --python_out=. --proto_path=. image.proto
#protoc -I=. --java_out=. video.proto && protoc -I=. --java_out=. frame.proto

mp4:
	ffmpeg -i meridianville1.mp4 \
      -codec: copy \
      -hls_time 10 \
      -hls_list_size 0 \
      -f hls \
      output.m3u8

build-base-image:
	cp source/requirements.txt docker && cd docker && docker build . -t earthtv:latest

build-app-image:
	cp -r source docker/app && cd docker/app && docker build . -t earthtvapp:latest && rm -rf docker/app/source

run-app-image:
	docker run earthtvapp:latest

add-jdbc-video-sink:
	curl -s -X POST -H "Content-Type: application/json" --data @kafka-connect/sinks/video-connector.json http://localhost:8073/connectors

add-jdbc-frame-sink:
	curl -s -X POST -H "Content-Type: application/json" --data @kafka-connect/sinks/frame-connector.json http://localhost:8073/connectors

add-jdbc-image-sink:
	curl -s -X POST -H "Content-Type: application/json" --data @kafka-connect/sinks/image-connector.json http://localhost:8073/connectors

sinks: add-jdbc-video-sink add-jdbc-frame-sink add-jdbc-image-sink

#
#add-jdbc-source:
#	curl -s -X POST -H "Content-Type: application/json" --data @kafka-connect/sources/debezium-connector.json http://localhost:8073/connectors

all: build-base-image build-app-image run-app-image
