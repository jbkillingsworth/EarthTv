create-protos:
	cd source/proto && protoc --python_out=. --proto_path=. video.proto && \
protoc --python_out=. --proto_path=. frame.proto && \
protoc -I=. --java_out=. video.proto && protoc -I=. --java_out=. frame.proto

build-base-image:
	cp source/requirements.txt docker && cd docker && docker build . -t earthtv:latest

build-app-image:
	cp -r source docker/app && cd docker/app && docker build . -t earthtvapp:latest && rm -rf docker/app/source

run-app-image:
	docker run earthtvapp:latest

all: build-base-image build-app-image run-app-image
