upload:
	ffmpeg -re -i output.mp4 -c:v libx264 -c:a aac -f flv rtmp://localhost:1935/live/stream

upload1:
	ffmpeg -re -i "meridianville1.mp4" -c:v copy -c:a aac -ar 44100 -ac 1 -f flv rtmp://localhost/live/stream

test:
	ffmpeg -re -i meridianville1.mp4 -listen 1 -f mp4 -movflags frag_keyframe+empty_moov rtmp://localhost:1981

test2:
	ffmpeg -i meridianville1.mp4 -v 0 -vcodec mpeg4 -f mpegts udp://127.0.0.1:23000

test3:
	ffmpeg -re -i output.mp4 -c copy -f mpegts udp://localhost:8084

working:
	ffmpeg -i meridianville1.mp4 -c:v libx264 -pix_fmt yuv420p -avoid_negative_ts make_zero output.mp4
	
working-stream:
	ffmpeg -re -i output.mp4 -c copy -c:a mp4 -f flv rtmp://localhost:1935/live/stream

