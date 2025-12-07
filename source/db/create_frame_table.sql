CREATE TABLE frame (
    frame_id SERIAL PRIMARY KEY,
    video_id BIGINT NOT NULL,
	frame_item_id VARCHAR(500) NOT NULL,
	frame_item_href VARCHAR(5000) NOT NULL,
	blue_href VARCHAR(5000) NOT NULL,
	green_href VARCHAR(5000) NOT NULL,
	red_href VARCHAR(5000) NOT NULL,
	min_lon DECIMAL NOT NULL,
	max_lon DECIMAL NOT NULL,
	min_lat DECIMAL NOT NULL,
	max_lat DECIMAL NOT NULL,
	collection_time_utc TIMESTAMPTZ NOT NULL,
	status SMALLINT NOT NULL,
	image_data BYTEA NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);