ALTER USER postgres WITH PASSWORD 'postgres';
ALTER SYSTEM SET max_connections TO '200';

CREATE TABLE video (
    video_id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
	start_time TIMESTAMPTZ NOT NULL,
	end_time TIMESTAMPTZ NOT NULL,
	lon DECIMAL NOT NULL,
	lat DECIMAL NOT NULL,
	min_lon DECIMAL NOT NULL,
	max_lon DECIMAL NOT NULL,
	min_lat DECIMAL NOT NULL,
	max_lat DECIMAL NOT NULL,
	status SMALLINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
	img_width SMALLINT NOT NULL,
	img_height SMALLINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION update_video() RETURNS TRIGGER AS $$
DECLARE
    r RECORD;
	s BOOLEAN;
BEGIN
    FOR r IN SELECT video_id FROM video WHERE status = 0 LOOP
		select (
			(select count(frame_id) from frame where (video_id = r.video_id) and (status = 1)) =
			(select count(frame_id) from frame where video_id = r.video_id)
		) INTO s;
		IF s THEN
			UPDATE video SET status=1 WHERE video_id=r.video_id;
		END IF;
    END LOOP;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_video_trigger
AFTER UPDATE ON frame
FOR EACH ROW
EXECUTE FUNCTION update_video();

ALTER TABLE video REPLICA IDENTITY FULL;