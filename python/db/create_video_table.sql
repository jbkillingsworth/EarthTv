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

-- INSERT INTO "video" (user_id, start_time, end_time, status)                  VALUES (0, '2023-01-10 00:00:00-05:00', '2025-12-20 00:00:00-05:00', 0)

-- SELECT * FROM video