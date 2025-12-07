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