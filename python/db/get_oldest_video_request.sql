SELECT user_id, start_time, end_time, lon, lat, min_lon,
max_lon, min_lat, max_lat, status
FROM video
WHERE status = 0
ORDER BY created_at ASC
LIMIT 1;