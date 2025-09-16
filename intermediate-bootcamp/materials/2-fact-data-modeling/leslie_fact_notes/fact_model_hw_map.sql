-- DDL for an user_devices_cumulated table.
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_id TEXT,
    -- browser_type TEXT,
    -- event_time TEXT,
    -- Object to track user active days by browser_type.
    device_activity_datelist JSON,
    -- The current date.
    date DATE,
    PRIMARY KEY (user_id, device_id, date)
);

-- Cumulative query to generate device_activity_datelist from events.
INSERT INTO user_devices_cumulated
WITH deduped AS (
	SELECT
		e.user_id,
        e.device_id,
        e.event_time,
        d.browser_type,
		ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id) as row_num
	FROM events e 
    JOIN devices d ON e.device_id = d.device_id
),

yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-16')
),

today AS (
    SELECT
	    CAST(user_id AS TEXT) AS user_id,
       	CAST(device_id AS TEXT) AS device_id,
        CAST(browser_type AS TEXT) AS browser_type,
        DATE(CAST(event_time AS TIMESTAMP)) as date_active
    FROM deduped
    WHERE row_num = 1
	    AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-17')
	    AND user_id IS NOT NULL
    GROUP BY user_id, device_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN 
			jsonb_build_object(t.browser_type, ARRAY[t.date_active])
        WHEN t.date_active IS NULL THEN y.device_activity_datelist::jsonb
		WHEN y.device_activity_datelist IS NOT NULL 
			AND y.device_activity_datelist::jsonb ? t.browser_type THEN
                jsonb_set(
                    y.device_activity_datelist::jsonb,
                    ARRAY[t.browser_type],
                    to_jsonb((y.device_activity_datelist -> t.browser_type)::jsonb || to_jsonb(t.date_active))
                )
		WHEN y.device_activity_datelist IS NOT NULL 
			AND NOT(y.device_activity_datelist::jsonb ? t.browser_type) 
			THEN y.device_activity_datelist::jsonb || jsonb_build_object(ARRAY[t.browser_type], ARRAY[t.date_active])
    END AS device_activity_datelist,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
    AND t.device_id = y.device_id
-- ON CONFLICT(user_id, device_id, date)
-- DO
--     UPDATE SET device_activity_datelist = EXCLUDED.device_activity_datelist;