-- DDL for users_cum
CREATE TABLE users_cumulated (
    user_id BIGINT,
    -- The list of dates in the past where the user was active.
    dates_active DATE[],
    -- The current date the user.
    date DATE,
    PRIMARY KEY (user_id, date)
);


WITH yesterday AS (
    SELECT
        *
    FROM users_cumulated
    WHERE date = DATE('2022-12-31')
), 

today AS (
    SELECT
        user_id,
        DATE(CAST(event_time AS TIMESTAMP)) as date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    NULL AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t 
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id