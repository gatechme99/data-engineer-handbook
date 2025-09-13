-- DDL for an actors table including film struct and quality_class enumerated type.
CREATE TYPE films AS (
	film TEXT,
    year INTEGER,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS ENUM (
    'star', 
    'good', 
    'average', 
    'bad'
);

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);


-- Query to populate the actors table one year at a time.
CREATE OR REPLACE FUNCTION populate_actors_by_year(
    start_year INT,
    end_year INT
)
RETURNS VOID AS $$
DECLARE 
    curr_year INT;
BEGIN
    FOR curr_year IN start_year..end_year LOOP
        INSERT INTO actors
        WITH yesterday AS (
            SELECT * FROM actors
            WHERE current_year = curr_year
        ),

        today AS (
            SELECT * FROM actor_films
            WHERE year = curr_year + 1
        ),

        actor_agg AS (
            SELECT
                actor,
                actorid,
                year AS agg_year,
                ARRAY_AGG(ROW(film, year, votes, rating, filmid)::films) AS new_films,
                AVG(rating) AS avg_rating
            FROM today
            GROUP BY actor, actorid, year
        )

        SELECT 
            COALESCE(a.actor, y.actor) AS actor,
            COALESCE(a.actorid, y.actorid) AS actorid,
            CASE
                WHEN y.films IS NULL
                    THEN a.new_films
                WHEN a.agg_year IS NOT NULL
                    THEN y.films || a.new_films
                ELSE y.films
            END as films,
            CASE
                WHEN a.agg_year IS NOT NULL THEN
                    CASE
                        WHEN a.avg_rating > 8 THEN 'star'
                        WHEN a.avg_rating > 7 THEN 'good'
                        WHEN a.avg_rating > 6 THEN 'average'
                        ELSE 'bad'
                    END::quality_class
                ELSE y.quality_class
            END as quality_class,
            COALESCE(a.agg_year IS NOT NULL, FALSE) AS is_active,
            COALESCE (a.agg_year, y.current_year + 1) AS current_year
        FROM actor_agg a 
        FULL OUTER JOIN yesterday y
            ON a.actorid = y.actorid;

        RAISE NOTICE 'Data loaded for year: %', curr_year;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Load data for years 1969 - 2021 using the following query:
-- SELECT populate_actors_by_year(1969, 2021);


-- DDL for an actors_history_scd table.
CREATE TABLE actors_history_scd (
	actor TEXT,
    actorid TEXT,
	quality_class quality_class, -- first column we are tracking
	is_active BOOLEAN, -- second column we are tracking
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actorid, start_year, current_year)
);


-- Backfill query for actors_history_scd
INSERT INTO actors_history_scd
WITH previous AS (
    SELECT
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active
    FROM actors
    WHERE current_year <= 2020
),

indicators AS (
    SELECT *,
        CASE
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END as change_indicator
    FROM previous
),

streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) as streak_identifier
    FROM indicators
)

SELECT
	actor,
    actorid,
	quality_class,
	is_active,
	MIN(current_year) AS start_year,
	MAX(current_year) AS end_year,
	2020 AS current_year
FROM streaks
GROUP BY actor, actorid, streak_identifier, is_active, quality_class
ORDER BY actor, actorid, streak_identifier;


-- Incremental query for actors_history_scd
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);

WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 2020
        AND end_year = 2020
),

this_year_scd AS (
    SELECT * FROM actors
    WHERE current_year = 2021
),

historical_scd AS (
    SELECT 
        actor,
        actorid,
        quality_class,
        is_active,
        start_year,
        end_year,
        current_year
    FROM actors_history_scd
    WHERE current_year = 2020
    AND end_year < 2020
),

unchanged_records AS (
    SELECT
        ty.actor,
        ty.actorid,
        ty.quality_class,
        ty.is_active,
        ly.start_year,
        ty.current_year AS end_year,
        ty.current_year
    FROM this_year_scd ty
        JOIN last_year_scd ly
        ON ty.actorid = ly.actorid
    WHERE ty.quality_class = ly.quality_class
        AND ty.is_active = ly.is_active
),

changed_records AS (
    SELECT
        ty.actor,
        ty.actorid,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_year,
                ly.end_year
            )::actor_scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
            )::actor_scd_type
        ]) AS records,
        ty.current_year
    FROM this_year_scd ty
    LEFT JOIN last_year_scd ly
        ON ty.actorid = ly.actorid
    WHERE ty.quality_class <> ly.quality_class
        OR ty.is_active <> ly.is_active
),

unnested_changed_records AS (
    SELECT 
        actor,
        actorid,
        (records::actor_scd_type).quality_class,
        (records::actor_scd_type).is_active,
        (records::actor_scd_type).start_year,
        (records::actor_scd_type).end_year,
        current_year
    FROM changed_records
),

new_records AS (
    SELECT
        ty.actor,
        ty.actorid,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_season,
        ty.current_year AS end_season,
        ty.current_year
    FROM this_year_scd ty
    LEFT JOIN last_year_scd ly
        ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL
)

SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records