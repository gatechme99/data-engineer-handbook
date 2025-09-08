-- Create DDL for an actors table.
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

WITH yesterday AS (
	SELECT * FROM actors
	WHERE current_year = 1969
),

today AS (
	SELECT * FROM actor_films
	WHERE year = 1970
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
	ON a.actor = y.actor