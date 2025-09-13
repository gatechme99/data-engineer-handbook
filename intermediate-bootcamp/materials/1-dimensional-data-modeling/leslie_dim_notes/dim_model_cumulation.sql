-- Create struct to store temporal data.
CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAl,
	ast REAL
);


-- Create enumerated type that ranks players based on points scored.
CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');


-- Create players table using fixed dims and season_stats struct.
-- Added later: scoring_class and years_since_last_season.
CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);


-- The first season in our table is 1996.
-- SELECT MIN(season) FROM player_seasons


-- Populate players table.
INSERT INTO players
WITH yesterday AS ( 
	SELECT * FROM players
	WHERE current_season = 1995 -- Create seed query for cumulation.
),

today AS (
	SELECT * FROM player_seasons
	WHERE season = 1996 -- Change this and above season to add more data.
)

SELECT
	COALESCE(t.player_name, y.player_name) as player_name,
	COALESCE(t.height, y.height) as height,
	COALESCE(t.college, y.college) as college,
	COALESCE(t.country, y.country) as country,
	COALESCE(t.draft_year, y.draft_year) as draft_year,
	COALESCE(t.draft_round, y.draft_round) as draft_round,
	COALESCE(t.draft_number, y.draft_number) as draft_number,
	CASE 
		WHEN y.season_stats IS NULL
			THEN ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
			)::season_stats]
		WHEN t.season IS NOT NULL 
			THEN y.season_stats || ARRAY[ROW(
				t.season,
				t.gp,
				t.pts,
				t.reb,
				t.ast
			)::season_stats]
		ELSE y.season_stats
	END as season_stats,
	CASE
		WHEN t.season IS NOT NULL THEN
			CASE
				WHEN t.pts > 20 THEN 'star'
				WHEN t.pts > 15 THEN 'good'
				WHEN t.pts > 10 THEN 'average'
				ELSE 'bad'
			END::scoring_class
		ELSE y.scoring_class
	END as scoring_class,
	CASE
		WHEN t.season IS NOT NULL THEN 0
		ELSE y.years_since_last_season + 1
	END as years_since_last_season,
	COALESCE(t.season, y.current_season + 1) as current_season
FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name;


-- After adding more seasons, you can go back to the old schema by unnesting. 
-- Sorting order stays avoiding run length encoding problem.
WITH unnested AS (
	SELECT player_name,
		UNNEST(season_stats) AS season_stats -- Keeps all of the temporal pieces together.
	FROM players
)

SELECT 
	player_name,
	(season_stats::season_stats).* -- Need to cast in postgres syntax.
FROM unnested;


-- Perform analytics to see which players had the most improvement (points) 
-- from their first season to their most recent season.
SELECT
	player_name,
	season_stats[1] AS first_season,
	season_stats[CARDINALITY(season_stats)] as latest_season
FROM players
WHERE current_season = 2001;

-- This query using cumulative pattern is very fast because there is no GROUP BY.
SELECT
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
		CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END
FROM players
WHERE current_season = 2001
AND scoring_class = 'star';
