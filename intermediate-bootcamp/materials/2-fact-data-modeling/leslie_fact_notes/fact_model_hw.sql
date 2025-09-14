-- Query to de-duplicate game_details and rename relevant data.
WITH deduped AS (
	SELECT
		*, 
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) as row_num
	FROM game_details
)

SELECT 
    game_id AS dim_game_id,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    COALESCE(POSITION('DNP' IN comment), 0) > 0 as dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0 as dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0 as dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" as m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;


-- DDL for an user_devices_cumulated table.
CREATE TABLE user_devices_cumulated (
    device_id NUMBERIC,
    -- The list of dates in the past where the user was active.
    dates_active DATE[],
    -- The current date the user.
    date DATE,
    PRIMARY KEY (user_id, date)
);

-- Cumulative query to generate device_activity_datelist from events.


-- A datelist_int generation query that converts the 
-- device_activity_datelist column into a datelist_int column.


-- DDL for hosts_cumulated table.


-- Incremental query to generate host_activity_datelist.


-- Monthly, reduced fact table DDL host_activity_reduced.


-- Incremental query that loads host_activity_reduced.

