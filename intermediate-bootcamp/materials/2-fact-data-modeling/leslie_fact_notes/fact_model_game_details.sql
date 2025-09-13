-- DDL for fct_game_details table.
CREATE TABLE fct_game_details(
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm REAL,
    m_fga REAL,
    m_fg3m REAL,
    m_fg2a REAL,
    m_ftm REAL,
    m_fta REAL,
    m_oreb REAL,
    m_dreb REAL,
    m_reb REAL, 
    m_ast REAL,
    m_stl REAL,
    m_blk REAL,
    m_turnovers REAL,
    m_pf REAL,
    m_pts REAL,
    m_plus_minus REAL,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);


-- Populate fct_game_details table with de-duplicated and relevant data renamed.
INSERT INTO fct_game_details
WITH deduped AS(
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*, 
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id, g.game_date_est) as row_num
    FROM game_details gd 
    JOIN games g ON gd.game_id = g.game_id
)

SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
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