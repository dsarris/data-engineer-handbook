-- Task 1
-- A query that does state change tracking for `players`
  -- A player entering the league should be `New`
  -- A player leaving the league should be `Retired`
  -- A player staying in the league should be `Continued Playing`
  -- A player that comes out of retirement should be `Returned from Retirement`
  -- A player that stays out of the league should be `Stayed Retired`

DROP TABLE IF EXISTS players_scd;
CREATE TABLE players_scd (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	current_season integer,
    first_active_season INTEGER,
    last_active_season INTEGER,
	status_flag TEXT,
	seasons_active INTEGER[],
    PRIMARY KEY(player_name,current_season)
);


DROP TABLE IF EXISTS previous_year;
CREATE TEMP TABLE previous_year AS (
SELECT 
	player_name,
	height,
	college,
	country,
	draft_year::TEXT,
	draft_round::TEXT,
	draft_number::TEXT,
	1995 AS current_season
FROM player_seasons 
WHERE season = 1995
);

DROP TABLE IF EXISTS current_year;
CREATE TEMP TABLE current_year AS (
SELECT 
	player_name,
	height,
	college,
	country,
	draft_year::TEXT,
	draft_round::TEXT,
	draft_number::TEXT,
	1996 AS current_season
FROM player_seasons 
WHERE season = 1996
);

INSERT INTO players_scd
SELECT 
	COALESCE(p.player_name,c.player_name) AS player_name,
	COALESCE(p.height,c.height) AS height,
	COALESCE(p.college,c.college) AS college,
	COALESCE(p.country,c.country) AS country,
	COALESCE(p.draft_year,c.draft_year) AS draft_year,
	COALESCE(p.draft_round,c.draft_round) AS draft_round,
	COALESCE(p.draft_number,c.draft_number) AS draft_number,
	COALESCE(p.current_season,c.current_season) AS first_active_season,
	COALESCE(c.current_season,p.current_season) AS last_active_season,
	1996 AS current_season,
	'New' AS status_flag,
	ARRAY [c.current_season] AS seasons_active
FROM previous_year p
FULL OUTER JOIN current_year c
ON p.player_name = c.player_name;

CREATE OR REPLACE FUNCTION load_data_for_years()
RETURNS void AS $$
DECLARE
    year_arg INTEGER;
BEGIN
    FOR year_arg IN 1996..2021 LOOP

	DROP TABLE IF EXISTS previous_year;
	CREATE TEMP TABLE previous_year AS (
		SELECT * FROM players_scd WHERE current_season = year_arg
	);
	
	DROP TABLE IF EXISTS current_year;
	CREATE TEMP TABLE current_year AS (
	SELECT 
		player_name,
		height,
		college,
		country,
		draft_year::TEXT,
		draft_round::TEXT,
		draft_number::TEXT,
		year_arg + 1 AS current_season
	FROM player_seasons 
	WHERE season = year_arg + 1
	);

	INSERT INTO players_scd
	SELECT 
		COALESCE(p.player_name,c.player_name) AS player_name,
		COALESCE(p.height,c.height) AS height,
		COALESCE(p.college,c.college) AS college,
		COALESCE(p.country,c.country) AS country,
		COALESCE(p.draft_year,c.draft_year) AS draft_year,
		COALESCE(p.draft_round,c.draft_round) AS draft_round,
		COALESCE(p.draft_number,c.draft_number) AS draft_number,
		COALESCE(c.current_season,p.current_season+1) AS current_season,
		COALESCE(p.first_active_season,c.current_season) AS first_active_season,
		COALESCE(c.current_season,p.last_active_season) AS last_active_season,
		CASE 
			WHEN p.player_name IS NULL AND c.player_name IS NOT NULL THEN 'New'
			WHEN c.current_season IS NULL AND p.last_active_season = year_arg THEN 'Retired'
			WHEN c.current_season IS NOT NULL AND c.current_season - p.last_active_season = 1 THEN 'Continued Playing'
			WHEN c.current_season IS NOT NULL AND c.current_season - p.last_active_season > 1 THEN 'Returned from Retirement'
		ELSE 'Stayed Retired' END AS status_flag,
		CASE 
			WHEN p.seasons_active IS NULL THEN ARRAY [c.current_season]
			WHEN c.current_season IS NULL THEN p.seasons_active
		ELSE p.seasons_active || ARRAY [c.current_season] END AS seasons_active
	FROM previous_year p
	FULL OUTER JOIN current_year c
	ON p.player_name = c.player_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT load_data_for_years();

-- Task 2
-- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  -- Aggregate this dataset along the following dimensions
   -- player and team
      -- Answer questions like who scored the most points playing for one team?
    -- player and season
      -- Answer questions like who scored the most points in one season?
    -- team
      -- Answer questions like which team has won the most games?
SELECT 
    CASE 
        WHEN GROUPING(player_name) = 1 THEN 'All Players'
        ELSE player_name 
    END AS player_name,
    CASE 
        WHEN GROUPING(team_abbreviation) = 1 THEN 'All Teams'
        ELSE team_abbreviation 
    END AS team_abbreviation,
    CASE 
        WHEN GROUPING(season) = 1 THEN 'All Seasons'
        ELSE CAST(season AS VARCHAR) 
    END AS season,
	SUM(COALESCE(pts,0)) AS points_scored,
	COUNT(DISTINCT(game_id_won)) AS games_won
FROM (
SELECT 
	gd.*,
	g.game_date_est AS game_date_est,
	g.season AS season,
	g.home_team_id,
	g.visitor_team_id,
	g.pts_home,
	g.pts_away,
	g.home_team_wins,
	CASE 
		WHEN team_id = home_team_id AND home_team_wins = 1 THEN 1
		WHEN team_id = home_team_id AND home_team_wins = 0 THEN 0
		WHEN team_id = visitor_team_id AND home_team_wins = 1 THEN 0
		WHEN team_id = visitor_team_id AND home_team_wins = 0 THEN 1
	END AS won,
	CASE 
		WHEN team_id = home_team_id AND home_team_wins = 1 THEN gd.game_id
		WHEN team_id = home_team_id AND home_team_wins = 0 THEN NULL
		WHEN team_id = visitor_team_id AND home_team_wins = 1 THEN NULL
		WHEN team_id = visitor_team_id AND home_team_wins = 0 THEN gd.game_id
	END AS game_id_won
FROM game_details gd
JOIN games g
ON gd.game_id = g.game_id
) t
GROUP BY GROUPING SETS (
	(player_name,team_abbreviation),
	(player_name,season),
	(team_abbreviation)
)
ORDER BY points_scored DESC

-- player with most points in a team
SELECT 
    player_name, 
    team_abbreviation, 
    SUM(COALESCE(pts,0)) AS total_points
FROM game_details
GROUP BY player_name, team_abbreviation
ORDER BY total_points DESC
LIMIT 1;

-- player with most points in a season
SELECT 
    gd.player_name, 
    g.season, 
    SUM(COALESCE(gd.pts,0)) AS total_points
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
GROUP BY gd.player_name, g.season
ORDER BY total_points DESC
LIMIT 1;

-- team won the most games
SELECT 
    team_abbreviation, 
    COUNT(DISTINCT game_id_won) AS total_wins
FROM (
    SELECT 
        gd.team_abbreviation,
        CASE 
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN gd.game_id
            WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN gd.game_id
            ELSE NULL
        END AS game_id_won
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
) t
WHERE game_id_won IS NOT NULL
GROUP BY team_abbreviation
ORDER BY total_wins DESC
LIMIT 1;

-- Task 3      
-- A query that uses window functions on `game_details` to find out the following things:
  -- What is the most games a team has won in a 90 game stretch? 
  -- How many games in a row did LeBron James score over 10 points a game?
DROP TABLE IF EXISTS base_table;
CREATE TEMP TABLE base_table AS (
SELECT 
	gd.game_id,
	gd.team_abbreviation,
	g.game_date_est AS game_date_est,
	CASE 
		WHEN team_id = home_team_id AND home_team_wins = 1 THEN 1
		WHEN team_id = home_team_id AND home_team_wins = 0 THEN 0
		WHEN team_id = visitor_team_id AND home_team_wins = 1 THEN 0
		WHEN team_id = visitor_team_id AND home_team_wins = 0 THEN 1
	END AS won
FROM game_details gd
JOIN games g
ON gd.game_id = g.game_id
GROUP BY 1,2,3,4
);

SELECT 
	team_abbreviation,
	SUM(won) OVER(
		PARTITION BY team_abbreviation 
		ORDER BY game_date_est ASC
		ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
	) AS ninety_game_streak
FROM base_table
ORDER BY ninety_game_streak DESC
LIMIT 1


DROP TABLE IF EXISTS streak_identifier;
CREATE TEMP TABLE streak_identifier AS (
SELECT 
	gd.game_id,
	gd.player_name,
	gd.team_abbreviation,
	g.game_date_est AS game_date_est,
	pts,
	CASE WHEN pts <= 10 
		 OR LAG(pts) OVER (PARTITION BY player_name ORDER BY game_date_est) <= 10 
		 OR LAG(pts) OVER (PARTITION BY player_name ORDER BY game_date_est) IS NULL 
	THEN 1 ELSE 0 END AS new_streak
FROM game_details gd
JOIN games g
ON gd.game_id = g.game_id
WHERE gd.player_name = 'LeBron James'
GROUP BY 1,2,3,4,5
ORDER BY game_date_est
);

DROP TABLE IF EXISTS streak_id;
CREATE TEMP TABLE streak_id AS (
SELECT 
	*,
	SUM(new_streak) OVER(
		PARTITION BY player_name
		ORDER BY game_date_est ASC
	) AS streak_id
FROM streak_identifier
);

SELECT 
	COUNT(*) AS games_streak
FROM streak_id
GROUP BY streak_id
ORDER BY COUNT(*) DESC
LIMIT 1