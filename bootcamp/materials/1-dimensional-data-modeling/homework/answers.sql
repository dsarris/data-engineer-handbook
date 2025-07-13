DROP TYPE IF EXISTS film_stats CASCADE;
DROP TABLE IF EXISTS actors CASCADE;
DROP TABLE IF EXISTS actors_history_scd CASCADE;

CREATE TYPE film_stats AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

-- Task 1
CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films film_stats[],
	quality_class TEXT,
	is_active BOOLEAN,
	current_season INTEGER,
	PRIMARY KEY(actorid,current_season)
);

INSERT INTO actors
		WITH last_year AS (
			SELECT 
				actor,
				actorid,
				ARRAY_AGG(ROW(film,votes,rating,filmid)::film_stats) AS films,
				CASE
					WHEN SUM(votes*rating)/SUM(votes) > 8 THEN 'star'
					WHEN SUM(votes*rating)/SUM(votes) > 7 AND SUM(votes*rating)/SUM(votes) <= 8 THEN 'good'
					WHEN SUM(votes*rating)/SUM(votes) > 6 AND SUM(votes*rating)/SUM(votes) <= 7 THEN 'average'
					ELSE 'bad'
				END AS quality_class,
				TRUE AS is_active,
				1969 AS current_season
			FROM actor_films
			WHERE year = 1969
			GROUP BY actor,actorid
		),
			current_year AS (
				SELECT 
					actor,
					actorid,
					ARRAY_AGG(ROW(film,votes,rating,filmid)::film_stats) AS films,
					CASE
						WHEN SUM(votes*rating)/SUM(votes) > 8 THEN 'star'
						WHEN SUM(votes*rating)/SUM(votes) > 7 AND SUM(votes*rating)/SUM(votes) <= 8 THEN 'good'
						WHEN SUM(votes*rating)/SUM(votes) > 6 AND SUM(votes*rating)/SUM(votes) <= 7 THEN 'average'
						ELSE 'bad'
					END AS quality_class,
					TRUE AS is_active,
					1970 AS current_season
				FROM actor_films
				WHERE year = 1970
				GROUP BY actor,actorid
			)
			SELECT 
				COALESCE(c.actor,l.actor) AS actor,
				COALESCE(c.actorid,l.actorid) AS actorid,
				COALESCE(c.films || l.films,l.films) AS films,
				COALESCE(c.quality_class,l.quality_class) AS quality_class,
				COALESCE(c.is_active,FALSE) AS is_active,
				COALESCE(c.current_season,l.current_season+1) AS current_season
			FROM current_year c
			FULL OUTER JOIN last_year l
			ON c.actorid = l.actorid;

CREATE OR REPLACE FUNCTION load_data_for_years()
RETURNS void AS $$
DECLARE
    year_arg INTEGER;
BEGIN
    FOR year_arg IN 1970..2020 LOOP
	INSERT INTO actors
		WITH last_year AS (
			SELECT 
				*
			FROM actors
			WHERE current_season = year_arg
		),
			current_year AS (
				SELECT 
					actor,
					actorid,
					ARRAY_AGG(ROW(film,votes,rating,filmid)::film_stats) AS films,
					CASE
						WHEN SUM(votes*rating)/SUM(votes) > 8 THEN 'star'
						WHEN SUM(votes*rating)/SUM(votes) > 7 AND SUM(votes*rating)/SUM(votes) <= 8 THEN 'good'
						WHEN SUM(votes*rating)/SUM(votes) > 6 AND SUM(votes*rating)/SUM(votes) <= 7 THEN 'average'
						ELSE 'bad'
					END AS quality_class,
					TRUE AS is_active,
					year_arg+1 AS current_season
				FROM actor_films
				WHERE year = year_arg + 1
				GROUP BY actor,actorid
			)
			SELECT 
				COALESCE(c.actor,l.actor) AS actor,
				COALESCE(c.actorid,l.actorid) AS actorid,
				COALESCE(c.films || l.films,l.films) AS films,
				COALESCE(c.quality_class,l.quality_class) AS quality_class,
				COALESCE(c.is_active,FALSE) AS is_active,
				COALESCE(c.current_season,l.current_season+1) AS current_season
			FROM current_year c
			FULL OUTER JOIN last_year l
			ON c.actorid = l.actorid;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Task 2
SELECT load_data_for_years();

-- Task 3
CREATE TABLE actors_history_scd (
	actor TEXT,
	actorid TEXT,
	quality_class TEXT,
	is_active BOOLEAN,
	current_season TEXT,
	start_date TEXT,
	end_date TEXT,
	PRIMARY KEY(actorid,start_date)
);

-- Task 4
INSERT INTO actors_history_scd (
WITH actors_previous_status AS (
	SELECT
		actor,
		actorid,
		quality_class,
		is_active,
		LAG(quality_class,1) OVER(PARTITION BY actorid ORDER BY current_season ASC) AS previous_quality_class,
		LAG(is_active,1) OVER(PARTITION BY actorid ORDER BY current_season ASC) AS previous_is_active,
		current_season
	FROM actors 
),
actors_with_indicators AS (
	SELECT 
		*,
		CASE
			WHEN quality_class != previous_quality_class THEN 1 
			WHEN is_active != previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM actors_previous_status
),
actors_with_streaks AS (
	SELECT 
		*,
		SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY current_season) AS indicator_streak
	FROM actors_with_indicators
)

SELECT 
	actor,
	actorid,
	quality_class,
	is_active,
	'2021' AS current_season,
	MIN(current_season) AS start_date,
	MAX(current_season) AS end_date
FROM actors_with_streaks
GROUP BY 
	actor,
	actorid,
	indicator_streak,
	quality_class,
	is_active
);

DROP TYPE IF EXISTS scd_type CASCADE;
CREATE TYPE scd_type AS (
	quality_class TEXT,
	is_active BOOLEAN,
	start_date TEXT,
	end_date TEXT
);

INSERT INTO actors_history_scd
WITH last_season_scd AS (
	SELECT * 
	FROM actors_history_scd
	WHERE current_season = '2021'
	AND end_date = '2021'
),
historical_scd AS (
	SELECT 
		actor,
		actorid,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM actors_history_scd
	WHERE current_season = '2021'
	AND end_date < '2021'
),
this_season AS (
	SELECT * 
	FROM actors
	WHERE current_season = '2022'
),
unchanged_records AS (
	SELECT 
		ts.actor,
		ts.actorid,
		ts.quality_class,
		ts.is_active,
		ls.start_date,
		ts.current_season AS end_date
	FROM this_season ts
	JOIN last_season_scd ls
	ON ts.actorid = ls.actorid
	WHERE ts.quality_class = ls.quality_class
	AND ts.is_active = ls.is_active
),
changed_records AS (
	SELECT 
		ts.actor,
		ts.actorid,
		UNNEST(ARRAY[
			ROW(
				ts.quality_class,
				ts.is_active,
				ts.current_season,
				ts.current_season
				)::scd_type,
			ROW(
				ls.quality_class,
				ls.is_active,
				ls.start_date,
				ls.end_date
				)::scd_type
			]
		) AS records
	FROM this_season ts
	LEFT JOIN last_season_scd ls
	ON ts.actorid = ls.actorid
	WHERE (ts.quality_class != ls.quality_class
	OR ts.is_active != ls.is_active)
),
unnested_changed_records AS (
	SELECT
		actor,
		actorid,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	FROM changed_records
),
new_records AS (
	SELECT 
		ts.actor,
		ts.actorid,
		ts.quality_class,
		ts.is_active,
		ts.current_season AS start_date,
		ts.current_season AS end_date
	FROM this_season ts
	LEFT JOIN last_season_scd ls
	ON ts.actorid = ls.actorid
	WHERE ls.actorid IS NULL
)

-- Task 5
SELECT 
	*, 
	'2022' AS current_season 
	FROM (
	  SELECT 
	  	actor,
		actorid,
		quality_class,
		is_active,
		start_date::TEXT,
		end_date::TEXT
	  FROM historical_scd

	  UNION ALL

	  SELECT 
	  	actor,
		actorid,
		quality_class,
		is_active,
		start_date::TEXT,
		end_date::TEXT
	  FROM unchanged_records

	  UNION ALL

	  SELECT 
	  	actor,
		actorid,
		quality_class,
		is_active,
		start_date::TEXT,
		end_date::TEXT
	  FROM unnested_changed_records

	  UNION ALL

	  SELECT 
	  	actor,
		actorid,
		quality_class,
		is_active,
		start_date::TEXT,
		end_date::TEXT
	  FROM new_records
	) a