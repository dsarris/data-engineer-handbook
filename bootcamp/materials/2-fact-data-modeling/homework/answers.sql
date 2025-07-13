-- Task 1: A query to deduplicate `game_details` from Day 1 so there's no duplicates
SELECT 
	*
FROM (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id,team_id,player_id) AS dedup_rn
	FROM game_details 
) t WHERE dedup_rn = 1

-- Task 2:
-- A DDL for an `user_devices_cumulated` table that has:
  -- a `device_activity_datelist` which tracks a users active days by `browser_type`
  -- data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    -- or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_id TEXT,
    browser_type TEXT,
    device_activity_datelist DATE[],
    curr_date DATE,
    PRIMARY KEY(user_id,device_id,curr_date)
);

-- Task 3:
-- A cumulative query to generate `device_activity_datelist` from `events`
DROP TABLE IF EXISTS yesterday;
CREATE TEMP TABLE yesterday AS (
SELECT 
	e.user_id,
	e.device_id,
	DATE(e.event_time) AS device_activity_datelist,
	d.browser_type
FROM events e
LEFT JOIN devices d
ON e.device_id = d.device_id
WHERE DATE(event_time) = DATE('2022-12-31')
AND e.user_id IS NOT NULL AND e.device_id IS NOT NULL
GROUP BY 
	e.user_id,
	e.device_id,
	DATE(e.event_time),
	d.browser_type
);

DROP TABLE IF EXISTS today;
CREATE TEMP TABLE today AS (
SELECT 
	e.user_id,
	e.device_id,
	DATE(e.event_time) AS device_activity_datelist,
	d.browser_type
FROM events e
LEFT JOIN devices d
ON e.device_id = d.device_id
WHERE DATE(event_time) = DATE('2023-01-01')
AND e.user_id IS NOT NULL AND e.device_id IS NOT NULL
GROUP BY 
	e.user_id,
	e.device_id,
	DATE(e.event_time),
	d.browser_type    
);

INSERT INTO user_devices_cumulated (
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE 
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.device_activity_datelist]
        WHEN t.device_activity_datelist IS NULL THEN ARRAY[y.device_activity_datelist]
        ELSE ARRAY[t.device_activity_datelist] || y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(t.device_activity_datelist, y.device_activity_datelist + INTERVAL '1 day') AS curr_date
FROM today t
FULL OUTER JOIN yesterday y
    ON y.user_id = t.user_id
    AND y.device_id = t.device_id
);

CREATE OR REPLACE FUNCTION load_data_for_days()
RETURNS void AS $$
DECLARE
    date_arg DATE;
BEGIN
    FOR date_arg IN 
        SELECT generate_series('2023-01-01'::date, '2023-01-31'::date, INTERVAL '1 day') 
    LOOP

	DROP TABLE IF EXISTS yesterday;
	CREATE TEMP TABLE yesterday AS (
		SELECT * FROM user_devices_cumulated WHERE curr_date = date_arg
	);
	
	DROP TABLE IF EXISTS today;
	CREATE TEMP TABLE today AS (
	SELECT 
		e.user_id::TEXT,
		e.device_id::TEXT,
		DATE(e.event_time) AS device_activity_datelist,
		d.browser_type::TEXT
	FROM events e
	LEFT JOIN devices d
	ON e.device_id = d.device_id
	WHERE DATE(event_time) = date_arg + 1
	AND e.user_id IS NOT NULL AND e.device_id IS NOT NULL
	GROUP BY 
		e.user_id,
		e.device_id,
		DATE(e.event_time),
		d.browser_type    
	);
	
	INSERT INTO user_devices_cumulated (
	SELECT 
	    COALESCE(t.user_id, y.user_id) AS user_id,
	    COALESCE(t.device_id, y.device_id) AS device_id,
		COALESCE(t.browser_type, y.browser_type) AS browser_type,
	    CASE 
	        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.device_activity_datelist]
	        WHEN t.device_activity_datelist IS NULL THEN y.device_activity_datelist
	        ELSE ARRAY[t.device_activity_datelist] || y.device_activity_datelist
	    END AS device_activity_datelist,
	    COALESCE(t.device_activity_datelist, y.curr_date + INTERVAL '1 day') AS curr_date
	FROM today t
	FULL OUTER JOIN yesterday y
	    ON y.user_id = t.user_id
	    AND y.device_id = t.device_id
	);

    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT load_data_for_days();

-- Task 4:
-- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 
DROP TABLE IF EXISTS series;
CREATE TEMP TABLE series AS(
	SELECT * FROM generate_series('2023-01-01','2023-01-31',INTERVAL '1 day') AS series_date
);

DROP TABLE IF EXISTS placeholder_int_values;
CREATE TEMP TABLE placeholder_int_values AS (
SELECT 
	*,
	CASE WHEN device_activity_datelist @> ARRAY[series_date::DATE] 
		THEN CAST(POW(2,32 - EXTRACT('DAY' FROM curr_date - series_date)) AS BIGINT) ELSE 0 END AS placeholder_int_value
FROM user_devices_cumulated 
CROSS JOIN series
);

SELECT 
	user_id,
	device_id,
	SUM(placeholder_int_value)::BIGINT::BIT(32) AS datelist_int,
	BIT_COUNT(SUM(placeholder_int_value)::BIGINT::BIT(32)) AS days_active
FROM placeholder_int_values
GROUP BY 
	user_id,
	device_id

-- Task 5:
-- A DDL for `hosts_cumulated` table 
  -- a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    curr_date DATE,
    PRIMARY KEY(host,curr_date)
);

-- Task 6:  
-- The incremental query to generate `host_activity_datelist`
DROP TABLE IF EXISTS yesterday;
CREATE TEMP TABLE yesterday AS (
SELECT 
	e.host,
	DATE(e.event_time) AS host_activity_datelist
FROM events e
WHERE DATE(event_time) = DATE('2022-12-31')
GROUP BY 
	e.host,
	DATE(e.event_time)
);

DROP TABLE IF EXISTS today;
CREATE TEMP TABLE today AS (
SELECT 
	e.host,
	DATE(e.event_time) AS host_activity_datelist
FROM events e
WHERE DATE(event_time) = DATE('2023-01-01')
GROUP BY 
	e.host,
	DATE(e.event_time)
);

INSERT INTO hosts_cumulated (
SELECT 
    COALESCE(t.host, y.host) AS host,
    CASE 
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.host_activity_datelist]
        WHEN t.host_activity_datelist IS NULL THEN ARRAY[y.host_activity_datelist]
        ELSE ARRAY[t.host_activity_datelist] || y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(t.host_activity_datelist, y.host_activity_datelist + INTERVAL '1 day') AS curr_date
FROM today t
FULL OUTER JOIN yesterday y
    ON y.host = t.host
);

CREATE OR REPLACE FUNCTION load_hosts_for_days()
RETURNS void AS $$
DECLARE
    date_arg DATE;
BEGIN
    FOR date_arg IN 
        SELECT generate_series('2023-01-01'::date, '2023-01-31'::date, INTERVAL '1 day') 
    LOOP

	DROP TABLE IF EXISTS yesterday;
	CREATE TEMP TABLE yesterday AS (
		SELECT * FROM hosts_cumulated WHERE curr_date = date_arg
	);
	
	DROP TABLE IF EXISTS today;
	CREATE TEMP TABLE today AS (
	SELECT 
		e.host::TEXT,
		DATE(e.event_time) AS host_activity_datelist
	FROM events e
	WHERE DATE(event_time) = date_arg + 1
	GROUP BY 
		e.host,
		DATE(e.event_time)
	);
	
	INSERT INTO hosts_cumulated (
	SELECT 
	    COALESCE(t.host, y.host) AS host,
	    CASE 
	        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.host_activity_datelist]
	        WHEN t.host_activity_datelist IS NULL THEN ARRAY[y.host_activity_datelist]
	        ELSE ARRAY[t.host_activity_datelist] || y.host_activity_datelist
	    END AS host_activity_datelist,
	    COALESCE(t.host_activity_datelist, y.curr_date + INTERVAL '1 day') AS curr_date
	FROM today t
	FULL OUTER JOIN yesterday y
	    ON y.host = t.host
	);

    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT load_hosts_for_days();

-- Task 7:
-- A monthly, reduced fact table DDL `host_activity_reduced`
   -- month
   -- host
   -- hit_array - think COUNT(1)
   -- unique_visitors array -  think COUNT(DISTINCT user_id)
DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE host_activity_reduced (
    host TEXT,
	month_start DATE,
    hits_array REAL[],
    unique_visitors_array REAL[],
	PRIMARY KEY (host,month_start)
);

-- Task 8:
-- An incremental query that loads `host_activity_reduced`
  -- day-by-day
INSERT INTO host_activity_reduced (
SELECT 
	host,
	DATE(DATE_TRUNC('month',DATE(event_time))) AS month_start,
	ARRAY[COUNT(*)] AS hits_array,
	ARRAY[COUNT(DISTINCT(user_id))] AS unique_visitors_array
FROM events
WHERE DATE(event_time) = '2023-01-01'
GROUP BY 1,2
);


CREATE OR REPLACE FUNCTION load_daily_metrics_for_days()
RETURNS void AS $$
DECLARE
    date_arg DATE;
BEGIN
    FOR date_arg IN 
        SELECT generate_series('2023-01-02'::date, '2023-01-31'::date, INTERVAL '1 day') 
    LOOP

	DROP TABLE IF EXISTS yesterday;
	CREATE TEMP TABLE yesterday AS (
	SELECT 
		*
	FROM host_activity_reduced
	);
	
	DROP TABLE IF EXISTS today;
	CREATE TEMP TABLE today AS (
	SELECT 
		host,
		COUNT(*) AS hits_array,
		COUNT(DISTINCT(user_id)) AS unique_visitors_array,
		DATE(event_time) AS curr_date,
		DATE(DATE_TRUNC('month',DATE(event_time))) AS month_start
	FROM events
	WHERE DATE(event_time) = date_arg 
		GROUP BY 1,4
	);
	
	INSERT INTO host_activity_reduced
	SELECT 
	    COALESCE(t.host, y.host) AS host,
	    COALESCE(t.month_start, y.month_start) AS month_start,
	    CASE 
	        WHEN y.hits_array IS NOT NULL THEN y.hits_array || ARRAY[COALESCE(t.hits_array, 0)]
	        WHEN y.hits_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(t.curr_date - y.month_start, 0)]) || ARRAY[COALESCE(t.hits_array, 0)]
	    END AS hits_array,
	    CASE 
	        WHEN y.unique_visitors_array IS NOT NULL THEN y.unique_visitors_array || ARRAY[COALESCE(t.unique_visitors_array, 0)]
	        WHEN y.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(t.curr_date - y.month_start, 0)]) || ARRAY[COALESCE(t.unique_visitors_array, 0)]
	    END AS unique_visitors_array
	FROM today t
	FULL OUTER JOIN yesterday y
	    ON t.host = y.host
	ON CONFLICT (host, month_start)
	DO UPDATE SET 
	    hits_array = EXCLUDED.hits_array,
	    unique_visitors_array = EXCLUDED.unique_visitors_array;

    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT load_daily_metrics_for_days();
