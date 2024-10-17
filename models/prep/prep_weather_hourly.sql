WITH hourly_data AS (
    SELECT *
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date -- only date part
		, timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- month name as a text
        , TO_CHAR(timestamp, 'Day') AS weekday -- weekday name as text
        , DATE_PART('day', timestamp) AS date_day -- numeric day of the month
		, DATE_PART('month', timestamp) AS date_month -- numeric month part
		, DATE_PART('year', timestamp) AS date_year -- numeric year part
		, DATE_PART('week', timestamp) AS cw -- ISO week number
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE
			WHEN time BETWEEN '00:00:00' AND '06:00:00' THEN 'night'
			WHEN time BETWEEN '06:00:01' AND '18:00:00' THEN 'day'
			WHEN time BETWEEN '18:00:01' AND '23:59:59' THEN 'evening'
		END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features
