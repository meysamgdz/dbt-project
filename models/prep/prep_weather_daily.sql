WITH daily_data AS (
    SELECT *
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day -- extract day part from date
		, DATE_PART('month', date) AS date_month -- extract month part from date
		, DATE_PART('year', date) AS date_year -- extract year part from date
		, DATE_PART('week', date) AS cw -- ISO week number from date
		, TO_CHAR(date, 'FMMonth') AS month_name -- full month name in text format
		, TO_CHAR(date, 'Day') AS weekday -- full weekday name in text format
    FROM daily_data
),
add_more_features AS (
    SELECT *
		, (CASE
			WHEN month_name IN ('December', 'January', 'February') THEN 'winter'
			WHEN month_name IN ('March', 'April', 'May') THEN 'spring'
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date
