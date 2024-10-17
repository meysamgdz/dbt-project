WITH flights_one_month AS (
    SELECT *
    FROM {{ref('staging_flights_one_month')}}
),
flights_cleaned AS (
    SELECT flight_date::DATE
           ,TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time -- departure time formatted as TIME
           ,TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time -- scheduled departure time formatted as TIME
           ,dep_delay
		   ,(dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval -- departure delay as INTERVAL
           ,TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time -- arrival time formatted as TIME
           ,TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time -- scheduled arrival time formatted as TIME
           ,arr_delay
           ,(arr_delay * '1 minute'::INTERVAL) AS arr_delay_interval -- arrival delay as INTERVAL
           ,airline
           ,tail_number
           ,flight_number
           ,origin
           ,dest
           ,air_time
           ,(air_time * '1 minute'::INTERVAL) AS air_time_interval -- airtime as INTERVAL
           ,actual_elapsed_time
           ,(actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval -- actual elapsed time as INTERVAL
           ,(distance * 1.60934)::NUMERIC(6,2) AS distance_km -- distance converted to kilometers
           ,cancelled
           ,diverted
    FROM flights_one_month
)
SELECT * FROM flights_cleaned
