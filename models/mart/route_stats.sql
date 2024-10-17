WITH routes AS (
    SELECT origin,
           dest,
           COUNT(*) AS n_flights_per_route,
           COUNT(DISTINCT tail_number) AS arr_tail,
           COUNT(DISTINCT airline) AS arr_nunique_airlines,
           AVG(actual_elapsed_time) AS avg_elapsed_time,
           AVG(arr_delay) AS avg_arr_delay,
           MAX(arr_delay) AS max_arr_delay,
           MIN(arr_delay) AS min_arr_delay,
           SUM(cancelled) AS tot_cancelled,
           SUM(diverted) AS tot_diverted
    FROM {{ref("prep_flights")}}
    GROUP BY origin, dest
)
-- First subquery to join prep_airports on destination
, routes_joined AS (
    SELECT city AS dest_city,
           country AS dest_country,
           name AS dest_name,
           routes.*
    FROM {{ref("prep_airports")}}
    JOIN routes
    ON routes.dest = {{ref("prep_airports")}}.faa
)
-- Outer query to join on origin
SELECT city AS origin_city,
       country AS origin_country,
       name AS origin_name,
       routes_joined.*
FROM {{ref("prep_airports")}}
JOIN routes_joined
ON routes_joined.origin = {{ref("prep_airports")}}.faa
ORDER BY origin_city

