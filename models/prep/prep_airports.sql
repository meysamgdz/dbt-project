WITH airports_reorder AS (
    SELECT faa
           , name  -- Airport name
           , lat   -- Latitude of the airport
           , lon   -- Longitude of the airport
           , alt   -- Altitude of the airport
           , city  -- City where the airport is located
           , country -- country where the airport is located
           , region
           , tz
           , dst
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder
