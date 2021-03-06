{{ config(
    materialized='table',
    partition_by={
      "field": "hire_start_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    })    
}}

WITH cycle_hires AS (
    SELECT
        rental_id,
        hire_duration,
        hire_end_timestamp,
        hire_start_timestamp,
        start_station_id,
        start_station_name,
        start_station_area,
        end_station_id,
        end_station_name,
        end_station_area

    FROM {{ ref('stg_cycle_hires') }}
),

cycle_stations AS (
    SELECT
        id,
        latitude,
        longitude,
        removal_date,
        ST_GEOGPOINT(longitude, latitude) AS geopoint

    FROM {{ ref('stg_cycle_stations') }}
),

joined AS (
    SELECT
        ch.rental_id,
        ch.hire_start_timestamp,
        ch.hire_end_timestamp,
        ch.start_station_name,
        ch.start_station_area,
        cs_start.latitude AS start_station_latitude,
        cs_start.longitude AS start_station_longitude,
        ch.end_station_name,
        ch.end_station_area,
        cs_end.latitude AS end_station_latitude,
        cs_end.longitude AS end_station_longitude,
        EXTRACT(HOUR FROM hire_start_timestamp) AS hire_start_hour,
        EXTRACT(DAYOFWEEK FROM hire_start_timestamp) AS hire_start_weekday,
        ROUND(CAST(ch.hire_duration / 60 AS NUMERIC), 2) AS hire_duration_mins,
        ROUND(
            CAST(
                ST_DISTANCE(
                    cs_start.geopoint, cs_end.geopoint
                ) / 1000 AS NUMERIC
            ),
            3
        ) AS journey_distance_direct_km

    FROM cycle_hires AS ch
    LEFT JOIN cycle_stations AS cs_start
        ON ch.start_station_id = cs_start.id
    LEFT JOIN cycle_stations AS cs_end
        ON ch.end_station_id = cs_end.id
),

manhattan_prelim AS (
    -- Columns used for calculating manhattan distance
    -- See https://gis.stackexchange.com/a/142327 for logic used in this section
    SELECT
        *,
        -- 1 degree is 0.0174533 radians
        -- Longitude lines are further apart the closer you get to the equator (COS(latitude) gets closer to 1)
        -- Latitude lines are always between -90 and 90 degrees, so the COS value is never negative
        COS(start_station_latitude * 0.0174533) AS cosine_start_latitude,
        ABS(
            start_station_longitude - end_station_longitude
        ) AS abs_longitude_diff,
        ABS(start_station_latitude - end_station_latitude) AS abs_latitude_diff

    FROM joined
),

manhattan_calc AS (
    SELECT
        * EXCEPT(cosine_start_latitude, abs_longitude_diff, abs_latitude_diff),
        -- Latitude lines are ~111km apart, longitude lines are 111 * COS(latitude in radians) apart
        ROUND(
            111 * (
                (cosine_start_latitude * abs_longitude_diff) + abs_latitude_diff
            ),
            3
        ) AS journey_distance_manhattan_km

    FROM manhattan_prelim
)

SELECT
    * EXCEPT(journey_distance_direct_km, hire_start_weekday),
    /*
    ~48k edge cases (out of ~22m rows) where the direct distance is
    less than the manhattan distance by <0.02km. Likely caused by
    some minor precision errors with our assumptions (e.g. 111km per latitude line)
    */
    LEAST(
        journey_distance_manhattan_km, journey_distance_direct_km
    ) AS journey_distance_direct_km,
    -- Hack to shift Sunday from 1 to 7, keeping Saturday/Sunday grouped > 5
    IF(hire_start_weekday = 1, 7, hire_start_weekday - 1) AS hire_start_weekday

FROM manhattan_calc
