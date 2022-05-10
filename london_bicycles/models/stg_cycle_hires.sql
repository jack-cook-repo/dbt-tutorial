{{ config(materialized='table') }}

SELECT
    ch.rental_id,
    ch.start_date AS hire_start_timestamp,
    ch.start_station_id,
    ch.start_station_name,
    cs_start.area AS start_station_area,
    ch.end_date AS hire_end_timestamp,
    ch.end_station_id,
    ch.end_station_name,
    cs_end.area AS end_station_area,
    DATE_DIFF(ch.end_date, ch.start_date, SECOND) AS hire_duration

FROM {{ source('london_bicycle_hire', 'src_cycle_hires') }} AS ch
-- Only keep rows with a corresponding station
INNER JOIN {{ ref('stg_cycle_stations') }} AS cs_start
    ON ch.start_station_id = cs_start.id
INNER JOIN {{ ref('stg_cycle_stations') }} AS cs_end
    ON ch.end_station_id = cs_end.id
WHERE ch.end_date > ch.start_date
