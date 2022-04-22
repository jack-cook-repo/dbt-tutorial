{{ config(materialized='table') }}

SELECT 
ch.rental_id,
DATE_DIFF(ch.end_date, ch.start_date, SECOND) AS hire_duration,
ch.end_date AS hire_end_timestamp,
ch.end_station_id,
ch.end_station_name,
ch.start_date AS hire_start_timestamp,
ch.start_station_id,
ch.start_station_name

FROM {{ source('london_bicycle_hire', 'src_cycle_hires') }} ch
-- Only keep rows with a corresponding station
INNER JOIN {{ ref('stg_cycle_stations')}} cd_start 
    ON ch.start_station_id = cd_start.id
INNER JOIN {{ ref('stg_cycle_stations')}} cd_end 
    ON ch.end_station_id = cd_end.id
WHERE ch.end_date > ch.start_date