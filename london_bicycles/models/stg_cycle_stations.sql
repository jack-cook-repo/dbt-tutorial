{{ config(materialized='table') }}

SELECT
id,
latitude,
longitude,
name,
docks_count,
install_date,
removal_date

FROM {{ source('london_bicycle_hire', 'src_cycle_stations')}} 