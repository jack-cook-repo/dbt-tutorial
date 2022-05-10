{{ config(materialized='table') }}

WITH name_splits AS (
    -- Grab source data and split station names by comma delimiter
    SELECT
        id,
        latitude,
        longitude,
        name,
        docks_count,
        install_date,
        removal_date,
        -- Stations are usually in the format of Road, Area
        -- e.g. Salmon Lane, Limehouse
        SPLIT(name, ',') AS name_split

    FROM {{ source('london_bicycle_hire', 'src_cycle_stations') }}
)

-- Parse the split station name to get the area
SELECT
    * EXCEPT(name_split),
    /*
    - Some stations have no split, e.g. Imperial Wharf Station
    - Some stations have >1 split, e.g. Clapham Road, Lingham Street, Stockwell
    - Reversing the array and taking the first element handles all cases
    */
    TRIM(ARRAY_REVERSE(name_split)[OFFSET(0)]) AS area

FROM name_splits
