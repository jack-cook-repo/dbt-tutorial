{% test on_or_after_date(model, column_name, start_date) %}

-- Returns any records where the column is not after the start date
-- Exclude any cases where 1 column is NULL
WITH non_null_dates AS (
    SELECT
    {{ column_name }} AS end_col,
    {{ start_date }} AS start_col

    FROM {{ model}}
    WHERE {{ column_name }} IS NOT NULL
        AND {{ start_date }} IS NOT NULL
)
SELECT
*
FROM non_null_dates
WHERE end_col < start_col

{% endtest %}