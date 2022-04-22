{% test all_values_equal_to(model, column_name, equal_to) %}

-- Returns any records where the column doesn't match a given value 
SELECT
*
FROM {{ model }}
WHERE {{ column_name }} != {{ equal_to }}

{% endtest %}