{% test column_is_identical_to(model, column_name, reference_model, reference_column) %}

-- Returns any records where values in either column aren't present in both
SELECT
COALESCE(m.{{ column_name }}, r.{{ reference_column }}) AS mismatched_column
FROM {{ model }} m
FULL OUTER JOIN {{ reference_model }} r
    ON m.{{ column_name }} = r.{{ reference_column }}
WHERE m.{{ column_name }} != r.{{ reference_column }}

{% endtest %}