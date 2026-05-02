-- Converts a boolean condition into a binary signal.
{% macro binary_signal(condition) %}
    CAST(CASE WHEN {{ condition }} THEN 1 ELSE 0 END AS INT64)
{% endmacro %}
