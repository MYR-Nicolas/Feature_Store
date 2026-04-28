-- Adds an incremental filter after an existing WHERE clause.
{% macro incremental_filter(column_name='ts') %}
    {% if is_incremental() %}
        AND {{ column_name }} > (
            SELECT COALESCE(MAX({{ column_name }}), '-infinity'::timestamptz)
            FROM {{ this }}
        )
    {% endif %}
{% endmacro %}