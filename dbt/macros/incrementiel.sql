-- Adds an incremental filter after an existing WHERE clause.
{% macro incremental_filter(source_column='ts', target_column='ts') %}
    {% if is_incremental() %}
        AND {{ source_column }} > (
            SELECT COALESCE(MAX({{ target_column }}), '-infinity'::timestamptz)
            FROM {{ this }}
        )
    {% endif %}
{% endmacro %}