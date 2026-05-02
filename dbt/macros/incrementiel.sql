-- Adds an incremental filter after an existing WHERE clause.
{% macro incremental_filter(source_column='ts', target_column='ts') %}
    {% if is_incremental() %}
        AND {{ source_column }} > (
            SELECT COALESCE(MAX({{ target_column }}), TIMESTAMP('1970-01-01'))
            FROM {{ this }}
        )
    {% endif %}
{% endmacro %}