-- Generates a binary shock label based on future return magnitude.
{% macro shock_label(price_col, horizon, threshold, order_col='ts') %}
    CASE
        WHEN ABS(
            LN(
                LEAD({{ price_col }}, {{ horizon }}) OVER (
                    ORDER BY {{ order_col }}
                ) / NULLIF({{ price_col }}, 0)
            )
        ) >= {{ threshold }}
        THEN 1
        ELSE 0
    END::SMALLINT
{% endmacro %}


-- Ensures labels are computed only when future data is available.
{% macro valid_label_horizon(ts_col='ts', horizon_minutes=15) %}
    {{ ts_col }} <= now() - INTERVAL '{{ horizon_minutes }} minutes'
{% endmacro %}