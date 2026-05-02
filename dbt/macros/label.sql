-- Generates a binary shock label based on future return magnitude.
{% macro shock_label(price_col, horizon, threshold, order_col='ts') %}
    CAST(
        CASE
            WHEN ABS(
                LN(
                    SAFE_DIVIDE(
                        LEAD({{ price_col }}, {{ horizon }}) OVER (ORDER BY {{ order_col }}),
                        NULLIF({{ price_col }}, 0)
                    )
                )
            ) >= {{ threshold }}
            THEN 1
            ELSE 0
        END
    AS INT64)
{% endmacro %}


-- Ensures labels are computed only when future data is available.
{% macro valid_label_horizon(ts_col='ts', horizon_minutes=15) %}
    {{ ts_col }} <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ horizon_minutes }} MINUTE)
{% endmacro %}
