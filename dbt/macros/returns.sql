-- Computes log return over a specified lag.
{% macro log_return(price_col, lag_value, order_col='ts') %}
    LN(
        {{ price_col }} / NULLIF(
            LAG({{ price_col }}, {{ lag_value }}) OVER (ORDER BY {{ order_col }}),
            0
        )
    )
{% endmacro %}


-- Computes future log return over a given prediction horizon.
{% macro future_log_return(price_col, horizon, order_col='ts') %}
    LN(
        LEAD({{ price_col }}, {{ horizon }}) OVER (
            ORDER BY {{ order_col }}
        ) / NULLIF({{ price_col }}, 0)
    )
{% endmacro %}