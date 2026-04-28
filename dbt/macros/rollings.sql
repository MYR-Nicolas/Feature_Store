-- Computes rolling standard deviation over a fixed window.
{% macro rolling_std(column_name, window_size, order_col='ts') %}
    STDDEV_SAMP({{ column_name }}) OVER (
        ORDER BY {{ order_col }}
        ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}


-- Computes rolling average over a fixed window.
{% macro rolling_avg(column_name, window_size, order_col='ts') %}
    AVG({{ column_name }}) OVER (
        ORDER BY {{ order_col }}
        ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}


-- Computes a lagged value of a column.
{% macro lag_feature(column_name, lag_value, order_col='ts') %}
    LAG({{ column_name }}, {{ lag_value }}) OVER (ORDER BY {{ order_col }})
{% endmacro %}