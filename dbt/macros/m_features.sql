{% macro safe_divide(numerator, denominator) %}
    {{ numerator }} / NULLIF({{ denominator }}, 0)
{% endmacro %}


-- Computes log return over a specified lag
=
{% macro log_return(price_col, lag_value, order_col='ts') %}
    LN(
        {{ price_col }} / NULLIF(
            LAG({{ price_col }}, {{ lag_value }}) OVER (ORDER BY {{ order_col }}),
            0
        )
    )
{% endmacro %}


-- Computes rolling standard deviation (volatility)

{% macro rolling_std(column_name, window_size, order_col='ts') %}
    STDDEV_SAMP({{ column_name }}) OVER (
        ORDER BY {{ order_col }}
        ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}



-- Computes rolling average

{% macro rolling_avg(column_name, window_size, order_col='ts') %}
    AVG({{ column_name }}) OVER (
        ORDER BY {{ order_col }}
        ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}


-- Computes lagged value of a column

{% macro lag_feature(column_name, lag_value, order_col='ts') %}
    LAG({{ column_name }}, {{ lag_value }}) OVER (ORDER BY {{ order_col }})
{% endmacro %}



-- Computes Ichimoku midpoint (used for Tenkan, Kijun, Span B)

{% macro ichimoku_midpoint(high_col, low_col, window_size, order_col='ts') %}
    (
        MAX({{ high_col }}) OVER (
            ORDER BY {{ order_col }}
            ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
        )
        +
        MIN({{ low_col }}) OVER (
            ORDER BY {{ order_col }}
            ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
        )
    ) / 2
{% endmacro %}



-- Converts a boolean condition into a binary signal (0 or 1)

{% macro binary_signal(condition) %}
    CASE WHEN {{ condition }} THEN 1 ELSE 0 END::SMALLINT
{% endmacro %}