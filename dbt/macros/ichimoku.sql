-- Computes Ichimoku midpoint used for Tenkan, Kijun, and Span B.
{% macro ichimoku_midpoint(high_col, low_col, window_size, order_col='ts') %}
    SAFE_DIVIDE(
        MAX({{ high_col }}) OVER (
            ORDER BY {{ order_col }}
            ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
        )
        +
        MIN({{ low_col }}) OVER (
            ORDER BY {{ order_col }}
            ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
        ),
        2
    )
{% endmacro %}