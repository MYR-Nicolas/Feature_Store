{{ config(materialized='view') }}

SELECT
    *,

    (
        MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)
        +
        MIN(low_price) OVER (ORDER BY ts ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)
    ) / 2 AS tenkan,

    (
        MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)
        +
        MIN(low_price) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)
    ) / 2 AS kijun,

    (
        MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
        +
        MIN(low_price) OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
    ) / 2 AS span_b

FROM {{ ref('int_btc_base_returns') }}