{{ config(materialized='view') }}

SELECT
    *,

    {{ ichimoku_midpoint('high_price', 'low_price', 9) }} AS tenkan,
    {{ ichimoku_midpoint('high_price', 'low_price', 26) }} AS kijun,
    {{ ichimoku_midpoint('high_price', 'low_price', 52) }} AS span_b

FROM {{ ref('int_btc_base_returns') }}