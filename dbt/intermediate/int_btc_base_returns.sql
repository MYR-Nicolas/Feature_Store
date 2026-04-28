{{ config(materialized='view') }}

SELECT
    ts,
    close_price,
    high_price,
    low_price,
    volume,
    ingested_at,
    source,

    {{ log_return('close_price', 1) }} AS logret_1m,
    {{ log_return('close_price', 5) }} AS logret_5m,
    {{ log_return('close_price', 15) }} AS logret_15m,
    {{ log_return('close_price', 60) }} AS logret_60m,

    LN(NULLIF(volume, 0)) AS log_volume

FROM {{ ref('stg_ohlcv') }}