{{ config(materialized='view') }}

SELECT
    ts,
    close_price,
    high_price,
    low_price,
    volume,
    ingested_at,
    source,

    LN(close_price / NULLIF(LAG(close_price, 1)  OVER w, 0)) AS logret_1m,
    LN(close_price / NULLIF(LAG(close_price, 5)  OVER w, 0)) AS logret_5m,
    LN(close_price / NULLIF(LAG(close_price, 15) OVER w, 0)) AS logret_15m,
    LN(close_price / NULLIF(LAG(close_price, 60) OVER w, 0)) AS logret_60m,

    LN(NULLIF(volume, 0)) AS log_volume

FROM {{ ref('stg_ohlcv_btc') }}

WINDOW w AS (ORDER BY ts)