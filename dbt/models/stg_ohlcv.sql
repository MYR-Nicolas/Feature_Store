{{ config(materialized='view') }}

SELECT
    symbol,
    ts,
    timeframe,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    source,
    ingested_at,
    is_closed
FROM {{ source('raw', 'ohlcv') }}
WHERE symbol = 'BTCUSDT'
  AND timeframe = '1m'
  AND is_closed = true