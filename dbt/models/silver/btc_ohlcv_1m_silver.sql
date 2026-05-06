{{ config(materialized='view') }}

SELECT
    symbol,
    open_time AS ts,

    '1m' AS timeframe,

    CAST(open AS FLOAT64) AS open_price,
    CAST(high AS FLOAT64) AS high_price,
    CAST(low AS FLOAT64) AS low_price,
    CAST(close AS FLOAT64) AS close_price,
    CAST(volume AS FLOAT64) AS volume,

    source,
    extracted_at AS ingested_at,

    TRUE AS is_closed

FROM {{ source('biglake_bronze', 'btc_ohlcv_1m_bronze') }}

WHERE open_time IS NOT NULL
  AND close IS NOT NULL
  AND high >= low