{{ config(materialized='incremental', unique_key='ts') }}

SELECT
    ts,

    LN(
        LEAD(close_price, 5) OVER (ORDER BY ts)
        / NULLIF(close_price, 0)
    ) AS y_logret_5m,

    LN(
        LEAD(close_price, 15) OVER (ORDER BY ts)
        / NULLIF(close_price, 0)
    ) AS y_logret_15m,

    CASE
        WHEN ABS(
            LN(LEAD(close_price, 15) OVER (ORDER BY ts) / NULLIF(close_price, 0))
        ) >= 0.01 THEN 1
        ELSE 0
    END AS y_shock_15m

FROM {{ ref('stg_ohlcv_btc') }}

{% if is_incremental() %}
WHERE ts > (
    SELECT COALESCE(MAX(ts), '-infinity'::timestamptz)
    FROM {{ this }}
)
{% endif %}