{{
    config(
        materialized='incremental',
        unique_key=['ts', 'version_id']
    )
}}

WITH active_version AS (

    SELECT version_id
    FROM {{ ref('feature_versions') }}
    WHERE is_active = true
    LIMIT 1

),

labels_base AS (

    SELECT
        s.ts,
        v.version_id,
        CURRENT_TIMESTAMP() AS computed_at,

        {{ future_log_return('s.close_price', 5) }}  AS y_logret_5m,
        {{ future_log_return('s.close_price', 15) }} AS y_logret_15m,

        {{ shock_label('s.close_price', 15, 0.01) }} AS y_shock_15m,

        s.symbol,
        s.timeframe

    FROM {{ ref('btc_ohlcv_1m_silver') }} s
    CROSS JOIN active_version v

    WHERE s.ts IS NOT NULL

    {{ incremental_filter('s.ts', 'ts') }}

)

SELECT *
FROM labels_base
WHERE y_logret_5m IS NOT NULL
  AND y_logret_15m IS NOT NULL