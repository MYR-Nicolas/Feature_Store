{{
    config(
        materialized='incremental',
        unique_key=['ts', 'version_id'],
        indexes=[
            {'columns': ['ts']},
            {'columns': ['version_id', 'ts']}
        ]
    )
}}

WITH active_version AS (

    SELECT version_id
    FROM feature_store.feature_versions
    WHERE is_active = true
    LIMIT 1

)

SELECT
    s.ts,
    v.version_id,

    CURRENT_TIMESTAMP AS computed_at,

    {{ future_log_return('s.close_price', 5) }}  AS y_logret_5m,
    {{ future_log_return('s.close_price', 15) }} AS y_logret_15m,

    {{ shock_label('s.close_price', 15, 0.01) }} AS y_shock_15m,

    s.symbol,
    s.timeframe

FROM {{ ref('stg_ohlcv') }} s
CROSS JOIN active_version v

WHERE {{ valid_label_horizon('s.ts', 15) }}

{{ incremental_filter('s.ts') }}