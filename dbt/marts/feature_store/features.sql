{{
    config(
        materialized='incremental',
        unique_key=['ts', 'version_id'],
        indexes=[
            {'columns': ['ts']},
            {'columns': ['source']},
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
    f.ts,
    v.version_id,

    CURRENT_TIMESTAMP AS computed_at,
    f.raw_ingested_at,
    f.source,

    f.logret_1m,
    f.logret_5m,
    f.logret_15m,
    f.logret_60m,

    f.lag_logret_1m_1,
    f.lag_logret_1m_5,
    f.lag_logret_1m_15,
    f.lag_logret_1m_60,

    f.vol_60m,
    f.vol_1d,
    f.vol_annualized,
    f.vol_ratio,

    f.log_volume,
    f.vol_ma_60m,
    f.vol_zscore,

    f.tenkan,
    f.kijun,
    f.span_a,
    f.span_b,

    f.tenkan_dist,
    f.kijun_dist,
    f.span_a_dist,
    f.span_b_dist,
    f.cloud_thickness,

    f.tenkan_sup_kijun,
    f.price_sup_cloud,
    f.price_sub_cloud,
    f.lag_price_sup_cloud_5,

    f.symbol,
    f.timeframe

FROM {{ ref('int_btc_features') }} f
CROSS JOIN active_version v

WHERE f.ts IS NOT NULL

{{ incremental_filter('f.ts') }}