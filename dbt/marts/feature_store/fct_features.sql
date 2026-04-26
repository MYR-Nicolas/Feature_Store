{{
    config(
        materialized='incremental',
        unique_key='ts',
        indexes=[
            {'columns': ['ts']},
            {'columns': ['source']}
        ]
    )
}}

SELECT
    ts,
    raw_ingested_at,
    source,

    logret_1m,
    logret_5m,
    logret_15m,
    logret_60m,

    lag_logret_1m_1,
    lag_logret_1m_5,
    lag_logret_1m_15,
    lag_logret_1m_60,

    vol_60m,
    vol_1d,
    vol_annualized,
    vol_ratio,

    log_volume,
    vol_ma_60m,
    vol_zscore,

    tenkan,
    kijun,
    span_a,
    span_b,

    tenkan_dist,
    kijun_dist,
    span_a_dist,
    span_b_dist,
    cloud_thickness,

    tenkan_sup_kijun,
    price_sup_cloud,
    price_sub_cloud,
    lag_price_sup_cloud_5

FROM {{ ref('int_btc_features') }}

WHERE ts IS NOT NULL

{% if is_incremental() %}
  AND ts > (
      SELECT COALESCE(MAX(ts), '-infinity'::timestamptz)
      FROM {{ this }}
  )
{% endif %}