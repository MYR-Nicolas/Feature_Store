{{ config(materialized='view') }}

WITH feat AS (
    SELECT
        *,
        (tenkan + kijun) / 2 AS span_a,

        STDDEV_SAMP(logret_1m) OVER (
            ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS vol_60m,

        STDDEV_SAMP(logret_1m) OVER (
            ORDER BY ts ROWS BETWEEN 1439 PRECEDING AND CURRENT ROW
        ) AS vol_1d,

        AVG(volume) OVER (
            ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS vol_ma_60m,

        STDDEV_SAMP(volume) OVER (
            ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) AS vol_std_60m

    FROM {{ ref('int_btc_ichimoku') }}
)

SELECT
    ts,
    ingested_at AS raw_ingested_at,
    source,

    logret_1m,
    logret_5m,
    logret_15m,
    logret_60m,

    LAG(logret_1m, 1)  OVER (ORDER BY ts) AS lag_logret_1m_1,
    LAG(logret_1m, 5)  OVER (ORDER BY ts) AS lag_logret_1m_5,
    LAG(logret_1m, 15) OVER (ORDER BY ts) AS lag_logret_1m_15,
    LAG(logret_1m, 60) OVER (ORDER BY ts) AS lag_logret_1m_60,

    vol_60m,
    vol_1d,
    vol_60m * SQRT(525600) AS vol_annualized,
    vol_60m / NULLIF(vol_1d, 0) AS vol_ratio,

    log_volume,
    vol_ma_60m,
    (volume - vol_ma_60m) / NULLIF(vol_std_60m, 0) AS vol_zscore,

    tenkan,
    kijun,
    span_a,
    span_b,

    (close_price - tenkan) / NULLIF(close_price, 0) AS tenkan_dist,
    (close_price - kijun)  / NULLIF(close_price, 0) AS kijun_dist,
    (close_price - span_a) / NULLIF(close_price, 0) AS span_a_dist,
    (close_price - span_b) / NULLIF(close_price, 0) AS span_b_dist,

    ABS(span_a - span_b) / NULLIF(close_price, 0) AS cloud_thickness,

    CASE WHEN tenkan > kijun THEN 1 ELSE 0 END::SMALLINT AS tenkan_sup_kijun,
    CASE WHEN close_price > GREATEST(span_a, span_b) THEN 1 ELSE 0 END::SMALLINT AS price_sup_cloud,
    CASE WHEN close_price < LEAST(span_a, span_b) THEN 1 ELSE 0 END::SMALLINT AS price_sub_cloud,

    LAG(
        CASE WHEN close_price > GREATEST(span_a, span_b) THEN 1 ELSE 0 END,
        5
    ) OVER (ORDER BY ts)::SMALLINT AS lag_price_sup_cloud_5

FROM feat