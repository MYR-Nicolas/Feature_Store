{{ config(materialized='view') }}

WITH feat AS (

    SELECT
        *,

        -- Ichimoku Span A
        (tenkan + kijun) / 2 AS span_a,

        -- Volatility
        {{ rolling_std('logret_1m', 60) }}   AS vol_60m,
        {{ rolling_std('logret_1m', 1440) }} AS vol_1d,

        -- Volume statistics
        {{ rolling_avg('volume', 60) }} AS vol_ma_60m,
        {{ rolling_std('volume', 60) }} AS vol_std_60m

    FROM {{ ref('int_btc_ichimoku') }}

)

SELECT
    symbol,
    timeframe,
    ts,
    ingested_at AS raw_ingested_at,
    source,

    -- Returns
    logret_1m,
    logret_5m,
    logret_15m,
    logret_60m,

    -- Lagged returns
    {{ lag_feature('logret_1m', 1) }}  AS lag_logret_1m_1,
    {{ lag_feature('logret_1m', 5) }}  AS lag_logret_1m_5,
    {{ lag_feature('logret_1m', 15) }} AS lag_logret_1m_15,
    {{ lag_feature('logret_1m', 60) }} AS lag_logret_1m_60,

    -- Volatility features
    vol_60m,
    vol_1d,
    vol_60m * SQRT(525600) AS vol_annualized,
    {{ safe_divide('vol_60m', 'vol_1d') }} AS vol_ratio,

    -- Volume features
    log_volume,
    vol_ma_60m,
    {{ safe_divide('(volume - vol_ma_60m)', 'vol_std_60m') }} AS vol_zscore,

    -- Ichimoku
    tenkan,
    kijun,
    span_a,
    span_b,

    -- Distance features
    {{ safe_divide('(close_price - tenkan)', 'close_price') }} AS tenkan_dist,
    {{ safe_divide('(close_price - kijun)', 'close_price') }} AS kijun_dist,
    {{ safe_divide('(close_price - span_a)', 'close_price') }} AS span_a_dist,
    {{ safe_divide('(close_price - span_b)', 'close_price') }} AS span_b_dist,

    -- Cloud thickness
    {{ safe_divide('ABS(span_a - span_b)', 'close_price') }} AS cloud_thickness,

    -- Binary signals
    {{ binary_signal('tenkan > kijun') }} AS tenkan_sup_kijun,
    {{ binary_signal('close_price > GREATEST(span_a, span_b)') }} AS price_sup_cloud,
    {{ binary_signal('close_price < LEAST(span_a, span_b)') }} AS price_sub_cloud,

    -- Lagged signal
    {{ lag_feature(
        "CASE WHEN close_price > GREATEST(span_a, span_b) THEN 1 ELSE 0 END",
        5
    ) }}::SMALLINT AS lag_price_sup_cloud_5

FROM feat