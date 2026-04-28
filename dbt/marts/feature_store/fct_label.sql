{{ config(materialized='incremental', unique_key='ts') }}

SELECT
    ts,

    -- Future log returns
    {{ future_log_return('close_price', 5) }}  AS y_logret_5m,
    {{ future_log_return('close_price', 15) }} AS y_logret_15m,

    -- Shock label (binary classification)
    {{ shock_label('close_price', 15, 0.01) }} AS y_shock_15m

FROM {{ ref('stg_ohlcv') }}

-- Ensure we only compute labels where future data exists
WHERE {{ valid_label_horizon('ts', 15) }}

-- Incremental logic
{{ incremental_filter('ts') }}