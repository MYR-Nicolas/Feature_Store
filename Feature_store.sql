CREATE MATERIALIZED VIEW stock_features_mv AS

WITH base AS (

  SELECT
    ts, 
	close_price, 
	high_price, 
	low_price, 
	volume,

    -- Logarithmic Returns (Returns)
    LN(close_price / LAG(close_price, 1) OVER (ORDER BY ts))  AS logret_1m,
    LN(close_price / LAG(close_price, 5) OVER (ORDER BY ts))  AS logret_5m,
    LN(close_price / LAG(close_price, 15) OVER (ORDER BY ts)) AS logret_15m,
    LN(close_price / LAG(close_price, 60) OVER (ORDER BY ts)) AS logret_60m,

    -- Log-Transformed Volume
    LN(NULLIF(volume,0)) AS log_volume
  FROM stock_prices
),




ichimoku AS (
  SELECT
    *,
	-- Tenkan-sen (9-period) – Short-Term Trend Indicator
    (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) +
     MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)) / 2 AS tenkan,
	 
	-- Kijun-sen (26-period) – Medium-Term Trend Indicator
    (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) +
     MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)) / 2 AS kijun,

	-- Senkou Span B (52-period) – Long-Term Trend Indicator (No Forward Shift)
    (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW) +
     MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)) / 2 AS span_b_mod

	 -- Span A is computed independently to enhance readability and maintainability
	 
  FROM base
),



feat AS (

  SELECT
    *,
	-- Senkou Span A
    (tenkan + kijun) / 2 AS span_a_mod,

    -- vol_60m – Short-Term Volatility Regime (Sensitivity to Immediate Market Shocks)
    STDDEV_SAMP(logret_1m) OVER (ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS vol_60m,

	-- vol_1d – Long-Term Volatility Regime (1 Day = 1440 Minutes)
    STDDEV_SAMP(logret_1m) OVER (ORDER BY ts ROWS BETWEEN 1439 PRECEDING AND CURRENT ROW) AS vol_1d,

    -- Rolling 60-Minute Average Volume 
    AVG(volume) OVER (ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS vol_ma_60m
  FROM ichimoku
)



SELECT
  ts,
  close_price,
  
  -- Multi-Horizon Logarithmic Returns
  logret_1m, 
  logret_5m, 
  logret_15m, 
  logret_60m,

  -- Lagged Logarithmic Returns
  LAG(logret_1m, 1)  OVER (ORDER BY ts) AS lag_logret_1m_1,
  LAG(logret_1m, 5)  OVER (ORDER BY ts) AS lag_logret_1m_5,
  LAG(logret_1m, 15) OVER (ORDER BY ts) AS lag_logret_1m_15,
  LAG(logret_1m, 60) OVER (ORDER BY ts) AS lag_logret_1m_60,

  -- Market Risk Regime
  vol_60m,
  vol_1d,

  -- Market Activity and Liquidity
  volume,
  log_volume,
  vol_ma_60m,

  -- Market Structure
  tenkan,
  kijun,
  span_a_mod,
  span_b_mod,

  -- Normalized Distance
  (close_price - tenkan) / close_price AS tenkan_dist,
  (close_price - kijun) / close_price AS kijun_dist,

  -- Cloud Thickness (Structural Market Uncertainty)
  ABS(span_a_mod - span_b_mod) / close_price AS cloud_large,

  -- Structural Signal – Tenkan Above Kijun
  CASE WHEN tenkan > kijun THEN 1 ELSE 0 END AS tenkan_sup_kijun,

  -- Trend Regime – Price Above the Upper Boundary of the Ichimoku Cloud
  CASE WHEN close_price > GREATEST(span_a_mod, span_b_mod) THEN 1 ELSE 0 END AS price_sup_cloud,

  -- Regime Persistence (False Signal Filter)
  LAG(CASE WHEN close_price > GREATEST(span_a_mod, span_b_mod) THEN 1 ELSE 0 END, 5) OVER (ORDER BY ts) AS lag_price_sup_cloud_5,

  -- Target y 
  LEAD(logret_1m, 5) OVER (ORDER BY ts) AS y_logret_5m
FROM feat;

 -- Indexation

CREATE INDEX ON stock_prices(ts);
CREATE INDEX ON stock_features_mv(ts);