-- Incremental feature computation function
-- Computes and inserts new features from raw OHLCV data

CREATE FUNCTION feature_store.compute_features_incremental()
RETURNS void LANGUAGE plpgsql AS $$


DECLARE
    v_version_id INT;            -- Active feature version
    v_last_ts    TIMESTAMPTZ;    -- Last successfully processed timestamp
    v_start      TIMESTAMPTZ := clock_timestamp(); -- Execution start time
    v_rows       INT;            -- Number of inserted rows

BEGIN
    -- Retrieve active feature version

    SELECT version_id INTO v_version_id
    FROM feature_store.feature_versions
    WHERE is_active = true;

    -- Ensure a version is active
    IF v_version_id IS NULL THEN
        RAISE EXCEPTION 'No active version';
    END IF;

    -- Retrieve last successful checkpoint

    SELECT COALESCE(MAX(last_ts), '-infinity'::TIMESTAMPTZ) INTO v_last_ts
    FROM feature_store.ingestion_checkpoints
    WHERE version_id = v_version_id AND status = 'ok';

    -- Insert newly computed features

    INSERT INTO feature_store.features (
        ts, version_id, raw_ingested_at, source,

        -- Returns
        logret_1m, logret_5m, logret_15m, logret_60m,

        -- Lagged returns
        lag_logret_1m_1, lag_logret_1m_5, lag_logret_1m_15, lag_logret_1m_60,

        -- Volatility
        vol_60m, vol_1d, vol_annualized, vol_ratio,

        -- Volume features
        log_volume, vol_ma_60m, vol_zscore,

        -- Ichimoku
        tenkan, kijun, span_a, span_b,

        -- Distance features
        tenkan_dist, kijun_dist, span_a_dist, span_b_dist, cloud_thickness,

        -- Binary signals
        tenkan_sup_kijun, price_sup_cloud, price_sub_cloud, lag_price_sup_cloud_5
    )

  
    -- Feature engineering pipeline
    
    WITH base AS (
        SELECT 
            ts, close_price, high_price, low_price, volume,
            ingested_at, source,

            -- Log returns over different horizons
            LN(close_price / NULLIF(LAG(close_price,  1) OVER w, 0)) AS logret_1m,
            LN(close_price / NULLIF(LAG(close_price,  5) OVER w, 0)) AS logret_5m,
            LN(close_price / NULLIF(LAG(close_price, 15) OVER w, 0)) AS logret_15m,
            LN(close_price / NULLIF(LAG(close_price, 60) OVER w, 0)) AS logret_60m,

            -- Log-transformed volume
            LN(NULLIF(volume, 0)) AS log_volume

        FROM raw.ohlcv_btc

        -- Only use closed candles and recent data (rolling window safety)
        WHERE symbol = 'BTC/USDT' 
          AND timeframe = '1m' 
          AND is_closed = true
          AND ts >= v_last_ts - INTERVAL '1440 minutes'

        -- Define time ordering for window functions
        WINDOW w AS (ORDER BY ts)
    ),

    
    -- Ichimoku indicator computation

    ichimoku AS (
        SELECT *,
            -- Tenkan-sen (9 period)
            (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN  8 PRECEDING AND CURRENT ROW) +
             MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN  8 PRECEDING AND CURRENT ROW)) / 2 AS tenkan,

            -- Kijun-sen (26 period)
            (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) +
             MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW)) / 2 AS kijun,

            -- Senkou Span B (52 period)
            (MAX(high_price) OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW) +
             MIN(low_price)  OVER (ORDER BY ts ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)) / 2 AS span_b
        FROM base
    ),

    
    -- Volatility and volume features
    
    feat AS (
        SELECT *,
            -- Senkou Span A
            (tenkan + kijun) / 2 AS span_a,

            -- Rolling volatility (60 minutes)
            STDDEV_SAMP(logret_1m) OVER (ORDER BY ts ROWS BETWEEN   59 PRECEDING AND CURRENT ROW) AS vol_60m,

            -- Rolling volatility (1 day)
            STDDEV_SAMP(logret_1m) OVER (ORDER BY ts ROWS BETWEEN 1439 PRECEDING AND CURRENT ROW) AS vol_1d,

            -- Annualized volatility
            STDDEV_SAMP(logret_1m) OVER (ORDER BY ts ROWS BETWEEN   59 PRECEDING AND CURRENT ROW)
                * SQRT(525600) AS vol_annualized,

            -- Rolling average volume
            AVG(volume) OVER (ORDER BY ts ROWS BETWEEN   59 PRECEDING AND CURRENT ROW) AS vol_ma_60m,

            -- Rolling volume standard deviation
            STDDEV_SAMP(volume) OVER (ORDER BY ts ROWS BETWEEN   59 PRECEDING AND CURRENT ROW) AS vol_std_60m
        FROM ichimoku
    )


    -- Final feature selection and transformations

    SELECT
        ts, v_version_id, ingested_at, source,

        -- Returns
        logret_1m, logret_5m, logret_15m, logret_60m,

        -- Lagged returns
        LAG(logret_1m,  1) OVER (ORDER BY ts),
        LAG(logret_1m,  5) OVER (ORDER BY ts),
        LAG(logret_1m, 15) OVER (ORDER BY ts),
        LAG(logret_1m, 60) OVER (ORDER BY ts),

        -- Volatility
        vol_60m, vol_1d, vol_annualized,
        vol_60m / NULLIF(vol_1d, 0),

        -- Volume features
        log_volume, vol_ma_60m,
        (volume - vol_ma_60m) / NULLIF(vol_std_60m, 0),

        -- Ichimoku features
        tenkan, kijun, span_a, span_b,

        -- Distance to indicators (normalized)
        (close_price - tenkan) / NULLIF(close_price, 0),
        (close_price - kijun)  / NULLIF(close_price, 0),
        (close_price - span_a) / NULLIF(close_price, 0),
        (close_price - span_b) / NULLIF(close_price, 0),

        -- Cloud thickness
        ABS(span_a - span_b) / NULLIF(close_price, 0),

        -- Binary signals
        (CASE WHEN tenkan > kijun THEN 1 ELSE 0 END)::SMALLINT,
        (CASE WHEN close_price > GREATEST(span_a, span_b) THEN 1 ELSE 0 END)::SMALLINT,
        (CASE WHEN close_price < LEAST(span_a, span_b)    THEN 1 ELSE 0 END)::SMALLINT,

        -- Lagged signal
        LAG(CASE WHEN close_price > GREATEST(span_a, span_b) THEN 1 ELSE 0 END, 5)
            OVER (ORDER BY ts)

    FROM feat
    WHERE ts > v_last_ts
    ON CONFLICT (ts, version_id) DO NOTHING;


    -- Retrieve number of inserted rows
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    -- Log successful execution in checkpoints table

    INSERT INTO feature_store.ingestion_checkpoints
        (version_id, last_ts, rows_inserted, duration_ms, status)
    SELECT 
        v_version_id, 
        MAX(ts), 
        v_rows,
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::INT,
        'ok'
    FROM feature_store.features 
    WHERE version_id = v_version_id;








-- Error handling: log failure and propagate error

EXCEPTION WHEN OTHERS THEN
    INSERT INTO feature_store.ingestion_checkpoints
        (version_id, last_ts, rows_inserted, duration_ms, status, error_msg)
    VALUES (
        v_version_id, 
        v_last_ts, 
        0,
        EXTRACT(MILLISECONDS FROM clock_timestamp() - v_start)::INT,
        'error', 
        SQLERRM
    );

    RAISE;
END;
$$;