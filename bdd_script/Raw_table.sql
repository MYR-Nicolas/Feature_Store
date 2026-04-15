-- Create schema for raw market data ingestion
CREATE SCHEMA raw;



-- Raw OHLCV table (BTC market)

CREATE TABLE raw.ohlcv_btc (

    -- Primary keys

    symbol          TEXT NOT NULL DEFAULT 'BTC/USDT', -- Trading pair
    ts              TIMESTAMPTZ NOT NULL,             -- Candle timestamp (open time)
    timeframe       TEXT NOT NULL DEFAULT '1m',

    -- OHLCV prices

    open_price      DOUBLE PRECISION NOT NULL,
    high_price      DOUBLE PRECISION NOT NULL,
    low_price       DOUBLE PRECISION NOT NULL,
    close_price     DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL, -- Traded volume during the interval

    -- Metadata
    source          TEXT NOT NULL,             -- Data source (e.g., Binance API)
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- Timestamp when data was ingested

    -- Constraints for data integrity

    -- Composite primary key ensures uniqueness per candle
    CONSTRAINT pk_ohlcv_btc PRIMARY KEY (symbol, ts, timeframe, source),

    -- Price consistency checks (OHLC coherence)
    CONSTRAINT chk_high_gte_low   CHECK (high_price >= low_price),
    CONSTRAINT chk_high_gte_open  CHECK (high_price >= open_price),
    CONSTRAINT chk_high_gte_close CHECK (high_price >= close_price),
    CONSTRAINT chk_low_lte_open   CHECK (low_price <= open_price),
    CONSTRAINT chk_low_lte_close  CHECK (low_price <= close_price),

    -- Volume must be non-negative
    CONSTRAINT chk_volume_pos CHECK (volume >= 0),

    -- Restrict allowed timeframes
    CONSTRAINT chk_timeframe CHECK (timeframe IN ('1m','5m','15m','1h','4h','12h','1d'))
);


-- Index optimized for time-series feature queries
CREATE INDEX idx_ohlcv_feature_query
    ON raw.ohlcv_btc (symbol, timeframe, ts DESC)
    WHERE is_closed = true;
