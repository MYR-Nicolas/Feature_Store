BEGIN;

-- =====================================
-- Add unique constraint on raw table
-- ======================================

ALTER TABLE raw.ohlcv
ADD CONSTRAINT uq_raw_ohlcv_symbol_ts_timeframe
UNIQUE (symbol, ts, timeframe);


-- =============================================
-- Add missing columns to feature_store features
-- =============================================

ALTER TABLE feature_store.features
ADD COLUMN symbol TEXT,
ADD COLUMN timeframe TEXT;

-- Backfill existing rows with default values
UPDATE feature_store.features
SET symbol = 'BTC/USDT'
WHERE symbol IS NULL;

UPDATE feature_store.features
SET timeframe = '1m'
WHERE timeframe IS NULL;

-- Enforce NOT NULL constraints after backfill
ALTER TABLE feature_store.features
ALTER COLUMN symbol SET NOT NULL,
ALTER COLUMN timeframe SET NOT NULL;

ALTER TABLE raw.ohlcv
ADD COLUMN is_closed BOOLEAN NOT NULL DEFAULT true;


-- ========================================
-- Add foreign key: features -> raw ohlcv
-- ========================================

ALTER TABLE feature_store.features
ADD CONSTRAINT fk_features_raw_ohlcv
FOREIGN KEY (symbol, ts, timeframe, source)
REFERENCES raw.ohlcv (symbol, ts, timeframe, source);


-- ==============================================
-- Add missing columns to feature_store labels
-- ==============================================

ALTER TABLE feature_store.labels
ADD COLUMN symbol TEXT,
ADD COLUMN timeframe TEXT;

-- Backfill existing rows
UPDATE feature_store.labels
SET symbol = 'BTC/USDT'
WHERE symbol IS NULL;

UPDATE feature_store.labels
SET timeframe = '1m'
WHERE timeframe IS NULL;

-- Enforce NOT NULL constraints
ALTER TABLE feature_store.labels
ALTER COLUMN symbol SET NOT NULL,
ALTER COLUMN timeframe SET NOT NULL;

ALTER TABLE feature_store.labels
ADD COLUMN y_shock_15m smallint;

-- ====================================
-- Add foreign key: labels -> raw ohlcv
-- ====================================

ALTER TABLE feature_store.labels
ADD CONSTRAINT fk_labels_raw_ohlcv
FOREIGN KEY (symbol, ts, timeframe)
REFERENCES raw.ohlcv (symbol, ts, timeframe);


COMMIT;