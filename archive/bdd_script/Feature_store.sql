-- Create schema for the Feature Store
CREATE SCHEMA feature_store;


-- Feature versions metadata

CREATE TABLE feature_store.feature_versions (
    version_id    SERIAL PRIMARY KEY,

    version_tag   TEXT NOT NULL UNIQUE,  
    description   TEXT,                  -- Description of feature set
    features_list TEXT[] NOT NULL,       -- List of included features

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active     BOOLEAN NOT NULL DEFAULT false, -- Only one active version at a time
    deprecated_at TIMESTAMPTZ
);

-- Feature table

CREATE TABLE feature_store.features (
    ts              TIMESTAMPTZ NOT NULL,  -- Event timestamp (minute-level)
    
    version_id      INT NOT NULL           -- Feature version (for reproducibility)
                    REFERENCES feature_store.feature_versions,
    
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- Feature computation timestamp
    raw_ingested_at TIMESTAMPTZ NOT NULL,               -- Raw data ingestion timestamp
    source          TEXT NOT NULL,                      -- Data source

  
    -- Returns (log returns)

    logret_1m        DOUBLE PRECISION,
    logret_5m        DOUBLE PRECISION,
    logret_15m       DOUBLE PRECISION,
    logret_60m       DOUBLE PRECISION,

    -- Lagged returns
    lag_logret_1m_1  DOUBLE PRECISION,
    lag_logret_1m_5  DOUBLE PRECISION,
    lag_logret_1m_15 DOUBLE PRECISION,
    lag_logret_1m_60 DOUBLE PRECISION,

 
    -- Volatility features
  
    vol_60m          DOUBLE PRECISION, -- Rolling volatility (60 minutes)
    vol_1d           DOUBLE PRECISION, -- Daily volatility (1440 minutes)
    vol_annualized   DOUBLE PRECISION, -- Annualized volatility
    vol_ratio        DOUBLE PRECISION, -- Short-term vs long-term volatility


    -- Volume-based features

    log_volume       DOUBLE PRECISION, -- Log-transformed volume
    vol_ma_60m       DOUBLE PRECISION, -- Moving average of volume
    vol_zscore       DOUBLE PRECISION, -- Volume anomaly detection


    -- Ichimoku indicators

    tenkan           DOUBLE PRECISION,
    kijun            DOUBLE PRECISION,
    span_a           DOUBLE PRECISION,
    span_b           DOUBLE PRECISION,

    -- Distance of price to indicators (normalized)
    tenkan_dist      DOUBLE PRECISION,
    kijun_dist       DOUBLE PRECISION,
    span_a_dist      DOUBLE PRECISION,
    span_b_dist      DOUBLE PRECISION,

    cloud_thickness  DOUBLE PRECISION,

  
    -- Binary signals

    tenkan_sup_kijun      SMALLINT, -- 1 if Tenkan > Kijun
    price_sup_cloud       SMALLINT, -- 1 if price above cloud
    price_sub_cloud       SMALLINT, -- 1 if price below cloud
    lag_price_sup_cloud_5 SMALLINT, -- Lagged cloud signal


    -- Constraints

    CONSTRAINT pk_features PRIMARY KEY (ts, version_id),

    CONSTRAINT chk_bin_tenkan CHECK (tenkan_sup_kijun IN (0,1)),
    CONSTRAINT chk_bin_sup    CHECK (price_sup_cloud  IN (0,1)),
    CONSTRAINT chk_bin_sub    CHECK (price_sub_cloud  IN (0,1))
);

-- Index optimized for time-series queries
CREATE INDEX idx_features_lookup
    ON feature_store.features (version_id, ts DESC);



-- Ensure only one active version exists
CREATE UNIQUE INDEX idx_one_active_version
    ON feature_store.feature_versions (is_active)
    WHERE is_active = true;



-- Initial version

INSERT INTO feature_store.feature_versions
    (version_tag, description, features_list, is_active)
VALUES (
    'v1.0',
    'Ichimoku + log-returns + volatility regimes + volume z-score',
    ARRAY[
        'logret_1m','logret_5m','logret_15m','logret_60m',
        'lag_logret_1m_1','lag_logret_1m_5','lag_logret_1m_15','lag_logret_1m_60',
        'vol_60m','vol_1d','vol_annualized','vol_ratio',
        'log_volume','vol_ma_60m','vol_zscore',
        'tenkan','kijun','span_a','span_b',
        'tenkan_dist','kijun_dist','span_a_dist','span_b_dist','cloud_thickness',
        'tenkan_sup_kijun','price_sup_cloud','price_sub_cloud','lag_price_sup_cloud_5'
    ],
    true
);




-- Labels table

CREATE TABLE feature_store.labels (
    ts           TIMESTAMPTZ NOT NULL,
    version_id   INT NOT NULL
                 REFERENCES feature_store.feature_versions,

    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Future returns (prediction targets)
    y_logret_5m  DOUBLE PRECISION,
    y_logret_15m DOUBLE PRECISION,

    CONSTRAINT pk_labels PRIMARY KEY (ts, version_id)
);




-- Ingestion checkpoints (monitoring & incremental processing)

CREATE TABLE feature_store.ingestion_checkpoints (
    id            SERIAL PRIMARY KEY,

    version_id    INT NOT NULL
                  REFERENCES feature_store.feature_versions,

    last_ts       TIMESTAMPTZ NOT NULL, -- Last successfully processed timestamp
    rows_inserted INT,                  -- Number of inserted rows
    duration_ms   INT,                  -- Execution time
    status        TEXT NOT NULL DEFAULT 'ok',
    error_msg     TEXT,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_status CHECK (status IN ('ok', 'error'))
);

-- Index for fast retrieval of latest checkpoints
CREATE INDEX idx_checkpoints_lookup
    ON feature_store.ingestion_checkpoints (version_id, created_at DESC);