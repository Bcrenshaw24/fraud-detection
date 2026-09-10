CREATE SCHEMA IF NOT EXISTS feature_store; 

CREATE TABLE IF NOT EXISTS feature_store.user_features ( 
    user_id VARCHAR(60) PRIMARY KEY, 

    tx_count_5m INT NOT NULL DEFAULT 0, 
    total_amount_5m NUMERIC(12, 2) NOT NULL DEFAULT 0,
    avg_amount_24h NUMERIC(12, 2) NOT NULL DEFAULT 0.00, 
    unique_categories_1h INT NOT NULL DEFAULT 1, 

    last_latitude NUMERIC(9, 6), 
    last_longitude NUMERIC(9, 6), 
    last_tx_timestamp TIMESTAMP WITH TIME ZONE, 

    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
); 

CREATE INDEX IF NOT EXISTS idx_user_updated_at
ON feature_store.user_features (updated_at DESC);

CREATE OR REPLACE FUNCTION update_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.updated_at = CURRENT_TIMESTAMP; 
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER trg_user_features_updated_at
BEFORE UPDATE ON feature_store.user_features
FOR EACH ROW 
EXECUTE FUNCTION update_timestamp_column();