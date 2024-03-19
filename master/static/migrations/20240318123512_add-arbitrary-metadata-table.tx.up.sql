-- ALTER RUNS TABLE
ALTER TABLE runs ADD COLUMN metadata JSONB;
-- CREATE INDEX TABLE
CREATE TABLE runs_metadata_index (
    id SERIAL PRIMARY KEY,
    run_id INTEGER,
    flat_key VARCHAR,
    value TEXT,
    data_type VARCHAR, -- 'string', 'number', 'boolean', 'timestamp', null
    project_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);
-- CREATE FILTER-OPTIMIZING INDEXES
CREATE INDEX idx_flat_key ON runs_metadata_index (flat_key);
CREATE INDEX idx_flat_key_value ON runs_metadata_index (flat_key, value);
