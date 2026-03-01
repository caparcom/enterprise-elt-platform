IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.events_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.events_raw (
        id BIGINT NULL,
        user_id int NULL,
        sequence_number int NULL,
        session_id VARCHAR(255) NULL,
        created_at VARCHAR(50) NULL,
        ip_address VARCHAR(45) NULL,
        city VARCHAR(100) NULL,
        state VARCHAR(100) NULL,
        postal_code VARCHAR(30) NULL,
        browser VARCHAR(50) NULL,
        traffic_source VARCHAR(30) NULL,
        uri VARCHAR(100) NULL,
        event_type VARCHAR(30) NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_be_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_be_run_id ON bronze.events_raw(_run_id);
END
GO