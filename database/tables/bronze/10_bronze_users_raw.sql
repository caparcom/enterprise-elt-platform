IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.users_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.users_raw (
        id BIGINT NULL,
        first_name VARCHAR(50) NULL,
        last_name VARCHAR(50) NULL,
        email VARCHAR(50) NULL,
        age int NULL,
        gender VARCHAR(10) NULL,
        state VARCHAR(100) NULL,
        street_address VARCHAR(75) NULL,
        postal_code VARCHAR(30) NULL,
        city VARCHAR(100) NULL,
        country VARCHAR(50) NULL,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        traffic_source VARCHAR(150) NULL,
        created_at VARCHAR(50) NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_bu_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_bu_run_id ON bronze.users_raw(_run_id);
END
GO