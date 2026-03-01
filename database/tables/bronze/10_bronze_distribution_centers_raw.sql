IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.distribution_centers_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.distribution_centers_raw (
        id BIGINT NULL,
        name VARCHAR(255) NULL,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_bdc_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_bdc_run_id ON bronze.distribution_centers_raw(_run_id);
END
GO