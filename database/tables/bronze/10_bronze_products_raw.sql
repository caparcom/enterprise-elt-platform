IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.products_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.products_raw (
        id BIGINT NULL,
        cost DECIMAL(38,18) NULL,
        category VARCHAR(30) NULL,
        name NVARCHAR(MAX) NULL,
        brand VARCHAR(50) NULL,
        retail_price DECIMAL(38,18) NULL,
        department VARCHAR(50) NULL,
        sku VARCHAR(50) NULL,
        distribution_center_id int NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_bp_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_bp_run_id ON bronze.products_raw(_run_id);
END
GO