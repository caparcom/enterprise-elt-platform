IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.orders_raw', 'u') IS NULL
BEGIN
    CREATE TABLE bronze.orders_raw (
        order_id int NULL,
        user_id int NULL,
        status VARCHAR(30) NULL,
        gender VARCHAR(30) NULL,
        created_at VARCHAR(50) NULL,
        returned_at VARCHAR(50) NULL,
        shipped_at VARCHAR(50) NULL,
        delivered_at VARCHAR(50) NULL,
        num_of_item int NULL,
        -- metadata
        _run_id VARCHAR(50) NOT NULL,
        _load_datetime DATETIME2(3) NOT NULL CONSTRAINT DF_bo_load_dt DEFAULT SYSUTCDATETIME(),
        _source_file VARCHAR(255) NOT NULL
    );

    CREATE INDEX IX_bo_run_id ON bronze.orders_raw(_run_id);
END
GO